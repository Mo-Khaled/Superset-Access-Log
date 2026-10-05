# Write-up

Plain-language notes on the decisions behind this lab. Setup and commands are in
[README.md](README.md); the demo script is in [intermittent-error/DEMO.md](intermittent-error/DEMO.md).

## 1. How the MySQL data is organised

Superset's activity is pulled through `LogRestApi` into MySQL like this:

| Table | What it holds | Why it exists |
|---|---|---|
| `raw_log_landing` | The untouched JSON of every Superset log row | Superset may delete old logs. With a raw copy we can rebuild the other tables later if we find a bug or want a new metric. |
| `fact_access_events` | One row per dashboard view, chart view or export | The usage numbers. |
| `fact_error_events` | One row per failure, with its reason | See section 3. |
| `dim_users`, `dim_dashboards` | Names and owners | Keeps the fact table small and lets a renamed dashboard show its new title everywhere. |
| `etl_sync_state` | "How far did we get last time" | Lets the ETL pick up where it stopped. Stored in MySQL, so any machine can run it. |

Two choices worth knowing:

- **Dashboard views and chart views are separate rows.** "Most visited dashboards"
  counts dashboard views only. Otherwise a 10-chart dashboard would look 10x more
  popular than a 1-chart one.
- **Repeat views are not merged.** A user refreshing is real activity. Instead the
  usage dashboard shows total views *and* unique viewers, so "one person refreshing"
  and "20 people looking once" are easy to tell apart.

## 2. Running the ETL in production

`etl/sync_logs.py` does one bounded run and exits. A failed run does not move the
"how far did we get" marker, so the next run simply retries the same data.

- **This lab:** cron every 10 minutes, calling `access-log-pipeline/run_etl.sh`.
- **Production, simple:** a Kubernetes `CronJob` with `concurrencyPolicy: Forbid`
  (so two runs never overlap) and an alert when a job fails.
- **Production, bigger:** an Airflow DAG, for retries, alerts and run history.
- **Alert on staleness too:** if `etl_sync_state.updated_at` stops moving, the
  scheduler itself has stopped, even if no single run reported a failure.

## 3. The intermittent chart timeout

**What happens.** A chart query takes longer than gunicorn's `timeout`
(15s here). The gunicorn master then kills the whole worker process with SIGKILL.
That happens outside Superset's code, so Superset never gets to log the request.
The user just sees an error.

**Where the error shows up, and where it does not (checked in this lab):**

| Place | Visible? |
|---|---|
| Container output (`docker compose logs superset`) | Yes: `WORKER TIMEOUT` then `SIGKILL`. The "out of memory?" text is gunicorn boilerplate, not a real OOM. |
| Superset's `logs` table, server side | No row for the killed request. |
| Superset's `logs` table, browser side | Sometimes: a `load_chart` row with `has_err = true` and `timeout`, but only if the browser tab survives long enough to send it. |
| `fact_access_events` (MySQL) | It reads the `logs` table, so it misses the server side. The browser row is even counted as a normal `chart_view`. |
| `fact_error_events` (MySQL) | Yes, once the ETL has run: one `browser` row and one `gunicorn` row per failure (see below). |

**Why it is "intermittent": the cache.** Query results are cached in Redis for 300s.
A killed request never writes a result, so a cold load always fails. If anything
else finishes the same query (another session, a warm-up job), the next load is
served from cache and works until the cache expires. So the same dashboard fails,
then works after a refresh, then fails again later.

**Making our pipeline record it.** `fact_error_events` is filled from two sources,
because neither is complete on its own:

- `browser`: knows the user, dashboard and chart, but only that it "timed out".
- `gunicorn`: knows the real reason (worker killed at the timeout). The timeout line
  itself has only a worker id, so each worker also logs a `REQTRACE` line (pid, user,
  URL) when a request starts. The ingest joins the timeout to that pid's last trace
  line and fills in the user, dashboard and chart.

How the pieces fit:

```
browser load_chart (has_err)   -> Superset logs table --sync_logs.py--------------> fact_error_events (source=browser)
worker REQTRACE line (pid,user,URL) --gunicorn WORKER TIMEOUT (pid)     ----+--ingest_container_errors.py---------------> fact_error_events (source=gunicorn)
                                      (joined on the worker pid)
```

The trace line is written when a request *starts*, because a killed request never
reaches any "request finished" hook. `run_etl.sh` runs both steps on one cron entry.

Example from this lab (one dashboard load, a few seconds apart):

| event_ts | source | user | dashboard | chart | reason |
|---|---|---|---|---|---|
| 08:24:24 | gunicorn | 1 | 13 | 204 | worker pid 8 exceeded the 15s timeout and was SIGKILLed; request was `POST /api/v1/chart/data` |
| 08:24:25 | browser | 1 | 13 | 204 | timeout after 15016 ms |

`fact_access_events` is left as is, so a failed load still counts as a view there.
That is deliberate: it shows why the usage table alone cannot be trusted for errors.

**The real fix (not applied in the demo).**

1. **Make the database fail first.** Set a Postgres `statement_timeout` *shorter*
   than the gunicorn timeout. The DB cancels the query, Superset catches the
   error, logs it and shows a proper message (`fix/apply_fix.py`).
2. **Order every timeout** from inside out: database, gunicorn, any proxy or load
   balancer (nginx, ALB, ingress), CDN. The database must always be the one to fail
   first, because it is the only layer that produces an error Superset can log.
   This lab has no proxy; production almost always does.
3. **Optional extras:** a gunicorn `worker_abort` hook (see the caveat below) and a
   browser-side error report.

**Two things that surprised us.**

- gunicorn's `timeout` only works with `worker_class = "sync"`. With `gthread` the
  worker keeps reporting "alive" while a request thread is stuck, so the timeout
  never fires and the bug does not reproduce. The lab uses `sync`.
- The `worker_abort` hook never ran. gunicorn sends SIGABRT first, but the worker is
  stuck inside the database driver (native code) and cannot handle the signal before
  SIGKILL arrives about a second later. So that hook helps for stuck Python code,
  not for a stuck DB call.

**Reproducing the "error, then fine after refresh" version** (what the demo uses):

The query must take longer than gunicorn's timeout but shorter than the DB's. Here:
15s gunicorn < **18s query** < 20s DB `statement_timeout`.

1. Clear the cache: `docker compose exec redis redis-cli -n 2 FLUSHDB`
2. Start a Superset with no gunicorn limit:
   `docker compose run -d --name superset-nolimit -p 8090:8088 superset superset run -h 0.0.0.0 -p 8088 --with-threads`
3. Load the dashboard on `localhost:8089`: it times out at about 15s.
4. Load it on `localhost:8090`: it succeeds at about 18s and fills the cache.
5. Refresh `localhost:8089`: instant. After 300s the error comes back.
6. Clean up: `docker rm -f superset-nolimit`

## 4. Two ETL bugs we found and fixed

Both only appeared when we pushed real browser traffic through the pipeline; the
seeded demo data hid them.

**Bug 1: the marker could jump into the future.** The ETL remembers progress by
timestamp. The seed script created some events later in the day than the actual
time. Once one of those became the marker, every real event before that time of day
was skipped forever (symptom: "0 rows staged" even though Superset had new
activity). *Fix:* the marker is never set later than the current time.

**Bug 2: real clicks were not being counted.** The seed script writes rows like
`action = 'dashboard_load'`. Superset 4.1.4's browser actually writes a generic
`action = 'log'` row and puts the real event name (`mount_dashboard`, `load_chart`,
`force_refresh_chart`) inside its JSON. The ETL only checked `action`, so genuine
usage was never counted, and the seeded data made the dashboard look healthy.
*Fix:* the ETL also reads `json.event_name`.

After both fixes, one real dashboard load followed by one ETL run raised the
`dashboard_view` count by exactly 1.

## 5. Known gaps

- `source_ip` is always empty: Superset's log model does not record the client IP.
- The user/dashboard/chart on gunicorn rows comes from the `REQTRACE` line, so it only
  exists for timeouts that happened after the trace was added to `superset_config.py`.
  Requests made with an API token (not a browser session) are traced with `user=-`.
- The browser error row exists only if the tab stays open long enough to send it.
- Superset 4.1.4 is pinned. Re-check the log event names if you upgrade.

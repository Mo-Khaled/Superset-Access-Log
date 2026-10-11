# Write-up

Plain-language notes on the decisions behind this lab. Setup and commands are in
[README.md](README.md); the demo script is in [intermittent-error/DEMO.md](intermittent-error/DEMO.md).

## 1. How the data is organised

Superset's activity is pulled through `LogRestApi` into Iceberg tables (read through Trino). There is no other database in the pipeline:

| Table | What it holds | Why it exists |
|---|---|---|
| `raw_log_landing` | The untouched JSON of every Superset log row | Superset may delete old logs. With a raw copy we can rebuild the other tables later if we find a bug or want a new metric. |
| `fact_access_events` | One row per dashboard view, chart view or export | The usage numbers. |
| `fact_error_events` | One row per failure, with its reason | See sections 3 and 5. |
| `dim_users`, `dim_dashboards` | Names and owners | Keeps the fact table small and lets a renamed dashboard show its new title everywhere. |
| `etl_lock`, `etl_run_log` | The single-writer lease, and one row per run | The "how far did we get" marker is not stored: it is `max(source_dttm)` of `raw_log_landing`, so it cannot disagree with the data. The run log is what to alert on. Stored in Iceberg, so any machine can run the ETL. |

Two choices worth knowing:

- **Dashboard views and chart views are separate rows.** "Most visited dashboards"
  counts dashboard views only. Otherwise a 10-chart dashboard would look 10x more
  popular than a 1-chart one.
- **Repeat views are not merged.** A user refreshing is real activity. Instead the
  usage dashboard shows total views *and* unique viewers, so "one person refreshing"
  and "20 people looking once" are easy to tell apart.

## 2. Running the ETL in production

`etl/sync_logs.py` does one bounded run and exits. The "how far did we get" marker is derived from the
data already stored (`raw_log_landing`, written last), so a failed run cannot move it and the next run
simply retries the same data.

- **This lab:** cron every 10 minutes, calling `access-log-pipeline/run_etl.sh`.
- **Production, simple:** a Kubernetes `CronJob` with `concurrencyPolicy: Forbid`
  (so two runs never overlap) and an alert when a job fails.
- **Production, bigger:** an Airflow DAG, for retries, alerts and run history.
- **Alert on staleness too:** every run appends a row to `etl_run_log`. If the latest row with
  `kind = 'sync'` and `status = 'ok'` gets older than a few intervals, the scheduler itself has stopped,
  even if no single run reported a failure.

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
| `fact_access_events` | It reads the `logs` table, so it misses the server side. The browser row is even counted as a normal `chart_view`. |
| `fact_error_events` | Yes, once the ETL has run: one `browser` row and one `gunicorn` row per failure (see below). |

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
2. Start a Superset with no gunicorn limit. The 60 s timeout also stops the browser from giving up at 15 s:
   `docker compose run -d --name superset-nolimit -e SUPERSET_WEBSERVER_TIMEOUT=60 -e CACHE_KEY_PREFIX=superset8090_ -p 8090:8088 superset superset run -h 0.0.0.0 -p 8088 --with-threads`
3. Load the dashboard on `localhost:8089`: it times out at about 15s.
4. Load it on `localhost:8090`: it succeeds at about 18s and fills the cache.
5. Refresh `localhost:8089`: instant. After 300s the error comes back.
6. Clean up: `docker rm -f superset-nolimit`

## 4. Three ETL bugs we found and fixed

The first two only appeared when we pushed real browser traffic through the pipeline; the
seeded demo data hid them. The third showed up when a second seed run was only partly read.

**Bug 1: the marker could jump into the future.** The ETL remembers progress by
timestamp. The seed script created some events later in the day than the actual
time. Once one of those became the marker, every real event before that time of day
was skipped forever (symptom: "0 rows staged" even though Superset had new
activity). *Fix:* the marker (now `max(source_dttm)` of the stored raw rows) ignores rows dated later than the current time.

**Bug 2: real clicks were not being counted.** The seed script writes rows like
`action = 'dashboard_load'`. Superset 4.1.4's browser actually writes a generic
`action = 'log'` row and puts the real event name (`mount_dashboard`, `load_chart`,
`force_refresh_chart`) inside its JSON. The ETL only checked `action`, so genuine
usage was never counted, and the seeded data made the dashboard look healthy.
*Fix:* the ETL also reads `json.event_name`.

After both fixes, one real dashboard load followed by one ETL run raised the
`dashboard_view` count by exactly 1.

**Bug 3: rows dated in the past were skipped for good.** The ETL asks Superset for rows with `dttm > watermark`,
because the log API cannot filter by id. Capping the watermark at the current time (bug 1) stopped the future-date
problem, but it does nothing for a row that is inserted *after* the watermark moves and carries an *older* `dttm`.
That row never matches the filter again. We hit it: a second seed run inserted 7,225 rows and the ETL read 221. In real
life the same thing happens with late browser flushes, clock skew between workers, ETL downtime and backfills.
*Fix, three layers (all safe to repeat, because every write is an Iceberg MERGE):*
1. **Lookback.** Every pull starts 24 hours (configurable) before the watermark.
2. **Reconciliation.** After a pull, the number of rows Superset dated before the start of the run (the `count` field of
   the list API with a `dttm` filter) is compared with the same window in `raw_log_landing`. If Superset has more, the
   whole log is re-pulled once and a warning with the gap is printed. A fixed window is essential: every API call the ETL
   makes is itself logged by Superset (about 50 rows per run), so a total count hid a few missing rows. Real numbers from
   testing: the first reconciliation run found 6,909 skipped rows, and after a second seed 6,808.
3. **`--since` / `--full`** for a deliberate backfill.

The seed script also stopped generating timestamps later than now, which was the original cause. The watermark is
derived from `raw_log_landing`, which is written last, so it can only advance after everything before it is stored. `tests/acceptance_watermark.py` checks a row dated
3 hours ago, one 10 days ago, one tomorrow, an idempotent re-run, and that nothing is missing.

A trap to remember: Superset 4.1.4's log list API silently ignores the filter operators `lte`, `le`, `gte` and `ge`
and returns everything; only `lt` and `gt` filter. A first version of the reconciliation used `lte` and reported a
gap that did not exist, until the counts were compared with Postgres directly.

## 5. Failure rate and load time in Iceberg

**Goal.** Know how reliable and how fast each dashboard is, per day, week and month, without adding a component to the
architecture. The bank already runs Iceberg and Trino, so the error data and the usage facts are written there and read back
through Trino by Superset.

**How it is built.**
- The ETL writes with Trino SQL (`MERGE INTO`), so the pipeline needs no catalog or object-store credentials of its own.
  A `MERGE ... WHEN NOT MATCHED` makes re-reading harmless, which Iceberg's lack of unique keys would otherwise prevent.
- Rows are merged in chunks, one statement per table per chunk, not one per API page. Each MERGE is a snapshot, so many
  small ones would mean many small files; raw payloads are chunked by size to stay under Trino's query length limit.
- One writer at a time, without locks: Iceberg has none, so the ETL, error ingest, backfill and maintenance share a
  lease row in `etl_lock`, taken with a `MERGE`. Iceberg's atomic commit decides a race: the loser fails with a commit
  conflict, reads the owner back and skips. The lease expires on its own, so a crashed run cannot block the pipeline.
  If two runs overlap anyway the data is still correct (idempotent MERGEs, commit conflicts retried with backoff).
- Everything lives in Iceberg (facts partitioned by day). Trino cannot commit two tables together, so write order is the
  consistency model: facts and dimensions first, `raw_log_landing` last; the watermark and the "already stored" check
  both read it. Rows already stored are not sent again, so a quiet cycle writes almost nothing.
- Compaction, snapshot expiry and orphan-file cleanup run from `etl/iceberg_maintenance.py` about once a day.
- For the lab, `lakehouse/` is a self-contained stand-in (MinIO, Iceberg REST catalog, Trino) with its own compose file. It is
  not part of the target architecture.

**Any error, not one known cause.** The first version only recognised the gunicorn timeout, which is the one cause
this lab was built around. Production will fail in ways nobody predicted, so capture was generalised instead of adding
a rule per cause:
- Superset now logs every dashboard/chart request that ends in HTTP 400 or above, with the status, duration and the
  message from the response, and every unhandled exception with its class. The ingest also reads gunicorn problems of
  any kind (timeout, a worker killed from outside) and failed Celery tasks.
- Every error row keeps the raw message, a coarse category, and a *signature*: the message with numbers, ids and quoted
  values removed, so that "Columns missing in dataset: [a, b]" and "[c, d]" count as one error.
- Anything the rules do not recognise is categorised `unclassified`, never dropped. A growing `unclassified` signature in
  the Top Error Messages table is how a new cause is noticed, and one line in the classifier names it.
- Tested with six different failures (two 404s, a 422, a 400 with a query message, a missing dataset, a killed worker)
  through the same code path, plus a parser test covering a SIGKILL with no timeout, an unknown arbiter message and a
  failed background task.

**Metrics.**
- *Failure rate* = failed chart loads / chart loads. An error is one failed chart load, so this keeps one unit on both
  sides. Dividing by dashboard views would mix units (a dashboard view loads many charts) and can exceed 1.
  It is computed as a ratio of sums so that week and month values weigh each day by its traffic.
- *Load time* (average, p95) and *slow-load rate* use successful chart loads only. Percentiles cannot be rebuilt from daily
  percentiles, so they are computed by Superset directly on the fact table at whatever time grain the chart uses.

**How to read it, honestly.**
- The failure rate is a **lower bound**. Failed loads are reported by the browser, and only while the tab stays open.
  The `server_kills` column is gunicorn's own count of killed workers. When it is well above the failed loads, the browser is
  under-reporting. Gunicorn rows are kept out of the numerator because they often cannot be tied to a dashboard and would
  double count a failure the browser also reported.
- Load times are measured in the browser, so they include network time.
- Server errors and server kills are shown next to the failure rate but not added to it, because the browser can report
  the same failure. A failure that leaves no trace in the browser event, a request error, a gunicorn line or a Celery
  log cannot be seen at all.
- The numbers are only as complete as Superset's own log. The reconciliation step makes missing rows visible, but rows
  deleted by Superset's retention cannot be recovered.

**Needs confirming in the bank's environment:** Trino version and catalog type, format-version 2 tables, the pipeline user's
permissions, authentication and TLS, who owns compaction, and the slow-load threshold. The lab values are not assumptions
about the bank.

## 6. Known gaps

- `source_ip` is always empty: Superset's log model does not record the client IP.
- The user/dashboard/chart on gunicorn rows comes from the `REQTRACE` line, so it only
  exists for timeouts that happened after the trace was added to `superset_config.py`.
  Requests made with an API token (not a browser session) are traced with `user=-`.
- The browser error row exists only if the tab stays open long enough to send it.
- Superset 4.1.4 is pinned. Re-check the log event names if you upgrade.
- The failure rate under-counts (browser rows only exist while the tab is open); see section 5.
- Container logs still reach the pipeline through a manual pipe. A log platform is the follow-up.
- Iceberg has no locks, so the single writer is a lease row (`etl_lock`); it is advisory (overlapping runs stay correct
  because every write is an idempotent MERGE). The lab's Iceberg REST catalog uses SQLite, which can answer a simultaneous
  commit with an HTTP 500 and then refuse further commits until `iceberg-rest` is restarted. A catalog with proper
  optimistic concurrency (a JDBC/Postgres-backed or managed catalog) does not have this problem.
- Compaction, snapshot expiry and orphan-file cleanup run from `etl/iceberg_maintenance.py`; confirm who owns them in the bank's platform.

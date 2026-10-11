# Demo guide: Superset access-log pipeline on Iceberg

A step-by-step script for demonstrating the pipeline. Every step gives the command, what you should see, and why it
matters. Numbers shown are from a real run of this lab and will differ a little on yours; the shape of the output is
what to look for.

**What the pipeline does, in one line:** it reads Superset's activity log and its container logs, stores everything in
Iceberg tables (read through Trino), and Superset charts failure rate, load time and usage from those tables.
There is no other database: no MySQL.

```
Superset logs (/api/v1/log/) ----+                                      +--> Superset "Superset Usage Analytics"
container logs (gunicorn, ...) --+--> ETL (python) --> Trino MERGE --> Iceberg tables --+--> Superset "Dashboard Reliability and Speed"
                                                                        |
                                                                        +--> DBeaver (SQL, over Trino)
```

Run every command from the repository root. On Windows use Git Bash (or WSL) for the `bash` and `docker compose` lines.

---

## 0. Prerequisites

- Docker Desktop running.
- `.env` and `lakehouse/.env` exist. First time only:
  ```bash
  cp .env.example .env
  cp lakehouse/.env.example lakehouse/.env
  ```
- DBeaver (any recent version) for part 7.

---

## 1. Start the stack

**1a. The lakehouse** (MinIO + Iceberg catalog + Trino, the lab stand-in for the bank's platform):

```bash
docker compose -f lakehouse/docker-compose.yml up -d
```

Expect: `minio`, `iceberg-rest` and `trino` containers started; `minio-bootstrap` and `catalog-init` run once and exit.

**1b. Superset and the pipeline container**, joined to the lakehouse network:

```bash
docker compose -f docker-compose.yml -f lakehouse/connect-pipeline.yml up -d --build \
    postgres redis superset pipeline
```

Expect: Postgres and Redis healthy, `superset-init` runs once and exits with code 0, then `superset` and `pipeline` start.
The first build takes a few minutes.

**1c. Check everything is up:**

```bash
curl -s http://localhost:8089/health                                   # Superset
docker compose -f lakehouse/docker-compose.yml exec -T trino trino --execute "select 1"   # Trino
```

Expect: `OK` from Superset (it can take a minute after the container starts) and `"1"` from Trino.

---

## 2. Create the Iceberg tables

```bash
docker compose exec -T pipeline python etl/iceberg_init.py
```

Expect:

```
Trino version: 483 (catalog iceberg, schema monitoring)
Iceberg objects ready; tables are format-version 2; view created; authorization denials are reported separately
```

This is idempotent: run it as often as you like. It creates seven tables and one view and checks every table is
Iceberg format version 2, which `MERGE` needs.

| Table | Holds |
|---|---|
| `raw_log_landing` | verbatim copy of every Superset log row; the ETL's watermark is read from it |
| `fact_access_events` | one row per dashboard view, chart view or export (with load time) |
| `fact_error_events` | one row per failure of any cause, with category, signature and raw message |
| `dim_dashboards`, `dim_users` | titles, owners, user names |
| `etl_run_log` | one row per run, with status: what to alert on |
| `etl_lock` | the single-writer lease |
| `vw_chart_failure_rate` (view) | chart loads and failures per day and dashboard |

---

## 3. Put data in

**3a. (Optional) simulate 30 days of traffic.** Skip this if Superset already has history.

```bash
docker compose exec -T pipeline python seed/generate_activity.py
```

Expect: `created user ...` lines, then `inserted N simulated log rows across M dashboards ...`. This writes into Superset's
own `logs` table, as real browser use would, including a small share of failed chart loads.

**3b. Run the ETL: Superset log -> Iceberg.**

```bash
docker compose exec -T pipeline python etl/sync_logs.py
```

Expect, on the first run against an existing Superset:

```
synced through log id 35069, watermark 2026-10-11 05:57:15: 35069 log rows read (35069 new, 0 already stored), 28942 access events, 663 browser errors; pull started at 1970-01-01 00:00:00
iceberg: sent 28942 access rows, 663 error rows, 35069 raw rows, 20 dashboards, 13 users to MERGE (rows already present are left untouched)
```

How to read it: "pull started at 1970" means a full first pull; later runs start 24 hours before the watermark.
The first run on a large log takes a minute or two.

**3c. Ingest server-side errors from the container logs** (gunicorn timeouts, HTTP errors, failed Celery tasks):

```bash
docker compose logs --no-log-prefix --since 24h superset superset-worker \
  | docker compose exec -T pipeline python etl/ingest_container_errors.py
```

Expect: `server-side errors found: 3 (server=3); sent to iceberg: 3` (counts depend on what the containers logged;
`found: 0 (none)` is normal on a quiet system).

**3d. Or do all of it in one command** (this is what cron runs every 10 minutes):

```bash
bash access-log-pipeline/run_etl.sh
```

Expect the output of `iceberg_init`, `sync_logs`, the error ingest, and `maintenance not due yet` (compaction really runs
about once a day). The script exits non-zero if any step fails.

---

## 4. Show the safety properties

These answer the "what if it runs twice / crashes / a row arrives late" questions.

**4a. Idempotent: run it again.**

```bash
docker compose exec -T pipeline python etl/sync_logs.py
```

Expect `... 373 log rows read (7 new, 366 already stored), 0 access events ...` and `sent 0 access rows`. Rows already
stored are not sent again, and the few new ones are Superset logging the ETL's own API calls. Nothing is duplicated.

**4b. Single writer: start two runs at the same time.**

```bash
docker compose exec -T pipeline python etl/sync_logs.py > /tmp/run1.txt 2>&1 &
docker compose exec -T pipeline python etl/sync_logs.py > /tmp/run2.txt 2>&1
wait; head -2 /tmp/run1.txt /tmp/run2.txt
```

Expect: one run prints `another run holds the single-writer lease 'etl'; skipping this run` and the other syncs normally.
Iceberg has no locks, so the lock is a row in `etl_lock` taken with an atomic `MERGE`. It expires by itself after 30 minutes
if a run crashes. Check there are no duplicates afterwards:

```bash
docker compose -f lakehouse/docker-compose.yml exec -T trino trino --catalog iceberg --schema monitoring \
  --execute "select count(*), count(distinct source_log_id) from raw_log_landing"
```

Expect two equal numbers.

**4c. Late and past-dated rows are not lost.** The acceptance test inserts rows dated 3 hours ago, 10 days ago and tomorrow,
runs the real ETL after each, and also checks the lease:

```bash
docker compose exec -T pipeline python tests/acceptance_watermark.py
```

Expect `PASS` lines for A to F and `ALL PASSED`. The 10-day-old row triggers a `WARNING ... Re-pulling the whole log once`:
that is the reconciliation working. (It leaves a few labelled rows, referrer `acceptance-test`, in the lab data.)

**4d. Unit tests, no services needed:**

```bash
docker compose exec -T pipeline python tests/test_iceberg_state.py
docker compose exec -T pipeline python tests/test_error_classify.py
docker compose exec -T pipeline python tests/test_ingest_parse.py
```

Expect `ALL PASSED` from each.

---

## 5. Build the Superset dashboards

```bash
docker compose -f docker-compose.yml -f lakehouse/connect-pipeline.yml run --rm superset-bootstrap
```

Expect lines like `created dataset ...`, `created chart ...` (or `already exists` / `updated chart ...` on a re-run) and
`Iceberg dashboard ready: 'Dashboard Reliability and Speed'`. It is idempotent.

Open **http://localhost:8089** (login `admin` / `admin`, set in `.env`) -> **Dashboards**:

| Dashboard | What to point at |
|---|---|
| **Superset Usage Analytics** | Total views, active users, most viewed dashboards, views over time, per-user activity, zero-view dashboards (deprecation candidates), exports |
| **Dashboard Reliability and Speed** | Failure rate by day / week / month, failed vs total loads, failure rate by dashboard, chart load time (average and p95), slow loads, **Errors by Category**, **Top Error Messages**, the individual failures |

Talking points:
- Failure rate = failed chart loads / chart loads, summed first and divided after (a ratio of sums, not an average of daily ratios).
- It is a **lower bound**: the browser only reports a failure while the tab stays open. The `Server kills` column is the
  server-side count that shows the gap.
- Load time uses **successful** loads only, because a timeout always reads about 15 s and would hide real slowness.
- **Top Error Messages** groups by what the error *says*. Anything the rules do not recognise is `unclassified`, which is how a
  new, unknown cause gets noticed.

---

## 6. (Optional) the live failure demo

To show a real, intermittent, unlogged timeout feeding the failure rate (a chart that takes 18 s against a 15 s gunicorn
limit, and a Redis cache that makes it come and go), follow [intermittent-error/DEMO.md](intermittent-error/DEMO.md). After
it, run `bash access-log-pipeline/run_etl.sh` and refresh **Dashboard Reliability and Speed**: the failed load appears as a
`browser` row and a `gunicorn` row for the same chart.

---

## 6a. What each log source contains, with proof

The pipeline reads two places because neither one sees every failure.

**Container logs** (stdout of `superset` and `superset-worker`). The error types the ETL recognises
([ingest_container_errors.py](access-log-pipeline/etl/ingest_container_errors.py)):

| Parser label | The actual line | Stored as `error_type` | Notes |
|---|---|---|---|
| `GUNICORN_TIMEOUT` | `[CRITICAL] WORKER TIMEOUT (pid:8)` | `worker_timeout` | The worker exceeded the timeout and was killed. |
| `GUNICORN_KILLED` | `[ERROR] Worker (pid:8) was sent SIGKILL! ...` | `worker_killed` | Only when **no timeout preceded it** (killed from outside, often out of memory). A SIGKILL right after a timeout is the same incident and is not stored twice. |
| `GUNICORN_OTHER` | any other gunicorn `[CRITICAL]` / `[ERROR]` line | `gunicorn_error` | Stored as `unclassified` unless a rule matches. |
| `REQEND` | `REQEND ... status=404 dur=17 url=... msg=Not found` | `http_<status>` | A dashboard/chart request that ended with HTTP 400 or above. |
| `REQEXC` | `REQEXC ... exc=ProgrammingError msg=...` | the exception class | An unhandled exception; it is merged into the `REQEND` row with the same request id. |
| `CELERY` | `ERROR/ForkPoolWorker-1] Task ...[id] raised unexpected: ...` | `celery_task_failed` | A failed background task. |
| `REQTRACE` | `REQTRACE ... pid=8 user=1 rid=... url=...` | *(not an error)* | Written at the **start** of every request. It is context: a request killed by gunicorn never reaches an end hook, so the timeout line (which only carries a pid) is joined to that pid's last `REQTRACE` to recover the user, dashboard and chart. |

`REQTRACE`, `REQEND` and `REQEXC` are **not native Superset output**: they are added by `FLASK_APP_MUTATOR` in
[docker/superset/superset_config.py](docker/superset/superset_config.py). The `GUNICORN_*` and `CELERY` names are this parser's
labels, not text you will see in the log.

**Superset's `logs` table** (metadata database, read through `/api/v1/log/`). Three kinds of row matter:

| Row | Where the name sits | What it is | Used by the pipeline |
|---|---|---|---|
| `mount_dashboard`, `load_chart`, `force_refresh_chart` | inside the JSON of a row whose `action` is **`log`** (`json.event_name`), written by the browser | dashboard opened, chart loaded (`has_err=true` when it failed), chart refresh | yes: `dashboard_view`, `chart_view`, and a browser error row when `has_err` is true |
| `dashboard_load`, `chart_data`, `export_csv` / `export_excel` as `action` | the `action` column itself | **written by this lab's seed script** (`seed/generate_activity.py`), not by Superset | yes, so the seeded demo data and real use are both counted |
| `ChartDataRestApi.data`, `DashboardRestApi.get`, `execute_sql`, `csrf_token`, `LogRestApi.get_list`, ... and `log` rows named `hide_browser_tab`, `spa_navigation`, `mount_explorer`, `render_chart` | the `action` column / `json.event_name` | Superset's own record of API calls and page activity | no: kept in `raw_log_landing`, not turned into facts |

**The key difference.** Container logs capture server and process failures, above all gunicorn timeouts and killed workers. The
`logs` table captures application activity plus what the **browser** reports. A worker killed by gunicorn writes no row of its own
to the `logs` table, because the process dies before Superset can log the request; the only trace there is the browser's
`load_chart` row, and only if the tab stays open.

### Proof 1: what is in the `logs` table (real counts from this lab)

```bash
docker compose exec -T postgres psql -U superset -d superset -c "
  select action, count(*) from logs group by 1 order by 2 desc limit 12"
docker compose exec -T postgres psql -U superset -d superset -c "
  select json::json->>'event_name' as event_name, count(*) as rows,
         count(*) filter (where json::json->>'has_err' = 'true') as failed
  from logs where action = 'log' group by 1 order by 2 desc"
```

Result: `chart_data` 23195, `dashboard_load` 4800, `LogRestApi.get_list` 4507, `log` 916, `DashboardRestApi.get` 673, `export_csv` 150,
`ChartDataRestApi.data` 141, ... and for the `log` rows: `load_chart` 764 (663 failed), `hide_browser_tab` 57, `spa_navigation` 55,
`mount_dashboard` 33, `force_refresh_chart` 3, `mount_explorer` 3, `render_chart` 1.

Proof that `chart_data`, `dashboard_load` and `export_csv` come from the seed script: every one of those rows carries the seed's
referrer pattern.

```bash
docker compose exec -T postgres psql -U superset -d superset -c "
  select action, count(*) filter (where referrer like '/superset/dashboard/%/') as seed_style_referrer, count(*) as total
  from logs where action in ('chart_data','dashboard_load','export_csv') group by 1"
```

Result: `export_csv` 150 of 150, `dashboard_load` 4800 of 4800, `chart_data` 23177 of 23195 (the other 18 are Superset's own).
`export_excel` has no rows in this lab, so a real Superset export is **not proven** here.

### Proof 2: what is in the error table, by source (real)

```sql
select source, error_type, count(*) n
from iceberg.monitoring.fact_error_events group by 1, 2 order by 1, 3 desc;
```

Result: `browser` `load_chart_error` 427 and `timeout` 236; `gunicorn` `worker_timeout` 9; `server` `http_404` 13, `http_400` 13,
`http_422` 1. These came from the lab's own runs. There are no `worker_killed`, `gunicorn_error`, `REQEXC` or `celery` rows, because
the lab never produced those failures.

### Proof 3: a gunicorn timeout leaves no server-side row in `logs`

The `gunicorn` rows carry the user, dashboard and chart recovered from `REQTRACE`:

```sql
select event_ts, user_id, dashboard_id, chart_id, request_path, substr(error_message, 1, 70) msg
from iceberg.monitoring.fact_error_events where source = 'gunicorn' order by event_ts desc limit 3;
```

Result (latest): `2026-10-06 13:41:53`, user 1, dashboard 13, chart 166, `/api/v1/chart/data`,
`gunicorn WORKER TIMEOUT: worker pid 2311 exceeded the 15s request time...`.

Now look for the same chart in Superset's own log around that minute:

```bash
docker compose exec -T postgres psql -U superset -d superset -c "
  select 'server-side ChartDataRestApi.data rows for chart 166', count(*) from logs
   where action = 'ChartDataRestApi.data' and slice_id = 166 and dttm between '2026-10-06 13:41:00' and '2026-10-06 13:42:30'
  union all
  select 'browser load_chart has_err rows for chart 166', count(*) from logs
   where action = 'log' and slice_id = 166 and json::json->>'event_name' = 'load_chart'
     and json::json->>'has_err' = 'true' and dttm between '2026-10-06 13:41:00' and '2026-10-06 13:42:30'"
```

Result: **0** server-side rows and **1** browser row (`13:41:53.62`, `error_details = timeout`). The failure is in the container log
and in the browser's report, and nowhere in Superset's own server-side log. That is why the pipeline reads both.

### Proof 4: the parser handles every container-log type

```bash
docker compose exec -T pipeline python tests/test_ingest_parse.py
```

Expect `ALL PASSED` with one `PASS` per type: `worker_timeout` (joined to its request through the pid), `http_404`, an exception
class (`ProgrammingError`), `worker_killed`, `gunicorn_error`, `celery_task_failed`, plus "timeout + its SIGKILL count as one
incident". These are sample lines in the test, **not** captured production output: the lab did not produce a killed worker, an
unhandled exception or a Celery failure, so those paths are verified by the parser test only.

---

## 7. View the data in DBeaver

DBeaver reads the Iceberg tables through Trino, which the lab publishes on host port **8085**.

**7a. Create the connection**

1. **Database -> New Database Connection**, choose **Trino**. If it is not listed, search "Trino" or use the generic
   category; DBeaver downloads the driver the first time you connect (accept the prompt).
2. **Main** tab:

   | Field | Value |
   |---|---|
   | Host | `localhost` |
   | Port | `8085` |
   | Database / Catalog | `iceberg` |
   | Username | `pipeline` |
   | Password | leave empty (the lab Trino has no authentication) |

3. Click **Test Connection**; expect `Connected`. If it complains about SSL, open **Driver properties** and set `SSL` to `false`.
4. **Finish**.

In the navigator open **Trino -> iceberg -> monitoring -> Tables**. You should see the seven tables and the view from part 2.
Right-click a table -> **View data** for a quick look, or open a SQL editor (`Ctrl+]`) on the connection and run the queries
below. Tables are in schema `monitoring`; either pick it as the active schema or write `iceberg.monitoring.<table>`.

**7b. Queries to show**

Row counts, to prove the data landed:

```sql
select 'fact_access_events' t, count(*) n from iceberg.monitoring.fact_access_events
union all select 'fact_error_events', count(*) from iceberg.monitoring.fact_error_events
union all select 'raw_log_landing',  count(*) from iceberg.monitoring.raw_log_landing
union all select 'dim_dashboards',   count(*) from iceberg.monitoring.dim_dashboards
union all select 'dim_users',        count(*) from iceberg.monitoring.dim_users;
```

Expect tens of thousands of access and raw rows, hundreds of error rows and a handful of dashboards and users.

Most viewed dashboards:

```sql
select d.title, count(*) views, count(distinct f.user_id) viewers
from iceberg.monitoring.fact_access_events f
join iceberg.monitoring.dim_dashboards d on d.dashboard_id = f.dashboard_id
where f.action = 'dashboard_view'
group by 1 order by 2 desc limit 10;
```

Failure rate per dashboard (the number the Superset chart shows):

```sql
select d.title,
       sum(v.chart_loads)  loads,
       sum(v.failed_loads) failed,
       round(sum(v.failed_loads) * 100.0 / nullif(sum(v.rated_loads), 0), 2) failure_pct
from iceberg.monitoring.vw_chart_failure_rate v
join iceberg.monitoring.dim_dashboards d using (dashboard_id)
group by 1 order by failure_pct desc limit 10;
```

What failed, and why (any cause, grouped):

```sql
select error_category, source, count(*) errors, min(error_signature) example_signature
from iceberg.monitoring.fact_error_events
group by 1, 2 order by errors desc;
```

Expect categories such as `timeout`, `query_error`, `connection`, `permission`, `not_found` and `unclassified`.

The individual failures with their raw message:

```sql
select event_ts, source, error_category, dashboard_id, chart_id, error_message
from iceberg.monitoring.fact_error_events
order by event_ts desc limit 20;
```

Slow successful loads (average and p95 load time per dashboard):

```sql
select dashboard_id, count(*) loads, round(avg(duration_ms)) avg_ms,
       approx_percentile(duration_ms, 0.95) p95_ms
from iceberg.monitoring.fact_access_events
where action = 'chart_view' and is_error = false and duration_ms is not null
group by 1 order by p95_ms desc limit 10;
```

Is the pipeline healthy? One row per run, newest first (alert when the last `ok` sync row gets old):

```sql
select run_ts, kind, status, rows_read, rows_new, detail
from iceberg.monitoring.etl_run_log order by run_ts desc limit 10;
```

Expect `sync` rows with `status = ok`, and `ingest` / `maintenance` rows when those have run.

Iceberg itself: how many files and snapshots a table has (what compaction cleans up):

```sql
select count(*) files, sum(file_size_in_bytes) bytes from iceberg.monitoring."fact_access_events$files";
select committed_at, operation from iceberg.monitoring."fact_access_events$snapshots" order by committed_at desc limit 5;
```

> Superset's own metadata (users, dashboards, charts and the `logs` table the ETL reads) is separate, in Postgres. To look at it
> in DBeaver use host `localhost`, port `15432`, database `superset`, and the Postgres credentials in `.env`.

---

## 8. Maintenance and rebuilds

**Compaction, snapshot expiry, orphan-file cleanup** (runs from `run_etl.sh` about once a day; force it now):

```bash
docker compose exec -T pipeline python etl/iceberg_maintenance.py --force
```

Expect `maintenance done for 7 tables (retention 7d)`. Re-run the files/snapshots queries above to see the effect.

**Rebuild facts from the stored raw rows** (after changing how log rows are classified), and **re-classify every error** (after
adding a rule to `etl/error_classify.py`):

```bash
docker compose exec -T pipeline python etl/backfill_iceberg.py
docker compose exec -T pipeline python etl/backfill_iceberg.py --reclassify
```

Expect `replayed N raw log rows; merged into Iceberg: ...` and `recomputed category and signature for N error rows`.

**Re-read the whole Superset log** (deliberate backfill): `docker compose exec -T pipeline python etl/sync_logs.py --full`.
`--since '2026-09-01 00:00:00'` re-reads from a date; `--reprocess` also re-sends rows already stored.

---

## 9. Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `another run holds the single-writer lease` on every run | A previous run was interrupted and its lease has not expired (30 min). Wait, or clear it: `... trino --execute "delete from iceberg.monitoring.etl_lock"` |
| Commits fail with `ICEBERG_COMMIT_ERROR ... 500`, even single statements | The lab's SQLite-backed Iceberg catalog got stuck after simultaneous commits. `docker compose -f lakehouse/docker-compose.yml restart iceberg-rest`, then clear the lease as above. A production catalog does not behave this way. |
| `Connection refused` to Trino from DBeaver | Check `docker compose -f lakehouse/docker-compose.yml ps` shows `trino` up; the host port is 8085 (`TRINO_PORT` in `lakehouse/.env`). |
| Dashboards empty | Run the ETL first (part 3), then the bootstrap (part 5). |
| Superset charts created by the bootstrap show no data when read through the API | Normal: API-created charts have no saved query context. They render in the UI. |

## 10. Stop

```bash
docker compose down                                                          # Superset stack, keeps its data
docker compose -f lakehouse/docker-compose.yml down                          # lakehouse, keeps the Iceberg data
docker compose -f lakehouse/docker-compose.yml down -v                       # ...and deletes the Iceberg data
```

---

More detail: [README.md](README.md) (setup and operating notes), [WRITEUP.md](WRITEUP.md) (design decisions),
[DBEAVER_GUIDE.md](DBEAVER_GUIDE.md) (all connections), [lakehouse/README.md](lakehouse/README.md) (the lab lakehouse),
[intermittent-error/DEMO.md](intermittent-error/DEMO.md) (the live timeout demo).

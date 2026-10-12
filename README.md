# Superset access-log pipeline

Pulls Superset's activity log (`LogRestApi`) and its container logs into **Iceberg** (written and read through Trino), and shows
usage, failure rate, load time and errors as native Superset dashboards. Iceberg is the only store; the pipeline is a few Python
scripts run on a schedule.

```
Superset logs (/api/v1/log/) ---+
                                +--> ETL (python) --> Trino MERGE --> Iceberg --+--> Superset dashboards
container logs (gunicorn, ...) -+                                               +--> any SQL client (DBeaver) over Trino
```

## Layout

```
docker-compose.yml           Superset (4.1.4), Postgres, Redis, Celery, the pipeline container, the dashboard bootstrap
.env.example                 copy to .env before the first run
docker/superset/             Superset image (+ Trino driver), superset_config.py (request logging), gunicorn_config.py
docker/postgres/             init-examples-db.sql: creates the "examples" database
lakehouse/                   local stand-in for the bank's Iceberg + Trino (MinIO, Iceberg REST catalog, Trino); lab only
access-log-pipeline/
  Dockerfile, requirements.txt   the pipeline container image
  superset_client.py         shared Superset API auth helper
  run_etl.sh                 one scheduled cycle: init, sync, container-log ingest, maintenance (what cron calls)
  schema_iceberg/            Iceberg DDL, applied in order by etl/iceberg_init.py: 001 fact_access_events, 002 fact_error_events,
                             003 dim_entities, 004 vw_chart_failure_rate, 005 raw_log_landing, 006 etl_run_log, 007 etl_lock,
                             008 container_log_lines, 009 vw_all_logs
  etl/                       sync_logs.py, ingest_container_errors.py, error_classify.py, iceberg_init.py, iceberg_sink.py,
                             iceberg_state.py, iceberg_maintenance.py
                             and backfill_iceberg.py (rebuild / re-classify / re-parse what is already stored)
  superset_bootstrap/        bootstrap_dashboard.py (usage dashboard, Trino connection) and bootstrap_iceberg.py (reliability dashboard)
  seed/generate_activity.py  inserts simulated traffic into Superset's own logs table (dev/test Superset only)
  tests/                     test_iceberg_state.py, test_ingest_parse.py, test_error_classify.py, acceptance_watermark.py,
                             browser_failure_test.py (end-to-end with a real Chrome, run on the host)
intermittent-error/          demo of a silent, cache-dependent chart timeout: repro/ (slow chart, trigger, evidence), fix/
                             (database statement timeout), logs_before/ and logs_after/ (captured evidence)
```

## Run it (local lab)

Run from the repository root; on Windows use Git Bash. Docker must be running.

```bash
cp .env.example .env && cp lakehouse/.env.example lakehouse/.env            # first time only
docker compose -f lakehouse/docker-compose.yml up -d                         # MinIO, Iceberg catalog, Trino
docker compose -f docker-compose.yml -f lakehouse/connect-pipeline.yml up -d --build postgres redis superset pipeline
docker compose exec -T pipeline python etl/iceberg_init.py                   # create the tables and views
docker compose exec -T pipeline python seed/generate_activity.py             # optional: simulated traffic
bash access-log-pipeline/run_etl.sh                                          # load Superset's log and the container logs
docker compose -f docker-compose.yml -f lakehouse/connect-pipeline.yml run --rm superset-bootstrap   # build the dashboards
```

Superset: http://localhost:8089 (`admin` / `admin`). Dashboards: **Superset Usage Analytics** (`/superset/dashboard/12/`) and
**Dashboard Reliability and Speed** (`/superset/dashboard/14/`). Schedule `run_etl.sh` with cron (`*/10 * * * *`), a Kubernetes
`CronJob` (`concurrencyPolicy: Forbid`) or Airflow. Stop with `docker compose down` and
`docker compose -f lakehouse/docker-compose.yml down` (add `-v` to delete the data).

In the bank, skip `lakehouse/` and `connect-pipeline.yml`: set `TRINO_HOST`, `TRINO_PORT`, `TRINO_USER`, `TRINO_CATALOG`,
`TRINO_SCHEMA` (and `TRINO_PASSWORD` for TLS/auth) in `.env`, then run the same `iceberg_init.py`.

## What is stored (`iceberg.monitoring`)

| Object | Holds |
|---|---|
| `raw_log_landing` | verbatim copy of every Superset log row; the ETL's watermark is read from it |
| `fact_access_events` | one row per dashboard view, chart view or export, with browser-measured `duration_ms` and `is_error` |
| `fact_error_events` | one row per failure of any cause (`browser`, `server`, `gunicorn`, `celery`) with category, signature and raw message |
| `container_log_lines` | container log lines with parsed `user_id`, `dashboard_id`, `chart_id`, `method`, `url_path`, `status_code`, `duration_ms`, `request_id`, `is_error` |
| `dim_entities` | `entity_type` (`dashboard`, `chart`, `user`), `entity_id`, `name`: ids to names |
| `etl_run_log`, `etl_lock` | one row per run (alert on it); the single-writer lease |
| `vw_all_logs` (view) | **all logs in one place**: Superset's log and the container logs, same columns, `where is_error` for failures |
| `vw_chart_failure_rate` (view) | per day and dashboard: `title`, `chart_loads`, `failed_loads`, `failure_rate` (that row's ratio, for reading), `rated_loads`, `authorization_denied`, `server_kills`, `server_errors` |

Failure rate = `SUM(failed_loads) / SUM(rated_loads)` (a ratio of sums). `failed_loads` are browser-reported failed chart loads;
`rated_loads` is chart loads minus permission denials (set `AUTHZ_COUNTS_AS_FAILURE=true` to count denials as failures). It is a
**lower bound**: the browser only reports while the tab stays open, so `server_kills` (gunicorn's own count) is shown beside it.
Load time uses successful loads only. Server errors are shown beside the rate, not added to it, to avoid double counting.

## Query the logs and trace an error

Connect DBeaver (or any SQL client) to Trino: host `localhost`, port `8085`, catalog `iceberg`, schema `monitoring`, user
`pipeline`, no password. (Superset's own Postgres: `localhost:15432`, database `superset`, user/password `superset`.)

`vw_all_logs` columns: `event_ts`, `source` (`superset_logs` or `container`), `component`, `kind`, `level`, `is_error`,
`user_id`/`username`, `dashboard_id`/`dashboard`, `chart_id`/`chart`, `request_id`, `pid`, `method`, `url_path`, `status_code`,
`duration_ms`, `message`. Names are never empty: `(not from a dashboard)`, `(no chart)`, `Chart 387 (removed)` and so on.

```sql
-- every failure in the last day, from both sources
select event_ts, source, component, kind, status_code, username, dashboard, chart, url_path, request_id, substr(message, 1, 100) message
from iceberg.monitoring.vw_all_logs
where is_error and event_ts > current_timestamp - interval '1' day
order by event_ts desc limit 50;

-- failures per source, kind and dashboard
select source, kind, dashboard, count(*) errors, max(event_ts) last_seen
from iceberg.monitoring.vw_all_logs where is_error group by 1, 2, 3 order by errors desc;
```

**Trace one error.** Take the `request_id` of a container failure row, and paste the real value (placeholders are not SQL).
Step 1, the request's own lines (start, end, user, chart, status, duration):

```sql
select event_ts, kind, pid, status_code, duration_ms, user_id, chart_id, url_path, substr(message, 1, 170) message
from iceberg.monitoring.container_log_lines
where request_id = '058d29284dbe' order by event_ts;
```

Step 2, what Superset's own log recorded for the same user and chart within 10 seconds (the browser's `load_chart` with its error text):

```sql
with req as (
    select event_ts, user_id, chart_id from iceberg.monitoring.container_log_lines
    where request_id = '058d29284dbe' and kind = 'reqend'
)
select s.event_ts, s.user_id, s.chart_id, s.kind, s.is_error, s.message
from req r
join iceberg.monitoring.vw_all_logs s
  on s.source = 'superset_logs' and s.user_id = r.user_id and s.chart_id = r.chart_id
 and s.event_ts between r.event_ts - interval '10' second and r.event_ts + interval '10' second
order by s.event_ts;
```

A gunicorn worker timeout only logs a worker pid; the ingest links it to the request that worker was serving, so its row already has
`request_id`, user, dashboard and chart and the same two queries trace it. If a request has no user or chart (an API call, `user=-`),
step 1 is the whole story.

**Which log has what.** Container logs hold server and process failures: gunicorn timeouts and killed workers, HTTP 400+ on
dashboard/chart paths (`reqend`), unhandled exceptions (`reqexc`), failed Celery tasks. `reqtrace`, `reqend` and `reqexc` lines are
added by `docker/superset/superset_config.py`, not by Superset. Superset's `logs` table holds application activity and what the
**browser** reports (`log` rows whose `event_name` is `load_chart`, `mount_dashboard`, ...). A worker killed by gunicorn writes no row
of its own to `logs`; the only trace there is the browser's `load_chart` with `has_err`, and only if the tab stays open. That is why
both are read. Health checks and the ETL's own polling of the log API (about 30% of the lines) are not stored (`LOG_DROP_PATHS`).
Docker deletes a container's logs when the container is removed, so what the pipeline has stored is the durable copy.

## How it works

- **Incremental sync.** `LogRestApi` cannot filter by id, so each run pulls `dttm > watermark` minus `ETL_LOOKBACK_HOURS` (24). The
  watermark is `max(source_dttm)` of `raw_log_landing`, derived from the data, ignoring future-dated rows. After the pull the row count
  Superset holds before the run's start is compared with what is stored; if Superset has more, the whole log is re-read once
  (idempotent) with a warning. `--since TIMESTAMP` / `--full` / `--reprocess` force a re-read.
- **Idempotent.** Every write is an Iceberg `MERGE`, and rows already stored are not sent again. Facts and dimensions are written
  first and `raw_log_landing` last, so a crash leaves the watermark at the last complete point and the next run repeats the rest.
- **Single writer without locks.** Iceberg has none, so runs share a lease row in `etl_lock` taken with an atomic `MERGE`; the loser
  skips. It expires after `ETL_LOCK_TTL_MINUTES` (30), and a commit conflict is retried with backoff. Also forbid overlapping runs in
  the scheduler.
- **Any error, whatever the cause.** Every failure keeps its raw message plus a category (`timeout`, `worker_failure`, `connection`,
  `permission`, `query_error`, `not_found`, `rate_limit`, `data_error`, `client_error`, `server_error`) and a signature (numbers and
  ids stripped, so the same error groups together). Anything unrecognised is `unclassified`, never dropped: add a rule to
  `etl/error_classify.py` once you understand it.
- **Maintenance.** `iceberg_maintenance.py` (from `run_etl.sh`, real work about daily): `optimize`, `expire_snapshots`,
  `remove_orphan_files`, and pruning of `container_log_lines` (30 days) and `etl_run_log` (90 days).
- **Alerting.** Alert when `select max(run_ts) from iceberg.monitoring.etl_run_log where kind = 'sync' and status = 'ok'` gets older
  than a few intervals. `run_etl.sh` exits non-zero if any step fails.

## Configuration (`.env`)

The defaults are fine for the lab. The ones you may change: `TRINO_*` (the bank's Trino), `ETL_LOOKBACK_HOURS`, `ETL_LOCK_TTL_MINUTES`,
`ICEBERG_RETENTION` (not below the catalog's minimum, Trino default 7d), `MAINTENANCE_INTERVAL_HOURS`, `LOG_LINES_RETENTION_DAYS`,
`LOG_SERVICES` (container services to store, default `superset superset-worker`), `LOG_DROP_PATHS`, `ERROR_TRACE_PREFIXES` (paths whose
HTTP errors Superset logs), `AUTHZ_COUNTS_AS_FAILURE`, `SLOW_LOAD_MS` (slow-load threshold, 5000); `SUPERSET_WEBSERVER_TIMEOUT`, `GUNICORN_WORKERS`, `CACHE_TIMEOUT_SECONDS`, `SLOW_QUERY_SLEEP_SECONDS` (Superset and the timeout demo). Batch sizes: `ICEBERG_BATCH_ROWS`,
`ICEBERG_RAW_BATCH_ROWS`, `ICEBERG_RAW_BATCH_CHARS` (keeps a statement under Trino's query length limit). The `SEED_*` values tune the
seed script.

## Rebuild what is already stored

`etl/backfill_iceberg.py` (run in the pipeline container) works from what Iceberg already holds, after you change how rows are classified:

```bash
docker compose exec -T pipeline python etl/backfill_iceberg.py              # rebuild access + browser-error facts from raw_log_landing
docker compose exec -T pipeline python etl/backfill_iceberg.py --reclassify # recompute error category and signature (after a rule change)
docker compose exec -T pipeline python etl/backfill_iceberg.py --log-lines  # re-parse stored container log lines (parsed columns, noise filter)
```

To re-read Superset's log itself: `python etl/sync_logs.py --full` (whole log) or `--since 'YYYY-MM-DD HH:MM:SS'`; `--reprocess`
also re-sends rows already stored.

## Demo: the intermittent, silent chart timeout

A chart query slower than gunicorn's timeout (15 s) gets its worker killed with SIGKILL: the user sees a generic error and Superset logs
nothing. Redis caches a successful result (300 s), so the same dashboard fails, then works after a refresh, then fails again. The
pipeline records it from both sides: the browser's `load_chart` (user, dashboard, chart, "timeout") and gunicorn's `WORKER TIMEOUT`
(joined to the request through the worker pid). `fact_access_events` still counts the failed load as an ordinary `chart_view`, which is
why errors have their own table. The demo needs 15 s gunicorn < **18 s query** (`SLOW_QUERY_SLEEP_SECONDS`) < 20 s database timeout.

Setup (stack up and data loaded as above), then a second Superset with a 60 s limit and its own cache prefix:

```bash
docker compose exec redis redis-cli -n 2 FLUSHDB                       # cold cache
docker compose run -d --name superset-nolimit -e SUPERSET_WEBSERVER_TIMEOUT=60 -e CACHE_KEY_PREFIX=superset8090_   -p 8090:8088 superset superset run -h 0.0.0.0 -p 8088 --with-threads
```

The dashboard is "Intermittent Error Repro (slow chart)". If it does not exist: `docker compose run --rm -e SLOW_QUERY_SLEEP_SECONDS=1
pipeline python /repro/create_slow_dataset.py`, then set the dataset SQL to `pg_sleep(18)` (in Git Bash prefix container paths such as
`/repro/...` with `MSYS_NO_PATHCONV=1`).

1. Open the `localhost:8089` dashboard: the chart fails after about 15 s.
2. `bash intermittent-error/repro/show_evidence.sh 5`: the gunicorn `WORKER TIMEOUT` / `SIGKILL` only in the container stdout, no
   `ChartDataRestApi.data` row for the failed request, only the browser's `load_chart` (`has_err=true`, timeout).
3. `bash access-log-pipeline/run_etl.sh`, then trace it as in "Query the logs and trace an error": a `container` row for the
   gunicorn timeout (already carrying the killed request's user, dashboard and chart) next to the browser's `load_chart`; the failure
   rate on **Dashboard Reliability and Speed** shows the slow chart's dashboard standing out.
4. Open the `localhost:8090` dashboard (about 18 s, succeeds, fills the cache), then refresh `localhost:8089`: instant. After 5 minutes
   the cache expires and the error returns. Clean up with `docker rm -f superset-nolimit`.

The fix is to make the database fail first: a Postgres `statement_timeout` shorter than the gunicorn timeout, so the query is cancelled,
Superset catches and logs a real error, and the worker is not killed. Order every timeout from inside out: database, gunicorn, any
proxy or load balancer, CDN. `python intermittent-error/repro/trigger_timeout.py before|after` and
`docker compose run --rm pipeline python /fix/apply_fix.py` script the before/after comparison (with `SLOW_QUERY_SLEEP_SECONDS=45` so
the query outlasts the 20 s database limit); evidence lands in `logs_before/` and `logs_after/`. gunicorn's `timeout` only works with
`sync` workers (`gthread` hides stalls).

## Tests

```bash
docker compose exec -T pipeline python tests/test_iceberg_state.py        # lease, retry, write order, skip-known (no services needed)
docker compose exec -T pipeline python tests/test_ingest_parse.py         # container-log parser, parsed columns, noise filter
docker compose exec -T pipeline python tests/test_error_classify.py       # error classifier
docker compose exec -T pipeline python tests/acceptance_watermark.py      # late, old and future rows; the lease (needs the lab stack)
python access-log-pipeline/tests/browser_failure_test.py                  # on the host: real Chrome (pip install playwright); a failure the
                                                                          # browser reports reaches Iceberg and is counted once
```

## To confirm in the bank's environment

Trino version (the lab runs 483) and catalog type; format-version 2 tables; permissions for the pipeline user (create schema/table,
`MERGE`, `UPDATE`/`DELETE` on the small state tables, `ALTER TABLE ... EXECUTE`); the catalog's minimum snapshot retention; TLS and
authentication; who owns compaction; retention of Superset's `logs` table (limits how far a re-read can go); a log platform to ship
container logs from (today they reach the pipeline by piping `docker compose logs`; on OpenShift replace that step in `run_etl.sh`).

## Known gaps

No client IP (Superset's log model does not record it). User/dashboard/chart on gunicorn rows come from `REQTRACE`, so only for
timeouts after it was enabled; API-token requests show `user=-`. The failure rate under-counts (browser rows need an open tab).
Superset 4.1.4 is pinned: re-check the log event names if you upgrade. The lab's Iceberg catalog keeps its metadata in SQLite and can
get stuck after simultaneous commits (a production catalog does not).

## Troubleshooting

| Symptom | Fix |
|---|---|
| `another run holds the single-writer lease` on every run | an interrupted run's lease expires after 30 min, or `delete from iceberg.monitoring.etl_lock` |
| `ICEBERG_COMMIT_ERROR ... 500` | lab catalog stuck: `docker compose -f lakehouse/docker-compose.yml restart iceberg-rest`, then rerun `iceberg_init.py` |
| dashboards empty | run `run_etl.sh`, then the bootstrap |
| `/repro/...` or `/fix/...` "No such file or directory" in Git Bash | prefix the command with `MSYS_NO_PATHCONV=1` |

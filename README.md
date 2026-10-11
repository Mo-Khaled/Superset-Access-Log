# Superset Local Lab: Access Log Pipeline + Intermittent Error Repro/Fix

A hands-on local reproduction of two Superset tickets:

1. **Access log pipeline** (`access-log-pipeline/`): pull Superset's activity data out via `LogRestApi` and its container
   logs, store everything in **Iceberg** (read through Trino) in a clean schema, and surface usage, failure rate and load time
   as **native Superset dashboards**. Iceberg is the only store: there is no MySQL.
2. **Intermittent silent error** (`intermittent-error/`): reproduce a gunicorn worker-timeout kill that shows a generic error
   to the user with nothing in Superset's application logs, show that the pipeline captures it, and show the fix.

Everything runs via Docker Compose. Superset is pinned to **`apache/superset:4.1.4`** (4.1.x for documentation maturity;
re-check the log event names if you move to a newer version).

## Contents

1. [Architecture and layout](#1-architecture-and-layout)
2. [Run it](#2-run-it)
3. [What is captured: data model, log sources, metrics](#3-what-is-captured-data-model-log-sources-metrics)
4. [How the ETL works and how to operate it](#4-how-the-etl-works-and-how-to-operate-it)
5. [Verify it works](#5-verify-it-works)
6. [The dashboards](#6-the-dashboards)
7. [Viewing the data: Superset and DBeaver](#7-viewing-the-data-superset-and-dbeaver)
8. [The intermittent chart timeout: demo, cause and fix](#8-the-intermittent-chart-timeout-demo-cause-and-fix)
9. [The lab lakehouse](#9-the-lab-lakehouse)
10. [Design decisions, bugs found, known gaps](#10-design-decisions-bugs-found-known-gaps)
11. [Troubleshooting](#11-troubleshooting)

---

## 1. Architecture and layout

```
Superset logs (/api/v1/log/) ----+                                            +--> Superset "Superset Usage Analytics"
container logs (gunicorn, ...) --+--> ETL (python) --> Trino MERGE --> Iceberg tables --+--> Superset "Dashboard Reliability and Speed"
                                                                              |
                                                                              +--> DBeaver (SQL, over Trino)
```

Two compose stacks run side by side:

| Stack | Services |
|---|---|
| Main (`docker-compose.yml`) | `postgres` (Superset metadata + examples DB), `redis` (cache, Celery), `superset-init` (one-shot), `superset` (gunicorn, host port 8089), `superset-worker` + `superset-worker-beat`, `pipeline` (tooling container with the ETL, seed and repro scripts), `superset-bootstrap` (one-shot dashboard provisioning) |
| Lab lakehouse (`lakehouse/docker-compose.yml`) | `minio`, `iceberg-rest`, `trino`, plus two one-shot helpers. A stand-in for the bank's own platform, see [section 9](#9-the-lab-lakehouse). |

`lakehouse/connect-pipeline.yml` is a compose override that joins `pipeline`, `superset` and `superset-bootstrap` to the
lakehouse network and sets the `TRINO_*` values.

```
docker-compose.yml               top-level orchestration
.env.example                     copy to .env (git-ignored) before first run
docker/superset/                 custom Superset image (config, gunicorn config)
docker/postgres/                 creates the separate "examples" DB on the same PG instance
lakehouse/                       lab stand-in for the bank's Iceberg + Trino (own compose file, all images)
access-log-pipeline/
  schema_iceberg/                Iceberg DDL (tables, failure-rate view, lock + run log), applied by etl/iceberg_init.py
  seed/generate_activity.py      simulates skewed multi-user dashboard traffic
  etl/sync_logs.py               incremental Superset logs -> Iceberg, with lookback + reconciliation
  etl/iceberg_sink.py            batched MERGE into Iceberg through Trino (retries commit conflicts)
  etl/iceberg_state.py           single-writer lease, watermark and run log, all in Iceberg
  etl/iceberg_maintenance.py     compaction, snapshot expiry, orphan-file cleanup (about daily)
  etl/iceberg_init.py            creates/verifies the Iceberg schema, tables (format-version 2) and view
  etl/ingest_container_errors.py server-side failures of ANY cause from the container logs -> error table
  etl/error_classify.py          cause-agnostic category + signature for any error message
  etl/backfill_iceberg.py        rebuild facts from raw_log_landing, or re-classify errors, after a rule change
  tests/acceptance_watermark.py  proves past/future-dated rows are neither skipped nor counted wrongly, and the lease
  tests/test_error_classify.py   classifier checks;  tests/test_ingest_parse.py  container-log parser checks
  tests/test_iceberg_state.py    lease, retry, write-order and skip-known checks (no services needed)
  run_etl.sh                     one scheduled cycle (init, sync, error ingest, maintenance); what cron calls
  superset_client.py             shared Superset API auth helper
  superset_bootstrap/            provisions the native Superset dashboards (usage; reliability and speed)
intermittent-error/
  repro/                         slow chart/dashboard, trigger, show_evidence.sh
  fix/                           applies the timeout-alignment fix
  logs_before/, logs_after/      captured evidence (created when you run the repro)
```

---

## 2. Run it

Run every command from the repository root. On Windows use Git Bash (or WSL) for the `bash` and `docker compose` lines.
In Git Bash, commands that take a leading-slash container path (`/repro/...`, `/fix/...`) need `MSYS_NO_PATHCONV=1` in front,
or MSYS rewrites the path and the command fails with "No such file or directory".

### 2.1 Prerequisites

- Docker Desktop running.
- First time only:
  ```bash
  cp .env.example .env                       # set real passwords outside the lab
  cp lakehouse/.env.example lakehouse/.env
  ```

### 2.2 Start the lakehouse

```bash
docker compose -f lakehouse/docker-compose.yml up -d
```

Expect `minio`, `iceberg-rest` and `trino` started; `minio-bootstrap` and `catalog-init` run once and exit.

### 2.3 Start Superset and the pipeline container

```bash
docker compose -f docker-compose.yml -f lakehouse/connect-pipeline.yml up -d --build \
    postgres redis superset pipeline
```

Expect Postgres and Redis healthy, `superset-init` runs once and exits with code 0 (`superset db upgrade`, creates the admin
user, `superset init`, `load_examples`), then `superset` and `pipeline` start. The first build takes a few minutes. This is
the minimum: no worker, beat or bootstrap is needed. Check it:

```bash
curl -s http://localhost:8089/health                                                      # Superset: OK (can take a minute)
docker compose -f lakehouse/docker-compose.yml exec -T trino trino --execute "select 1"   # Trino: "1"
```

Superset is at **http://localhost:8089** (login `admin` / `admin`, from `.env`; mapped from the container's 8088, which is
already in use on the host). Superset's `DBEventLogger` (writes every action to the metadata DB's `logs` table, which
`LogRestApi` reads) is enabled explicitly in `docker/superset/superset_config.py`.

### 2.4 Create the Iceberg tables

```bash
docker compose exec -T pipeline python etl/iceberg_init.py
```

```
Trino version: 483 (catalog iceberg, schema monitoring)
Iceberg objects ready; tables are format-version 2; view created; authorization denials are reported separately
```

Idempotent. It creates the tables and view described in [section 3.1](#31-tables) and checks every table is Iceberg format
version 2, which `MERGE` needs.

### 2.5 Put data in

**Optional: simulate 30 days of traffic** (skip if Superset already has history). It creates about 12 fake users and inserts a
skewed history of dashboard/chart-view rows, including a small share of failed chart loads, directly into Superset's own `logs`
table (see the docstring in `seed/generate_activity.py` for why it writes there rather than driving a headless browser):

```bash
docker compose exec -T pipeline python seed/generate_activity.py
```

Expect `created user ...` lines, then `inserted N simulated log rows across M dashboards ...`.

**Run the ETL: Superset log -> Iceberg.**

```bash
docker compose exec -T pipeline python etl/sync_logs.py
```

Expect, on the first run against an existing Superset:

```
synced through log id 35069, watermark 2026-10-11 05:57:15: 35069 log rows read (35069 new, 0 already stored), 28942 access events, 663 browser errors; pull started at 1970-01-01 00:00:00
iceberg: sent 28942 access rows, 663 error rows, 35069 raw rows, 20 dashboards, 13 users to MERGE (rows already present are left untouched)
```

"Pull started at 1970" means a full first pull; later runs start 24 hours before the watermark. The first run on a large log
takes a minute or two.

**Ingest server-side errors from the container logs** (gunicorn timeouts, HTTP errors, failed Celery tasks). The pipeline
container has no docker access, so pipe the logs in from the host:

```bash
docker compose logs --no-log-prefix --since 24h superset superset-worker \
  | docker compose exec -T pipeline python etl/ingest_container_errors.py
```

Expect `server-side errors found: 3 (server=3); sent to iceberg: 3` (counts depend on what the containers logged; `found: 0
(none)` is normal on a quiet system).

**Or all of it in one command**, which is what cron runs:

```bash
bash access-log-pipeline/run_etl.sh
```

Expect the output of `iceberg_init`, `sync_logs`, the error ingest, and `maintenance not due yet` (compaction really runs about
once a day). The script exits non-zero if any step fails. Its log window (`ERROR_LOG_WINDOW`, default 15m) must be longer than
the cron interval; the overlap is harmless because rows are de-duplicated.

### 2.6 Build the Superset dashboards

```bash
docker compose -f docker-compose.yml -f lakehouse/connect-pipeline.yml run --rm superset-bootstrap
```

Expect lines like `created dataset ...`, `created chart ...` (or `already exists` / `updated chart ...` on a re-run) and
`Iceberg dashboard ready: 'Dashboard Reliability and Speed'`. It registers the Trino connection to the Iceberg tables as a
Superset database, registers the datasets, and builds both dashboards ([section 6](#6-the-dashboards)). It is idempotent, safe
to re-run any time, and on an install made when the pipeline still used MySQL it also points the existing datasets at Trino and
removes the old MySQL connection. (`docker compose logs -f superset-init` and `... superset-bootstrap` follow the one-shots.)

### 2.7 Against the bank's real Trino, and moving from the MySQL version

Do not use the `lakehouse/` files in the bank: set the `TRINO_HOST/PORT/USER/CATALOG/SCHEMA` values in `.env` (and
`TRINO_PASSWORD` for TLS/auth), then run the same `iceberg_init.py`.

Moving from the earlier MySQL-backed version: the MySQL service is gone. Run `iceberg_init.py` (creates the new tables) and
`sync_logs.py --full` once: it re-reads Superset's whole log, so the facts and the raw copy are rebuilt from the source of
truth. Server-side errors (gunicorn, server, celery) already in Iceberg are kept; any that existed only in MySQL are not
migrated (Superset's container logs are the only other source of those).

### 2.8 Stop

```bash
docker compose down                                       # Superset stack, keeps its data
docker compose -f lakehouse/docker-compose.yml down       # lakehouse, keeps the Iceberg data
docker compose -f lakehouse/docker-compose.yml down -v    # ...and deletes the Iceberg data
```

`docker compose down -v` on the main stack also wipes Superset's own data (next startup is a blank slate).

---

## 3. What is captured: data model, log sources, metrics

### 3.1 Tables

Everything is in Iceberg (`iceberg.monitoring`, Parquet, format version 2), read through Trino:

| Object | What it holds | Why it exists |
|---|---|---|
| `raw_log_landing` | Verbatim JSON of every Superset log row, keyed by Superset's own log id | Superset may delete old logs. With a raw copy the other tables can be rebuilt after a bug fix or a new metric. The ETL's watermark is read from it. |
| `fact_access_events` | One row per dashboard view, chart view or export, with `duration_ms` (browser-measured load time) and `is_error` | The usage numbers. Partitioned by day. |
| `fact_error_events` | One row per failure of any cause: `source`, category, signature, raw message, status, user, dashboard, chart | Errors need their own table: `fact_access_events` counts a failed `load_chart` as an ordinary `chart_view`, deliberately, because it is a usage table. |
| `dim_dashboards`, `dim_users` | Titles/owners, user names | Keeps the fact table small; a renamed dashboard shows its new title everywhere. |
| `etl_lock` | The single-writer lease (one row) | Iceberg has no locks, see [4.2](#42-single-writer-without-locks). |
| `etl_run_log` | One row per run (sync, ingest, maintenance) with status and counts | What to alert on, see [4.4](#44-scheduling-and-alerting). |
| `vw_chart_failure_rate` (view) | Per day and dashboard: `chart_loads`, `rated_loads`, `failed_loads`, `authorization_denied`, `server_kills`, `server_errors` (future-dated rows excluded) | The failure-rate dashboards. |

Two modelling choices:

- **Dashboard views and chart views are separate rows.** "Most visited dashboards" counts dashboard views only; otherwise a
  10-chart dashboard would look 10x more popular than a 1-chart one (a 10-chart dashboard produces up to 11 log rows per view).
- **Repeat views are not merged.** A user refreshing is real activity. The usage dashboard shows total views *and* unique
  viewers, so "one person refreshing" and "20 people looking once" are easy to tell apart.

### 3.2 Two log sources, because neither sees every failure

**Container logs** (stdout of `superset` and `superset-worker`). The error types the ETL recognises
([ingest_container_errors.py](access-log-pipeline/etl/ingest_container_errors.py)):

| Parser label | The actual line | Stored as `error_type` | Notes |
|---|---|---|---|
| `GUNICORN_TIMEOUT` | `[CRITICAL] WORKER TIMEOUT (pid:8)` | `worker_timeout` | The worker exceeded the timeout and was killed. |
| `GUNICORN_KILLED` | `[ERROR] Worker (pid:8) was sent SIGKILL! ...` | `worker_killed` | Only when **no timeout preceded it** (killed from outside, often out of memory). A SIGKILL right after a timeout is the same incident and is not stored twice. |
| `GUNICORN_OTHER` | any other gunicorn `[CRITICAL]` / `[ERROR]` line | `gunicorn_error` | Stored as `unclassified` unless a rule matches. |
| `REQEND` | `REQEND ... status=404 dur=17 url=... msg=Not found` | `http_<status>` | A dashboard/chart request that ended with HTTP 400 or above. |
| `REQEXC` | `REQEXC ... exc=ProgrammingError msg=...` | the exception class | An unhandled exception; merged into the `REQEND` row with the same request id. |
| `CELERY` | `ERROR/ForkPoolWorker-1] Task ...[id] raised unexpected: ...` | `celery_task_failed` | A failed background task. |
| `REQTRACE` | `REQTRACE ... pid=8 user=1 rid=... url=...` | *(not an error)* | Written at the **start** of every request. Context: a request killed by gunicorn never reaches an end hook, so the timeout line (which only carries a pid) is joined to that pid's last `REQTRACE` (at most 120 s old) to recover the user, dashboard and chart. |

`REQTRACE`, `REQEND` and `REQEXC` are **not native Superset output**: they are added by `FLASK_APP_MUTATOR` in
[docker/superset/superset_config.py](docker/superset/superset_config.py). `ERROR_TRACE_PREFIXES` sets which paths count as
dashboard/chart requests (default: charts, dashboards, explore, datasets, SQL Lab). The `GUNICORN_*` and `CELERY` names are the
parser's labels, not text you will see in the log. Python `ERROR` log lines are not ingested separately: the request hooks
already capture them with more context. A failure that leaves no trace in the browser event, a request hook, a gunicorn line
or a Celery log cannot be seen.

**Superset's `logs` table** (metadata database, read through `/api/v1/log/`). Three kinds of row matter:

| Row | Where the name sits | What it is | Used by the pipeline |
|---|---|---|---|
| `mount_dashboard`, `load_chart`, `force_refresh_chart` | inside the JSON of a row whose `action` is **`log`** (`json.event_name`), written by the browser | dashboard opened, chart loaded (`has_err=true` when it failed), chart refresh | yes: `dashboard_view`, `chart_view`, and a browser error row when `has_err` is true |
| `dashboard_load`, `chart_data`, `export_csv` / `export_excel` as `action` | the `action` column itself | **written by this lab's seed script** (`seed/generate_activity.py`), not by Superset | yes, so the seeded demo data and real use are both counted |
| `ChartDataRestApi.data`, `DashboardRestApi.get`, `execute_sql`, `csrf_token`, `LogRestApi.get_list`, ... and `log` rows named `hide_browser_tab`, `spa_navigation`, `mount_explorer`, `render_chart` | the `action` column / `json.event_name` | Superset's own record of API calls and page activity | no: kept in `raw_log_landing`, not turned into facts |

What `LogRestApi` exposes: `action`, `dttm`, `user_id`, `dashboard_id`, `slice_id` (chart id), `duration_ms`, `referrer` and a
free-form `json` payload (the `Log` model in `superset/models/core.py`). It has **no client IP**, so there is no `source_ip`
column in our schema; that is a gap in Superset's instrumentation, not in the pipeline. Cached chart renders may not always
re-log a fresh `chart_data` event, filter-only interactions on a loaded dashboard are not logged as new views, and embedded
dashboard views cannot be told from normal ones without extra referrer parsing.

**The key difference.** Container logs capture server and process failures, above all gunicorn timeouts and killed workers. The
`logs` table captures application activity plus what the **browser** reports. A worker killed by gunicorn writes no row of its own
to the `logs` table, because the process dies before Superset can log the request; the only trace there is the browser's
`load_chart` row, and only if the tab stays open.

#### Proof 1: what is in the `logs` table (real counts from this lab)

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

#### Proof 2: what is in the error table, by source (real)

```sql
select source, error_type, count(*) n
from iceberg.monitoring.fact_error_events group by 1, 2 order by 1, 3 desc;
```

Result: `browser` `load_chart_error` 427 and `timeout` 236; `gunicorn` `worker_timeout` 9; `server` `http_404` 13, `http_400` 13,
`http_422` 1. There are no `worker_killed`, `gunicorn_error`, `REQEXC` or `celery` rows, because the lab never produced those failures.

#### Proof 3: a gunicorn timeout leaves no server-side row in `logs`

The `gunicorn` rows carry the user, dashboard and chart recovered from `REQTRACE`:

```sql
select event_ts, user_id, dashboard_id, chart_id, request_path, substr(error_message, 1, 70) msg
from iceberg.monitoring.fact_error_events where source = 'gunicorn' order by event_ts desc limit 3;
```

Result (latest): `2026-10-06 13:41:53`, user 1, dashboard 13, chart 166, `/api/v1/chart/data`,
`gunicorn WORKER TIMEOUT: worker pid 2311 exceeded the 15s request time...`. Now look for the same chart in Superset's own log
around that minute:

```bash
docker compose exec -T postgres psql -U superset -d superset -c "
  select 'server-side ChartDataRestApi.data rows for chart 166', count(*) from logs
   where action = 'ChartDataRestApi.data' and slice_id = 166 and dttm between '2026-10-06 13:41:00' and '2026-10-06 13:42:30'
  union all
  select 'browser load_chart has_err rows for chart 166', count(*) from logs
   where action = 'log' and slice_id = 166 and json::json->>'event_name' = 'load_chart'
     and json::json->>'has_err' = 'true' and dttm between '2026-10-06 13:41:00' and '2026-10-06 13:42:30'"
```

Result: **0** server-side rows and **1** browser row (`13:41:53.62`, `error_details = timeout`). The failure is in the container
log and in the browser's report, and nowhere in Superset's own server-side log. That is why the pipeline reads both.

#### Proof 4: the parser handles every container-log type

```bash
docker compose exec -T pipeline python tests/test_ingest_parse.py
```

Expect `ALL PASSED` with one `PASS` per type: `worker_timeout` (joined to its request through the pid), `http_404`, an exception
class (`ProgrammingError`), `worker_killed`, `gunicorn_error`, `celery_task_failed`, plus "timeout + its SIGKILL count as one
incident". These are sample lines in the test, **not** captured production output: the lab did not produce a killed worker, an
unhandled exception or a Celery failure, so those paths are verified by the parser test only.

### 3.3 Any error, whatever the cause

The first version only recognised the gunicorn timeout, the one cause this lab was built around. Production will fail in ways
nobody predicted, so capture is not tied to one kind of failure. Every failure becomes a row with the **raw message** (never
dropped), a coarse **category**, a **signature** (the message with numbers, ids and quoted values removed, so "Columns missing in
dataset: [a, b]" and "[c, d]" count as one error) and the HTTP status.

| `source` | What it catches |
|---|---|
| `browser` | any `load_chart` event the browser reports as failed (`has_err`), with its own message |
| `server` | any dashboard/chart request that ended with HTTP 400 or above: status, duration, the message in the response, plus the exception class for unhandled exceptions |
| `gunicorn` | worker timeouts, workers killed from outside (SIGKILL/OOM) and any other arbiter error, tied to the request by worker pid |
| `celery` | failed background tasks (`raised unexpected`) |

Categories: `timeout`, `worker_failure`, `connection`, `permission`, `query_error`, `not_found`, `rate_limit`, `data_error`,
`client_error`, `server_error`, and **`unclassified`** for anything the rules do not recognise. That last one is the point: an
unknown cause is stored and shows up instead of being lost. Open **Top Error Messages** on the Reliability dashboard, look at the
biggest `unclassified` or fastest-growing signature, and add a rule to `etl/error_classify.py` once you understand it; existing
rows are re-classified with `etl/backfill_iceberg.py --reclassify`.

### 3.4 Metrics

Superset applies its own time grain, so one definition serves day, week and month.

- **Failure rate** = `SUM(failed_loads) / SUM(rated_loads)`. A failed load is one browser-reported failed chart load, so
  numerator and denominator share a unit (dividing by dashboard views would mix units and can exceed 1). `rated_loads` is
  `chart_loads` minus loads denied for lack of permission, and those denials are left out of `failed_loads` too, because a
  user without access is expected behaviour. They are reported separately as `authorization_denied`; set
  `AUTHZ_COUNTS_AS_FAILURE=true` if a wave of denials would mean something is broken in your environment. It is a ratio of
  sums, not an average of daily ratios, so week and month values weigh each day by its traffic.
- **Load time** = average and p95 of the browser-measured load time of *successful* chart loads (a timeout always reads about
  15 s and would hide real slowness). Percentiles cannot be rebuilt from daily percentiles, so Superset computes them directly on
  the fact table at whatever grain the chart uses. **Slow-load rate** = share of successful loads above `SLOW_LOAD_MS` (default
  5000; agree the value with the business).
- **Server errors and server kills** are shown beside the failure rate, not added to it: the browser may report the same
  failure, and adding them would double count. Gunicorn rows are also kept out of the numerator because they often cannot be
  tied to a dashboard.

**Read the failure rate as a lower bound.** Failed loads come from the browser, which only reports while the tab stays open. A
user who closes the tab during a 15 s timeout leaves no browser row. `server_kills` (gunicorn's own count of killed workers) sits
next to it: a value well above `failed_loads` means the browser is under-reporting. Load times are measured in the browser, so
they include network time. The numbers are only as complete as Superset's own log: reconciliation makes missing rows visible, but
rows deleted by Superset's retention cannot be recovered.

---

## 4. How the ETL works and how to operate it

### 4.1 Past-dated rows are not skipped (watermark)

`LogRestApi` cannot filter by id, so the ETL pulls rows with `dttm > watermark`. A row inserted later with an *older* `dttm`
(late browser flush, clock skew between workers, downtime, a backfill, the seed script) would never match that filter again.
We hit it: a second seed run inserted 7,225 rows and the ETL read 221. Guards:

1. **Lookback.** Each pull starts `ETL_LOOKBACK_HOURS` (default 24) before the watermark.
2. **Reconciliation.** After the pull, the number of rows Superset dated before the start of the run (the `count` field of the
   list API with a `dttm` filter) is compared with the same window in `raw_log_landing` (`source_dttm`). If Superset has more,
   the whole log is re-pulled once and a `WARNING` with the gap is printed (idempotent). A fixed window is essential: the ETL's
   own API calls are logged by Superset (about 50 rows per run), so a total count hid a few missing rows. Real numbers from
   testing: the first reconciliation run found 6,909 skipped rows, and after a second seed 6,808.
3. **`--since` / `--full`**: `python etl/sync_logs.py --since '2026-09-01 00:00:00'` (or `--full`) for a deliberate backfill;
   `--reprocess` also re-sends rows already stored.
4. **The watermark is derived from the data**: `max(source_dttm)` of `raw_log_landing`, ignoring future-dated rows, so it never
   moves backwards or into the future. Raw rows are written **last**, after the facts and dimensions they produced, so it can
   only advance once everything before it is stored. There is no separate state to fall out of step, and a failed run is simply
   retried. The seed script no longer creates future-dated rows, and the view ignores any that exist.

A trap found while building this: Superset 4.1.4's log list API **silently ignores** the filter operators `lte`, `le`, `gte` and
`ge` and returns the unfiltered result. Only `lt` and `gt` filter, and the ETL uses only those. (A first version of the
reconciliation used `lte` and reported a gap that did not exist, until the counts were compared with Postgres directly.)

### 4.2 Single writer, without locks

Iceberg has no locks, so the ETL, error ingest, backfill and maintenance share a lease row in `etl_lock`
(`etl/iceberg_state.py`). It is taken with a `MERGE`, and Iceberg's atomic commit decides a race: the loser fails with a commit
conflict, reads the owner back and skips its run. A run that sees a *free* lease but whose own commit failed (a catalog hiccup)
retries a few times. The lease expires on its own (`ETL_LOCK_TTL_MINUTES`, default 30) so a crashed run never blocks the
pipeline, and a long run renews it. In production also forbid overlapping runs in the scheduler (`concurrencyPolicy: Forbid`,
Airflow `max_active_runs=1`). If two runs ever do overlap, the data is still correct: every write is an idempotent `MERGE`, and a
statement that loses a commit race is retried with backoff (`ICEBERG_COMMIT_RETRIES`).

### 4.3 Consistency, batching and maintenance

- **No cross-table transaction.** Trino cannot commit two tables together. Write order is the consistency model: facts and
  dimensions first, `raw_log_landing` last, and both the watermark and the "already stored" check read `raw_log_landing`. A crash
  leaves the watermark at the last fully written point and the next run repeats the rest.
- **Idempotent writes.** Iceberg has no unique keys, so every write is a `MERGE ... WHEN NOT MATCHED`. The ETL writes with Trino
  SQL, so the pipeline needs no catalog or object-store credentials of its own. `MERGE` needs format-version 2; `iceberg_init.py`
  checks it.
- **Batching and small files.** Rows are merged in chunks (`ICEBERG_BATCH_ROWS`, default 2000; raw payloads are also chunked by
  size to stay under Trino's query length limit), one statement per table per chunk rather than one per API page, and rows
  already stored are not sent again, so a quiet cycle writes almost nothing.
- **Maintenance.** `etl/iceberg_maintenance.py` runs from `run_etl.sh` and does real work about once a day
  (`MAINTENANCE_INTERVAL_HOURS`): `optimize` (compaction), `expire_snapshots` and `remove_orphan_files` (`ICEBERG_RETENTION`,
  default 7d, which must not be below the catalog's minimum). Force it with `docker compose exec -T pipeline python
  etl/iceberg_maintenance.py --force` (expect `maintenance done for 7 tables (retention 7d)`). If another team already compacts
  these tables, hand them those statements and skip the call.
- **Rebuilds.** After changing how log rows are classified: `docker compose exec -T pipeline python etl/backfill_iceberg.py`
  (replays `raw_log_landing`; expect `replayed N raw log rows; ...`). After adding a classifier rule:
  `... etl/backfill_iceberg.py --reclassify` (expect `recomputed category and signature for N error rows`).

### 4.4 Scheduling and alerting

The ETL is designed to be invoked externally, not to loop. Each run does one bounded incremental sync and exits.

- **This lab:** cron every 10 minutes, calling `access-log-pipeline/run_etl.sh` (needs the `pipeline` container up):
  ```cron
  */10 * * * * /path/to/repo/access-log-pipeline/run_etl.sh >> /var/log/superset-log-etl.log 2>&1
  ```
- **Production, simple:** a Kubernetes `CronJob` with `concurrencyPolicy: Forbid` and `restartPolicy: OnFailure`.
- **Production, bigger:** an Airflow DAG (a `PythonOperator` or `KubernetesPodOperator` running the same script, with a failure
  callback to Slack/PagerDuty) for retries, alerts and run history.
- **Alert on staleness too:** every run appends a row to `etl_run_log` with status `ok` or `failed`. If the latest `ok` row for
  `kind = 'sync'` gets older than a few intervals, the scheduler itself has stopped even if no run reported a failure:
  `select max(run_ts) from iceberg.monitoring.etl_run_log where kind = 'sync' and status = 'ok'`.

### 4.5 To confirm in the bank's environment

Trino version (the lab runs 483); catalog type and storage; format-version 2 by default; permissions for the pipeline's Trino user
(create schema/table, `MERGE`, `UPDATE`/`DELETE` on the small state tables, `ALTER TABLE ... EXECUTE`); the catalog's minimum
snapshot retention; authentication and TLS to Trino; compaction owner and schedule; whether the bank's Superset can use the same
catalog and the `trino` SQLAlchemy driver; retention of Superset's `logs` table (limits how far a rescan can go); the slow-load
threshold. The lab values are not assumptions about the bank.

---

## 5. Verify it works

These answer the "what if it runs twice / crashes / a row arrives late" questions.

**Idempotent: run the sync again.**

```bash
docker compose exec -T pipeline python etl/sync_logs.py
```

Expect `... 373 log rows read (7 new, 366 already stored), 0 access events ...` and `sent 0 access rows`. Rows already stored are
not sent again, and the few new ones are Superset logging the ETL's own API calls. Nothing is duplicated.

**Single writer: start two runs at the same time.**

```bash
docker compose exec -T pipeline python etl/sync_logs.py > /tmp/run1.txt 2>&1 &
docker compose exec -T pipeline python etl/sync_logs.py > /tmp/run2.txt 2>&1
wait; head -2 /tmp/run1.txt /tmp/run2.txt
docker compose -f lakehouse/docker-compose.yml exec -T trino trino --catalog iceberg --schema monitoring \
  --execute "select count(*), count(distinct source_log_id) from raw_log_landing"
```

Expect one run to print `another run holds the single-writer lease 'etl'; skipping this run` while the other syncs normally, and
two equal numbers from the count.

**Late and past-dated rows are not lost.** The acceptance test inserts rows dated 3 hours ago, 10 days ago and tomorrow, runs the
real ETL after each, checks that nothing is missing, and checks the lease:

```bash
docker compose exec -T pipeline python tests/acceptance_watermark.py
```

Expect `PASS` lines for A to F and `ALL PASSED`. The 10-day-old row triggers `WARNING ... Re-pulling the whole log once`: that is
the reconciliation working. It leaves a few labelled rows (referrer `acceptance-test`) in the lab data, so run it on a lab stack.

**Unit tests** (the first needs no services):

```bash
docker compose exec -T pipeline python tests/test_iceberg_state.py
docker compose exec -T pipeline python tests/test_error_classify.py
docker compose exec -T pipeline python tests/test_ingest_parse.py
```

Expect `ALL PASSED` from each.

---

## 6. The dashboards

Open **http://localhost:8089** (`admin` / `admin`) -> **Dashboards**. They are ordinary Superset charts, so you also get the native
dashboard time-range filter, drill-downs and export-to-CSV.

| Dashboard | Charts |
|---|---|
| **Superset Usage Analytics** | Total Dashboard Views and Active Users (KPIs), Most Viewed Dashboards (views + unique viewers), Dashboard Views Over Time, User Activity, Zero-View Dashboards (deprecation candidates), Export Activity |
| **Dashboard Reliability and Speed** | Failure Rate (daily, weekly, monthly), Failed vs Total Chart Loads, Failure Rate by Dashboard, Chart Load Time (average and p95), Slow Loads by Dashboard, Errors by Category, Top Error Messages, Chart Load Errors (the individual failures) |

Talking points: the failure rate is a ratio of sums and a **lower bound** (the `Server kills` column is the server-side count that
shows the gap); load time uses successful loads only; **Top Error Messages** groups by what the error *says*, and anything the
rules do not recognise is `unclassified`, which is how a new cause is noticed (all defined in [section 3.4](#34-metrics)).

---

## 7. Viewing the data: Superset and DBeaver

### 7.1 Superset

Log in as above. **Data > Databases** shows the registered connections: `examples` (the seeded Superset demo DB) and `Lakehouse
(Trino Iceberg)` (the pipeline's own Iceberg tables, registered automatically by `superset-bootstrap`). **Dashboards** shows the
example dashboards (`load_examples` seeds these), the two pipeline dashboards, plus anything the repro scripts created.

### 7.2 DBeaver connections

Two systems are involved. **Postgres** hosts two logical databases on one server (Superset's own metadata, and the `examples`
content DB); **Trino over Iceberg** is separate and holds the pipeline's tables. Postgres is published on a non-standard host port
(15432) to avoid clashing with a local instance; the lab Trino is on 8085. The credentials are the plaintext lab-only ones from
`.env`: never reuse them anywhere real.

**Trino (the pipeline's Iceberg tables)**

1. **Database -> New Database Connection**, choose **Trino**. If it is not listed, search "Trino"; DBeaver downloads the driver the
   first time you connect (accept the prompt).
2. **Main** tab:

   | Field | Value |
   |---|---|
   | Host | `localhost` |
   | Port | `8085` |
   | Database / Catalog | `iceberg` |
   | Username | `pipeline` |
   | Password | leave empty (the lab Trino has no authentication) |

3. **Test Connection**; expect `Connected`. If it complains about SSL, open **Driver properties** and set `SSL` to `false`. **Finish**.

In the navigator open **Trino -> iceberg -> monitoring -> Tables**: you should see the tables and the view from
[section 3.1](#31-tables). Right-click a table -> **View data**, or open a SQL editor (`Ctrl+]`) and run the queries below. Write
`iceberg.monitoring.<table>` or pick `monitoring` as the active schema.

**Postgres, Superset's metadata DB** (host `localhost`, port `15432`, database `superset`, user `superset`, password `superset`):
dashboards, charts, users and, most relevant here, the `logs` table the ETL reads. Useful tables: `logs`, `dashboards`, `slices`,
`tables` (Superset's dataset registry, where the `slow_query_demo` virtual dataset lives after the repro), `ab_user`.

**Postgres, the `examples` content DB** (same server and credentials, database `superset_examples`): the example datasets Superset's
charts query (World Bank, COVID vaccine data, ...). The repro's `pg_sleep(...)` query runs against this connection.

### 7.3 Queries to show

Row counts, to prove the data landed (expect tens of thousands of access and raw rows, hundreds of error rows, a handful of
dashboards and users):

```sql
select 'fact_access_events' t, count(*) n from iceberg.monitoring.fact_access_events
union all select 'fact_error_events', count(*) from iceberg.monitoring.fact_error_events
union all select 'raw_log_landing',  count(*) from iceberg.monitoring.raw_log_landing
union all select 'dim_dashboards',   count(*) from iceberg.monitoring.dim_dashboards
union all select 'dim_users',        count(*) from iceberg.monitoring.dim_users;
```

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

What failed, and why (any cause, grouped; expect categories such as `timeout`, `query_error`, `connection`, `permission`,
`not_found`, `unclassified`):

```sql
select error_category, source, count(*) errors, min(error_signature) example_signature
from iceberg.monitoring.fact_error_events
group by 1, 2 order by errors desc;
```

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

Is the pipeline healthy? One row per run, newest first (expect `sync` rows with `status = ok`, and `ingest` / `maintenance` rows when
those have run):

```sql
select run_ts, kind, status, rows_read, rows_new, detail
from iceberg.monitoring.etl_run_log order by run_ts desc limit 10;
```

Iceberg itself: how many files and snapshots a table has (what compaction cleans up):

```sql
select count(*) files, sum(file_size_in_bytes) bytes from iceberg.monitoring."fact_access_events$files";
select committed_at, operation from iceberg.monitoring."fact_access_events$snapshots" order by committed_at desc limit 5;
```

From a terminal instead of DBeaver: `docker compose -f lakehouse/docker-compose.yml exec -T trino trino --execute "<sql>"`.

---

## 8. The intermittent chart timeout: demo, cause and fix

### 8.1 What happens, and where it shows up

A chart query takes longer than gunicorn's `timeout` (15 s here). The gunicorn master kills the whole worker process with SIGKILL.
That happens outside Superset's code, so Superset never gets to log the request, and the user just sees a generic error.

Where the error shows up, and where it does not (checked in this lab):

| Place | Visible? |
|---|---|
| Container output (`docker compose logs superset`) | Yes: `WORKER TIMEOUT` then `SIGKILL`. The "out of memory?" text is gunicorn boilerplate, not a real OOM. |
| Superset's `logs` table, server side | No row for the killed request. |
| Superset's `logs` table, browser side | Sometimes: a `load_chart` row with `has_err = true` and `timeout`, but only if the browser tab survives long enough to send it. |
| `fact_access_events` | It reads the `logs` table, so it misses the server side; the browser row is even counted as a normal `chart_view`. |
| `fact_error_events` | Yes, once the ETL has run: one `browser` row and one `gunicorn` row per failure. |

**Why it is "intermittent": the cache.** Query results are cached in Redis (`CACHE_TIMEOUT_SECONDS`, 300 s). A killed request never
writes a result, so a cold load always fails. If anything else finishes the same query (another session, a warm-up job), the next
load is served from cache and works until the cache expires. So the same dashboard fails, then works after a refresh, then fails again.

**How the pipeline records it.** `fact_error_events` is filled from two sources, because neither is complete on its own: the
`browser` row knows the user, dashboard and chart but only that it "timed out"; the `gunicorn` row knows the real reason, but its
timeout line carries only a worker id. Each worker therefore logs a `REQTRACE` line (pid, user, URL) when a request *starts*
(a killed request never reaches any "request finished" hook), and the ingest joins the timeout to that pid's last trace line.

```
browser load_chart (has_err)   -> Superset logs table --sync_logs.py--------------> fact_error_events (source=browser)
worker REQTRACE line (pid,user,URL) --gunicorn WORKER TIMEOUT (pid)     ----+--ingest_container_errors.py---------------> fact_error_events (source=gunicorn)
                                      (joined on the worker pid)
```

Example from this lab (one dashboard load, a few seconds apart):

| event_ts | source | user | dashboard | chart | reason |
|---|---|---|---|---|---|
| 08:24:24 | gunicorn | 1 | 13 | 204 | worker pid 8 exceeded the 15s timeout and was SIGKILLed; request was `POST /api/v1/chart/data` |
| 08:24:25 | browser | 1 | 13 | 204 | timeout after 15016 ms |

### 8.2 Live demo script (no fix applied)

Shows: (a) the error is intermittent (cache-driven), (b) it is invisible in Superset's own event log, (c) the pipeline records it
in Iceberg and turns it into a failure rate per day, week and month.

**Setup.** Bring the stack up and load data as in [section 2](#2-run-it) (tables, optional seed, `run_etl.sh`, bootstrap), then:

```bash
docker compose exec redis redis-cli -n 2 FLUSHDB                       # cold cache
docker compose run -d --name superset-nolimit -e SUPERSET_WEBSERVER_TIMEOUT=60 -e CACHE_KEY_PREFIX=superset8090_ \
  -p 8090:8088 superset superset run -h 0.0.0.0 -p 8088 --with-threads  # a Superset with no 15 s limit
```

The dashboard is "Intermittent Error Repro (slow chart)"; its chart sleeps 18 s (`SLOW_QUERY_SLEEP_SECONDS` in `.env`) against the
15 s gunicorn timeout. The query must take longer than gunicorn's timeout but shorter than the DB's: 15 s gunicorn < **18 s query** <
20 s DB `statement_timeout`. The 60 s timeout on the second server also stops the browser from giving up at 15 s, and
`CACHE_KEY_PREFIX` keeps it from reading the main server's cached 15 s browser timeout (chart results still share the
`superset_data_` prefix, so a result computed on 8090 is served on 8089). If the dashboard does not exist:
`docker compose run --rm -e SLOW_QUERY_SLEEP_SECONDS=1 pipeline python /repro/create_slow_dataset.py`, then set the dataset SQL to
`pg_sleep(18)` (creating it with 18 s hits the same 15 s kill). That script's introspection runs the query at creation time, so it
can take several multiples of the sleep.

**Script**

1. Open the `localhost:8089` dashboard: the chart fails after about 15 s. **Error #1.**
2. `bash intermittent-error/repro/show_evidence.sh 5` shows (1) gunicorn `WORKER TIMEOUT` / `SIGKILL`, only in container stdout; (2) no
   `ChartDataRestApi.data` row for the failed request; (3) only the browser's `load_chart` row (`has_err=true, timeout`).
3. Run one ETL cycle (`bash access-log-pipeline/run_etl.sh`), then ask Trino:
   ```bash
   docker compose -f lakehouse/docker-compose.yml exec trino trino --execute "
     select event_ts, source, user_id, dashboard_id, chart_id, substr(reason, 1, 90) reason
     from iceberg.monitoring.fact_error_events order by event_ts desc limit 5"
   ```
   You get a `browser` row (user, dashboard, chart, "timeout after ~15000 ms") and a `gunicorn` row ("worker pid N exceeded the 15s
   request timeout and was SIGKILLed; request was POST /api/v1/chart/data") joined through the worker pid. `fact_access_events` still
   counts the failed load as an ordinary `chart_view`, which is why errors have their own table.
4. Show the ratio, which is the point of it all:
   ```bash
   docker compose -f lakehouse/docker-compose.yml exec trino trino --execute "
     select date_trunc('week', day) week, sum(chart_loads) loads, sum(failed_loads) failed, sum(server_kills) kills,
            round(sum(failed_loads) * 100.0 / nullif(sum(rated_loads), 0), 2) failure_pct
     from iceberg.monitoring.vw_chart_failure_rate group by 1 order by 1"
   ```
   Then open **Dashboard Reliability and Speed** ([section 6](#6-the-dashboards)): the slow chart's dashboard stands out in the
   failure rate by dashboard.
5. Show that it is not about one cause. Cause a few different failures and run the cycle again:
   ```bash
   TOK=$(curl -s -X POST localhost:8089/api/v1/security/login -H 'Content-Type: application/json' \
         -d '{"username":"admin","password":"admin","provider":"db"}' | python -c 'import sys,json;print(json.load(sys.stdin)["access_token"])')
   curl -s -o /dev/null -H "Authorization: Bearer $TOK" localhost:8089/api/v1/chart/99999           # 404
   curl -s -o /dev/null -H 'Authorization: Bearer nope' localhost:8089/api/v1/dashboard/1            # 422
   bash access-log-pipeline/run_etl.sh
   docker compose -f lakehouse/docker-compose.yml exec trino trino --execute "
     select error_category, count(*) errors, min(error_signature) example
     from iceberg.monitoring.fact_error_events group by 1 order by 2 desc"
   ```
   **Top Error Messages** now has one row per kind of error, whatever its cause.
6. Open the `localhost:8090` dashboard (about 18 s, succeeds, fills the cache), then refresh `localhost:8089`: it loads instantly.
   **Intermittent.** After 5 minutes the cache expires and the error returns.
7. Teardown: `docker rm -f superset-nolimit`.

### 8.3 Reproduce and fix with the scripts

An alternative to the live demo, with captured evidence. The fixture is a virtual dataset whose query sleeps
`SLOW_QUERY_SLEEP_SECONDS`, a chart on it and a dashboard. For this flow the query must outlast the DB's `statement_timeout`
(20 s), so run it with `SLOW_QUERY_SLEEP_SECONDS=45` (or lower `FIX_STATEMENT_TIMEOUT_MS`). These scripted steps were not re-run in
the latest verification of this README.

```bash
MSYS_NO_PATHCONV=1 docker compose run --rm -e SLOW_QUERY_SLEEP_SECONDS=45 pipeline python /repro/create_slow_dataset.py   # idempotent
docker compose exec redis redis-cli FLUSHALL                  # the result is cacheable: flush before every trigger
python intermittent-error/repro/trigger_timeout.py before     # run on the HOST, so it can shell out to `docker compose logs`
```

`.env` starts with `SUPERSET_WEBSERVER_TIMEOUT=15`, shorter than the slow query, so gunicorn's arbiter SIGKILLs the worker
mid-request. In `intermittent-error/logs_before/superset_container_logs.txt` you see (at most) gunicorn's own terse
`[CRITICAL] WORKER TIMEOUT` line: **no Superset application-level exception, no new failed-request row in the `logs` table**,
because the worker is killed before any Superset/Flask exception handler runs.

**The real fix.** Make the database fail first: set a Postgres `statement_timeout` *shorter* than the gunicorn timeout, so the DB
cancels the query and Superset catches the error, logs it and shows a proper message.

1. Raise the webserver timeout so it is no longer the first thing to fire: edit `.env` (`SUPERSET_WEBSERVER_TIMEOUT=30`), then
   `docker compose up -d --no-deps superset`.
2. Apply a Postgres `statement_timeout` (20 s, shorter than the new 30 s webserver timeout) to the `examples` database connection:
   `MSYS_NO_PATHCONV=1 docker compose run --rm pipeline python /fix/apply_fix.py`.
3. Prove it: `python intermittent-error/repro/trigger_timeout.py after`. In `logs_after/superset_container_logs.txt` Postgres cancels
   the query at about 20 s, the chart-data endpoint catches the `QueryCanceled` error and logs it, and the client gets a real
   (still user-facing, but now *logged*) error instead of a silent hang-then-kill.

**In production, order every timeout from inside out:** database, gunicorn, any proxy or load balancer (nginx, ALB, ingress), CDN.
The database must always fail first, because it is the only layer that produces an error Superset can log. This lab has no proxy;
production almost always does. Optional extras: a gunicorn `worker_abort` hook and a browser-side error report.

**Two things that surprised us**

- gunicorn's `timeout` only works with `worker_class = "sync"`. With `gthread` the worker keeps reporting "alive" while a request
  thread is stuck, so the timeout never fires and the bug does not reproduce. The lab uses `sync`.
- The `worker_abort` hook (`docker/superset/gunicorn_config.py`) never ran. gunicorn sends SIGABRT first and escalates to SIGKILL on its
  next sweep about a second later, but the worker is blocked inside a synchronous `psycopg2` call in native code, and a Python signal
  handler cannot run until the interpreter regains control. The logs confirm it: `[CRITICAL] WORKER TIMEOUT` followed directly by
  `[ERROR] ... sent SIGKILL`, no `WORKER_ABORT` line. The hook would still catch a slow-but-Python-level stall; it is not the
  catch-all for a blocked DB call. (After the fix it is silent for a simpler reason: Postgres cancels the query before gunicorn's
  timeout is reached.)

---

## 9. The lab lakehouse

A self-contained Iceberg + Trino for the lab (`lakehouse/`, project name `scb-lakehouse`). It stands in for the bank's existing
platform so the pipeline can be built and tested without depending on anything outside this repo. **It is not part of the target
architecture:** in the real environment the pipeline and Superset connect to the bank's own Trino through the `TRINO_*` settings,
and nothing in this directory is deployed.

| Service | Image | Role |
|---|---|---|
| `minio` | `quay.io/minio/minio` | Object storage for the Iceberg data and metadata files |
| `minio-bootstrap` | `quay.io/minio/mc` | One-shot: creates the `warehouse` bucket |
| `catalog-init` | `busybox` | One-shot: fixes permissions on the catalog volume |
| `iceberg-rest` | `tabulario/iceberg-rest` | Iceberg REST catalog (SQLite on a volume) |
| `trino` | `trinodb/trino` | SQL engine. Catalog `iceberg`, schema `monitoring` |

No event listener, no custom image build. Start it as in [section 2.2](#22-start-the-lakehouse); `docker compose -f
lakehouse/docker-compose.yml ps` shows its state. Host ports: Trino `8085`, MinIO API `9102`, MinIO console `9103`, REST catalog
`8183`. They differ from the main stack (Superset on `8089`, Postgres on `15432`) and from the older `trino-query-history` project, so
all can run together.

**Lab limitation.** The REST catalog keeps its metadata in SQLite. If two writers commit at the same instant it can answer with an
HTTP 500 and then refuse commits until it is restarted (see [section 11](#11-troubleshooting)). The pipeline's single-writer lease makes
that rare. A production catalog with proper optimistic concurrency (a JDBC/Postgres-backed or managed catalog) does not behave this way.

---

## 10. Design decisions, bugs found, known gaps

### 10.1 Three ETL bugs we found and fixed

The first two only appeared when real browser traffic went through the pipeline; the seeded demo data hid them. The third showed up
when a second seed run was only partly read.

**Bug 1: the marker could jump into the future.** The ETL remembered progress by timestamp, and the seed script created some events
later in the day than the actual time. Once one of those became the marker, every real event before that time of day was skipped
forever (symptom: "0 rows staged" although Superset had new activity). *Fix:* the marker (now `max(source_dttm)` of the stored raw
rows) ignores rows dated later than the current time.

**Bug 2: real clicks were not being counted.** The seed script writes rows like `action = 'dashboard_load'`, but Superset 4.1.4's
browser writes a generic `action = 'log'` row and puts the real event name (`mount_dashboard`, `load_chart`, `force_refresh_chart`)
inside its JSON. The ETL only checked `action`, so genuine usage was never counted and the seeded data made the dashboard look
healthy. *Fix:* the ETL also reads `json.event_name`. After both fixes, one real dashboard load followed by one ETL run raised the
`dashboard_view` count by exactly 1.

**Bug 3: rows dated in the past were skipped for good.** Capping the watermark at the current time (bug 1) does nothing for a row
inserted *after* the watermark moves with an *older* `dttm`. *Fix:* the three guards in [section 4.1](#41-past-dated-rows-are-not-skipped-watermark)
(lookback, reconciliation, `--since` / `--full`). `tests/acceptance_watermark.py` checks a row dated 3 hours ago, one 10 days ago, one
tomorrow, an idempotent re-run, and that nothing is missing.

### 10.2 Why failure rate and load time live in Iceberg

The goal is to know how reliable and how fast each dashboard is, per day, week and month, without adding a component to the
architecture: the bank already runs Iceberg and Trino, so the error data and the usage facts are written there and read back through
Trino by Superset. Capture was generalised from one known cause to any cause ([section 3.3](#33-any-error-whatever-the-cause)) and
tested with six different failures (two 404s, a 422, a 400 with a query message, a missing dataset, a killed worker) through the
same code path, plus a parser test covering a SIGKILL with no timeout, an unknown arbiter message and a failed background task.

### 10.3 Known gaps

- `source_ip` is not stored: Superset's log model does not record the client IP.
- The user/dashboard/chart on gunicorn rows comes from the `REQTRACE` line, so it only exists for timeouts that happened after the
  trace was added to `superset_config.py`. Requests made with an API token (not a browser session) are traced with `user=-`.
- The browser error row exists only if the tab stays open long enough to send it, so the failure rate under-counts
  ([section 3.4](#34-metrics)).
- Container logs still reach the pipeline through a manual `docker compose logs` pipe; shipping them from a log platform is the
  follow-up.
- The single-writer lease is advisory (overlapping runs stay correct because every write is an idempotent `MERGE`), and compaction
  and snapshot expiry need an owner in the bank's platform ([section 4.5](#45-to-confirm-in-the-banks-environment)).
- Superset 4.1.4 is pinned; re-check the log event names if you upgrade.

---

## 11. Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `another run holds the single-writer lease` on every run | A previous run was interrupted and its lease has not expired (30 min). Wait, or clear it: `docker compose -f lakehouse/docker-compose.yml exec -T trino trino --execute "delete from iceberg.monitoring.etl_lock"` |
| Commits fail with `ICEBERG_COMMIT_ERROR ... 500`, even single statements | The lab's SQLite-backed Iceberg catalog got stuck after simultaneous commits. `docker compose -f lakehouse/docker-compose.yml restart iceberg-rest`, then clear the lease as above. A production catalog does not behave this way. |
| `Connection refused` to Trino from DBeaver | Check `docker compose -f lakehouse/docker-compose.yml ps` shows `trino` up; the host port is 8085 (`TRINO_PORT` in `lakehouse/.env`). |
| DBeaver can't connect to Postgres | Confirm the containers are up and healthy with `docker compose ps` (Postgres has a healthcheck); the host port is 15432. |
| Dashboards empty | Run the ETL first ([2.5](#25-put-data-in)), then the bootstrap ([2.6](#26-build-the-superset-dashboards)). |
| Charts created by the bootstrap show no data when read through the API (`Chart has no query context saved`) | Normal: API-created charts have no saved query context. They render in the UI. |
| `/repro/...` or `/fix/...` "No such file or directory" in Git Bash | MSYS path conversion: prefix the command with `MSYS_NO_PATHCONV=1`. |

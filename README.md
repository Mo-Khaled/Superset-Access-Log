# Superset Local Lab: Access Log Pipeline + Intermittent Error Repro/Fix

A hands-on local reproduction of two Superset tickets:

1. **Access log pipeline** (`access-log-pipeline/`) -- pull Superset's activity data
   out via `LogRestApi`, land it in **Iceberg** (queried through Trino) in a clean schema, and surface a
   "most visited dashboards" usage dashboard and a failure-rate / load-time dashboard as **native Superset
   dashboards** (auto-provisioned against those same tables). Iceberg is the only store: no MySQL.
2. **Intermittent silent error** (`intermittent-error/`) -- reproduce a gunicorn
   worker-timeout kill that shows a generic error to the user with nothing in
   Superset's application logs, then fix it. (`intermittent-error/DEMO.md` walks through showing the
   unlogged, cache-dependent version without applying the fix.)

Everything runs via Docker Compose. Superset version: **`apache/superset:4.1.4`**
(pinned; note that 5.x/6.x exist upstream -- this lab uses 4.1.x for documentation
maturity, see `WRITEUP.md` for what to re-check before using this as a template
against a newer version).

## Layout

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
  tests/acceptance_watermark.py  proves past/future-dated rows are neither skipped nor counted wrongly
  tests/test_error_classify.py   classifier checks;  tests/test_ingest_parse.py  container-log parser checks
  tests/test_iceberg_state.py    lease, retry, write-order and skip-known checks (no services needed)
  run_etl.sh                     one scheduled cycle (both ETL steps); what cron calls
  superset_client.py             shared Superset API auth helper
  superset_bootstrap/            provisions the native Superset dashboards (usage; reliability and speed)
intermittent-error/
  DEMO.md                        step-by-step demo of the intermittent, unlogged timeout
  repro/                         slow chart/dashboard, trigger, show_evidence.sh
  fix/                           applies the timeout-alignment fix
  logs_before/, logs_after/      captured evidence (created when you run the repro)
WRITEUP.md                       schema rationale, ETL scheduling, root cause + prod fix, cache-driven repro
```

## 1. Bring up the stack

```bash
cp .env.example .env   # first time only; set real passwords
docker compose up -d --build
```

This starts Postgres (Superset's metadata + examples DB), Redis, the Superset
webserver + Celery worker/beat, the `pipeline` tooling container, and the
`superset-bootstrap` one-shot container. The pipeline's data lives in Iceberg, so the
lab lakehouse (`lakehouse/`, MinIO + Iceberg REST + Trino) must be running too: see
"Run it in the lab" below for the exact commands, which use the compose override that
connects the two stacks.

`superset-init` is a one-shot service that runs `superset db upgrade`, creates the
admin user (`admin` / `admin`, see `.env`), runs `superset init` (roles/perms), and
`superset load_examples` (seeds real example dashboards/charts). The `superset`
webserver waits for it to complete successfully. Watch it with:

```bash
docker compose logs -f superset-init
```

Once `superset-init` is done and the webserver is accepting connections,
`superset-bootstrap` runs automatically: it registers the Trino connection to
the Iceberg tables as a Superset database, registers the relevant tables as Superset
datasets, and creates the native **Superset Usage Analytics** dashboard + its 7
charts, then the **Dashboard Reliability and Speed** dashboard (see
`access-log-pipeline/superset_bootstrap/`). On an install made when the pipeline still
used MySQL it also points the existing datasets at Trino and removes the old MySQL connection.
It's idempotent -- safe to re-run (`docker compose run --rm superset-bootstrap`)
any time. Watch it with:

```bash
docker compose logs -f superset-bootstrap
```

Once done, Superset is at **http://localhost:8089** (admin/admin; mapped from the
container's 8088 -- kept off 8088 on the host since that's already in use by
another local project). Inspect the pipeline tables with Trino:

```bash
docker compose -f lakehouse/docker-compose.yml exec trino trino --execute "select count(*) from iceberg.monitoring.fact_access_events"
```

Superset's `DBEventLogger` (writes every action to the metadata DB's `logs` table,
which `LogRestApi` reads from) is enabled explicitly in
`docker/superset/superset_config.py` -- it's Superset's default, but we set it
explicitly so it's not an implicit/undocumented dependency.

## 2. Task 1 -- generate activity, run the ETL, view the dashboard

**Simulate traffic** (creates ~12 fake Superset users and inserts a skewed 30-day
history of dashboard/chart-view log rows directly into Superset's own `logs`
table -- see the docstring in `seed/generate_activity.py` for why it writes there
directly rather than driving a headless browser):

```bash
docker compose run --rm pipeline python seed/generate_activity.py
```

**Run the ETL** (pulls incrementally from `/api/v1/log/`, lands raw payloads +
cleaned dim/fact tables in Iceberg):

```bash
docker compose run --rm pipeline python etl/sync_logs.py
```

Re-run it any time -- it's idempotent (the high-water-mark is `max(source_dttm)` of
`raw_log_landing`, and every write is a keyed `MERGE`, so re-runs don't duplicate rows).

**View the usage dashboard**: log into Superset at http://localhost:8089
(admin/admin) and open **Dashboards -> Superset Usage Analytics**. It's a native
Superset dashboard, auto-provisioned by `superset-bootstrap` against the same
Iceberg tables the ETL writes to (through Trino), with 7 charts:

- **Total Dashboard Views** / **Active Users** -- headline KPI numbers.
- **Most Viewed Dashboards** -- ranked table (view count + unique viewers per
  dashboard), same ranking logic the old standalone dashboard used.
- **Dashboard Views Over Time** -- daily trend line.
- **User Activity** -- per-user view/chart-view counts and last-active time.
- **Zero-View Dashboards** -- near-zero-views list (deprecation candidates).
- **Export Activity** -- `export_csv` / `export_excel` counts.

A second dashboard, **Dashboard Reliability and Speed**, shows failure rate (daily, weekly,
monthly), failed vs total chart loads, failure rate by dashboard, load time (average and p95), slow loads by
dashboard, and **Chart Load Errors** (moved there).

Since these are ordinary Superset charts, you get Superset's native dashboard
time-range filter, drill-downs, and export-to-CSV for free -- no separate service
to keep running. (The project previously shipped a standalone FastAPI + Chart.js
dashboard on port 8091 for this; it's been replaced by this native dashboard.)

**Scheduling in prod**: this ETL is designed to be invoked externally, not to loop
internally. In this sandbox, cron works fine:

```cron
*/10 * * * * /path/to/repo/access-log-pipeline/run_etl.sh >> /var/log/superset-log-etl.log 2>&1
```

`run_etl.sh` is one cycle: create/verify the tables, the Superset `logs` ETL, ingestion of
server-side errors from the containers' stdout into `fact_error_events` (see below), and
table maintenance (real work about once a day). Needs the `pipeline` container up. The log window it re-reads
(`ERROR_LOG_WINDOW`, default 15m) must be longer than the cron interval; overlap is
harmless because rows are de-duplicated.

In prod, prefer an orchestrator with retry/alerting built in -- an Airflow DAG
(`PythonOperator` or a `KubernetesPodOperator` running this same script, with a
failure callback wired to Slack/PagerDuty) or a Kubernetes `CronJob` with
`restartPolicy: OnFailure` and `concurrencyPolicy: Forbid`, plus an alert if no `etl_run_log` row with
`kind = 'sync'` and `status = 'ok'` appeared in the last N intervals:
`select max(run_ts) from iceberg.monitoring.etl_run_log where kind = 'sync' and status = 'ok'`.
See `WRITEUP.md` for detail.

### Data model: everything is in Iceberg (queried through Trino)

`fact_access_events` counts every `load_chart` as a `chart_view`, so a failed chart load looks like a normal view
there (deliberate: it is a usage table). Failures and their reasons are stored separately, next to it, so one engine
can answer "how often do dashboards fail, and how fast are they?".

```
Superset logs --LogRestApi--> ETL --+--> Iceberg (via Trino MERGE)  raw_log_landing, dims, usage + error facts,
gunicorn/server/celery lines -------+                              etl_lock, etl_run_log
                                                       |
                                       Trino view vw_chart_failure_rate --> Superset dashboards
```

| Iceberg object (`iceberg.monitoring`) | What it holds |
|---|---|
| `fact_access_events` | dashboard views, chart views, exports; `duration_ms` (browser-measured load time) and `is_error` |
| `fact_error_events` | one row per failure: `source` = `browser` or `gunicorn`, `reason`, user, dashboard, chart |
| `dim_dashboards`, `dim_users` | dashboard titles/owners, user names |
| `raw_log_landing` | verbatim copy of every Superset log row; the watermark is read from it, and facts can be rebuilt from it |
| `etl_lock` | the single-writer lease (one row) |
| `etl_run_log` | one row per run (sync, ingest, maintenance) with status and counts: what to alert on |
| `vw_chart_failure_rate` | per day and dashboard: `chart_loads`, `failed_loads`, `server_kills` (future-dated rows excluded) |

#### Any error, whatever the cause

In production the cause of a data error is not known in advance, so capture is not tied to one kind of failure.
Every failure becomes a row with the **raw message** (never dropped), a coarse **category**, a **signature** (the
message with numbers, ids and quoted values removed, so "the same error" groups together) and the HTTP status.

| `source` | What it catches |
|---|---|
| `browser` | any `load_chart` event the browser reports as failed (`has_err`), with its own message |
| `server` | any dashboard/chart request that ended with HTTP 400 or above: status, duration, the message in the response, plus the exception class for unhandled exceptions |
| `gunicorn` | worker timeouts, workers killed from outside (SIGKILL/OOM) and any other arbiter error, tied to the request by worker pid |
| `celery` | failed background tasks (`raised unexpected`) |

Superset logs these through `FLASK_APP_MUTATOR` in `docker/superset/superset_config.py` (`REQTRACE` at request start,
`REQEND` for error responses, `REQEXC` for unhandled exceptions). `ERROR_TRACE_PREFIXES` sets which paths count as
dashboard/chart requests (default: charts, dashboards, explore, datasets, SQL Lab). The superset-worker log is read too.

Categories: `timeout`, `worker_failure`, `connection`, `permission`, `query_error`, `not_found`, `rate_limit`,
`data_error`, `client_error`, `server_error`, and **`unclassified`** for anything the rules do not recognise. That last
one is the point: an unknown cause is stored and shows up, instead of being lost. Open **Top Error Messages** on the
Reliability dashboard, look at the biggest `unclassified` or fastest-growing signature, and add a rule to
`etl/error_classify.py` once you understand it. Existing rows are re-classified by `etl/backfill_iceberg.py`.

Counting: the **failure rate** uses browser-reported failures of any category. **Server errors** and **server kills**
are shown beside it, not added to it, because the browser may report the same failure and adding them would double
count. Python `ERROR` log lines are not ingested separately for the same reason (the request hooks already capture
them with more context); a failure that leaves no trace in any of these four places cannot be seen.

**Metrics** (Superset applies its own time grain, so one definition serves day, week and month):

- **Failure rate** = `SUM(failed_loads) / SUM(chart_loads)`: failed chart loads divided by chart loads (the same
  unit on both sides). A ratio of sums, not an average of daily ratios, which would overweight quiet days.
- **Load time** = average and p95 of the browser-measured load time of *successful* chart loads (a timeout always
  reads about 15 s and would hide real slowness). **Slow-load rate** = share of successful loads above
  `SLOW_LOAD_MS` (default 5000; agree the value with the business).

**Read the failure rate as a lower bound.** Failed loads come from the browser, which only reports while the tab
stays open. A user who closes the tab during a 15 s timeout leaves no browser row. `server_kills` (gunicorn's own count
of killed workers) is shown next to it: a `server_kills` value well above `failed_loads` means the browser is
under-reporting. Load times are measured in the browser, so they include network time.

#### Run it in the lab (only the containers that are needed)

```bash
cp lakehouse/.env.example lakehouse/.env                      # first time only
docker compose -f lakehouse/docker-compose.yml up -d          # minio, iceberg-rest, trino (+2 one-shot helpers)
docker compose -f docker-compose.yml -f lakehouse/connect-pipeline.yml up -d --build \
    postgres redis superset pipeline                           # no worker, beat or bootstrap needed

docker compose exec -T pipeline python etl/iceberg_init.py       # creates tables/view, checks format-version 2
bash access-log-pipeline/run_etl.sh                              # every cycle writes to Iceberg
docker compose -f docker-compose.yml -f lakehouse/connect-pipeline.yml run --rm superset-bootstrap   # the dashboard

# ask Trino
docker compose -f lakehouse/docker-compose.yml exec trino trino --execute "
  select date_trunc('week', day) week, sum(chart_loads) loads, sum(failed_loads) failed,
         round(sum(failed_loads) * 100.0 / nullif(sum(chart_loads), 0), 2) failure_pct
  from iceberg.monitoring.vw_chart_failure_rate group by 1 order by 1"
```

Against the bank's real Trino, do not use the `lakehouse/` files: set the `TRINO_*` values in `.env` (and
`TRINO_PASSWORD` for TLS/auth), then run the same `iceberg_init.py`.

Moving from the earlier MySQL-backed version: the MySQL service is gone. Run `iceberg_init.py` (creates the new tables)
and `sync_logs.py --full` once: it re-reads Superset's whole log, so the facts and the raw copy are rebuilt from the
source of truth. Server-side errors (gunicorn, server, celery) already in Iceberg are kept; any that existed only in
MySQL are not migrated (Superset's container logs are the only other source of those).

#### Past-dated rows are not skipped (watermark)

LogRestApi cannot filter by id, so the ETL pulls rows with `dttm > watermark`. A row inserted later with an *older*
`dttm` (late browser flush, clock skew, downtime, a backfill, the seed script) would never be seen. The ETL guards it:

1. **Lookback**: each pull starts `ETL_LOOKBACK_HOURS` (default 24) before the watermark.
2. **Reconciliation**: after the pull, the number of rows Superset dated before the start of the run is compared with
   the same window in `raw_log_landing` (column `source_dttm`). If Superset has more, the whole log
   is re-pulled once and a `WARNING` with the gap is printed (idempotent). Comparing a fixed window matters: the ETL's own
   API calls are logged by Superset, so a total count would hide a few missing rows.
3. **`--since`**: `python etl/sync_logs.py --since '2026-09-01 00:00:00'` (or `--full`) for a deliberate backfill.
4. The watermark is **derived from the data**: `max(source_dttm)` of `raw_log_landing`, ignoring future-dated rows, so
   it never moves backwards or into the future. Raw rows are written **last**, after the facts and dimensions they
   produced, so it can only advance once everything before it is stored. There is no separate state to fall out of
   step, and a failed run is simply retried.
5. The seed script no longer creates future-dated rows, and the view ignores any that exist.

Trap found while building this: Superset 4.1.4's log list API **silently ignores** the filter operators `lte`, `le`,
`gte` and `ge` and returns the unfiltered result. Only `lt` and `gt` filter. The ETL uses only those two.

`python tests/acceptance_watermark.py` (inside the pipeline container) proves all of this.

#### Operating notes

- **Single writer, without locks.** Iceberg has none, so the ETL, error ingest, backfill and maintenance share a lease
  row in `etl_lock` (`etl/iceberg_state.py`). It is taken with a `MERGE`, and Iceberg's atomic commit decides a race:
  the loser fails with a commit conflict, reads the owner back and skips its run. The lease expires on its own
  (`ETL_LOCK_TTL_MINUTES`, default 30) so a crashed run never blocks the pipeline, and a long run renews it. In
  production also forbid overlapping runs in the scheduler (`concurrencyPolicy: Forbid`, Airflow `max_active_runs=1`).
  If two runs ever do overlap, the data is still correct: every write is an idempotent `MERGE`, and a statement that
  loses a commit race is retried with backoff (`ICEBERG_COMMIT_RETRIES`).
- **No cross-table transaction.** Trino cannot commit two tables together. Order is the consistency model: facts and
  dimensions first, `raw_log_landing` last, and the watermark and the "already stored" check both read
  `raw_log_landing`. A crash leaves the watermark at the last fully written point and the next run repeats the rest.
- **Batching and small files.** Rows are merged in chunks (`ICEBERG_BATCH_ROWS`, default 2000; raw payloads are chunked
  by size too, to stay under Trino's query length limit), one statement per table per chunk, not one per API page, and
  rows already stored are not sent again, so a quiet cycle writes almost nothing. `etl/iceberg_maintenance.py` runs
  from `run_etl.sh` and does real work about once a day (`MAINTENANCE_INTERVAL_HOURS`): `optimize` (compaction),
  `expire_snapshots` and `remove_orphan_files` (`ICEBERG_RETENTION`, default 7d, must not be below the catalog minimum).
  If another team already compacts these tables, hand them those statements and skip the call.
- **MERGE needs format-version 2.** `iceberg_init.py` checks it and fails with a clear message otherwise.
- **Scheduler alert.** `etl_run_log` gets a row per run with status `ok` or `failed`; alert when the latest `ok` row
  for `kind = 'sync'` is older than a few intervals. `run_etl.sh` exits non-zero when any step fails.

#### Still to confirm in the bank's environment

Trino version (the lab runs 483); catalog type and storage; format-version 2 by default; permissions for the pipeline's
Trino user (create schema/table, `MERGE`, `UPDATE`/`DELETE` on the small state tables, `ALTER TABLE ... EXECUTE`);
the catalog's minimum snapshot retention; authentication and TLS to Trino; compaction owner and schedule;
whether the bank's Superset can use the same catalog and the `trino` SQLAlchemy driver; retention of Superset's
`logs` table (limits how far a rescan can go); the slow-load threshold.

**Follow-up, deliberately out of scope:** shipping container logs from a log platform instead of the
`docker compose logs` pipe.

### What `LogRestApi` does and doesn't capture

- Captures: `action`, `dttm` (timestamp), `user_id`, `dashboard_id`, `slice_id`
  (chart id), `duration_ms`, `referrer`, and a free-form `json` payload -- see
  `superset/models/core.py`'s `Log` model (mapped 1:1 to what the API exposes).
- **Does not capture client/source IP at all** -- there's no IP column on the `Log`
  model, so there is no `source_ip` column in our schema. This is
  a real gap in Superset's own instrumentation, not a limitation of our pipeline.
- Dashboard-level "opens" and per-chart-tile fetches are logged as **separate**
  events (`dashboard_load` vs. `chart_data`) -- a 10-chart dashboard produces up to
  11 log rows per view. Our `fact_access_events` keeps them as separate `action`
  values (`dashboard_view` / `chart_view`) for exactly this reason (see comments in
  `schema_iceberg/001_fact_access_events.sql`), and the "most visited dashboards" ranking
  uses `dashboard_view` rows only so multi-chart dashboards aren't over-counted.
- Exports are distinguishable (`export_csv` / `export_excel` actions exist
  separately from `chart_data`), so "views" vs. "downloads" can be split out.
- Known gaps affecting ranking accuracy: cached chart renders may not always
  re-log a fresh `chart_data` event; filter-only interactions on an already-loaded
  dashboard aren't separately logged as new "views"; embedded/iframe dashboard
  views go through the same log path but can't be distinguished from normal
  browser views without extra referrer/context parsing.

## 3. Task 2 -- reproduce and fix the intermittent silent error

**Create the repro fixture** (a virtual dataset whose query sleeps
`SLOW_QUERY_SLEEP_SECONDS` (default 45s), a chart on it, and a dashboard):

```bash
docker compose run --rm pipeline python /repro/create_slow_dataset.py
```

Idempotent -- re-running it deletes any leftover `slow_query_demo` dataset from a
previous run before creating a fresh one. Note that Superset introspects the
virtual dataset's columns by actually running the query at creation time, so this
call can take several multiples of `SLEEP_SECONDS`, not just `SLEEP_SECONDS` --
the client timeout accounts for that.

> **Git Bash / MSYS on Windows**: leading-slash paths like `/repro/...` and
> `/fix/...` get rewritten to a Windows path by MSYS's automatic path conversion
> and the container command will fail with "No such file or directory". Prefix the
> command with `MSYS_NO_PATHCONV=1` if you hit this, e.g.
> `MSYS_NO_PATHCONV=1 docker compose run --rm pipeline python /repro/create_slow_dataset.py`.

**Trigger the "before" repro** (run on the HOST, not in a container, so it can
shell out to `docker compose logs`):

```bash
python intermittent-error/repro/trigger_timeout.py before
```

The slow chart's query result is cacheable, and its SQL text never changes between
runs, so Superset/Redis will happily serve a cached (fast, non-representative)
response on any repeat trigger within the chart's cache window. If you re-run
`before`, or `after`, or re-run the same phase twice, flush the cache first so the
query actually executes:
```bash
docker compose exec redis redis-cli FLUSHALL
```

`.env` starts with `SUPERSET_WEBSERVER_TIMEOUT=15`, shorter than the 45s slow
query -- gunicorn's arbiter SIGKILLs the worker mid-request. Inspect
`intermittent-error/logs_before/superset_container_logs.txt`: you'll see (at most)
gunicorn's own terse `[CRITICAL] WORKER TIMEOUT` arbiter line -- **no Superset
application-level exception, no new failed-request row in the `logs` table** --
because the worker process is killed before any Superset/Flask exception handler
ever runs.

**Apply the fix**:

1. Raise the webserver timeout so it's no longer the first thing to fire -- edit
   `.env`: `SUPERSET_WEBSERVER_TIMEOUT=30`, then recreate the webserver:
   ```bash
   docker compose up -d --no-deps superset
   ```
2. Apply a Postgres `statement_timeout` (20s, shorter than the new 30s webserver
   timeout, still shorter than the 45s slow query) to the `examples` database
   connection, so the *database* cancels the query and raises a real, catchable
   exception before gunicorn would ever kill the worker:
   ```bash
   docker compose run --rm pipeline python /fix/apply_fix.py
   ```

**Prove it**:

```bash
python intermittent-error/repro/trigger_timeout.py after
```

Inspect `intermittent-error/logs_after/superset_container_logs.txt`: this time the
query is cancelled by Postgres at ~20s, Superset's chart-data endpoint catches the
`QueryCanceled` error and logs it properly, and the client gets a real (still
user-facing, but now *logged*) error instead of a silent hang-then-kill.

A gunicorn `worker_abort` hook (`docker/superset/gunicorn_config.py`) is intended as
a second, independent safety net in the "before" scenario -- meant to fire right
before the arbiter kills a worker and log the in-flight request path + duration to
the container's log stream, regardless of what Superset's own app-level logging
does. **In practice, in this repro, it doesn't fire**: the arbiter sends `SIGABRT`
first (which is what should trigger the hook) and only escalates to `SIGKILL` on
its *next* sweep roughly a second later, but the worker is blocked the whole time
inside a synchronous `psycopg2` call in native code -- and a Python signal handler
can't run until the interpreter regains control, which never happens before
`SIGKILL` lands. The captured logs confirm this: `[CRITICAL] WORKER TIMEOUT`
followed directly by `[ERROR] ... sent SIGKILL`, no `WORKER_ABORT` line in between.
The hook would still catch a slow-but-Python-level stall (e.g. a hung pure-Python
computation that periodically returns to the interpreter loop); it's not the
reliable catch-all for a blocked C-level DB call the original design intended. In
the "after" state it's silent for a simpler reason -- Postgres cancels the query
well before gunicorn's timeout is even reached, so the worker is never a `SIGKILL`
candidate in the first place.

See `WRITEUP.md` for the full root-cause writeup, and an explicit callout on what
differs in a real prod deployment (this sandbox has no reverse proxy/load balancer
in front of Superset).

# Superset Local Lab: Access Log Pipeline + Intermittent Error Repro/Fix

A hands-on local reproduction of two Superset tickets:

1. **Access log pipeline** (`access-log-pipeline/`) -- pull Superset's activity data
   out via `LogRestApi`, land it in MySQL in a clean schema, and surface a
   "most visited dashboards" usage dashboard as a **native Superset dashboard**
   (auto-provisioned against that same MySQL data).
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
access-log-pipeline/
  schema/                        MySQL DDL (auto-applied on first mysql container boot)
  seed/generate_activity.py      simulates skewed multi-user dashboard traffic
  etl/sync_logs.py               incremental Superset logs -> MySQL ETL (+ browser errors)
  etl/ingest_container_errors.py gunicorn WORKER TIMEOUT lines -> fact_error_events
  etl/backfill_browser_errors.py one-off: derive error rows from already-staged raw logs
  run_etl.sh                     one scheduled cycle (both ETL steps); what cron calls
  superset_client.py             shared Superset API auth helper
  superset_bootstrap/            provisions the native Superset usage dashboard (DB conn, datasets, charts)
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

This starts Postgres (Superset's metadata + examples DB), Redis, MySQL (the
pipeline's destination DB, entirely separate from Superset's own DB), the Superset
webserver + Celery worker/beat, the `pipeline` tooling container, and the
`superset-bootstrap` one-shot container.

`superset-init` is a one-shot service that runs `superset db upgrade`, creates the
admin user (`admin` / `admin`, see `.env`), runs `superset init` (roles/perms), and
`superset load_examples` (seeds real example dashboards/charts). The `superset`
webserver waits for it to complete successfully. Watch it with:

```bash
docker compose logs -f superset-init
```

Once `superset-init` is done and the webserver is accepting connections,
`superset-bootstrap` runs automatically: it registers the pipeline's MySQL
database as a Superset connection, registers the relevant tables as Superset
datasets, and creates the native **Superset Usage Analytics** dashboard + its 7
charts (see `access-log-pipeline/superset_bootstrap/bootstrap_dashboard.py`).
It's idempotent -- safe to re-run (`docker compose run --rm superset-bootstrap`)
any time. Watch it with:

```bash
docker compose logs -f superset-bootstrap
```

Once done, Superset is at **http://localhost:8089** (admin/admin; mapped from the
container's 8088 -- kept off 8088 on the host since that's already in use by
another local project). MySQL isn't
published to the host (to avoid clashing with any MySQL already running there) --
inspect the pipeline tables with:

```bash
docker compose exec mysql mysql -u pipeline -ppipeline_pw access_log_pipeline
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
cleaned dim/fact tables in MySQL):

```bash
docker compose run --rm pipeline python etl/sync_logs.py
```

Re-run it any time -- it's idempotent (tracks a high-water-mark in
`etl_sync_state`, and every insert is keyed so re-runs don't duplicate rows).

**View the usage dashboard**: log into Superset at http://localhost:8089
(admin/admin) and open **Dashboards -> Superset Usage Analytics**. It's a native
Superset dashboard, auto-provisioned by `superset-bootstrap` against the same
MySQL tables the ETL writes to, with 7 charts:

- **Total Dashboard Views** / **Active Users** -- headline KPI numbers.
- **Most Viewed Dashboards** -- ranked table (view count + unique viewers per
  dashboard), same ranking logic the old standalone dashboard used.
- **Dashboard Views Over Time** -- daily trend line.
- **User Activity** -- per-user view/chart-view counts and last-active time.
- **Zero-View Dashboards** -- near-zero-views list (deprecation candidates).
- **Export Activity** -- `export_csv` / `export_excel` counts.

Since these are ordinary Superset charts, you get Superset's native dashboard
time-range filter, drill-downs, and export-to-CSV for free -- no separate service
to keep running. (The project previously shipped a standalone FastAPI + Chart.js
dashboard on port 8091 for this; it's been replaced by this native dashboard.)

**Scheduling in prod**: this ETL is designed to be invoked externally, not to loop
internally. In this sandbox, cron works fine:

```cron
*/10 * * * * /path/to/repo/access-log-pipeline/run_etl.sh >> /var/log/superset-log-etl.log 2>&1
```

`run_etl.sh` is one cycle: the Superset `logs` ETL, then ingestion of gunicorn
`WORKER TIMEOUT` lines from the container's stdout into `fact_error_events` (see
below). Needs the `pipeline` container up. The log window it re-reads
(`ERROR_LOG_WINDOW`, default 15m) must be longer than the cron interval; overlap is
harmless because rows are de-duplicated.

In prod, prefer an orchestrator with retry/alerting built in -- an Airflow DAG
(`PythonOperator` or a `KubernetesPodOperator` running this same script, with a
failure callback wired to Slack/PagerDuty) or a Kubernetes `CronJob` with
`restartPolicy: OnFailure` plus a liveness/log-based alert if `etl_sync_state.
updated_at` goes stale beyond N missed intervals. See `WRITEUP.md` for detail.

### Errors table (`fact_error_events`)

`fact_access_events` counts every `load_chart` as a `chart_view`, so a chart that
failed to load looks like a normal view there (deliberate: it is a usage table).
Failures and their reasons go to `fact_error_events` instead, from two sources:

- `browser`: `load_chart` rows in Superset's `logs` table with `has_err = true`
  (user, dashboard, chart, error such as `timeout`, duration).
- `gunicorn`: `WORKER TIMEOUT` lines from the superset container's stdout, the only
  server-side trace of a request killed by the gunicorn timeout. Each worker also
  logs a `REQTRACE` line (pid, user, URL) per request (`FLASK_APP_MUTATOR` in
  `docker/superset/superset_config.py`); the ingest joins the two on the worker pid
  to add user, dashboard and chart. After changing that config, rebuild:
  `docker compose up -d --build superset superset-worker superset-worker-beat`.

```bash
# schema is auto-applied on a fresh mysql volume; on an existing one:
docker compose exec -T mysql sh -c 'mysql -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DATABASE"'   < access-log-pipeline/schema/006_fact_error_events.sql
# one-off, only if raw_log_landing already holds failures from before this table:
docker compose exec -T pipeline python etl/backfill_browser_errors.py
```

### What `LogRestApi` does and doesn't capture

- Captures: `action`, `dttm` (timestamp), `user_id`, `dashboard_id`, `slice_id`
  (chart id), `duration_ms`, `referrer`, and a free-form `json` payload -- see
  `superset/models/core.py`'s `Log` model (mapped 1:1 to what the API exposes).
- **Does not capture client/source IP at all** -- there's no IP column on the `Log`
  model, so `fact_access_events.source_ip` in our schema is always `NULL`. This is
  a real gap in Superset's own instrumentation, not a limitation of our pipeline.
- Dashboard-level "opens" and per-chart-tile fetches are logged as **separate**
  events (`dashboard_load` vs. `chart_data`) -- a 10-chart dashboard produces up to
  11 log rows per view. Our `fact_access_events` keeps them as separate `action`
  values (`dashboard_view` / `chart_view`) for exactly this reason (see comments in
  `schema/004_fact_access_events.sql`), and the "most visited dashboards" ranking
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

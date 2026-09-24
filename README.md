# Superset Local Lab: Access Log Pipeline + Intermittent Error Repro/Fix

A hands-on local reproduction of two Superset tickets:

1. **Access log pipeline** (`access-log-pipeline/`) -- pull Superset's activity data
   out via `LogRestApi`, land it in MySQL in a clean schema, and serve a standalone
   "most visited dashboards" usage dashboard from it.
2. **Intermittent silent error** (`intermittent-error/`) -- reproduce a gunicorn
   worker-timeout kill that shows a generic error to the user with nothing in
   Superset's application logs, then fix it.

Everything runs via Docker Compose. Superset version: **`apache/superset:4.1.4`**
(pinned; note that 5.x/6.x exist upstream -- this lab uses 4.1.x for documentation
maturity, see `WRITEUP.md` for what to re-check before using this as a template
against a newer version).

## Layout

```
docker-compose.yml, .env         top-level orchestration
docker/superset/                 custom Superset image (config, gunicorn config)
docker/postgres/                 creates the separate "examples" DB on the same PG instance
access-log-pipeline/
  schema/                        MySQL DDL (auto-applied on first mysql container boot)
  seed/generate_activity.py      simulates skewed multi-user dashboard traffic
  etl/sync_logs.py               incremental Superset logs -> MySQL ETL
  superset_client.py             shared Superset API auth helper
  dashboard/                     standalone FastAPI + Chart.js usage dashboard
intermittent-error/
  repro/                         creates the slow chart/dashboard + triggers the timeout
  fix/                           applies the timeout-alignment fix
  logs_before/, logs_after/      captured evidence (created when you run the repro)
WRITEUP.md                       schema rationale, ETL scheduling, root cause + prod fix
```

## 1. Bring up the stack

```bash
docker compose up -d --build
```

This starts Postgres (Superset's metadata + examples DB), Redis, MySQL (the
pipeline's destination DB, entirely separate from Superset's own DB), the Superset
webserver + Celery worker/beat, the `pipeline` tooling container, and the
standalone `usage-dashboard` container.

`superset-init` is a one-shot service that runs `superset db upgrade`, creates the
admin user (`admin` / `admin`, see `.env`), runs `superset init` (roles/perms), and
`superset load_examples` (seeds real example dashboards/charts). The `superset`
webserver waits for it to complete successfully. Watch it with:

```bash
docker compose logs -f superset-init
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

**View the standalone usage dashboard**: http://localhost:8091 -- reads only from
MySQL, so it stays up even if Superset itself is down. Shows: top-N most-visited
dashboards (selectable 7d/30d/90d/all window), a trend chart for the current top
dashboards, unique viewers vs. raw view counts per dashboard, and a
zero/near-zero-views list (deprecation candidates).

**Scheduling in prod**: this ETL is designed to be invoked externally, not to loop
internally. In this sandbox, cron works fine:

```cron
*/10 * * * * docker compose run --rm pipeline python etl/sync_logs.py >> /var/log/superset-log-etl.log 2>&1
```

In prod, prefer an orchestrator with retry/alerting built in -- an Airflow DAG
(`PythonOperator` or a `KubernetesPodOperator` running this same script, with a
failure callback wired to Slack/PagerDuty) or a Kubernetes `CronJob` with
`restartPolicy: OnFailure` plus a liveness/log-based alert if `etl_sync_state.
updated_at` goes stale beyond N missed intervals. See `WRITEUP.md` for detail.

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

**Trigger the "before" repro** (run on the HOST, not in a container, so it can
shell out to `docker compose logs`):

```bash
python intermittent-error/repro/trigger_timeout.py before
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

A gunicorn `worker_abort` hook (`docker/superset/gunicorn_config.py`) runs in both
scenarios as a second, independent safety net -- it fires right before the arbiter
kills a worker and logs the in-flight request path + duration to the container's
log stream, regardless of what Superset's own app-level logging does. In the
"after" state you should see it stay silent (proof the DB-side fix is catching the
timeout first); it only fires in the "before" state or for a slow path that isn't
DB-bound at all.

See `WRITEUP.md` for the full root-cause writeup, and an explicit callout on what
differs in a real prod deployment (this sandbox has no reverse proxy/load balancer
in front of Superset).

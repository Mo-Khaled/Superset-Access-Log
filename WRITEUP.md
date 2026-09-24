# Write-up

## 1. MySQL schema design rationale

**Why a raw landing table (`raw_log_landing`) in addition to fact/dim tables?**
`LogRestApi` is our only window into Superset's activity history, and Superset's
own retention on the `logs` table is whatever ops configures (often short, to keep
the metadata DB small). If we only ever wrote directly to `fact_access_events` and
later discovered a bug in the transform (e.g. a misclassified action, or a new
action type we want to start tracking), we'd have no way to re-derive the fact
table without re-pulling data that may have already aged out on the Superset side.
Staging the raw JSON payload first (keyed by `source_log_id`, Superset's own log
row id) means the transform can be re-run against history we already have,
independent of Superset's retention window.

**Why separate dashboard-view and chart-view events instead of one "visit" row?**
Superset logs these as genuinely separate events (`dashboard_load` vs.
`chart_data`), and they answer different questions:
- A **dashboard view** (`fact_access_events.action = 'dashboard_view'`) means a
  user opened the dashboard page. This is what "most visited dashboards" should
  rank on -- it doesn't scale with how many charts happen to be on the page.
- A **chart view** (`action = 'chart_view'`) means one specific chart tile's data
  was fetched. Useful for a different question ("which charts within a busy
  dashboard are actually being looked at") but wrong to use for the top-level
  ranking, since a 10-chart dashboard would look 10x more "popular" than a
  1-chart one purely from tile count, not distinct visits.

We do **not** collapse repeat views from the same user into one row. Real usage
(a user refreshing, or checking back several times a day) is real activity; the
usage dashboard instead reports raw view count *and* `COUNT(DISTINCT user_id)`
("unique viewers") side by side specifically so "1 person refreshing constantly"
and "20 people each checking once" are visibly distinguishable, per the ticket's
requirement.

**Why dim tables instead of embedding names in the fact table?** Keeps the fact
table narrow (just IDs + the event), avoids repeating a dashboard's title on every
one of its thousands of view rows, and lets a title/owner change propagate without
rewriting history -- standard star-schema reasoning, appropriate here because the
usage dashboard's whole job is human-readable ranking, not raw ID lists.

**High-water-mark table (`etl_sync_state`)**: a single dedicated table rather than
a local file, so the ETL is safe to run from any container/host/scheduler
instance without needing shared local disk -- state lives in the same MySQL
instance the data lands in, and advances only after a batch is committed.

## 2. ETL scheduling and monitoring in production

The script (`etl/sync_logs.py`) is intentionally a single bounded run, not a
long-lived loop -- this makes it trivial to schedule with any external
orchestrator and to reason about failure (a failed run just doesn't advance the
high-water-mark; the next scheduled run picks up from the same point).

- **This sandbox**: cron, e.g. every 10 minutes (see README).
- **Production, simple case**: a Kubernetes `CronJob` running the same container
  image with `restartPolicy: OnFailure`, `concurrencyPolicy: Forbid` (so overlapping
  runs can't race on the high-water-mark), and a `startingDeadlineSeconds` so a
  missed run doesn't silently vanish. Alert on: job failure (via whatever the
  cluster's CronJob-failure alerting is, e.g. a Prometheus `kube_job_status_failed`
  rule), and staleness (`SELECT TIMESTAMPDIFF(MINUTE, updated_at, NOW()) FROM
  etl_sync_state` exceeding N intervals means the scheduler itself stopped firing,
  not just that one run failed).
- **Production, more orchestration**: an Airflow DAG with a single task running
  this script (or a thin wrapper), so failures get Airflow's retry/backoff,
  SLA-miss alerting, and a visible run history/log per execution out of the box --
  worth it once there are other pipelines this needs to coordinate with (e.g. "run
  after the nightly Superset metadata backup").
- Either way: emit a simple run summary (rows staged, rows inserted, final
  high-water-mark) to whatever the org's log aggregation is -- the script already
  prints this to stdout, so a CronJob/Airflow task just needs its logs shipped
  normally.

## 3. Task 2 root cause and recommended production fix

**Root cause**: gunicorn's own worker `timeout` setting is what actually kills a
request that runs too long -- and it does so by SIGKILLing the whole worker
*process* from the arbiter, not by raising an exception inside it. That happens
completely outside Superset's / Flask's control flow: no exception handler in the
chart-data view runs, so `logging.exception()` never fires and Superset's
`EVENT_LOGGER` never gets a chance to write a `logs` row for the failed request.
The browser sees the connection drop or a generic 5xx from nowhere; app-level logs
(and the `logs` table, which is what our own pipeline reads from!) show nothing
useful -- at best a terse arbiter line in a different log stream that nobody was
watching. This is exactly the "intermittent, unreproducible, invisible to logs"
signature described in the ticket, and it reproduces reliably any time a query
takes longer than `SUPERSET_WEBSERVER_TIMEOUT` / the gunicorn `timeout`.

**Recommended fix, in priority order**:

1. **Align timeouts so the database fails first, loudly.** Set a
   `statement_timeout` (Postgres) / equivalent on each registered database
   connection, tuned shorter than the webserver timeout. This turns "worker gets
   killed with no trace" into "query gets cancelled by the DB, Superset's view
   function catches a real `OperationalError`/`QueryCanceled`, logs it, and
   returns a real (if still unfriendly) error to the user." This is fix #1,
   implemented in `intermittent-error/fix/apply_fix.py`.
2. **Add a server-side watchdog independent of app logging**, for the cases the
   DB-side fix can't cover (e.g. a genuinely slow non-DB computation, a hung
   network call to some other service). Gunicorn's `worker_abort` hook
   (`docker/superset/gunicorn_config.py`) fires on the worker process right before
   it's killed and logs the request path + how long it had been running -- this
   works precisely because it's the one hook gunicorn still calls even when app
   code has lost control.
3. **(Not implemented here, noted as further work)** a frontend-side reporting
   hook: Superset's chart components already know when a fetch fails or never
   resolves; wiring that to POST a small "client observed a failed/timed-out chart
   load" event to a logging endpoint would catch failures that never even reach
   the backend in a loggable way at all (e.g. the connection dropping before any
   response, a CDN/proxy timeout in front of Superset). Lower priority than #1/#2
   here because #1 addresses the actual reported root cause; a frontend hook is a
   good defense-in-depth addition, not the fix for *this* bug.

**What's different in this sandbox vs. real prod, and what to check there**: this
lab has no reverse proxy or load balancer in front of Superset -- the client talks
to gunicorn directly. A real deployment almost always has one (nginx, an ALB/ELB,
an ingress controller), and that layer has **its own** timeout, which must also be
longer than the webserver timeout for this fix to actually surface an error
instead of just moving the silent-kill point one layer out (e.g. nginx's
`proxy_read_timeout`, an ALB's idle timeout, or an ingress controller's
`proxy-read-timeout` annotation). Before applying this fix's timeout values in a
real environment: enumerate every hop between the browser and gunicorn, and order
their timeouts strictly increasing from database -> gunicorn -> any internal
proxy -> the outermost load balancer/CDN, so whichever layer fails first is always
the *database*, which is the only layer in this chain that reliably produces a
catchable, loggable exception rather than a raw connection kill.

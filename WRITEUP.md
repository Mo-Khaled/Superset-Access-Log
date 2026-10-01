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
native Superset "Superset Usage Analytics" dashboard (see
`access-log-pipeline/superset_bootstrap/`) instead reports raw view count *and*
`COUNT(DISTINCT user_id)` ("unique viewers") side by side specifically so "1
person refreshing constantly" and "20 people each checking once" are visibly
distinguishable, per the ticket's requirement.

**Why dim tables instead of embedding names in the fact table?** Keeps the fact
table narrow (just IDs + the event), avoids repeating a dashboard's title on every
one of its thousands of view rows, and lets a title/owner change propagate without
rewriting history -- standard star-schema reasoning, appropriate here because the
usage dashboard's whole job (now a native Superset dashboard rather than a
standalone app, but same underlying MySQL tables) is human-readable ranking, not
raw ID lists.

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
takes longer than `SUPERSET_WEBSERVER_TIMEOUT` / the gunicorn `timeout` -- **with
one important precondition**: gunicorn's `timeout` only actually protects against
this if `worker_class = "sync"` (the default). With `worker_class = "gthread"`
(tried first in this lab), the worker's main event loop calls the arbiter's
`notify()` heartbeat on every iteration of its own accept/select loop, completely
independent of whether a pooled request-handling thread is stuck in a slow query --
so the arbiter never sees the worker go stale, `timeout` silently stops doing
anything, and the "before" repro just returns a slow-but-successful response
instead of reproducing the bug. `docker/superset/gunicorn_config.py` uses `sync`
for exactly this reason.

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
   (`docker/superset/gunicorn_config.py`) is *intended* to fire on the worker
   process right before it's killed and log the request path + how long it had
   been running, on the theory that it's the one hook gunicorn still calls even
   when app code has lost control. **Caveat found while validating this repro**:
   that theory only holds if the worker is blocked somewhere that still yields to
   the interpreter's signal handling. When it's blocked inside a synchronous
   C-level call (e.g. `psycopg2` executing a query, as in this repro's own DB
   timeout), the arbiter's `SIGABRT` (which should trigger the hook) can't be
   handled until the call returns -- and by then `SIGKILL` has already landed on
   the arbiter's next sweep, ~1s later. Confirmed empirically: the captured
   "before" logs show `[CRITICAL] WORKER TIMEOUT` directly followed by
   `[ERROR] ... sent SIGKILL`, with no `WORKER_ABORT` line from the hook in
   between. So this watchdog is real defense-in-depth for a hung *Python-level*
   computation, but not for a hung synchronous DB driver call -- which is
   precisely the failure mode fix #1 already targets. A more complete version of
   this watchdog would run the blocking call in a separate thread so the main
   worker thread stays responsive to signals -- not implemented here.
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

## 4. Two ETL correctness bugs found (and fixed) while verifying the usage dashboard end-to-end

Standing up the pipeline and checking the dashboard *looked* fine in isolation,
but driving real traffic through it end-to-end (seed data -> ETL -> dashboard,
then real browser clicks -> ETL -> dashboard) surfaced two real bugs in
`etl/sync_logs.py`'s incremental sync, both now fixed:

**Bug 1 -- the `dttm`-based watermark could advance into the future.**
`LogRestApi` doesn't support filtering/ordering by `id`, so the incremental sync
has to use `dttm > last_synced_ts` as its high-water-mark (see the comment in
`fetch_log_pages`). `seed/generate_activity.py`'s `random_timestamp()` picks a
random hour (7am-8pm) without clamping to the actual current time, so a seeded row
can land *later in the day* than when the seed script actually ran. Once such a
row advances the watermark, it sits ahead of real wall-clock time -- and every
*real* event logged before that time of day (including genuine admin/user
dashboard views) falls below the watermark and is silently, permanently excluded
by the `>` filter. Symptom: re-running the ETL after real usage reports `0 raw
rows staged, 0 fact rows inserted` indefinitely, even though new activity clearly
exists in Superset's own `logs` table. **Fix**: cap the persisted watermark at
`min(max_dttm_seen, now())` (`_parse_dttm` + the clamp at the end of `run()`) --
a future-dated row still gets ingested in the run that sees it, it just can't push
the *watermark* ahead of real time, so later real events stay visible to the
`>` filter.

**Bug 2 -- `ACTION_MAP` didn't recognize Superset's real frontend event names.**
The seed script writes rows shaped like Superset's logging docs describe
(`action = 'dashboard_load'` / `'chart_data'`), and `ACTION_MAP` was built to
match that. Superset 4.1.4's actual frontend logging pipeline doesn't write those
literal strings for organic browser usage, though -- it POSTs a generic
`action = 'log'` row and nests the real event under `json.event_name`
(`mount_dashboard`, `load_chart`, `force_refresh_chart`, confirmed by inspecting
real rows in Postgres after clicking around the dashboard). Since `ACTION_MAP`
only matched the top-level `action` column, **genuine usage was never being
counted into `fact_access_events` at all** -- independent of bug 1, and much
easier to miss, since the seeded demo data made the dashboard look populated and
"working." **Fix**: `_classify_action()` now also unpacks `json.event_name` for
generic `action = 'log'` rows via `LOG_EVENT_NAME_MAP`, so both the seeded demo
traffic and real organic usage land in the fact table with the same semantics.

Verified fixed end-to-end: after both fixes, a single real dashboard page load in
the browser, followed by one incremental `etl/sync_logs.py` run, moved
`fact_access_events`'s `dashboard_view` count by exactly +1 and was visible on the
"Superset Usage Analytics" dashboard after a cache flush/refresh.

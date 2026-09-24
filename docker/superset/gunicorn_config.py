"""
Gunicorn config for the Superset webserver container.

TASK 2 context: gunicorn's own `timeout` setting controls how long a worker can run
before the *arbiter* process sends it SIGKILL. When a chart query is slower than this,
the worker is killed mid-request -- before Flask's exception handlers, Superset's
EVENT_LOGGER, or any `logging.exception()` call in the view function ever executes.
The browser just sees the connection drop / a generic 500 from nowhere, and nothing
appears in Superset's application-level logs or the `logs` DB table. Only gunicorn's
own arbiter log (a separate stream) gets a terse "[CRITICAL] WORKER TIMEOUT" line --
and in the "before" scenario we don't even capture/highlight that stream.

Fix layer 2 (defense in depth even after the DB-side statement_timeout fix in
superset_config.py / intermittent-error/fix): a `worker_abort` hook that fires when
the arbiter is about to kill a worker, so we get a structured, greppable log line with
the request path and how long it had been running -- independent of whatever Superset's
own app-level logging does or doesn't catch.
"""

import logging
import os
import time

bind = "0.0.0.0:8088"
workers = int(os.environ.get("GUNICORN_WORKERS", "2"))
# "sync" (gunicorn's default) -- one request blocks the whole worker process, so the
# arbiter's timeout check genuinely applies to a stuck request. "gthread"'s main event
# loop keeps calling self.notify() every iteration independent of whether a pool thread
# is blocked in a slow query, so the arbiter's liveness check never sees a stall and
# `timeout` silently stops doing anything -- which defeats the whole point of this repro.
worker_class = "sync"

# Kept in sync with SUPERSET_WEBSERVER_TIMEOUT via env so the two never drift apart.
timeout = int(os.environ.get("SUPERSET_WEBSERVER_TIMEOUT", "15"))
graceful_timeout = timeout

accesslog = "-"
errorlog = "-"
loglevel = "info"

_worker_abort_logger = logging.getLogger("superset.worker_abort_watchdog")
_request_start_times: dict[int, float] = {}


def pre_request(worker, req):
    _request_start_times[worker.pid] = time.time()
    worker._watchdog_path = getattr(req, "path", "?")


def post_request(worker, req, environ, resp):
    _request_start_times.pop(worker.pid, None)


def worker_abort(worker):
    """Fires on the worker process right before the arbiter SIGKILLs it for
    exceeding `timeout`. This is the one hook that still runs in that scenario,
    so it's the only reliable place to log a slow/aborted request from the
    server side when the app code itself never gets control back."""
    started = _request_start_times.get(worker.pid)
    duration = f"{time.time() - started:.1f}s" if started else "unknown"
    path = getattr(worker, "_watchdog_path", "unknown")
    _worker_abort_logger.critical(
        "WORKER_ABORT pid=%s path=%s duration=%s timeout=%ss "
        "-- worker exceeded timeout and is about to be killed; "
        "the in-flight request will show as a generic error client-side "
        "with no application-level exception log",
        worker.pid,
        path,
        duration,
        timeout,
    )

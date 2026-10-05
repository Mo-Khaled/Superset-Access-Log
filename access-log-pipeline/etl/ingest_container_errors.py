"""
Ingests gunicorn "WORKER TIMEOUT" lines from the superset container's stdout into
fact_error_events (source = 'gunicorn'). These lines are the only server-side
trace of a request killed by the gunicorn timeout: Superset never gets to log it.

The timeout line itself only has a worker pid and a time. To say who/what it was,
each worker also logs a `REQTRACE ... pid=N user=U url=...` line when a request
starts (see FLASK_APP_MUTATOR in docker/superset/superset_config.py). The last trace
line of the timed-out pid, within TRACE_MAX_AGE_S before the timeout, is the request
that was killed; its user, dashboard_id and slice_id (chart) are attached.
Timeouts with no matching trace (older logs) are stored without that context.

The pipeline container has no docker access, so pipe the logs in from the host:
    docker compose logs --no-log-prefix --since 1h superset \\
        | docker compose exec -T pipeline python etl/ingest_container_errors.py

Idempotent: dedupe_key is derived from the line's own timestamp + pid; re-running
fills in context on an existing row but never duplicates it.
"""
import os
import re
import sys
from datetime import datetime
from urllib.parse import unquote

from sync_logs import mysql_conn

TIMEOUT_LINE = re.compile(
    r"\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) [+-]\d{4}\] \[\d+\] \[CRITICAL\] "
    r"WORKER TIMEOUT \(pid:(\d+)\)"
)
TRACE_LINE = re.compile(
    r"REQTRACE ts=(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) pid=(\d+) user=(\S+) "
    r"method=(\S+) url=(\S+)"
)
TIMEOUT_S = os.environ.get("SUPERSET_WEBSERVER_TIMEOUT", "?")
TRACE_MAX_AGE_S = 120  # a request killed by the timeout started at most this long ago
TS_FORMAT = "%Y-%m-%d %H:%M:%S"


def _first_int(pattern, text):
    m = re.search(pattern, text)
    return int(m.group(1)) if m else None


def _context(trace):
    """(user_id, dashboard_id, chart_id, 'METHOD /path') from a trace tuple."""
    _ts, user, method, url = trace
    decoded = unquote(url)
    chart_id = _first_int(r'"slice_id":\s*(\d+)', decoded) or _first_int(
        r"/chart/(\d+)", decoded
    )
    dashboard_id = _first_int(r"dashboard_id=(\d+)", decoded) or _first_int(
        r"/dashboard/(\d+)", decoded
    )
    user_id = int(user) if user.isdigit() else None
    return user_id, dashboard_id, chart_id, f"{method} {decoded.split('?')[0]}"


def main():
    conn = mysql_conn()
    last_trace = {}  # pid -> (ts, user, method, url) of that worker's latest request
    seen = inserted = matched = 0
    with conn.cursor() as cur:
        for line in sys.stdin:
            t = TRACE_LINE.search(line)
            if t:
                ts, pid, user, method, url = t.groups()
                last_trace[pid] = (ts, user, method, url)
                continue
            m = TIMEOUT_LINE.search(line)
            if not m:
                continue
            ts, pid = m.groups()
            seen += 1

            user_id = dashboard_id = chart_id = None
            request_desc = ""
            trace = last_trace.get(pid)
            if trace:
                age = (datetime.strptime(ts, TS_FORMAT) - datetime.strptime(trace[0], TS_FORMAT)).total_seconds()
                if 0 <= age <= TRACE_MAX_AGE_S:
                    user_id, dashboard_id, chart_id, request_desc = _context(trace)
                    matched += 1

            reason = (
                f"gunicorn WORKER TIMEOUT: worker pid {pid} exceeded the {TIMEOUT_S}s "
                "request timeout and was SIGKILLed (slow query/request, not an OOM)"
            )
            if request_desc:
                reason += f"; request was {request_desc}"
            cur.execute(
                """
                INSERT INTO fact_error_events
                    (event_ts, source, error_type, reason, user_id, dashboard_id,
                     chart_id, dedupe_key)
                VALUES (%s, 'gunicorn', 'worker_timeout', %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    reason       = VALUES(reason),
                    user_id      = COALESCE(VALUES(user_id), user_id),
                    dashboard_id = COALESCE(VALUES(dashboard_id), dashboard_id),
                    chart_id     = COALESCE(VALUES(chart_id), chart_id)
                """,
                (ts, reason[:500], user_id, dashboard_id, chart_id, f"gunicorn:{ts}:{pid}"),
            )
            inserted += 1 if cur.rowcount == 1 else 0
    conn.commit()
    conn.close()
    print(
        f"gunicorn timeout lines seen: {seen}, matched to a request: {matched}, "
        f"new rows inserted: {inserted}"
    )


if __name__ == "__main__":
    main()

"""
Ingests server-side failures of ANY cause from the Superset containers' logs into the error table
(fact_error_events: Iceberg when ICEBERG_SINK_ENABLED is on, else the legacy MySQL table).

We do not know in advance which errors production will produce, so nothing here is specific to one cause. Every
signal Superset leaves behind is turned into a row with the raw message, a category and a signature (see
etl/error_classify.py):

  REQEND   a dashboard/chart request that ended with HTTP >= 400: status, duration, the message from the response
           body, tied to a user, dashboard and chart (docker/superset/superset_config.py logs these)
  REQEXC   an unhandled exception: its class is added to the matching REQEND row (same request id)
  gunicorn arbiter lines: worker timeout, killed worker (SIGKILL/OOM), any other [CRITICAL]/[ERROR] line. These
           requests never reach an end-of-request hook, so the row is tied to its request by the worker pid and the
           last REQTRACE line that worker wrote
  celery   a failed background task ("raised unexpected")

Anything that is not recognised is still stored (category `unclassified`) when it is an error line; nothing is dropped.

The pipeline container has no docker access, so pipe the logs in from the host (superset-worker is optional):
    docker compose logs --no-log-prefix --since 1h superset superset-worker \\
        | docker compose exec -T pipeline python etl/ingest_container_errors.py

Idempotent: every row has a stable dedupe_key; re-running fills in missing context but never duplicates.
Single writer: shares the ETL's advisory lock, so it never overlaps an ETL run (Iceberg MERGE conflicts).
"""
import hashlib
import os
import re
import sys
from datetime import datetime
from urllib.parse import unquote

import iceberg_sink
from error_classify import classify, one_line, signature
from sync_logs import acquire_etl_lock, mysql_conn

TS = r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})"
GUNICORN_TIMEOUT = re.compile(rf"\[{TS} [+-]\d{{4}}\] \[\d+\] \[CRITICAL\] WORKER TIMEOUT \(pid:(\d+)\)")
GUNICORN_KILLED = re.compile(rf"\[{TS} [+-]\d{{4}}\] \[\d+\] \[ERROR\] Worker \(pid:(\d+)\) was sent (SIG\w+)")
GUNICORN_OTHER = re.compile(rf"\[{TS} [+-]\d{{4}}\] \[\d+\] \[(CRITICAL|ERROR)\] (.+)$")
TRACE = re.compile(rf"REQTRACE ts={TS} pid=(\d+) user=(\S+) method=(\S+)(?: rid=(\S+))? url=(\S+)")
REQEND = re.compile(rf"REQEND ts={TS} pid=(\d+) user=(\S+) method=(\S+) rid=(\S+) status=(\d+) dur=(\d+) url=(\S+) msg=(.*)$")
REQEXC = re.compile(rf"REQEXC ts={TS} pid=(\d+) rid=(\S+) exc=(\S+) msg=(.*)$")
CELERY = re.compile(rf"\[{TS},\d+: ERROR/[^\]]+\] Task (\S+)\[(\S+)\] raised unexpected: (.*)$")

TIMEOUT_S = os.environ.get("SUPERSET_WEBSERVER_TIMEOUT", "?")
TRACE_MAX_AGE_S = 120  # a request killed by the timeout started at most this long ago
KILL_FOLLOWS_TIMEOUT_S = 10  # gunicorn sends SIGKILL right after a timeout; that is one incident, not two
TS_FORMAT = "%Y-%m-%d %H:%M:%S"


def _first_int(pattern, text):
    m = re.search(pattern, text)
    return int(m.group(1)) if m else None


def _url_context(url):
    """(dashboard_id, chart_id, path) from a request URL."""
    decoded = unquote(url)
    chart_id = _first_int(r'"slice_id":\s*(\d+)', decoded) or _first_int(r"/chart/(\d+)", decoded)
    dashboard_id = _first_int(r"dashboard_id=(\d+)", decoded) or _first_int(r"/dashboard/(\d+)", decoded)
    return dashboard_id, chart_id, decoded.split("?")[0]


def _row(**kw):
    """A fact_error_events row; unspecified fields are None."""
    base = {k: None for k, _ in iceberg_sink.ERROR_SPEC}
    base.update(kw)
    base["_matched"] = kw.get("_matched", False)
    return base


def _short(text):
    return hashlib.sha1(text.encode("utf-8", "replace")).hexdigest()[:10]


def parse_lines(lines):
    """Yields one row per error found in the container log lines."""
    lines = [l for l in lines]
    # exceptions first, so the REQEND row of the same request can carry the exception class
    exceptions = {}
    for line in lines:
        m = REQEXC.search(line)
        if m:
            exceptions[m.group(3)] = (m.group(4), one_line(m.group(5)))

    last_trace = {}  # pid -> (ts, user, method, url, rid) of that worker's latest request
    timeout_at = {}  # pid -> ts of its latest worker timeout

    def request_for(pid, ts):
        trace = last_trace.get(pid)
        if not trace:
            return None
        age = (datetime.strptime(ts, TS_FORMAT) - datetime.strptime(trace[0], TS_FORMAT)).total_seconds()
        return trace if 0 <= age <= TRACE_MAX_AGE_S else None

    def killed_request_fields(trace):
        if not trace:
            return {}
        _ts, user, method, url, rid = trace
        dash, chart, path = _url_context(url)
        return {
            "user_id": int(user) if user.isdigit() else None, "dashboard_id": dash, "chart_id": chart,
            "request_path": path, "request_id": rid, "_matched": True,
            "_desc": f"{method} {path}",
        }

    for line in lines:
        t = TRACE.search(line)
        if t:
            ts, pid, user, method, rid, url = t.groups()
            last_trace[pid] = (ts, user, method, url, rid)
            continue

        m = REQEND.search(line)
        if m:
            ts, pid, user, method, rid, status, dur, url, msg = m.groups()
            status, msg = int(status), one_line(msg)
            exc = exceptions.get(rid)
            if exc and not msg:
                msg = f"{exc[0]}: {exc[1]}"
            dash, chart, path = _url_context(url)
            yield _row(
                dedupe_key=f"server:{rid}", event_ts=datetime.strptime(ts, TS_FORMAT), source="server",
                error_type=exc[0] if exc else f"http_{status}",
                reason=f"HTTP {status} on {method} {path}: {msg or 'no message'}"[:500],
                user_id=int(user) if user.isdigit() else None, dashboard_id=dash, chart_id=chart,
                duration_ms=int(dur), error_category=classify(msg, status), error_signature=signature(msg or f"http {status}"),
                error_message=msg or f"HTTP {status}", status_code=status, request_path=path, request_id=rid,
                _matched=True,
            )
            continue

        m = GUNICORN_TIMEOUT.search(line)
        if m:
            ts, pid = m.groups()
            timeout_at[pid] = ts
            ctx = killed_request_fields(request_for(pid, ts))
            msg = (f"gunicorn WORKER TIMEOUT: worker pid {pid} exceeded the {TIMEOUT_S}s request timeout and was "
                   "SIGKILLed (slow query/request, not an OOM)")
            reason = msg + (f"; request was {ctx['_desc']}" if ctx else "")
            ctx.pop("_desc", None)
            yield _row(
                dedupe_key=f"gunicorn:{ts}:{pid}", event_ts=datetime.strptime(ts, TS_FORMAT), source="gunicorn",
                error_type="worker_timeout", reason=reason[:500], error_category=classify(msg),
                error_signature=signature("gunicorn worker timeout"), error_message=msg, **ctx,
            )
            continue

        m = GUNICORN_KILLED.search(line)
        if m:
            ts, pid, sig = m.groups()
            prev = timeout_at.get(pid)
            if prev and 0 <= (datetime.strptime(ts, TS_FORMAT) - datetime.strptime(prev, TS_FORMAT)).total_seconds() <= KILL_FOLLOWS_TIMEOUT_S:
                continue  # the SIGKILL that follows a timeout is the same incident
            ctx = killed_request_fields(request_for(pid, ts))
            msg = f"gunicorn worker pid {pid} was sent {sig} without a preceding timeout (killed from outside, often out of memory)"
            reason = msg + (f"; request was {ctx['_desc']}" if ctx else "")
            ctx.pop("_desc", None)
            yield _row(
                dedupe_key=f"gunicorn_kill:{ts}:{pid}", event_ts=datetime.strptime(ts, TS_FORMAT), source="gunicorn",
                error_type="worker_killed", reason=reason[:500], error_category="worker_failure",
                error_signature=signature(f"gunicorn worker was sent {sig}"), error_message=msg, **ctx,
            )
            continue

        m = CELERY.search(line)
        if m:
            ts, task, task_id, msg = m.groups()
            msg = one_line(msg)
            yield _row(
                dedupe_key=f"celery:{task_id}", event_ts=datetime.strptime(ts, TS_FORMAT), source="celery",
                error_type="celery_task_failed", reason=f"Task {task} failed: {msg}"[:500],
                error_category=classify(msg), error_signature=signature(msg), error_message=msg, request_id=task_id,
            )
            continue

        m = GUNICORN_OTHER.search(line)
        if m and "WORKER TIMEOUT" not in line and "was sent SIG" not in line:
            ts, level, msg = m.groups()
            msg = one_line(msg)
            yield _row(
                dedupe_key=f"gunicorn_err:{ts}:{_short(msg)}", event_ts=datetime.strptime(ts, TS_FORMAT),
                source="gunicorn", error_type="gunicorn_error", reason=f"gunicorn {level}: {msg}"[:500],
                error_category=classify(msg), error_signature=signature(msg), error_message=msg,
            )


def _write_mysql(conn, rows):
    """Legacy path (Iceberg off): the old MySQL table only has the basic columns."""
    inserted = 0
    with conn.cursor() as cur:
        for r in rows:
            cur.execute(
                """
                INSERT INTO fact_error_events
                    (event_ts, source, error_type, reason, user_id, dashboard_id, chart_id, duration_ms, dedupe_key)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    reason       = VALUES(reason),
                    user_id      = COALESCE(VALUES(user_id), user_id),
                    dashboard_id = COALESCE(VALUES(dashboard_id), dashboard_id),
                    chart_id     = COALESCE(VALUES(chart_id), chart_id)
                """,
                (r["event_ts"], r["source"], r["error_type"], r["reason"], r["user_id"], r["dashboard_id"],
                 r["chart_id"], r["duration_ms"], r["dedupe_key"]),
            )
            inserted += 1 if cur.rowcount == 1 else 0
    conn.commit()
    return inserted


def main():
    conn = mysql_conn()
    if not acquire_etl_lock(conn):
        print("the ETL (or another ingest) holds the single-writer lock; skipping this run")
        conn.close()
        return 0
    rows = list(parse_lines(sys.stdin))
    by_source = {}
    for r in rows:
        by_source[r["source"]] = by_source.get(r["source"], 0) + 1
    clean = [{k: v for k, v in r.items() if k != "_matched"} for r in rows]
    if iceberg_sink.enabled():
        sink = iceberg_sink.IcebergSink()
        for r in clean:
            sink.add_error(r)
        written = sink.flush()["errors"]
        target = "iceberg"
    else:
        written = _write_mysql(conn, clean)
        target = "mysql"
    conn.close()
    detail = ", ".join(f"{k}={v}" for k, v in sorted(by_source.items())) or "none"
    print(f"server-side errors found: {len(rows)} ({detail}); sent to {target}: {written}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

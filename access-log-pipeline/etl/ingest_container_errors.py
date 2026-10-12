"""
Ingests server-side failures of ANY cause from the Superset containers' logs into the Iceberg error
table (fact_error_events).

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

Every line is also stored in container_log_lines (service, docker timestamp, level, kind, pid, request id, user, dashboard,
chart, method, path, status, duration, an is_error flag, text), so the
raw logs can be queried in Trino. That needs the service name and docker's timestamps, so pipe the logs in WITH the
prefix and `--timestamps` (run_etl.sh does). A line with no timestamp cannot be keyed and is skipped.

Anything that is not recognised is still stored (category `unclassified`) when it is an error line; nothing is dropped.

The pipeline container has no docker access, so pipe the logs in from the host (superset-worker is optional):
    docker compose logs --timestamps --since 1h superset superset-worker \\
        | docker compose exec -T pipeline python etl/ingest_container_errors.py

Idempotent: every row has a stable dedupe_key; re-running fills in missing context but never duplicates.
Single writer: shares the ETL's lease (etl/iceberg_state.py), so it never overlaps an ETL run; commit conflicts that
still happen are retried.
"""
import hashlib
import os
import re
import sys
from datetime import datetime
from urllib.parse import unquote

import iceberg_sink
import iceberg_state
from error_classify import classify, one_line, row_category_and_signature, signature
from sync_logs import LOCK_NAME

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


PREFIX = re.compile(r"^(?:(?P<svc>\S+?)-\d+\s+\|\s?)?(?:(?P<ts>\d{4}-\d{2}-\d{2}T[\d:.]+Z)\s)?(?P<msg>.*)$")
LEVEL = re.compile(r"\[(CRITICAL|ERROR|WARNING|INFO|DEBUG)\]|: (CRITICAL|ERROR|WARNING|INFO|DEBUG)/|^(CRITICAL|ERROR|WARNING|INFO|DEBUG)\b")
ACCESS = re.compile(r'^\S+ - \S* \[[^\]]+\] "(?P<method>[A-Z]+) (?P<url>\S+) [^"]*" (?P<status>\d{3}) ')
# Request paths whose lines are not stored in container_log_lines: the ETL's own polling of Superset's log API plus health
# checks are about 30% of the lines and never matter when tracing an error. Comma separated path prefixes.
DROP_PATHS = [p.strip() for p in os.environ.get("LOG_DROP_PATHS", "/health,/api/v1/log/").split(",") if p.strip()]
MAX_MESSAGE = 4000


def split_line(raw):
    """(service, docker timestamp as a naive UTC datetime or None, message) from one `docker compose logs --timestamps`
    line. The service prefix and the timestamp are both optional."""
    m = PREFIX.match(raw.rstrip("\n"))
    ts = None
    if m.group("ts"):
        base, _, frac = m.group("ts").rstrip("Z").partition(".")
        ts = datetime.strptime(base, "%Y-%m-%dT%H:%M:%S").replace(microsecond=int((frac + "000000")[:6] or 0))
    return m.group("svc") or "unknown", ts, m.group("msg")


def _int_or_none(text):
    return int(text) if text and text.isdigit() else None


def extract_fields(msg):
    """kind, level and the fields worth filtering on, from one log message (all None / False when a line has none).
    kind is what produced the line: reqtrace | reqend | reqexc (added by superset_config.py), gunicorn, celery, access
    (HTTP access line) or app (anything else)."""
    f = {"kind": "app", "level": None, "pid": None, "request_id": None, "user_id": None, "dashboard_id": None,
         "chart_id": None, "method": None, "url_path": None, "status_code": None, "duration_ms": None, "is_error": False}
    m = REQEND.search(msg)
    if m:
        _ts, pid, user, method, rid, status, dur, url, _msg = m.groups()
        dash, chart, path = _url_context(url)
        f.update(kind="reqend", pid=int(pid), request_id=rid, user_id=_int_or_none(user), dashboard_id=dash,
                 chart_id=chart, method=method, url_path=path, status_code=int(status), duration_ms=int(dur), is_error=True)
        return f
    m = TRACE.search(msg)
    if m:
        _ts, pid, user, method, rid, url = m.groups()
        dash, chart, path = _url_context(url)
        f.update(kind="reqtrace", pid=int(pid), request_id=rid, user_id=_int_or_none(user), dashboard_id=dash,
                 chart_id=chart, method=method, url_path=path)
        return f
    m = REQEXC.search(msg)
    if m:
        _ts, pid, rid, _exc, _msg = m.groups()
        f.update(kind="reqexc", pid=int(pid), request_id=rid, is_error=True)
        return f
    m = ACCESS.match(msg)
    if m:
        status = int(m.group("status"))
        # only 5xx counts as an error here: a 4xx on a dashboard/chart path already has a (richer) reqend line, and the other
        # 4xx are client noise; 5xx on any other path would otherwise be invisible
        f.update(kind="access", method=m.group("method"), url_path=m.group("url").split("?")[0], status_code=status,
                 is_error=status >= 500)
        return f
    if re.search(r"\[\d{4}-\d{2}-\d{2} [\d:]+ [+-]\d{4}\] \[\d+\] \[", msg):
        f["kind"] = "gunicorn"
    elif re.search(r"\[\d{4}-\d{2}-\d{2} [\d:,]+: \w+/", msg):
        f["kind"] = "celery"
    lv = LEVEL.search(msg)
    f["level"] = next((g for g in lv.groups() if g), None) if lv else None
    f["is_error"] = f["level"] in ("ERROR", "CRITICAL")
    pid = re.search(r"pid[=:](\d+)", msg)
    f["pid"] = int(pid.group(1)) if pid else None
    return f


def log_line_row(service, ts, msg):
    f = extract_fields(msg)
    f.update(
        line_id=hashlib.sha1(f"{service}|{ts.isoformat()}|{msg}".encode("utf-8", "replace")).hexdigest(),
        event_ts=ts, service=service, message=msg[:MAX_MESSAGE],
    )
    return f


def _dropped(row):
    return row["kind"] in ("access", "reqtrace") and any((row["url_path"] or "").startswith(p) for p in DROP_PATHS)


def build_log_rows(lines):
    """container_log_lines rows from (service, timestamp, message) tuples: drops the noise (LOG_DROP_PATHS) and ties each
    gunicorn timeout / killed worker to the request its worker pid was serving (the pid's last REQTRACE, at most
    TRACE_MAX_AGE_S earlier), so the request id, user, dashboard and chart are columns too. Lines without a timestamp
    cannot be keyed and are left out."""
    rows = [log_line_row(svc, ts, msg) for svc, ts, msg in lines if ts is not None]
    rows = [r for r in rows if not _dropped(r)]
    rows.sort(key=lambda r: (r["service"], r["event_ts"]))
    last_trace = {}
    for r in rows:
        key = (r["service"], r["pid"])
        if r["kind"] == "reqtrace":
            last_trace[key] = r
        elif r["kind"] == "gunicorn" and ("WORKER TIMEOUT" in r["message"] or "was sent SIG" in r["message"]):
            r["is_error"] = True
            t = last_trace.get(key)
            if t is not None and 0 <= (r["event_ts"] - t["event_ts"]).total_seconds() <= TRACE_MAX_AGE_S:
                for col in ("request_id", "user_id", "dashboard_id", "chart_id", "method", "url_path"):
                    r[col] = t[col]
    return rows


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
            category, sig = row_category_and_signature("server", exc[0] if exc else f"http_{status}", status, path, msg)
            yield _row(
                dedupe_key=f"server:{rid}", event_ts=datetime.strptime(ts, TS_FORMAT), source="server",
                error_type=exc[0] if exc else f"http_{status}",
                reason=f"HTTP {status} on {method} {path}: {msg or 'no message'}"[:500],
                user_id=int(user) if user.isdigit() else None, dashboard_id=dash, chart_id=chart,
                duration_ms=int(dur), error_category=category,
                # the endpoint and status are part of what makes this error this error; the record id is not
                error_signature=sig,
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
                error_type="worker_timeout", reason=reason[:500],
                error_category=row_category_and_signature("gunicorn", "worker_timeout", None, None, msg)[0],
                error_signature=row_category_and_signature("gunicorn", "worker_timeout", None, None, msg)[1],
                error_message=msg, **ctx,
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
                error_signature=row_category_and_signature("gunicorn", "worker_killed", None, None, msg)[1],
                error_message=msg, **ctx,
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
                error_category=classify(msg), error_signature="gunicorn " + signature(msg), error_message=msg,
            )


def main():
    conn = iceberg_sink.trino_conn()
    lease = iceberg_state.Lease(conn, LOCK_NAME)
    parsed = [split_line(raw) for raw in sys.stdin if raw.strip()]  # parse before taking the lease: it needs no Trino
    rows = list(parse_lines([msg for _svc, _ts, msg in parsed]))
    log_rows = build_log_rows(parsed)
    skipped = sum(1 for _svc, ts, _msg in parsed if ts is None)
    dropped = len(parsed) - skipped - len(log_rows)
    if not lease.acquire():
        print("the ETL (or another ingest) holds the single-writer lease; skipping this run")
        return 0
    try:
        by_source = {}
        for r in rows:
            by_source[r["source"]] = by_source.get(r["source"], 0) + 1
        sink = iceberg_sink.IcebergSink(conn, on_progress=lease.renew)
        for r in rows:
            sink.add_error({k: v for k, v in r.items() if k != "_matched"})
        for r in log_rows:
            sink.add_log_line(r)
        flushed = sink.flush()
        written = flushed["errors"]
        detail = ", ".join(f"{k}={v}" for k, v in sorted(by_source.items())) or "none"
        iceberg_state.record_run(conn, "ingest", "ok", len(rows), written, detail)
        print(f"server-side errors found: {len(rows)} ({detail}); sent to iceberg: {written}")
        print(f"container log lines: {len(log_rows)} sent to iceberg ({dropped} noise lines dropped)"
              + (f", {skipped} skipped (no docker timestamp: use `docker compose logs --timestamps`)" if skipped else ""))
        return 0
    except Exception as exc:  # noqa: BLE001
        iceberg_state.record_run(conn, "ingest", "failed", len(rows), 0, str(exc))
        raise
    finally:
        lease.release()


if __name__ == "__main__":
    sys.exit(main())

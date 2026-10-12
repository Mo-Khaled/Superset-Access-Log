"""
Checks that the container-log parser turns every kind of server-side failure into an error row, with no cause-specific
code: request errors with a status, unhandled exceptions, gunicorn problems, failed background tasks, and an unknown
arbiter message. No services needed beyond the pipeline's own Python packages:
    docker compose exec -T pipeline python tests/test_ingest_parse.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "etl"))
import ingest_container_errors as ing  # noqa: E402

LINES = """
REQTRACE ts=2026-10-05 13:19:00 pid=8 user=1 method=POST rid=aaaa11112222 url=/api/v1/chart/data?form_data=%7B%22slice_id%22%3A166%7D&dashboard_id=13
[2026-10-05 13:19:16 +0000] [1] [CRITICAL] WORKER TIMEOUT (pid:8)
[2026-10-05 13:19:17 +0000] [1] [ERROR] Worker (pid:8) was sent SIGKILL! Perhaps out of memory?
REQTRACE ts=2026-10-05 13:20:00 pid=7 user=2 method=GET rid=bbbb11112222 url=/api/v1/chart/99999?
REQEND ts=2026-10-05 13:20:00 pid=7 user=2 method=GET rid=bbbb11112222 status=404 dur=12 url=/api/v1/chart/99999? msg=Not found
REQEXC ts=2026-10-05 13:21:00 pid=7 rid=cccc11112222 exc=ProgrammingError msg=column "foo" does not exist
REQEND ts=2026-10-05 13:21:00 pid=7 user=2 method=POST rid=cccc11112222 status=500 dur=40 url=/api/v1/chart/data?dashboard_id=5 msg=
[2026-10-05 13:22:00 +0000] [1] [ERROR] Worker (pid:9) was sent SIGKILL! Perhaps out of memory?
[2026-10-05 13:23:00 +0000] [1] [CRITICAL] Something odd happened in the arbiter
[2026-10-05 13:24:00,123: ERROR/ForkPoolWorker-1] Task superset.tasks.thumbnails.cache_chart_thumbnail[abc-123] raised unexpected: OperationalError('connection refused')
172.19.0.1 - - [05/Oct/2026:13:25:00 +0000] "GET /health HTTP/1.1" 200 2 "-" "curl"
""".strip().splitlines()

EXPECTED = [
    # (error_type, category, status, dashboard, chart, user)
    ("worker_timeout", "timeout", None, 13, 166, 1),            # tied to its request through the worker pid
    ("http_404", "not_found", 404, None, 99999, 2),
    ("ProgrammingError", "query_error", 500, 5, None, 2),       # exception class joined by request id
    ("worker_killed", "worker_failure", None, None, None, None),  # SIGKILL with no preceding timeout is its own incident
    ("gunicorn_error", "unclassified", None, None, None, None),   # unknown line: kept, not dropped
    ("celery_task_failed", "connection", None, None, None, None),
]


def main():
    rows = list(ing.parse_lines(LINES))
    got = [(r["error_type"], r["error_category"], r["status_code"], r["dashboard_id"], r["chart_id"], r["user_id"]) for r in rows]
    bad = 0
    for i in range(max(len(EXPECTED), len(got))):
        want = EXPECTED[i] if i < len(EXPECTED) else None
        have = got[i] if i < len(got) else None
        ok = want == have
        bad += not ok
        print(("PASS  " if ok else "FAIL  ") + f"{want}" + ("" if ok else f"  got {have}"))
    # the SIGKILL right after a timeout must not become a second error
    only_one_for_pid8 = sum(1 for r in rows if "8" in r["dedupe_key"].split(":")[-1:] or r["dedupe_key"].endswith(":8")) == 1
    bad += not only_one_for_pid8
    print(("PASS  " if only_one_for_pid8 else "FAIL  ") + "timeout + its SIGKILL count as one incident")
    ids = [r["dedupe_key"] for r in rows]
    unique = len(ids) == len(set(ids))
    bad += not unique
    print(("PASS  " if unique else "FAIL  ") + "every row has a distinct dedupe_key")
    # container_log_lines: every `docker compose logs --timestamps` line is split and classified
    from datetime import datetime
    cases = [
        ("superset-1  | 2026-10-12T05:57:12.123456789Z REQEND ts=2026-10-12 05:57:12 pid=7 user=- method=GET rid=8cf4fc5870fd status=404 dur=17 url=/x? msg=Not found",
         ("superset", datetime(2026, 10, 12, 5, 57, 12, 123456), "reqend", None, 7, "8cf4fc5870fd")),
        ("superset-1  | 2026-10-12T05:57:13.5Z [2026-10-12 05:57:13 +0000] [1] [CRITICAL] WORKER TIMEOUT (pid:8)",
         ("superset", datetime(2026, 10, 12, 5, 57, 13, 500000), "gunicorn", "CRITICAL", 8, None)),
        ("superset-worker-1  | 2026-10-12T05:57:14Z [2026-10-12 05:57:14,123: ERROR/ForkPoolWorker-1] Task x[abc] raised unexpected: boom",
         ("superset-worker", datetime(2026, 10, 12, 5, 57, 14), "celery", "ERROR", None, None)),
        ('superset-1  | 2026-10-12T05:57:15Z 172.19.0.1 - - [12/Oct/2026:05:57:15 +0000] "GET /health HTTP/1.1" 200 2 "-" "curl"',
         ("superset", datetime(2026, 10, 12, 5, 57, 15), "access", None, None, None)),
        ("superset-worker-beat-1  | 2026-10-12T05:57:16Z beat: Starting...",
         ("superset-worker-beat", datetime(2026, 10, 12, 5, 57, 16), "app", None, None, None)),
    ]
    for raw, want in cases:
        svc, ts, msg = ing.split_line(raw)
        f = ing.extract_fields(msg)
        have = (svc, ts, f["kind"], f["level"], f["pid"], f["request_id"])
        ok = have == want
        bad += not ok
        print(("PASS  " if ok else "FAIL  ") + f"log line -> {want[2]}" + ("" if ok else f"  want {want} got {have}"))
    # parsed columns
    f = ing.extract_fields("REQEND ts=2026-10-12 05:57:12 pid=7 user=2 method=POST rid=abc123 status=400 dur=463 url=/api/v1/chart/data?form_data=%7B%22slice_id%22%3A125%7D&dashboard_id=9 msg=boom")
    want = dict(kind="reqend", user_id=2, dashboard_id=9, chart_id=125, method="POST", url_path="/api/v1/chart/data", status_code=400, duration_ms=463, is_error=True, request_id="abc123", pid=7)
    ok = all(f[k] == v for k, v in want.items())
    bad += not ok
    print(("PASS  " if ok else "FAIL  ") + "reqend -> user, dashboard, chart, method, path, status, duration, is_error" + ("" if ok else f"  got {f}"))
    for msg, expect, label in [
        ('172.19.0.1 - - [12/Oct/2026:05:57:15 +0000] "GET /api/v1/chart/7 HTTP/1.1" 404 2 "-" "curl"', False, "an access 404 is not flagged (reqend carries it)"),
        ('172.19.0.1 - - [12/Oct/2026:05:57:15 +0000] "GET /api/v1/chart/7 HTTP/1.1" 502 2 "-" "curl"', True, "an access 5xx is flagged"),
        ("[2026-10-12 05:57:13 +0000] [1] [INFO] Booting worker with pid: 8", False, "an INFO line is not an error"),
        ("[2026-10-12 05:57:13 +0000] [1] [ERROR] Worker (pid:8) was sent SIGKILL!", True, "an ERROR line is an error"),
    ]:
        ok = ing.extract_fields(msg)["is_error"] is expect
        bad += not ok
        print(("PASS  " if ok else "FAIL  ") + label)
    # noise is dropped; a gunicorn timeout is tied to the request its worker pid was serving
    t0 = datetime(2026, 10, 12, 5, 0, 0)
    lines = [
        ("superset", t0, "REQTRACE ts=2026-10-12 05:00:00 pid=8 user=1 method=POST rid=r1 url=/api/v1/chart/data?form_data=%7B%22slice_id%22%3A166%7D&dashboard_id=13"),
        ("superset", datetime(2026, 10, 12, 5, 0, 16), "[2026-10-12 05:00:16 +0000] [1] [CRITICAL] WORKER TIMEOUT (pid:8)"),
        ("superset", t0, 'x - - [12/Oct/2026:05:00:00 +0000] "GET /health HTTP/1.1" 200 2 "-" "curl"'),
        ("superset", t0, "REQTRACE ts=2026-10-12 05:00:00 pid=7 user=- method=GET rid=r2 url=/api/v1/log/?q=1"),
        ("superset", None, "a line with no timestamp"),
    ]
    rows = ing.build_log_rows(lines)
    kinds = sorted(r["kind"] for r in rows)
    ok = kinds == ["gunicorn", "reqtrace"]
    bad += not ok
    print(("PASS  " if ok else "FAIL  ") + "noise (/health, log API polling) and lines without a timestamp are dropped" + ("" if ok else f"  got {kinds}"))
    g = next(r for r in rows if r["kind"] == "gunicorn")
    ok = (g["request_id"], g["user_id"], g["dashboard_id"], g["chart_id"], g["is_error"]) == ("r1", 1, 13, 166, True)
    bad += not ok
    print(("PASS  " if ok else "FAIL  ") + "a worker timeout carries its request's id, user, dashboard and chart" + ("" if ok else f"  got {g}"))
    a = ing.log_line_row("superset", datetime(2026, 10, 12, 5, 0, 0), "same text")
    b = ing.log_line_row("superset", datetime(2026, 10, 12, 5, 0, 0), "same text")
    c = ing.log_line_row("superset-worker", datetime(2026, 10, 12, 5, 0, 0), "same text")
    ok = a["line_id"] == b["line_id"] and a["line_id"] != c["line_id"]
    bad += not ok
    print(("PASS  " if ok else "FAIL  ") + "a re-read line keeps its id, the same text from another service gets another")
    print()
    print("ALL PASSED" if not bad else f"{bad} FAILED")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())

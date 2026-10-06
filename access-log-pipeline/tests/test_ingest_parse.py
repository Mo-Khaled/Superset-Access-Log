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
    print()
    print("ALL PASSED" if not bad else f"{bad} FAILED")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())

"""
Checks the cause-agnostic classifier on realistic messages. No services needed:
    python tests/test_error_classify.py        (inside the pipeline container, or anywhere with Python)
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "etl"))
from error_classify import classify, signature  # noqa: E402

CASES = [
    # (message, status, expected category)
    ("timeout", None, "timeout"),
    ("gunicorn WORKER TIMEOUT: worker pid 8 exceeded the 15s request timeout", None, "timeout"),
    ("canceling statement due to statement timeout", 400, "timeout"),
    ("Worker (pid:204) was sent SIGKILL! Perhaps out of memory?", None, "worker_failure"),
    ("Error: Columns missing in dataset: ['title', 'owner_names']", 400, "query_error"),
    ("syntax error at or near \"FROM\"", 400, "query_error"),
    ("Access is Denied", 401, "permission"),
    ("You do not have permission to view this dashboard", 403, "permission"),
    ("Dashboard 77 not found", 404, "not_found"),
    ("Dataset 12 not found", 404, "not_found"),
    ("ProgrammingError: column \"foo\" does not exist", 500, "query_error"),
    ("relation \"orders\" does not exist", 400, "query_error"),
    ("Failed to fetch", None, "connection"),
    ("HTTPConnectionPool(host='trino', port=8080): Max retries exceeded", 500, "connection"),
    ("something nobody has seen before", None, "unclassified"),
    ("", 500, "server_error"),
    ("", 422, "client_error"),
]


def main():
    bad = 0
    for msg, status, want in CASES:
        got = classify(msg, status)
        ok = got == want
        bad += not ok
        print(("PASS  " if ok else "FAIL  ") + f"{want:15} <- {msg[:60]!r} (status={status}) got {got}")

    # the same error with different details must give the same signature; different errors must differ
    a = signature("Columns missing in dataset: ['title', 'owner_names']")
    b = signature("Columns missing in dataset: ['username', 'event_count']")
    c = signature("Dashboard 12 not found")
    d = signature("Dashboard 90 not found")
    e = signature("canceling statement due to statement timeout")
    for name, ok in (("signature groups variants", a == b), ("signature groups ids", c == d), ("signature separates errors", a != e and c != e)):
        bad += not ok
        print(("PASS  " if ok else "FAIL  ") + name)
    print("signature examples:", a, "|", c, "|", e)
    print()
    print("ALL PASSED" if not bad else f"{bad} FAILED")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())

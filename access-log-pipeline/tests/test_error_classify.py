"""
Checks the cause-agnostic classifier on realistic messages. No services needed:
    python tests/test_error_classify.py        (inside the pipeline container, or anywhere with Python)
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "etl"))
from error_classify import classify, path_template, row_category_and_signature, signature  # noqa: E402

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

    # ---- signatures: remove what is specific to one occurrence, keep what is informative ----
    sig = signature
    checks = [
        ("ids are normalised",            sig("Dataset 41 not found") == sig("Dataset 7 not found")),
        ("the kind of record is kept",    sig("Dashboard 7 not found") != sig("Dataset 7 not found")),
        ("HTTP status is kept",           sig("HTTP 422") != sig("HTTP 500") and sig("HTTP 500") == "http 500"),
        ("status in prose is kept",       sig("error 502 bad gateway") != sig("error 504 gateway timeout")),
        ("identifiers are kept",          sig("column \"foo\" does not exist") != sig("column \"bar\" does not exist")),
        ("missing column names are kept", sig("Columns missing in dataset: ['title']") != sig("Columns missing in dataset: ['region']")),
        ("literal values are removed",    sig("invalid input syntax for integer: 'abc 123'") == sig("invalid input syntax for integer: 'x 9'")),
        ("durations are removed",         sig("timeout after 15041 ms") == sig("timeout after 15566 ms")),
        ("uuid/ip/timestamp are removed", sig("task 3f2b9c1e-1111-4222-8333-444455556666 on 10.0.0.12:8088 at 2026-10-05 13:19:16 failed")
                                           == sig("task aaaaaaaa-1111-4222-8333-444455556666 on 10.9.9.9:8088 at 2026-01-01 00:00:01 failed")),
        ("different errors stay apart",   sig("canceling statement due to statement timeout") != sig("Failed to fetch")),
        ("never empty",                   sig("") == "(no message)"),
        ("path template",                 path_template("/api/v1/chart/166/data") == "/api/v1/chart/{id}/data"
                                           and path_template("/api/v1/chart/data") == "/api/v1/chart/data"),
    ]
    # ---- the single function every writer uses ----
    s404a = row_category_and_signature("server", "http_404", 404, "/api/v1/chart/99999", "Not found")
    s404b = row_category_and_signature("server", "http_404", 404, "/api/v1/chart/5", "Not found")
    s404c = row_category_and_signature("server", "http_404", 404, "/api/v1/dashboard/5", "Not found")
    checks += [
        ("server 404s on one endpoint group", s404a == s404b and s404a[0] == "not_found"),
        ("server 404s on other endpoints don't", s404a[1] != s404c[1]),
        ("gunicorn: one signature per incident kind",
         row_category_and_signature("gunicorn", "worker_timeout", None, None, "pid 7 ...")[1] == "gunicorn worker timeout"
         and row_category_and_signature("gunicorn", "worker_timeout", None, None, "pid 9 ...")[1] == "gunicorn worker timeout"),
        ("gunicorn killed is worker_failure", row_category_and_signature("gunicorn", "worker_killed", None, None, "x")[0] == "worker_failure"),
    ]
    for name, ok in checks:
        bad += not ok
        print(("PASS  " if ok else "FAIL  ") + name)
    print("examples:", sig("HTTP 422"), "|", sig("Columns missing in dataset: ['title']"), "|", s404a[1])
    print()
    print("ALL PASSED" if not bad else f"{bad} FAILED")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())

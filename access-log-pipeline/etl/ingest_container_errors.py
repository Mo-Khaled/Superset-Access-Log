"""
Ingests gunicorn "WORKER TIMEOUT" lines from the superset container's stdout into
fact_error_events (source = 'gunicorn'). These lines are the only server-side
trace of a request killed by the gunicorn timeout: Superset never gets to log it.

The pipeline container has no docker access, so pipe the logs in from the host:
    docker compose logs --no-log-prefix --since 1h superset \
        | docker compose exec -T pipeline python etl/ingest_container_errors.py

Idempotent: dedupe_key is derived from the line's own timestamp + pid.
"""
import os
import re
import sys

from sync_logs import mysql_conn

LINE = re.compile(
    r"\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) [+-]\d{4}\] \[\d+\] \[CRITICAL\] "
    r"WORKER TIMEOUT \(pid:(\d+)\)"
)
TIMEOUT_S = os.environ.get("SUPERSET_WEBSERVER_TIMEOUT", "?")


def main():
    conn = mysql_conn()
    seen = inserted = 0
    with conn.cursor() as cur:
        for line in sys.stdin:
            m = LINE.search(line)
            if not m:
                continue
            ts, pid = m.groups()
            seen += 1
            reason = (
                f"gunicorn WORKER TIMEOUT: worker pid {pid} exceeded the {TIMEOUT_S}s "
                "request timeout and was SIGKILLed (slow query/request, not an OOM)"
            )
            cur.execute(
                """
                INSERT IGNORE INTO fact_error_events
                    (event_ts, source, error_type, reason, dedupe_key)
                VALUES (%s, 'gunicorn', 'worker_timeout', %s, %s)
                """,
                (ts, reason, f"gunicorn:{ts}:{pid}"),
            )
            inserted += cur.rowcount
    conn.commit()
    conn.close()
    print(f"gunicorn timeout lines seen: {seen}, new rows inserted: {inserted}")


if __name__ == "__main__":
    main()

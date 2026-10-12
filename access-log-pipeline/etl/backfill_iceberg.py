"""
Rebuilds Iceberg facts from what Iceberg already holds. Use it after changing how log rows are classified
(sync_logs.build_event_rows, error_classify.py), so history follows the new rules.

  python etl/backfill_iceberg.py              replay raw_log_landing through the SAME code the ETL uses
                                              (sync_logs.build_event_rows) and MERGE the access and browser-error rows
  python etl/backfill_iceberg.py --reclassify recompute category and signature for EVERY stored error row with the
                                              current rules, and update the rows whose values differ

  python etl/backfill_iceberg.py --log-lines  re-parse every stored container log line with the current rules: fills the parsed
                                              columns (user, chart, status, is_error, ...), links gunicorn timeouts to their
                                              request, and deletes the noise lines (LOG_DROP_PATHS)

Idempotent (MERGE), so it is safe to run more than once. Takes the same single-writer lease as the ETL.
Server-side rows (gunicorn, server, celery) cannot be rebuilt from raw_log_landing, so only --reclassify touches them.
"""
import json
import sys

import iceberg_sink
import iceberg_state
from error_classify import row_category_and_signature
from ingest_container_errors import build_log_rows
from sync_logs import LOCK_NAME, build_event_rows


def reclassify(conn, sink):
    """Recompute category and signature for EVERY stored error row with the current rules (error_classify.py), and
    update the rows that differ. Run this after changing the rules."""
    rows = []
    for dedupe_key, ts, source, error_type, status, path, message, reason in iceberg_sink.run_sql(
        conn,
        "SELECT dedupe_key, event_ts, source, error_type, status_code, request_path, error_message, reason "
        "FROM fact_error_events",
    ):
        category, sig = row_category_and_signature(source, error_type, status, path, message or reason)
        rows.append({"dedupe_key": dedupe_key, "event_ts": ts, "error_category": category, "error_signature": sig})
    sent = sink.reclassify(rows)
    print(f"recomputed category and signature for {sent} error rows; rows whose values changed were updated")
    return 0


def rebuild_log_lines(conn, sink):
    """Re-parses every stored container log line: fills the parsed columns, resolves gunicorn timeouts to their request,
    and deletes lines that are now considered noise."""
    stored = iceberg_sink.run_sql(conn, "SELECT line_id, event_ts, service, message FROM container_log_lines")
    ids = {(svc, ts, msg): line_id for line_id, ts, svc, msg in stored}
    rows = build_log_rows([(svc, ts, msg) for _id, ts, svc, msg in stored])
    kept = set()
    for r in rows:
        r["line_id"] = ids.get((r["service"], r["event_ts"], r["message"]), r["line_id"])  # keep the stored id
        kept.add(r["line_id"])
    sent = sink.update_log_columns(rows)
    drop = [(line_id, ts) for line_id, ts, _svc, _msg in stored if line_id not in kept]
    for i in range(0, len(drop), 500):
        chunk = drop[i:i + 500]
        iceberg_sink.run_sql(conn, "DELETE FROM container_log_lines WHERE line_id IN (%s)"
                             % ", ".join("'%s'" % line_id for line_id, _ts in chunk))
    print(f"re-parsed {sent} container log lines (rows whose values changed were updated); deleted {len(drop)} noise lines")
    return 0


def replay(_conn, sink):
    """Rebuilds access and browser-error facts from raw_log_landing."""
    replayed = 0
    cur = iceberg_sink.trino_conn().cursor()  # its own connection: it stays open while the sink writes
    cur.execute("SELECT source_log_id, payload FROM raw_log_landing ORDER BY source_log_id")
    while True:
        batch = cur.fetchmany(2000)
        if not batch:
            break
        for log_id, payload in batch:
            row = json.loads(payload)
            row["id"] = log_id
            access, error = build_event_rows(row)
            if access is not None:
                sink.add_access(access)
            if error is not None:
                sink.add_error(error)
            replayed += 1
    written = sink.flush()
    print(f"replayed {replayed} raw log rows; merged into Iceberg: {written['access']} access, {written['errors']} errors")
    return 0


def main():
    conn = iceberg_sink.trino_conn()
    lease = iceberg_state.Lease(conn, LOCK_NAME)
    if not lease.acquire():
        print("the ETL holds the single-writer lease; try again when it has finished")
        return 1
    try:
        sink = iceberg_sink.IcebergSink(on_progress=lease.renew)
        if "--reclassify" in sys.argv:
            return reclassify(conn, sink)
        if "--log-lines" in sys.argv:
            return rebuild_log_lines(conn, sink)
        return replay(conn, sink)
    finally:
        lease.release()


if __name__ == "__main__":
    sys.exit(main())

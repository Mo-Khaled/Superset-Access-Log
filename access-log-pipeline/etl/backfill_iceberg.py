"""
One-off: fills Iceberg from what MySQL already holds, so history is not lost when the Iceberg sink is switched on.

  * access and browser-error rows are rebuilt by replaying raw_log_landing through the SAME code the ETL uses
    (sync_logs.build_event_rows), so duration_ms and is_error are filled for old rows too
  * gunicorn error rows cannot be derived from raw_log_landing, so they are copied from MySQL fact_error_events
    (their category, signature and message are derived from the stored reason)
  * dim_dashboards is copied from MySQL

Idempotent (MERGE), so it is safe to run more than once. Takes the same single-writer lock as the ETL.
    python etl/backfill_iceberg.py
"""
import json
import sys

import iceberg_sink
from error_classify import classify, row_category_and_signature, signature
from sync_logs import acquire_etl_lock, build_event_rows, mysql_conn


def reclassify():
    """Recompute category and signature for EVERY stored error row with the current rules (error_classify.py), and
    update the rows that differ. Run this after changing the rules."""
    if not iceberg_sink.enabled():
        print("ICEBERG_SINK_ENABLED is off; nothing to do")
        return 0
    conn = mysql_conn()
    if not acquire_etl_lock(conn):
        print("the ETL holds the single-writer lock; try again when it has finished")
        return 1
    sink = iceberg_sink.IcebergSink()
    cur = sink.conn.cursor()
    cur.execute("SELECT dedupe_key, event_ts, source, error_type, status_code, request_path, error_message, reason "
                "FROM fact_error_events")
    rows = []
    for dedupe_key, ts, source, error_type, status, path, message, reason in cur.fetchall():
        category, sig = row_category_and_signature(source, error_type, status, path, message or reason)
        rows.append({"dedupe_key": dedupe_key, "event_ts": ts, "error_category": category, "error_signature": sig})
    sent = sink.reclassify(rows)
    conn.close()
    print(f"recomputed category and signature for {sent} error rows; rows whose values changed were updated")
    return 0


def main():
    if "--reclassify" in sys.argv:
        return reclassify()
    if not iceberg_sink.enabled():
        print("ICEBERG_SINK_ENABLED is off; nothing to do")
        return 0
    conn = mysql_conn()
    if not acquire_etl_lock(conn):
        print("the ETL holds the single-writer lock; try again when it has finished")
        return 1
    sink = iceberg_sink.IcebergSink()
    replayed = 0
    with conn.cursor() as cur:
        cur.execute("SELECT source_log_id, payload FROM raw_log_landing ORDER BY source_log_id")
        for r in cur.fetchall():
            row = json.loads(r["payload"]) if isinstance(r["payload"], str) else r["payload"]
            row["id"] = r["source_log_id"]
            access, error = build_event_rows(row)
            if access is not None:
                sink.add_access(access)
            if error is not None:
                sink.add_error(error)
            replayed += 1

        cur.execute(
            "SELECT dedupe_key, event_ts, source, error_type, reason, user_id, dashboard_id, chart_id, "
            "duration_ms, raw_log_id FROM fact_error_events WHERE source = 'gunicorn'"
        )
        gunicorn = 0
        for r in cur.fetchall():
            row = dict(r)
            # these predate the cause-agnostic columns: derive them from the stored reason
            category, sig = row_category_and_signature("gunicorn", "worker_timeout", None, None, row["reason"])
            row.update(error_category=category, error_signature=sig, error_message=row["reason"])
            sink.add_error(row)
            gunicorn += 1

        cur.execute("SELECT dashboard_id, title FROM dim_dashboards")
        for r in cur.fetchall():
            sink.add_dashboard({"dashboard_id": r["dashboard_id"], "title": r["title"]})

    written = sink.flush()

    # Last step, any source: classify rows that still have no category (written before the columns existed, and not
    # re-derivable from raw_log_landing or MySQL).
    cols = ["dedupe_key", "event_ts", "source", "error_type", "reason", "user_id", "dashboard_id", "chart_id",
            "duration_ms", "raw_log_id"]
    tcur = sink.conn.cursor()
    tcur.execute("SELECT %s FROM fact_error_events WHERE error_category IS NULL" % ", ".join(cols))
    for values in tcur.fetchall():
        row = dict(zip(cols, values))
        text = row["reason"] or ""
        category, sig = row_category_and_signature(row["source"], row["error_type"], None, None, text)
        row.update(error_category=category, error_signature=sig, error_message=text)
        sink.add_error(row)
        gunicorn += 0  # (counted separately below)
    classified = sink.flush()["errors"] - written["errors"]
    written = dict(written, errors=written["errors"] + classified)
    conn.close()
    print(
        f"replayed {replayed} raw log rows and {gunicorn} gunicorn error rows; merged into Iceberg: "
        f"{written['access']} access, {written['errors']} errors, {written['dashboards']} dashboards"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

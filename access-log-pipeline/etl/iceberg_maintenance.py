"""
Table maintenance for the Iceberg tables the pipeline writes. Every MERGE is a snapshot and writes small files, so
without this the tables slowly get slower to query and the object store fills with files nobody references.

For each table:
  optimize            merges small data files into larger ones (and applies row-level deletes)
  expire_snapshots    drops snapshots older than ICEBERG_RETENTION (time travel beyond that is lost)
  remove_orphan_files deletes files no snapshot references (left by failed or conflicting writes)
It also deletes etl_run_log rows older than ETL_RUN_LOG_DAYS.

Run by run_etl.sh every cycle but only does work when the last successful maintenance is older than
MAINTENANCE_INTERVAL_HOURS (default 24), so it effectively runs daily. `--force` runs it now.
In the bank's platform, if another team already compacts these tables, set MAINTENANCE_INTERVAL_HOURS=0 and skip
the call in run_etl.sh; the statements here are the ones to hand over.

It takes the same single-writer lease as the ETL: optimize rewrites files a concurrent MERGE may be reading.
ICEBERG_RETENTION must not be shorter than the catalog's minimum (Trino default 7d), or the statements are rejected.
"""
import os
import sys

import iceberg_sink
import iceberg_state
from sync_logs import LOCK_NAME

TABLES = ["fact_access_events", "fact_error_events", "raw_log_landing", "etl_run_log",
          "dim_dashboards", "dim_charts", "dim_users", "etl_lock"]
RETENTION = os.environ.get("ICEBERG_RETENTION", "7d")
FILE_SIZE_THRESHOLD = os.environ.get("ICEBERG_OPTIMIZE_FILE_SIZE", "128MB")
INTERVAL_HOURS = float(os.environ.get("MAINTENANCE_INTERVAL_HOURS", "24"))
RUN_LOG_DAYS = int(os.environ.get("ETL_RUN_LOG_DAYS", "90"))


def statements(table):
    return [
        "ALTER TABLE %s EXECUTE optimize(file_size_threshold => '%s')" % (table, FILE_SIZE_THRESHOLD),
        "ALTER TABLE %s EXECUTE expire_snapshots(retention_threshold => '%s')" % (table, RETENTION),
        "ALTER TABLE %s EXECUTE remove_orphan_files(retention_threshold => '%s')" % (table, RETENTION),
    ]


def due(conn):
    age_s = iceberg_sink.run_sql(
        conn, "SELECT date_diff('second', max(run_ts), %s) FROM etl_run_log "
              "WHERE kind = 'maintenance' AND status = 'ok'" % iceberg_state.NOW_UTC)[0][0]
    return age_s is None or age_s / 3600.0 >= INTERVAL_HOURS


def main(force=False):
    conn = iceberg_sink.trino_conn()
    if not force and not due(conn):
        print("maintenance not due yet")
        return 0
    lease = iceberg_state.Lease(conn, LOCK_NAME)
    if not lease.acquire():
        print("another run holds the single-writer lease; maintenance will run next cycle")
        return 0
    failed = []
    try:
        for table in TABLES:
            for stmt in statements(table):
                try:
                    iceberg_sink.run_sql(conn, stmt)
                except Exception as exc:  # noqa: BLE001  one failing table must not stop the others
                    failed.append("%s: %s" % (stmt.split(" EXECUTE ")[1].split("(")[0], str(exc)[:150]))
                    print("WARNING: %s failed: %s" % (stmt, str(exc)[:200]), file=sys.stderr)
                lease.renew()
        try:
            iceberg_sink.run_sql(conn, "DELETE FROM etl_run_log WHERE run_ts < %s - INTERVAL '%d' DAY"
                                 % (iceberg_state.NOW_UTC, RUN_LOG_DAYS))
        except Exception as exc:  # noqa: BLE001
            failed.append("prune etl_run_log: " + str(exc)[:150])
        status = "ok" if not failed else "failed"
        iceberg_state.record_run(conn, "maintenance", status, detail="; ".join(failed))
        print("maintenance %s for %d tables (retention %s)" % ("done" if not failed else "finished with errors",
                                                                 len(TABLES), RETENTION))
        return 0 if not failed else 1
    finally:
        lease.release()


if __name__ == "__main__":
    sys.exit(main(force="--force" in sys.argv))

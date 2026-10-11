"""
Creates the Iceberg schema, tables and view (idempotent) and checks the platform can do what the pipeline needs.

Run by run_etl.sh before each cycle, and once by hand against a new Trino:
    python etl/iceberg_init.py

Checks, failing with a clear message instead of a confusing error later:
  * Trino is reachable, and the Trino version is printed (record it; MERGE on Iceberg needs a recent release)
  * every table is Iceberg format version 2 (required for MERGE)
  * the view can be created. Some catalogs cannot store views; then Superset uses the same SQL as a virtual dataset.
"""
import glob
import os
import sys

from iceberg_sink import CATALOG, SCHEMA, enabled, run_sql, trino_conn

# Whether authorization denials (HTTP 401/403, "access denied") count as dashboard failures. Default false: a user
# without access is expected behaviour, not a reliability problem. They are still reported as `authorization_denied`.
AUTHZ_COUNTS_AS_FAILURE = os.environ.get("AUTHZ_COUNTS_AS_FAILURE", "false").strip().lower() in ("1", "true", "yes", "on")
SQL_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "schema_iceberg")
TABLES = ["fact_access_events", "fact_error_events", "dim_dashboards"]


def _statements(path):
    # drop whole-line and trailing "-- comments" first, so a ';' inside a comment cannot split a statement
    lines = [l.split("--", 1)[0] for l in open(path, encoding="utf-8").read().splitlines()]
    text = "\n".join(lines).replace("{{AUTHZ_COUNTS}}", "true" if AUTHZ_COUNTS_AS_FAILURE else "false")
    return [s.strip() for s in text.split(";") if s.strip()]


def main():
    if not enabled():
        print("ICEBERG_SINK_ENABLED is off; nothing to do")
        return 0
    conn = trino_conn(schema=None)
    version = run_sql(conn, "SELECT node_version FROM system.runtime.nodes LIMIT 1")[0][0]
    print("Trino version: %s (catalog %s, schema %s)" % (version, CATALOG, SCHEMA))

    run_sql(conn, "CREATE SCHEMA IF NOT EXISTS %s.%s" % (CATALOG, SCHEMA))
    conn = trino_conn()
    view_ok = True
    for path in sorted(glob.glob(os.path.join(SQL_DIR, "*.sql"))):
        is_view = "vw_" in os.path.basename(path)
        for stmt in _statements(path):
            try:
                run_sql(conn, stmt)
            except Exception as exc:  # noqa: BLE001
                if is_view:
                    view_ok = False
                    print("WARNING: could not create view (%s): %s" % (os.path.basename(path), str(exc)[:200]))
                    print("         Superset will use the same SQL as a virtual dataset instead.")
                else:
                    print("ERROR creating objects from %s: %s" % (os.path.basename(path), exc), file=sys.stderr)
                    return 1

    bad = []
    for t in TABLES:
        ddl = run_sql(conn, "SHOW CREATE TABLE %s" % t)[0][0]
        if "format_version = 2" not in ddl:
            bad.append(t)
    if bad:
        print("ERROR: tables not format-version 2 (MERGE needs it): %s" % ", ".join(bad), file=sys.stderr)
        return 1
    print("Iceberg objects ready; tables are format-version 2; view %s; authorization denials %s" % (
        "created" if view_ok else "NOT created",
        "count as failures" if AUTHZ_COUNTS_AS_FAILURE else "are reported separately"))
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""
Iceberg sink: writes the fact tables through Trino (`MERGE INTO`), so the pipeline needs no catalog or object
store credentials of its own. Trino is the only dependency, and it already exists in the target architecture.

Why MERGE: Iceberg has no unique keys, so idempotency is done in the statement. Re-reading the same log rows
(lookback overlap, reconciliation rescans, retries after a failed run) must never create duplicates.

Batching: rows are collected and written with one MERGE per table per chunk (ICEBERG_BATCH_ROWS, default 2000),
not one MERGE per API page. Every MERGE is a snapshot, so many small ones mean many small files.
Compaction (`ALTER TABLE ... EXECUTE optimize`) is a scheduled production task, see README.

Single writer: concurrent MERGEs on the same table conflict. sync_logs.py takes a MySQL advisory lock for this.
"""
import os
from datetime import datetime

CATALOG = os.environ.get("TRINO_CATALOG", "iceberg")
SCHEMA = os.environ.get("TRINO_SCHEMA", "monitoring")
BATCH_ROWS = int(os.environ.get("ICEBERG_BATCH_ROWS", "2000"))


def enabled():
    return os.environ.get("ICEBERG_SINK_ENABLED", "false").strip().lower() in ("1", "true", "yes", "on")


def trino_conn(schema=SCHEMA):
    import trino

    kwargs = {}
    if os.environ.get("TRINO_PASSWORD"):
        kwargs["auth"] = trino.auth.BasicAuthentication(
            os.environ.get("TRINO_USER", "pipeline"), os.environ["TRINO_PASSWORD"]
        )
        kwargs["http_scheme"] = "https"
    else:
        kwargs["http_scheme"] = os.environ.get("TRINO_HTTP_SCHEME", "http")
    return trino.dbapi.connect(
        host=os.environ["TRINO_HOST"],
        port=int(os.environ.get("TRINO_PORT", "8080")),
        user=os.environ.get("TRINO_USER", "pipeline"),
        catalog=CATALOG,
        schema=schema,
        **kwargs,
    )


def run_sql(conn, sql):
    cur = conn.cursor()
    cur.execute(sql)
    return cur.fetchall()  # must be consumed for the statement to finish


# ---------- literal helpers (values are built into the statement; strings are escaped here) ----------

def _lit(value):
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, datetime):
        return "TIMESTAMP '%s'" % value.strftime("%Y-%m-%d %H:%M:%S.%f")
    return "'%s'" % str(value).replace("'", "''")


def _values(rows, cols):
    return ",\n".join("(" + ", ".join(_lit(r.get(c)) for c in cols) + ")" for r in rows)


def _typed_source(rows, spec):
    """spec: list of (column, sql type). Returns a SELECT with every column cast, so an all-NULL column still has a type."""
    cols = [c for c, _ in spec]
    casts = ", ".join("CAST(%s AS %s) AS %s" % (c, t, c) for c, t in spec)
    return "SELECT %s FROM (VALUES\n%s\n) AS v(%s)" % (casts, _values(rows, cols), ", ".join(cols))


ACCESS_SPEC = [
    ("event_id", "bigint"), ("event_ts", "timestamp(6)"), ("user_id", "integer"), ("dashboard_id", "integer"),
    ("chart_id", "integer"), ("action", "varchar"), ("raw_log_id", "bigint"), ("duration_ms", "integer"),
    ("is_error", "boolean"),
]
ERROR_SPEC = [
    ("dedupe_key", "varchar"), ("event_ts", "timestamp(6)"), ("source", "varchar"), ("error_type", "varchar"),
    ("reason", "varchar"), ("user_id", "integer"), ("dashboard_id", "integer"), ("chart_id", "integer"),
    ("duration_ms", "integer"), ("raw_log_id", "bigint"),
    # cause-agnostic fields (see etl/error_classify.py): the raw message is always kept, the category and
    # signature make unknown causes groupable
    ("error_category", "varchar"), ("error_signature", "varchar"), ("error_message", "varchar"),
    ("status_code", "integer"), ("request_path", "varchar"), ("request_id", "varchar"),
]
DASH_SPEC = [("dashboard_id", "integer"), ("title", "varchar")]


def _insert_cols(spec):
    cols = [c for c, _ in spec]
    return "(" + ", ".join(cols) + ") VALUES (" + ", ".join("s." + c for c in cols) + ")"


def merge_access_sql(rows):
    # event_ts is part of the match so Iceberg can prune partitions; the same event always has the same timestamp.
    return (
        "MERGE INTO fact_access_events t USING (%s) s\n"
        "ON t.event_id = s.event_id AND t.event_ts = s.event_ts\n"
        "WHEN NOT MATCHED THEN INSERT %s" % (_typed_source(rows, ACCESS_SPEC), _insert_cols(ACCESS_SPEC))
    )


def merge_error_sql(rows):
    # Update only when the new row adds information: a row enriched with its request (user/dashboard/chart), or a row
    # written before the category columns existed. Unchanged rows are left alone, so the lookback overlap does not
    # rewrite files on every run.
    return (
        "MERGE INTO fact_error_events t USING (%s) s\n"
        "ON t.dedupe_key = s.dedupe_key AND t.event_ts = s.event_ts\n"
        "WHEN MATCHED AND ((t.user_id IS NULL AND s.user_id IS NOT NULL)\n"
        "               OR (t.dashboard_id IS NULL AND s.dashboard_id IS NOT NULL)\n"
        "               OR (t.chart_id IS NULL AND s.chart_id IS NOT NULL)\n"
        "               OR (t.error_category IS NULL AND s.error_category IS NOT NULL)) THEN UPDATE SET\n"
        "    reason = s.reason,\n"
        "    user_id = coalesce(s.user_id, t.user_id),\n"
        "    dashboard_id = coalesce(s.dashboard_id, t.dashboard_id),\n"
        "    chart_id = coalesce(s.chart_id, t.chart_id),\n"
        "    error_category = coalesce(s.error_category, t.error_category),\n"
        "    error_signature = coalesce(s.error_signature, t.error_signature),\n"
        "    error_message = coalesce(s.error_message, t.error_message),\n"
        "    status_code = coalesce(s.status_code, t.status_code),\n"
        "    request_path = coalesce(s.request_path, t.request_path),\n"
        "    request_id = coalesce(s.request_id, t.request_id)\n"
        "WHEN NOT MATCHED THEN INSERT %s" % (_typed_source(rows, ERROR_SPEC), _insert_cols(ERROR_SPEC))
    )


def merge_dashboards_sql(rows):
    return (
        "MERGE INTO dim_dashboards t USING (%s) s\n"
        "ON t.dashboard_id = s.dashboard_id\n"
        "WHEN MATCHED AND t.title IS DISTINCT FROM s.title THEN UPDATE SET title = s.title\n"
        "WHEN NOT MATCHED THEN INSERT %s" % (_typed_source(rows, DASH_SPEC), _insert_cols(DASH_SPEC))
    )


class IcebergSink:
    """Collects rows and writes them in batches. Call flush() before advancing any watermark."""

    def __init__(self, conn=None):
        self.conn = conn or trino_conn()
        self.access, self.errors, self.dashboards = {}, {}, {}
        self.written = {"access": 0, "errors": 0, "dashboards": 0}

    # rows are keyed so a duplicate inside one batch cannot insert twice
    def add_access(self, row):
        self.access[row["event_id"]] = row
        if len(self.access) >= BATCH_ROWS:
            self._flush_access()

    def add_error(self, row):
        self.errors[row["dedupe_key"]] = row
        if len(self.errors) >= BATCH_ROWS:
            self._flush_errors()

    def add_dashboard(self, row):
        self.dashboards[row["dashboard_id"]] = row

    def _flush_access(self):
        if self.access:
            run_sql(self.conn, merge_access_sql(list(self.access.values())))
            self.written["access"] += len(self.access)
            self.access = {}

    def _flush_errors(self):
        if self.errors:
            run_sql(self.conn, merge_error_sql(list(self.errors.values())))
            self.written["errors"] += len(self.errors)
            self.errors = {}

    def flush(self):
        self._flush_access()
        self._flush_errors()
        if self.dashboards:
            run_sql(self.conn, merge_dashboards_sql(list(self.dashboards.values())))
            self.written["dashboards"] += len(self.dashboards)
            self.dashboards = {}
        return dict(self.written)

"""
Iceberg sink: the pipeline's only data store. Everything is written through Trino (`MERGE INTO`), so the pipeline
needs no catalog or object store credentials of its own. Trino is the only dependency, and it already exists in the
target architecture.

Why MERGE: Iceberg has no unique keys, so idempotency is done in the statement. Re-reading the same log rows
(lookback overlap, reconciliation rescans, retries after a failed run) must never create duplicates.

Batching: rows are collected and written with one MERGE per table per chunk (ICEBERG_BATCH_ROWS, default 2000),
not one MERGE per API page. Every MERGE is a snapshot, so many small ones mean many small files. Raw log payloads are
large, so they are chunked by size too (ICEBERG_RAW_BATCH_ROWS / ICEBERG_RAW_BATCH_CHARS) to stay under Trino's
query length limit. Compaction is etl/iceberg_maintenance.py.

Ordering is the consistency model (Trino cannot commit two tables atomically): facts and dimensions are written first,
raw_log_landing LAST, and the watermark is read back from raw_log_landing. A crash anywhere leaves the watermark at
the last fully written point, and the next run repeats the work, which is harmless because every statement is idempotent.

Concurrent writers: Iceberg commits are optimistic, so two overlapping MERGEs on one table make one of them fail with a
commit conflict. run_sql() retries those with backoff (safe, the statements are idempotent), and etl/iceberg_state.py
adds a lease so overlapping runs normally never start.
"""
import os
import random
import time
from datetime import datetime

CATALOG = os.environ.get("TRINO_CATALOG", "iceberg")
SCHEMA = os.environ.get("TRINO_SCHEMA", "monitoring")
BATCH_ROWS = int(os.environ.get("ICEBERG_BATCH_ROWS", "2000"))
RAW_BATCH_ROWS = int(os.environ.get("ICEBERG_RAW_BATCH_ROWS", "300"))
RAW_BATCH_CHARS = int(os.environ.get("ICEBERG_RAW_BATCH_CHARS", "400000"))  # Trino rejects statements over ~1,000,000 chars
COMMIT_RETRIES = int(os.environ.get("ICEBERG_COMMIT_RETRIES", "6"))


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


def is_commit_conflict(exc):
    """True for the error Trino raises when another writer committed to the same Iceberg table first."""
    text = str(exc).lower()
    return any(m in text for m in ("failed to commit", "commit failed", "concurrent", "stale table metadata",
                                   "cannot commit", "iceberg_commit_error", "commitfailedexception"))


def run_sql(conn, sql, retries=None):
    """Runs a statement and returns its rows. Retries commit conflicts with exponential backoff and jitter: every
    statement this pipeline sends is idempotent, so repeating one is always safe."""
    attempts = (COMMIT_RETRIES if retries is None else retries) + 1
    for attempt in range(attempts):
        try:
            cur = conn.cursor()
            cur.execute(sql)
            return cur.fetchall()  # must be consumed for the statement to finish
        except Exception as exc:  # noqa: BLE001
            if attempt + 1 >= attempts or not is_commit_conflict(exc):
                raise
            time.sleep(min(30.0, 2.0 ** attempt) * (0.5 + random.random()))


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
DASH_SPEC = [("dashboard_id", "integer"), ("title", "varchar"), ("owner_names", "varchar"),
             ("created_on", "timestamp(6)"), ("changed_on", "timestamp(6)")]
CHART_SPEC = [("chart_id", "integer"), ("name", "varchar"), ("changed_on", "timestamp(6)")]
USER_SPEC = [("user_id", "integer"), ("username", "varchar"), ("first_name", "varchar"), ("last_name", "varchar"),
             ("email", "varchar"), ("is_active", "boolean")]
RAW_SPEC = [("source_log_id", "bigint"), ("source_dttm", "timestamp(6)"), ("payload", "varchar")]


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


RECLASSIFY_SPEC = [("dedupe_key", "varchar"), ("event_ts", "timestamp(6)"),
                    ("error_category", "varchar"), ("error_signature", "varchar")]


def merge_reclassify_sql(rows):
    """Rewrites error_category and error_signature on existing rows whose value differs (used after the classification
    rules change). Rows that already carry the right values are not touched."""
    return (
        "MERGE INTO fact_error_events t USING (%s) s\n"
        "ON t.dedupe_key = s.dedupe_key AND t.event_ts = s.event_ts\n"
        "WHEN MATCHED AND (t.error_category IS DISTINCT FROM s.error_category\n"
        "              OR t.error_signature IS DISTINCT FROM s.error_signature) THEN UPDATE SET\n"
        "    error_category = s.error_category, error_signature = s.error_signature" % _typed_source(rows, RECLASSIFY_SPEC)
    )


def merge_dashboards_sql(rows):
    return (
        "MERGE INTO dim_dashboards t USING (%s) s\n"
        "ON t.dashboard_id = s.dashboard_id\n"
        "WHEN MATCHED AND (t.title IS DISTINCT FROM s.title OR t.owner_names IS DISTINCT FROM s.owner_names\n"
        "              OR t.changed_on IS DISTINCT FROM s.changed_on) THEN UPDATE SET\n"
        "    title = s.title, owner_names = s.owner_names, created_on = s.created_on, changed_on = s.changed_on\n"
        "WHEN NOT MATCHED THEN INSERT %s" % (_typed_source(rows, DASH_SPEC), _insert_cols(DASH_SPEC))
    )


def merge_charts_sql(rows):
    return (
        "MERGE INTO dim_charts t USING (%s) s\n"
        "ON t.chart_id = s.chart_id\n"
        "WHEN MATCHED AND (t.name IS DISTINCT FROM s.name OR t.changed_on IS DISTINCT FROM s.changed_on) THEN UPDATE SET\n"
        "    name = s.name, changed_on = s.changed_on\n"
        "WHEN NOT MATCHED THEN INSERT %s" % (_typed_source(rows, CHART_SPEC), _insert_cols(CHART_SPEC))
    )


def merge_users_sql(rows):
    return (
        "MERGE INTO dim_users t USING (%s) s\n"
        "ON t.user_id = s.user_id\n"
        "WHEN MATCHED AND (t.username IS DISTINCT FROM s.username OR t.first_name IS DISTINCT FROM s.first_name\n"
        "              OR t.last_name IS DISTINCT FROM s.last_name OR t.email IS DISTINCT FROM s.email\n"
        "              OR t.is_active IS DISTINCT FROM s.is_active) THEN UPDATE SET\n"
        "    username = s.username, first_name = s.first_name, last_name = s.last_name, email = s.email,\n"
        "    is_active = s.is_active\n"
        "WHEN NOT MATCHED THEN INSERT %s" % (_typed_source(rows, USER_SPEC), _insert_cols(USER_SPEC))
    )


def merge_raw_sql(rows):
    # source_dttm is part of the match so Iceberg can prune partitions; the same log row always has the same dttm.
    cols = [c for c, _ in RAW_SPEC]
    return (
        "MERGE INTO raw_log_landing t USING (%s) s\n"
        "ON t.source_log_id = s.source_log_id AND t.source_dttm = s.source_dttm\n"
        "WHEN NOT MATCHED THEN INSERT (%s, ingested_at) VALUES (%s, CAST(current_timestamp AT TIME ZONE 'UTC' AS timestamp(6)))"
        % (_typed_source(rows, RAW_SPEC), ", ".join(cols), ", ".join("s." + c for c in cols))
    )


class IcebergSink:
    """Collects rows and writes them in batches. Call flush() before advancing any watermark.

    Write order inside a run (see the module docstring): facts and dimensions first, raw_log_landing last. A raw chunk
    is therefore only ever written after the facts and dimensions collected so far."""

    def __init__(self, conn=None, on_progress=None):
        self.conn = conn or trino_conn()
        self.on_progress = on_progress  # called after every statement, e.g. to renew the single-writer lease
        self.access, self.errors, self.dashboards, self.users, self.raw = {}, {}, {}, {}, {}
        self.charts = {}
        self.raw_chars = 0
        self.written = {"access": 0, "errors": 0, "dashboards": 0, "charts": 0, "users": 0, "raw": 0}

    def _run(self, sql):
        run_sql(self.conn, sql)
        if self.on_progress:
            self.on_progress()

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

    def add_chart(self, row):
        self.charts[row["chart_id"]] = row

    def add_user(self, row):
        self.users[row["user_id"]] = row

    def add_raw(self, row):
        self.raw[row["source_log_id"]] = row
        self.raw_chars += len(row["payload"])
        if len(self.raw) >= RAW_BATCH_ROWS or self.raw_chars >= RAW_BATCH_CHARS:
            self._flush_raw()

    def _flush_access(self):
        if self.access:
            self._run(merge_access_sql(list(self.access.values())))
            self.written["access"] += len(self.access)
            self.access = {}

    def _flush_errors(self):
        if self.errors:
            self._run(merge_error_sql(list(self.errors.values())))
            self.written["errors"] += len(self.errors)
            self.errors = {}

    def _flush_dims(self):
        if self.dashboards:
            self._run(merge_dashboards_sql(list(self.dashboards.values())))
            self.written["dashboards"] += len(self.dashboards)
            self.dashboards = {}
        if self.charts:
            self._run(merge_charts_sql(list(self.charts.values())))
            self.written["charts"] += len(self.charts)
            self.charts = {}
        if self.users:
            self._run(merge_users_sql(list(self.users.values())))
            self.written["users"] += len(self.users)
            self.users = {}

    def _flush_raw(self):
        if self.raw:
            # everything derived from these log rows must be stored before the rows themselves are
            self._flush_access()
            self._flush_errors()
            self._flush_dims()
            self._run(merge_raw_sql(list(self.raw.values())))
            self.written["raw"] += len(self.raw)
            self.raw, self.raw_chars = {}, 0

    def reclassify(self, rows):
        """Apply new category/signature values to existing error rows, in batches. Returns rows sent."""
        sent = 0
        for i in range(0, len(rows), BATCH_ROWS):
            chunk = rows[i:i + BATCH_ROWS]
            self._run(merge_reclassify_sql(chunk))
            sent += len(chunk)
        return sent

    def flush(self):
        self._flush_access()
        self._flush_errors()
        self._flush_dims()
        self._flush_raw()
        return dict(self.written)

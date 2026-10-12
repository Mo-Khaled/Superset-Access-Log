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
ENTITY_SPEC = [("entity_type", "varchar"), ("entity_id", "integer"), ("name", "varchar"), ("owner_names", "varchar"),
               ("created_on", "timestamp(6)"), ("changed_on", "timestamp(6)")]
LOG_SPEC = [("line_id", "varchar"), ("event_ts", "timestamp(6)"), ("service", "varchar"), ("level", "varchar"),
            ("kind", "varchar"), ("pid", "integer"), ("request_id", "varchar"), ("user_id", "integer"),
            ("dashboard_id", "integer"), ("chart_id", "integer"), ("method", "varchar"), ("url_path", "varchar"),
            ("status_code", "integer"), ("duration_ms", "integer"), ("is_error", "boolean"), ("message", "varchar")]
LOG_EXTRACT_COLS = ["level", "kind", "pid", "request_id", "user_id", "dashboard_id", "chart_id", "method", "url_path",
                    "status_code", "duration_ms", "is_error"]
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


def merge_entities_sql(rows):
    return (
        "MERGE INTO dim_entities t USING (%s) s\n"
        "ON t.entity_type = s.entity_type AND t.entity_id = s.entity_id\n"
        "WHEN MATCHED AND (t.name IS DISTINCT FROM s.name OR t.owner_names IS DISTINCT FROM s.owner_names\n"
        "              OR t.changed_on IS DISTINCT FROM s.changed_on) THEN UPDATE SET\n"
        "    name = s.name, owner_names = s.owner_names, created_on = s.created_on, changed_on = s.changed_on\n"
        "WHEN NOT MATCHED THEN INSERT %s" % (_typed_source(rows, ENTITY_SPEC), _insert_cols(ENTITY_SPEC))
    )


def merge_log_lines_sql(rows):
    cols = [c for c, _ in LOG_SPEC]
    return (
        "MERGE INTO container_log_lines t USING (%s) s\n"
        "ON t.line_id = s.line_id AND t.event_ts = s.event_ts\n"
        "WHEN NOT MATCHED THEN INSERT (%s, ingested_at) VALUES (%s, CAST(current_timestamp AT TIME ZONE 'UTC' AS timestamp(6)))"
        % (_typed_source(rows, LOG_SPEC), ", ".join(cols), ", ".join("s." + c for c in cols))
    )


def merge_log_extract_sql(rows):
    """Rewrites the parsed columns of existing container_log_lines rows (used by the backfill after the parsing changes)."""
    diff = " OR ".join("t.%s IS DISTINCT FROM s.%s" % (c, c) for c in LOG_EXTRACT_COLS)
    sets = ", ".join("%s = s.%s" % (c, c) for c in LOG_EXTRACT_COLS)
    return (
        "MERGE INTO container_log_lines t USING (%s) s\n"
        "ON t.line_id = s.line_id AND t.event_ts = s.event_ts\n"
        "WHEN MATCHED AND (%s) THEN UPDATE SET %s" % (_typed_source(rows, LOG_SPEC), diff, sets)
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
        self.access, self.errors, self.entities, self.raw = {}, {}, {}, {}
        self.log_lines, self.log_chars = {}, 0
        self.raw_chars = 0
        self.written = {"access": 0, "errors": 0, "entities": 0, "raw": 0, "log_lines": 0}

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

    def add_entity(self, entity_type, entity_id, name, owner_names=None, created_on=None, changed_on=None):
        self.entities[(entity_type, entity_id)] = {
            "entity_type": entity_type, "entity_id": entity_id, "name": name, "owner_names": owner_names,
            "created_on": created_on, "changed_on": changed_on}

    def add_dashboard(self, row):
        self.add_entity("dashboard", row["dashboard_id"], row["title"], row.get("owner_names"), row.get("created_on"),
                        row.get("changed_on"))

    def add_chart(self, row):
        self.add_entity("chart", row["chart_id"], row["name"], changed_on=row.get("changed_on"))

    def add_user(self, row):
        self.add_entity("user", row["user_id"], row["username"])

    def add_log_line(self, row):
        self.log_lines[row["line_id"]] = row
        self.log_chars += len(row["message"])
        if len(self.log_lines) >= RAW_BATCH_ROWS or self.log_chars >= RAW_BATCH_CHARS:
            self._flush_log_lines()

    def _flush_log_lines(self):
        if self.log_lines:
            self._run(merge_log_lines_sql(list(self.log_lines.values())))
            self.written["log_lines"] += len(self.log_lines)
            self.log_lines, self.log_chars = {}, 0

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
        if self.entities:
            self._run(merge_entities_sql(list(self.entities.values())))
            self.written["entities"] += len(self.entities)
            self.entities = {}

    def _flush_raw(self):
        if self.raw:
            # everything derived from these log rows must be stored before the rows themselves are
            self._flush_access()
            self._flush_errors()
            self._flush_dims()
            self._run(merge_raw_sql(list(self.raw.values())))
            self.written["raw"] += len(self.raw)
            self.raw, self.raw_chars = {}, 0

    def update_log_columns(self, rows):
        """Rewrite the parsed columns on existing container_log_lines rows, in batches. Returns rows sent."""
        sent = 0
        for i in range(0, len(rows), RAW_BATCH_ROWS):
            chunk = rows[i:i + RAW_BATCH_ROWS]
            self._run(merge_log_extract_sql(chunk))
            sent += len(chunk)
        return sent

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
        self._flush_log_lines()
        return dict(self.written)

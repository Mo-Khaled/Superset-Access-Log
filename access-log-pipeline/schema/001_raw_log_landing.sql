-- Raw landing table: one row per LogRestApi record, verbatim, before any transform.
-- Kept so we can re-derive dim_/fact_ tables if transform logic changes later,
-- without re-hitting the Superset API (which only retains logs per its own retention
-- config -- once they age out on the Superset side, this table is the only copy left).
CREATE TABLE IF NOT EXISTS raw_log_landing (
    source_log_id   BIGINT UNSIGNED NOT NULL,
    payload         JSON NOT NULL,
    ingested_at     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (source_log_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

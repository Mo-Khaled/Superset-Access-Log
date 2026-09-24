-- High-water-mark tracking so re-running the ETL is incremental, not a full re-pull.
-- Single row keyed by a fixed source name (only one source -- the local Superset --
-- today, but keyed anyway so a second Superset source could be added without a
-- schema change).
CREATE TABLE IF NOT EXISTS etl_sync_state (
    source_name         VARCHAR(100) NOT NULL,
    last_synced_log_id  BIGINT UNSIGNED NOT NULL DEFAULT 0,
    last_synced_ts       DATETIME NULL,
    updated_at           DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (source_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT IGNORE INTO etl_sync_state (source_name, last_synced_log_id, last_synced_ts)
VALUES ('local_superset', 0, NULL);

-- Errors observed around Superset chart/dashboard loads, with the reason.
--
-- Why a separate table: fact_access_events deliberately counts every load_chart
-- as a chart_view (usage ranking), so a failed load looks like a normal view
-- there. This table records the failure itself and why it happened.
--
-- Two sources feed it (see etl/sync_logs.py and etl/ingest_container_errors.py):
--   source = 'browser'  : a load_chart row in Superset's logs table with
--                         has_err = true. Has user/dashboard/chart, but the
--                         reason is only what the browser knew ("timeout").
--   source = 'gunicorn' : a "WORKER TIMEOUT" line from the superset container's
--                         stdout. Real server-side reason, but no user/dashboard/
--                         chart (the line only carries a worker pid and a time).
-- Join the two on event_ts (within a few seconds) to see "who hit it and why".
--
-- dedupe_key makes re-running either ingest idempotent.
CREATE TABLE IF NOT EXISTS fact_error_events (
    error_id      BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    event_ts      DATETIME NOT NULL,
    source        VARCHAR(20) NOT NULL,
    error_type    VARCHAR(50) NOT NULL,
    reason        VARCHAR(500) NOT NULL,
    user_id       INT UNSIGNED NULL,
    dashboard_id  INT UNSIGNED NULL,
    chart_id      INT UNSIGNED NULL,
    duration_ms   INT UNSIGNED NULL,
    raw_log_id    BIGINT UNSIGNED NULL,
    dedupe_key    VARCHAR(100) NOT NULL,
    PRIMARY KEY (error_id),
    UNIQUE KEY uq_dedupe (dedupe_key),
    KEY idx_ts (event_ts),
    KEY idx_dashboard_ts (dashboard_id, event_ts)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Iceberg (Trino) copy of the usage facts. One row per dashboard view, chart view or export.
-- Idempotent loads: the ETL runs MERGE ... ON (event_id, event_ts) WHEN NOT MATCHED THEN INSERT.
-- Partitioned by day so day/week/month queries prune files. format_version 2 is required for MERGE.
--
-- duration_ms: browser-measured load time of a chart view (NULL for dashboard views).
-- is_error:    the same client event reported a failed load (has_err). Failed loads are excluded from latency.
CREATE TABLE IF NOT EXISTS fact_access_events (
    event_id     bigint,
    event_ts     timestamp(6),
    user_id      integer,
    dashboard_id integer,
    chart_id     integer,
    action       varchar,
    raw_log_id   bigint,
    duration_ms  integer,
    is_error     boolean
)
WITH (format = 'PARQUET', format_version = 2, partitioning = ARRAY['day(event_ts)'])

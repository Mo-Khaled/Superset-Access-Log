-- Failures observed around chart loads, with the reason.
--   source = 'browser'  : a load_chart row in Superset's logs table with has_err = true
--   source = 'gunicorn' : a gunicorn arbiter problem (worker timeout, killed worker, ...), joined to its request via the worker pid
--   source = 'server'   : a dashboard/chart request that ended with HTTP >= 400 (status, message, exception class)
--   source = 'celery'   : a failed background task
-- dedupe_key is the merge key: 'browser:<log id>' or 'gunicorn:<timestamp>:<pid>'.
CREATE TABLE IF NOT EXISTS fact_error_events (
    dedupe_key   varchar,
    event_ts     timestamp(6),
    source       varchar,
    error_type   varchar,
    reason       varchar,
    user_id      integer,
    dashboard_id integer,
    chart_id     integer,
    duration_ms  integer,
    raw_log_id   bigint,
    -- cause-agnostic fields: we cannot know in advance which errors production will produce
    error_category  varchar,   -- coarse kind (timeout, query_error, permission, ..., unclassified); see etl/error_classify.py
    error_signature varchar,   -- message with numbers/ids/quoted values removed, to group the same error
    error_message   varchar,   -- the raw message, never dropped
    status_code     integer,   -- HTTP status when the error came from a request
    request_path    varchar,
    request_id      varchar
)
WITH (format = 'PARQUET', format_version = 2, partitioning = ARRAY['day(event_ts)'])

-- Every container log line the ETL has seen (superset, superset-worker, ...), so the raw logs can be queried in Trino.
-- Fed by etl/ingest_container_errors.py from `docker compose logs --timestamps`; the error rows in fact_error_events are
-- derived from the same lines. One row per line, merged on line_id (a hash of service + docker timestamp + text), so
-- re-reading an overlapping log window never duplicates. Pruned by etl/iceberg_maintenance.py (LOG_LINES_RETENTION_DAYS).
-- Health checks and the ETL's own calls to Superset (log API polling, user lookup) are not stored (LOG_DROP_PATHS).
--   kind        reqtrace | reqend | reqexc (added by superset_config.py), gunicorn, celery, access (HTTP access line), app
--   level       the line's own level when it has one (CRITICAL, ERROR, WARNING, INFO, DEBUG)
--   request_id  ties a request's lines together (reqtrace, reqend, reqexc); also set on a gunicorn timeout / killed worker,
--               resolved through the worker pid to the request it was serving
--   user_id, dashboard_id, chart_id, method, url_path   the request, for reqtrace / reqend / access and those gunicorn lines
--   status_code, duration_ms            the response, for reqend and access lines
--   is_error    true for a failed request (reqend, HTTP 400+), an unhandled exception, an access line with HTTP 5xx, an ERROR /
--               CRITICAL line, a worker timeout or a killed worker
CREATE TABLE IF NOT EXISTS container_log_lines (
    line_id      varchar,
    event_ts     timestamp(6),
    service      varchar,
    level        varchar,
    kind         varchar,
    pid          integer,
    request_id   varchar,
    user_id      integer,
    dashboard_id integer,
    chart_id     integer,
    method       varchar,
    url_path     varchar,
    status_code  integer,
    duration_ms  integer,
    is_error     boolean,
    message      varchar,
    ingested_at  timestamp(6)
)
WITH (format = 'PARQUET', format_version = 2, partitioning = ARRAY['day(event_ts)'])

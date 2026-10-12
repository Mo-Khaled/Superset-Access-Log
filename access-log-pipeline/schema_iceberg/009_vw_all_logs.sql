-- ALL the logs in one view: Superset's metadata log (parsed out of raw_log_landing) and the container logs, with the same
-- columns. Order by event_ts yourself. To see only failures: WHERE is_error. The complete original Superset row is in
-- raw_log_landing.payload.
--   source / component   'superset_logs' / 'logs table', or 'container' / the service (superset, superset-worker, ...)
--   kind      Superset rows: the browser event name when there is one (load_chart, mount_dashboard, ...), else the action
--             (ChartDataRestApi.data, ...). Container rows: reqtrace | reqend | reqexc | gunicorn | celery | access | app
--   level     container rows: the line's own level; Superset rows: ERROR when the browser reported a failed chart load
--   is_error  the flag to filter failures on: a failed browser chart load, an HTTP 400+ response on a dashboard/chart path,
--             an unhandled exception, an HTTP 5xx, an ERROR / CRITICAL line, a worker timeout or a killed worker
--   user_id, dashboard_id, chart_id and their names   Superset rows, and container request lines (a gunicorn timeout is tied
--             to the request its worker was serving). Never empty names: '(not from a dashboard)' for a request opened in
--             Explore or called through the API, '(no chart)', '(no user)', and 'Chart 387 (removed)' etc. for an id that no
--             longer exists in Superset
--   request_id, pid, method, url_path, status_code   container rows only
--   message   the container line, or for Superset rows the action, event and error text
CREATE OR REPLACE VIEW vw_all_logs AS
WITH superset AS (
    SELECT
        source_dttm AS event_ts,
        json_extract_scalar(payload, '$.action') AS action,
        json_extract_scalar(json_extract_scalar(payload, '$.json'), '$.event_name') AS event_name,
        try_cast(json_extract_scalar(payload, '$.user_id') AS integer) AS user_id,
        try_cast(json_extract_scalar(payload, '$.dashboard_id') AS integer) AS dashboard_id,
        try_cast(json_extract_scalar(payload, '$.slice_id') AS integer) AS chart_id,
        try_cast(json_extract_scalar(payload, '$.duration_ms') AS integer) AS duration_ms,
        try_cast(json_extract_scalar(json_extract_scalar(payload, '$.json'), '$.has_err') AS boolean) AS has_err,
        json_extract_scalar(json_extract_scalar(payload, '$.json'), '$.error_details') AS error_details
    FROM raw_log_landing
),
logs AS (
    SELECT
        event_ts,
        'superset_logs' AS source,
        'logs table' AS component,
        coalesce(event_name, action) AS kind,
        CASE WHEN has_err THEN 'ERROR' END AS level,
        coalesce(has_err, false) AS is_error,
        user_id,
        dashboard_id,
        chart_id,
        CAST(NULL AS varchar) AS request_id,
        CAST(NULL AS integer) AS pid,
        CAST(NULL AS varchar) AS method,
        CAST(NULL AS varchar) AS url_path,
        CAST(NULL AS integer) AS status_code,
        duration_ms,
        action || coalesce(' / ' || event_name, '') || coalesce(': ' || error_details, '') AS message
    FROM superset
    UNION ALL
    SELECT
        event_ts,
        'container' AS source,
        service AS component,
        kind,
        level,
        coalesce(is_error, false) AS is_error,
        user_id,
        dashboard_id,
        chart_id,
        request_id,
        pid,
        method,
        url_path,
        status_code,
        duration_ms,
        message
    FROM container_log_lines
)
SELECT
    l.event_ts,
    l.source,
    l.component,
    l.kind,
    l.level,
    l.is_error,
    l.user_id,
    coalesce(u.name, CASE WHEN l.user_id IS NULL THEN '(no user)' ELSE 'user_' || CAST(l.user_id AS varchar) END) AS username,
    l.dashboard_id,
    coalesce(d.name, CASE WHEN l.dashboard_id IS NULL THEN '(not from a dashboard)'
                          ELSE 'Dashboard ' || CAST(l.dashboard_id AS varchar) || ' (removed)' END) AS dashboard,
    l.chart_id,
    coalesce(c.name, CASE WHEN l.chart_id IS NULL THEN '(no chart)'
                          ELSE 'Chart ' || CAST(l.chart_id AS varchar) || ' (removed)' END) AS chart,
    l.request_id,
    l.pid,
    l.method,
    l.url_path,
    l.status_code,
    l.duration_ms,
    l.message
FROM logs l
LEFT JOIN dim_entities u ON u.entity_type = 'user' AND u.entity_id = l.user_id
LEFT JOIN dim_entities d ON d.entity_type = 'dashboard' AND d.entity_id = l.dashboard_id
LEFT JOIN dim_entities c ON c.entity_type = 'chart' AND c.entity_id = l.chart_id

-- Day grain, per dashboard. Superset sums these and applies its own time grain (day, week, month), so the rate is
-- SUM(failed_loads) / SUM(rated_loads): a ratio of sums, not an average of daily ratios.
--
--   chart_loads   = chart views (every chart load attempt, including failed ones)
--   failed_loads  = browser-reported failures: the same client event as a chart view, so numerator and denominator share
--                   a unit. Authorization denials are left out unless AUTHZ_COUNTS_AS_FAILURE is true (see below).
--   rated_loads   = the denominator of the failure rate: chart_loads, minus the loads that were denied when denials are
--                   not counted as failures (a denied load is neither a success nor a failure of the dashboard)
--   authorization_denied = permission-category failures (HTTP 401/403, "access denied", ...), as the larger of what the
--                   browser and the server each saw, so one denial seen by both is not counted twice
--   server_kills  = gunicorn worker problems (timeouts, killed workers), kept separate
--   server_errors = every server-side failure of any cause on a dashboard/chart request (source server, gunicorn, celery),
--                   again without denials by default. Kept separate from failed_loads: the browser may report the same
--                   failure, so adding them would double count.
--
-- AUTHZ_COUNTS_AS_FAILURE (env, default false) is substituted into {{AUTHZ_COUNTS}} by etl/iceberg_init.py, which
-- re-creates this view on every run, so changing the setting takes effect on the next cycle.
-- Rows dated in the future are excluded, so a bad timestamp can never create or distort a day.
CREATE OR REPLACE VIEW vw_chart_failure_rate AS
WITH loads AS (
    SELECT date(event_ts) AS day, dashboard_id, count(*) AS chart_loads
    FROM fact_access_events
    WHERE action = 'chart_view'
      AND event_ts <= CAST(current_timestamp AT TIME ZONE 'UTC' AS timestamp(6))
    GROUP BY 1, 2
),
errs AS (
    SELECT date(event_ts) AS day, dashboard_id,
           count_if(source = 'browser' AND (error_category IS DISTINCT FROM 'permission' OR {{AUTHZ_COUNTS}})) AS failed_loads,
           count_if(source = 'browser' AND error_category = 'permission') AS browser_denied,
           count_if(source = 'server' AND error_category = 'permission') AS server_denied,
           count_if(source = 'gunicorn') AS server_kills,
           count_if(source IN ('server', 'gunicorn', 'celery')
                    AND (error_category IS DISTINCT FROM 'permission' OR {{AUTHZ_COUNTS}})) AS server_errors
    FROM fact_error_events
    WHERE event_ts <= CAST(current_timestamp AT TIME ZONE 'UTC' AS timestamp(6))
    GROUP BY 1, 2
)
SELECT
    coalesce(l.day, e.day) AS day,
    coalesce(l.dashboard_id, e.dashboard_id) AS dashboard_id,
    d.title AS title,
    coalesce(l.chart_loads, 0) AS chart_loads,
    greatest(coalesce(l.chart_loads, 0) - CASE WHEN {{AUTHZ_COUNTS}} THEN 0 ELSE coalesce(e.browser_denied, 0) END, 0) AS rated_loads,
    coalesce(e.failed_loads, 0) AS failed_loads,
    greatest(coalesce(e.browser_denied, 0), coalesce(e.server_denied, 0)) AS authorization_denied,
    coalesce(e.server_kills, 0) AS server_kills,
    coalesce(e.server_errors, 0) AS server_errors
FROM loads l
FULL OUTER JOIN errs e
    ON l.day = e.day AND l.dashboard_id IS NOT DISTINCT FROM e.dashboard_id
LEFT JOIN dim_dashboards d
    ON d.dashboard_id = coalesce(l.dashboard_id, e.dashboard_id)

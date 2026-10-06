-- Day grain, per dashboard. Superset sums these and applies its own time grain (day, week, month), so the rate is
-- SUM(failed_loads) / SUM(chart_loads): a ratio of sums, not an average of daily ratios.
--
--   chart_loads  = chart views (every chart load attempt, including failed ones)
--   failed_loads = browser-reported failures: the same client event as a chart view, so numerator and denominator share a unit
--   server_kills = gunicorn worker problems (timeouts, killed workers), kept separate
--   server_errors = every server-side failure of any cause on a dashboard/chart request (source server, gunicorn, celery).
--                   Kept separate from failed_loads: the browser may report the same failure, so adding them would double count.
--
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
           count_if(source = 'browser') AS failed_loads,
           count_if(source = 'gunicorn') AS server_kills,
           count_if(source IN ('server', 'gunicorn', 'celery')) AS server_errors
    FROM fact_error_events
    WHERE event_ts <= CAST(current_timestamp AT TIME ZONE 'UTC' AS timestamp(6))
    GROUP BY 1, 2
)
SELECT
    coalesce(l.day, e.day) AS day,
    coalesce(l.dashboard_id, e.dashboard_id) AS dashboard_id,
    d.title AS title,
    coalesce(l.chart_loads, 0) AS chart_loads,
    coalesce(e.failed_loads, 0) AS failed_loads,
    coalesce(e.server_kills, 0) AS server_kills,
    coalesce(e.server_errors, 0) AS server_errors
FROM loads l
FULL OUTER JOIN errs e
    ON l.day = e.day AND l.dashboard_id IS NOT DISTINCT FROM e.dashboard_id
LEFT JOIN dim_dashboards d
    ON d.dashboard_id = coalesce(l.dashboard_id, e.dashboard_id)

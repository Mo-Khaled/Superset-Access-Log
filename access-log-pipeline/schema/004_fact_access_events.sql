-- Fact table: one row per classified access event pulled from LogRestApi.
--
-- Grain: one row per Superset log record that we classify into an action we care
-- about. Superset logs a distinct event per HTTP call it instruments, so a single
-- dashboard page load typically produces:
--   - one 'dashboard_view' event (action = 'dashboard_load' in Superset's raw log)
--   - N 'chart_view' events, one per chart tile rendered on that dashboard
--     (action = 'chart_data' / 'query' in Superset's raw log)
-- We keep dashboard-level and chart-level events as SEPARATE rows (not collapsed
-- into one "dashboard visit") because they answer different questions: dashboard_view
-- answers "who opened this dashboard", chart_view answers "which underlying charts
-- are actually being looked at" (useful when a dashboard is opened but a user only
-- ever looks at 2 of its 10 charts). The standalone usage dashboard's "most visited
-- dashboards" ranking uses dashboard_view rows only, to avoid a 10-chart dashboard
-- looking 10x more "popular" than a 1-chart one.
--
-- Deduplication: source_log_id (from raw_log_landing) is unique per Superset log
-- row, so re-running the ETL against overlapping API pages is naturally idempotent
-- via the PRIMARY KEY on event_id (mirrors source_log_id 1:1) -- no separate dedupe
-- pass needed. We do NOT attempt to collapse rapid repeat views (e.g. a user
-- refreshing) into one event: each is a real event, and "unique viewers" (a
-- separate COUNT DISTINCT user_id query) is how the dashboard avoids over-counting
-- a single active user as broad adoption.
CREATE TABLE IF NOT EXISTS fact_access_events (
    event_id       BIGINT UNSIGNED NOT NULL,
    event_ts       DATETIME NOT NULL,
    user_id        INT UNSIGNED,
    dashboard_id   INT UNSIGNED,
    chart_id       INT UNSIGNED,
    action         VARCHAR(50) NOT NULL,
    source_ip      VARCHAR(45),
    raw_log_id     BIGINT UNSIGNED NOT NULL,
    PRIMARY KEY (event_id),
    KEY idx_dashboard_ts (dashboard_id, event_ts),
    KEY idx_user_ts (user_id, event_ts),
    KEY idx_action_ts (action, event_ts)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

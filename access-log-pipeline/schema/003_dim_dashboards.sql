-- Dimension: Superset dashboards. Upserted from /api/v1/dashboard/ so the usage
-- dashboard can show titles/owners instead of bare dashboard ids, and so
-- "zero/near-zero views" candidates (schema note: a dashboard with no fact rows at
-- all) can still be listed by joining from this table rather than only from activity.
CREATE TABLE IF NOT EXISTS dim_dashboards (
    dashboard_id   INT UNSIGNED NOT NULL,
    title          VARCHAR(500) NOT NULL,
    owner_names    VARCHAR(500),
    created_on     DATETIME,
    changed_on     DATETIME,
    updated_at     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (dashboard_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

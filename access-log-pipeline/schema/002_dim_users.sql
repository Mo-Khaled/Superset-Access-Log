-- Dimension: Superset users. Upserted from /api/v1/user/ (or embedded log fields as a
-- fallback) so fact rows don't need to carry raw usernames -- keeps the dashboard
-- readable (names, not bare ids) and lets a user's display name change without
-- rewriting history in the fact table.
CREATE TABLE IF NOT EXISTS dim_users (
    user_id       INT UNSIGNED NOT NULL,
    username      VARCHAR(255) NOT NULL,
    first_name    VARCHAR(255),
    last_name     VARCHAR(255),
    email         VARCHAR(255),
    is_active     BOOLEAN,
    updated_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

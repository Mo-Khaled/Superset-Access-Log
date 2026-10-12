-- One small dimension for the Superset objects the other tables refer to by id, so reports show names instead of ids.
-- Synced from Superset's API (dashboards, charts) and from the log rows (users) on every run, merged on (entity_type, entity_id).
--   entity_type  'dashboard' | 'chart' | 'user'
--   name         dashboard title, chart name, or username
--   owner_names  dashboards only
--   created_on, changed_on   when Superset reports them (dashboards, charts)
CREATE TABLE IF NOT EXISTS dim_entities (
    entity_type varchar,
    entity_id   integer,
    name        varchar,
    owner_names varchar,
    created_on  timestamp(6),
    changed_on  timestamp(6)
)
WITH (format = 'PARQUET', format_version = 2)

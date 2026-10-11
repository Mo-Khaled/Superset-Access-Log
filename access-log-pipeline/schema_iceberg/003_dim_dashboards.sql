-- Dashboard dimension, so charts can show titles and owners. Merged on every run.
CREATE TABLE IF NOT EXISTS dim_dashboards (
    dashboard_id integer,
    title        varchar,
    owner_names  varchar,
    created_on   timestamp(6),
    changed_on   timestamp(6)
)
WITH (format = 'PARQUET', format_version = 2)

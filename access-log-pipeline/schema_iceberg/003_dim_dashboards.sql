-- Small dimension so charts can show dashboard titles. Merged on every run.
CREATE TABLE IF NOT EXISTS dim_dashboards (
    dashboard_id integer,
    title        varchar
)
WITH (format = 'PARQUET', format_version = 2)

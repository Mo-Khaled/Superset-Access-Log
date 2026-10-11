-- Chart dimension, so usage charts can show chart names instead of bare ids. Merged on every run.
CREATE TABLE IF NOT EXISTS dim_charts (
    chart_id   integer,
    name       varchar,
    changed_on timestamp(6)
)
WITH (format = 'PARQUET', format_version = 2)

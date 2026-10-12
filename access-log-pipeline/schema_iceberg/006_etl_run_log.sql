-- One row per ETL run (append only): when it ran, how it ended and what it wrote. It is the thing to alert on:
-- "no row with status 'ok' in the last N minutes" means the pipeline is stale.
-- Also records maintenance runs (kind = 'maintenance').
CREATE TABLE IF NOT EXISTS etl_run_log (
    run_ts      timestamp(6),
    kind        varchar,
    status      varchar,
    rows_read   bigint,
    rows_new    bigint,
    detail      varchar
)
WITH (format = 'PARQUET', format_version = 2, partitioning = ARRAY['day(run_ts)'])

-- Verbatim copy of every Superset log row, before any transform. Lets the facts be rebuilt when the transform logic
-- changes (etl/backfill_iceberg.py) without re-reading Superset, which only keeps logs for its own retention period.
-- It is also the ETL's memory: the watermark is max(source_dttm) here, and the reconciliation compares its row count
-- with Superset's. Written LAST in every run, after the facts, so a row present here has its facts too.
CREATE TABLE IF NOT EXISTS raw_log_landing (
    source_log_id bigint,
    source_dttm   timestamp(6),
    ingested_at   timestamp(6),
    payload       varchar
)
WITH (format = 'PARQUET', format_version = 2, partitioning = ARRAY['day(source_dttm)'])

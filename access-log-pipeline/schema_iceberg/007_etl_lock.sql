-- Single-writer lease (Iceberg has no locks). One row per lock name; see etl/iceberg_state.py.
-- A lease expires on its own (expires_at), so a crashed run can never block the pipeline for good.
CREATE TABLE IF NOT EXISTS etl_lock (
    lock_name   varchar,
    owner       varchar,
    acquired_at timestamp(6),
    expires_at  timestamp(6)
)
WITH (format = 'PARQUET', format_version = 2)

-- User dimension, so usage charts can show names instead of bare ids. Merged on every run.
CREATE TABLE IF NOT EXISTS dim_users (
    user_id    integer,
    username   varchar,
    first_name varchar,
    last_name  varchar,
    email      varchar,
    is_active  boolean
)
WITH (format = 'PARQUET', format_version = 2)

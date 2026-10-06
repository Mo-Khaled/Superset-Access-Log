-- Adds the cause-agnostic columns to a fact_error_events table created before they existed (schema evolution in place,
-- no data is rewritten). A no-op on a fresh install, where 002 already creates them.
ALTER TABLE fact_error_events ADD COLUMN IF NOT EXISTS error_category varchar;
ALTER TABLE fact_error_events ADD COLUMN IF NOT EXISTS error_signature varchar;
ALTER TABLE fact_error_events ADD COLUMN IF NOT EXISTS error_message varchar;
ALTER TABLE fact_error_events ADD COLUMN IF NOT EXISTS status_code integer;
ALTER TABLE fact_error_events ADD COLUMN IF NOT EXISTS request_path varchar;
ALTER TABLE fact_error_events ADD COLUMN IF NOT EXISTS request_id varchar

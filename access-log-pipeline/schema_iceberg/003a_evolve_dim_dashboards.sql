-- Adds the owner/date columns to a dim_dashboards table created before they existed. A no-op on a fresh install.
ALTER TABLE dim_dashboards ADD COLUMN IF NOT EXISTS owner_names varchar;
ALTER TABLE dim_dashboards ADD COLUMN IF NOT EXISTS created_on timestamp(6);
ALTER TABLE dim_dashboards ADD COLUMN IF NOT EXISTS changed_on timestamp(6)

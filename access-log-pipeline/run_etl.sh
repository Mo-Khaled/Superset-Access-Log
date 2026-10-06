#!/usr/bin/env bash
# One scheduled ETL cycle. Run from anywhere; cron calls this single script.
#   0. (Iceberg only) create/verify the Iceberg tables and view     (etl/iceberg_init.py)
#   1. Superset `logs` table (via LogRestApi) -> MySQL (+ Iceberg)    (etl/sync_logs.py)
#   2. gunicorn WORKER TIMEOUT lines from container stdout -> error table
#                                                                    (etl/ingest_container_errors.py)
# Step 2 re-reads a window slightly longer than the cron interval; dedupe_key makes the
# overlap harmless. Requires the `pipeline` container to be up (docker compose up -d).
# Single writer: steps 1 and 2 share a MySQL advisory lock, so two cycles can never write at once.
set -euo pipefail
cd "$(dirname "$0")/.."

SINCE="${ERROR_LOG_WINDOW:-15m}"   # keep > cron interval

docker compose exec -T pipeline python etl/iceberg_init.py
docker compose exec -T pipeline python etl/sync_logs.py
docker compose logs --no-log-prefix --since "$SINCE" superset superset-worker \
  | docker compose exec -T pipeline python etl/ingest_container_errors.py

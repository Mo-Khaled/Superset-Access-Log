#!/usr/bin/env bash
# One scheduled ETL cycle. Run from anywhere; cron calls this single script.
#   1. Superset `logs` table (via LogRestApi) -> MySQL        (etl/sync_logs.py)
#   2. gunicorn WORKER TIMEOUT lines from container stdout -> fact_error_events
#                                                             (etl/ingest_container_errors.py)
# Step 2 re-reads a window slightly longer than the cron interval; dedupe_key makes the
# overlap harmless. Requires the `pipeline` container to be up (docker compose up -d).
set -euo pipefail
cd "$(dirname "$0")/.."

SINCE="${ERROR_LOG_WINDOW:-15m}"   # keep > cron interval

docker compose exec -T pipeline python etl/sync_logs.py
docker compose logs --no-log-prefix --since "$SINCE" superset \
  | docker compose exec -T pipeline python etl/ingest_container_errors.py

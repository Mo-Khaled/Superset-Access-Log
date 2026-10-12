#!/usr/bin/env bash
# One scheduled ETL cycle. Run from anywhere; cron calls this single script.
#   0. create/verify the Iceberg tables and view                      (etl/iceberg_init.py)
#   1. Superset `logs` table (via LogRestApi) -> Iceberg              (etl/sync_logs.py)
#   2. container log lines -> container_log_lines, and the error lines among them -> fact_error_events
#                                                                    (etl/ingest_container_errors.py)
#   3. table maintenance: compaction, snapshot expiry, orphan files   (etl/iceberg_maintenance.py)
#      does real work about once a day (MAINTENANCE_INTERVAL_HOURS), a no-op in the other cycles
# Step 2 re-reads a window slightly longer than the cron interval; dedupe_key makes the
# overlap harmless. Requires the `pipeline` container to be up (docker compose up -d).
# Single writer: every step takes the same lease row in Iceberg (etl/iceberg_state.py), so two cycles
# never write at once. In production also set the scheduler to forbid overlapping runs
# (k8s CronJob concurrencyPolicy: Forbid, Airflow max_active_runs=1) -- the lease is the safety net.
# A step that fails does not stop the later ones, but the script exits non-zero so the scheduler alerts.
set -uo pipefail
cd "$(dirname "$0")/.."

SINCE="${ERROR_LOG_WINDOW:-15m}"   # keep > cron interval
LOG_SERVICES="${LOG_SERVICES:-superset superset-worker}"   # main-stack services whose logs are stored and queried in Trino
LAKEHOUSE_LOG_SERVICES="${LAKEHOUSE_LOG_SERVICES:-}"       # optional, e.g. "trino iceberg-rest" (very verbose: opt in)
rc=0

docker compose exec -T pipeline python etl/iceberg_init.py || { echo "iceberg_init failed; stopping" >&2; exit 1; }
docker compose exec -T pipeline python etl/sync_logs.py || rc=1
# --timestamps and the service prefix are needed: they key each line in container_log_lines (and tell services apart)
docker compose logs --timestamps --since "$SINCE" $LOG_SERVICES   | docker compose exec -T pipeline python etl/ingest_container_errors.py || rc=1
if [ -n "$LAKEHOUSE_LOG_SERVICES" ]; then
  docker compose -f lakehouse/docker-compose.yml logs --timestamps --since "$SINCE" $LAKEHOUSE_LOG_SERVICES     | docker compose exec -T pipeline python etl/ingest_container_errors.py || rc=1
fi
docker compose exec -T pipeline python etl/iceberg_maintenance.py || rc=1
exit $rc

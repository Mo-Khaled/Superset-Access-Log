#!/usr/bin/env bash
# Demo helper: shows where the chart timeout is (and is not) logged.
# Usage: bash intermittent-error/repro/show_evidence.sh [minutes=10]
M="${1:-10}"
cd "$(dirname "$0")/../.."

echo "=== 1. Container stdout (gunicorn arbiter): the ONLY server-side trace ==="
docker compose logs --since "${M}m" superset 2>&1 | grep -E 'WORKER TIMEOUT|SIGKILL' | cut -c1-140
echo

echo "=== 2. Superset 'logs' table: server-side chart-data requests ==="
echo "    (killed requests never appear here)"
docker compose exec -T postgres psql -U superset -d superset -c \
 "select dttm, action, slice_id from logs where dttm > now() - interval '${M} minutes' and action='ChartDataRestApi.data' order by dttm;"

echo "=== 3. Superset 'logs' table: browser-reported load_chart (client side) ==="
docker compose exec -T postgres psql -U superset -d superset -c \
 "select dttm, slice_id, json::json->>'has_err' as has_err, json::json->>'error_details' as error, json::json->>'duration' as ms from logs where dttm > now() - interval '${M} minutes' and json::json->>'event_name'='load_chart' order by dttm;"

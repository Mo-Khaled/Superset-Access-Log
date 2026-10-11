# Demo: intermittent chart timeout that Superset's logs miss, and the failure rate it feeds

Shows: (a) the error is intermittent (cache-driven), (b) it is invisible in Superset's own event log,
(c) our pipeline records it in Iceberg and turns it into a failure rate per day, week and month.
No fix is applied.

## Setup (before the audience arrives)
Only the containers that are needed:
```
docker compose -f lakehouse/docker-compose.yml up -d                       # minio, iceberg-rest, trino
docker compose -f docker-compose.yml -f lakehouse/connect-pipeline.yml up -d postgres redis superset pipeline
docker compose exec -T pipeline python etl/iceberg_init.py                 # once
docker compose exec -T pipeline python seed/generate_activity.py           # optional: 30 days of history incl. synthetic failures
bash access-log-pipeline/run_etl.sh                                         # load it
docker compose -f docker-compose.yml -f lakehouse/connect-pipeline.yml run --rm superset-bootstrap   # dashboards
docker compose exec redis redis-cli -n 2 FLUSHDB                           # cold cache
docker compose run -d --name superset-nolimit -e SUPERSET_WEBSERVER_TIMEOUT=60 -e CACHE_KEY_PREFIX=superset8090_ \
  -p 8090:8088 superset superset run -h 0.0.0.0 -p 8088 --with-threads      # server with no 15 s limit
```
Dashboard: "Intermittent Error Repro (slow chart)"; chart sleeps 18s, gunicorn timeout is 15s.
If it does not exist: `docker compose run --rm -e SLOW_QUERY_SLEEP_SECONDS=1 pipeline python /repro/create_slow_dataset.py`,
then set the dataset SQL to `pg_sleep(18)` (creating it with 18 s hits the same 15 s kill).

## Script
1. Open `localhost:8089` dashboard -> chart fails after ~15s. **Error #1.**
2. Run `bash intermittent-error/repro/show_evidence.sh 5`
   - Section 1: gunicorn `WORKER TIMEOUT` / `SIGKILL` (only in container stdout).
   - Section 2: no `ChartDataRestApi.data` row for the failed request.
   - Section 3: only the browser's `load_chart` row (`has_err=true, timeout`).
3. Run one ETL cycle: `bash access-log-pipeline/run_etl.sh`, then ask Trino:
   ```
   docker compose -f lakehouse/docker-compose.yml exec trino trino --execute "
     select event_ts, source, user_id, dashboard_id, chart_id, substr(reason, 1, 90) reason
     from iceberg.monitoring.fact_error_events order by event_ts desc limit 5"
   ```
   - a `browser` row (user, dashboard, chart, "timeout after ~15000 ms") and a `gunicorn` row ("worker pid N exceeded
     the 15s request timeout and was SIGKILLed; request was POST /api/v1/chart/data") joined through the worker pid.
   - `fact_access_events` still counts the failed load as an ordinary `chart_view`, which is why errors have their own table.
4. Show the ratio, which is the point of it all:
   ```
   docker compose -f lakehouse/docker-compose.yml exec trino trino --execute "
     select date_trunc('week', day) week, sum(chart_loads) loads, sum(failed_loads) failed, sum(server_kills) kills,
            round(sum(failed_loads) * 100.0 / nullif(sum(chart_loads), 0), 2) failure_pct
     from iceberg.monitoring.vw_chart_failure_rate group by 1 order by 1"
   ```
   Then open Superset, **Dashboard Reliability and Speed**: failure rate daily/weekly/monthly, failure rate by dashboard
   (the slow chart's dashboard stands out), load time (average, p95) and slow loads.
   Then show that it is not about one cause. Cause a few different failures and run the cycle again:
   ```
   TOK=$(curl -s -X POST localhost:8089/api/v1/security/login -H 'Content-Type: application/json' \
         -d '{"username":"admin","password":"admin","provider":"db"}' | python -c 'import sys,json;print(json.load(sys.stdin)["access_token"])')
   curl -s -o /dev/null -H "Authorization: Bearer $TOK" localhost:8089/api/v1/chart/99999           # 404
   curl -s -o /dev/null -H 'Authorization: Bearer nope' localhost:8089/api/v1/dashboard/1            # 422
   bash access-log-pipeline/run_etl.sh
   docker compose -f lakehouse/docker-compose.yml exec trino trino --execute "
     select error_category, count(*) errors, min(error_signature) example
     from iceberg.monitoring.fact_error_events group by 1 order by 2 desc"
   ```
   Open **Top Error Messages** on the dashboard: one row per kind of error, whatever its cause. Anything the rules
   do not know is `unclassified`, which is how a new cause is noticed.
   Say it plainly: the failure rate is a **lower bound** (browser rows exist only while the tab is open); `kills` is the
   server-side count that shows the gap.
5. Open `localhost:8090` dashboard (~18s, succeeds, fills the cache).
6. Refresh `localhost:8089` -> loads instantly. **Intermittent.**
7. After 5 min the cache expires and the error returns.

## Optional: show that late or past-dated rows are not lost
`docker compose exec -T pipeline python tests/acceptance_watermark.py` inserts rows dated 3 hours ago, 10 days ago and
tomorrow, runs the real ETL after each and prints PASS/FAIL. The 10-day-old row triggers the reconciliation warning.

## Teardown
```
docker rm -f superset-nolimit
docker compose -f lakehouse/docker-compose.yml down        # add -v to delete the Iceberg data
```

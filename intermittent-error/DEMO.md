# Demo: intermittent chart timeout that Superset's logs miss

Shows: (a) the error is intermittent (cache-driven), (b) it is invisible in
Superset's own event log. No fix is applied.

## Setup (before the audience arrives)
```
docker compose up -d
docker compose exec redis redis-cli -n 2 FLUSHDB          # cold cache
docker compose run -d --name superset-nolimit -p 8090:8088 superset \
  superset run -h 0.0.0.0 -p 8088 --with-threads            # no-timeout server
```
Dashboard: "Intermittent Error Repro (slow chart)"; chart sleeps 18s,
gunicorn timeout is 15s, DB statement_timeout is 20s.

## Script
1. Open `localhost:8089` dashboard -> chart fails after ~15s. **Error #1.**
2. Run `bash intermittent-error/repro/show_evidence.sh 5`
   - Section 1: gunicorn `WORKER TIMEOUT` / `SIGKILL` (only in container stdout).
   - Section 2: no `ChartDataRestApi.data` row for the failed request.
   - Section 3: only the browser's `load_chart` row (`has_err=true, timeout`).
3. Run the pipeline, then show both MySQL tables:
   ```
   docker compose exec -T pipeline python etl/sync_logs.py
   docker compose logs --no-log-prefix --since 10m superset | docker compose exec -T pipeline python etl/ingest_container_errors.py
   ```
   - `fact_access_events`: the failed load is counted as an ordinary `chart_view`
     (`select action, count(*) from fact_access_events where chart_id=204 group by action;`).
   - `fact_error_events`: the failure is logged with its reason: a `browser` row
     (user/dashboard/chart + "timeout after ~15000 ms") and a `gunicorn` row
     ("worker pid N exceeded the 15s request timeout and was SIGKILLed").
4. Open `localhost:8090` dashboard (~18s, succeeds, fills the cache).
5. Refresh `localhost:8089` -> loads instantly. **Intermittent.**
6. After 5 min the cache expires and the error returns.

## Teardown
```
docker rm -f superset-nolimit
```

# Accessing Superset UI + the databases in DBeaver

Quick reference for poking around this lab by hand: logging into Superset, and
connecting DBeaver (or any other SQL client) to the two Postgres databases and to the
Trino/Iceberg lakehouse that holds the pipeline's data.

Bring the stack up first, if it isn't already:
```bash
docker compose -f lakehouse/docker-compose.yml up -d
docker compose -f docker-compose.yml -f lakehouse/connect-pipeline.yml up -d --build
docker compose logs -f superset-init   # wait for this to finish before using Superset
```

## 1. Superset UI

| | |
|---|---|
| URL | http://localhost:8089 |
| Username | `admin` |
| Password | `admin` |

(Mapped from the container's internal port 8088 -> host 8089, kept off 8088
because that's already in use by another local project -- see `.env` /
`docker-compose.yml` if you ever need to change it.)

Once logged in: **Data > Databases** shows the registered connections --
`examples` (the seeded Superset demo DB) and `Lakehouse (Trino Iceberg)` (the
pipeline's own Iceberg tables through Trino, registered automatically by
`superset-bootstrap`/`access-log-pipeline/superset_bootstrap/bootstrap_dashboard.py`).
**Dashboards** shows the example dashboards (`load_examples` seeds these), the
native **Superset Usage Analytics** dashboard, plus anything the Task 2 repro
scripts created.

## 2. Usage dashboard

**Dashboards -> Superset Usage Analytics**, inside Superset itself (same URL/login
as above) -- a native Superset dashboard built on the Iceberg pipeline tables below.
(The project previously shipped a standalone dashboard on port 8091; it's been
replaced by this native one, so there's nothing separate to log into.)

## 3. Databases in DBeaver

Two separate systems are involved. **Postgres** hosts two logical
databases on the *same* server/port (Superset's own metadata, and Superset's
`examples` content DB) -- **Trino over Iceberg** is entirely separate, and is where our own
pipeline's tables live.

Postgres isn't published on its standard 5432 port (to avoid clashing with any local
instance you may already be running), so `docker-compose.yml` publishes it on a
non-standard host port instead, and the lab Trino is on 8085:

### 3a. Postgres -- Superset's metadata DB

| Setting | Value |
|---|---|
| Host | `localhost` |
| Port | `15432` |
| Database | `superset` |
| Username | `superset` |
| Password | `superset` |

This is Superset's own application DB: dashboards, charts, users, and -- most
relevant to this lab -- the `logs` table that `LogRestApi` reads from (the source
the ETL pulls out of). Handy tables to look at: `logs`, `dashboards`, `slices`,
`tables` (Superset's own dataset registry -- this is where you'll find the
`slow_query_demo` virtual dataset if you've run the Task 2 repro), `ab_user`.

### 3b. Postgres -- the `examples` content DB

Same server as above, different database, so in DBeaver this is just a second
"database" under the same Postgres connection (or a second connection with
Database set to `superset_examples`):

| Setting | Value |
|---|---|
| Host | `localhost` |
| Port | `15432` |
| Database | `superset_examples` |
| Username | `superset` |
| Password | `superset` |

This holds the actual example datasets Superset's charts query against (World
Bank data, COVID vaccine data, etc.), plus, if you've run the Task 2 repro, the
`slow_query_demo` virtual dataset's underlying `pg_sleep(...)` query runs against
*this* connection.

### 3c. Trino -- the access log pipeline's Iceberg tables

The pipeline's only data store. Written by the ETL scripts through Trino `MERGE`, and read by Superset (registered
as a database connection) to power the **Superset Usage Analytics** and **Dashboard Reliability and Speed**
dashboards. Use DBeaver's Trino driver.

| Setting | Value |
|---|---|
| Host | `localhost` |
| Port | `8085` |
| Catalog / schema | `iceberg` / `monitoring` |
| Username | `pipeline` (no password in the lab) |

Tables (see `access-log-pipeline/schema_iceberg/*.sql` for full DDL + comments):

| Table | What it is |
|---|---|
| `raw_log_landing` | Verbatim JSON of every row pulled from `/api/v1/log/`, keyed by Superset's own log row id. The watermark is read from it, and facts are rebuilt from it. |
| `dim_users` | Superset users, from the user API. |
| `dim_dashboards` | Dashboard id -> title and owners. |
| `fact_access_events` | The cleaned event fact table -- one row per `dashboard_view` / `chart_view` / `export_csv` / etc., with `duration_ms` and `is_error` for chart loads. |
| `fact_error_events` | One row per failure of any cause, with category, signature and raw message. |
| `etl_run_log` | One row per ETL / ingest / maintenance run, with status. |
| `etl_lock` | The single-writer lease. |
| `vw_chart_failure_rate` | View: chart loads and failures per day and dashboard. |

A quick sanity query once you're connected:
```sql
SELECT action, COUNT(*) FROM fact_access_events GROUP BY action ORDER BY 2 DESC;
```

## 4. Notes

- The connections use plaintext lab-only credentials from `.env` -- fine
  here, never reuse them anywhere real (see the comment at the top of `.env`).
- If DBeaver can't connect: confirm the containers are actually up and healthy
  with `docker compose ps` -- Postgres has a healthcheck, so "healthy"
  means the DB inside is actually accepting connections, not just that the
  container started.
- `docker compose down` (no `-v`) stops everything but keeps all this data around
  for your next session. `docker compose down -v` wipes it -- next startup is a
  blank slate.

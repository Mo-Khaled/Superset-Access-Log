# Accessing Superset UI + the databases in DBeaver

Quick reference for poking around this lab by hand: logging into Superset, and
connecting DBeaver (or any other SQL client) to the two Postgres databases and the
MySQL pipeline database.

Bring the stack up first, if it isn't already:
```bash
docker compose up -d --build
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
`examples` (the seeded Superset demo DB) and `Access Log Pipeline (MySQL)` (the
pipeline's own MySQL database, registered automatically by
`superset-bootstrap`/`access-log-pipeline/superset_bootstrap/bootstrap_dashboard.py`).
**Dashboards** shows the example dashboards (`load_examples` seeds these), the
native **Superset Usage Analytics** dashboard, plus anything the Task 2 repro
scripts created.

## 2. Usage dashboard

**Dashboards -> Superset Usage Analytics**, inside Superset itself (same URL/login
as above) -- a native Superset dashboard built on the MySQL pipeline tables below.
(The project previously shipped a standalone dashboard on port 8091; it's been
replaced by this native one, so there's nothing separate to log into.)

## 3. Databases in DBeaver

Three separate database engines are involved. **Postgres** hosts two logical
databases on the *same* server/port (Superset's own metadata, and Superset's
`examples` content DB) -- **MySQL** is entirely separate, and is where our own
pipeline's tables live.

By default `mysql`/`postgres` aren't published to the host (to avoid clashing with
any local instances you may already be running on the standard 3306/5432 ports),
so `docker-compose.yml` publishes them on non-standard host ports instead:

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

### 3c. MySQL -- the access log pipeline's destination DB

A separate database engine from Superset's own metadata store -- only ever
written to by `etl/sync_logs.py`, and read from by Superset itself (registered as
a database connection) to power the **Superset Usage Analytics** dashboard.

| Setting | Value |
|---|---|
| Host | `localhost` |
| Port | `13306` |
| Database | `access_log_pipeline` |
| Username | `pipeline` |
| Password | `pipeline_pw` |

(A `root` / `root_pw_lab_only` superuser also exists if you need it -- see
`.env`.)

Tables (see `access-log-pipeline/schema/*.sql` for full DDL + comments):

| Table | What it is |
|---|---|
| `raw_log_landing` | Staged raw JSON payloads pulled from `/api/v1/log/`, keyed by Superset's own log row id. Re-transform source of truth. |
| `dim_users` | Superset users, denormalized from the raw logs. |
| `dim_dashboards` | Dashboard id -> title lookup. |
| `fact_access_events` | The cleaned event fact table -- one row per `dashboard_view` / `chart_view` / `export_csv` / etc. This is what the usage dashboard queries. |
| `etl_sync_state` | Single-row high-water-mark the ETL uses to stay idempotent/incremental. |

A quick sanity query once you're connected:
```sql
SELECT action, COUNT(*) FROM fact_access_events GROUP BY action ORDER BY 2 DESC;
```

## 4. Notes

- All three connections use plaintext lab-only credentials from `.env` -- fine
  here, never reuse them anywhere real (see the comment at the top of `.env`).
- If DBeaver can't connect: confirm the containers are actually up and healthy
  with `docker compose ps` -- Postgres/MySQL both have healthchecks, so "healthy"
  means the DB inside is actually accepting connections, not just that the
  container started.
- `docker compose down` (no `-v`) stops everything but keeps all this data around
  for your next session. `docker compose down -v` wipes it -- next startup is a
  blank slate.

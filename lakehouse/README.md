# lakehouse/ (lab stand-in)

A self-contained Iceberg + Trino for the lab. It stands in for the bank's existing platform so the error-log
pipeline can be built and tested without depending on anything outside this repo.

**It is not part of the target architecture.** In the real environment the pipeline and Superset connect to the bank's
own Trino through the `TRINO_*` settings, and nothing in this directory is deployed.

| Service | Image | Role |
|---|---|---|
| `minio` | `quay.io/minio/minio` | Object storage for the Iceberg data and metadata files |
| `minio-bootstrap` | `quay.io/minio/mc` | One-shot: creates the `warehouse` bucket |
| `catalog-init` | `busybox` | One-shot: fixes permissions on the catalog volume |
| `iceberg-rest` | `tabulario/iceberg-rest` | Iceberg REST catalog (SQLite on a volume) |
| `trino` | `trinodb/trino` | SQL engine. Catalog `iceberg`, schema `monitoring` |

No event listener, no custom image build. The pipeline stores everything here: there is no other database.

## Use

```bash
cp lakehouse/.env.example lakehouse/.env                 # first time only
docker compose -f lakehouse/docker-compose.yml up -d     # starts only the services above
docker compose -f lakehouse/docker-compose.yml ps

# main stack, joined to the lakehouse network (only what is needed)
docker compose -f docker-compose.yml -f lakehouse/connect-pipeline.yml up -d postgres redis superset pipeline

# ask Trino something
docker compose -f lakehouse/docker-compose.yml exec trino trino --execute "select version()"
```

Host ports: Trino `8085`, MinIO API `9102`, MinIO console `9103`, REST catalog `8183`. They differ from the main stack
(Superset on `8089`, Postgres on `15432`) and from the older `trino-query-history` project, so all can run together.

Stop and remove everything, including the data: `docker compose -f lakehouse/docker-compose.yml down -v`.

Lab limitation: the REST catalog keeps its metadata in SQLite. If two writers commit at the same instant it can answer with an
HTTP 500 and then refuse commits until it is restarted (`docker compose -f lakehouse/docker-compose.yml restart iceberg-rest`).
The pipeline's single-writer lease makes that rare, and a lease left behind by an interrupted run expires by itself after 30 minutes
(or clear it now: `delete from iceberg.monitoring.etl_lock`). A production catalog with proper optimistic concurrency does not behave this way.

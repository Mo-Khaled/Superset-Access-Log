FROM apache/superset:4.1.4

USER root

# psycopg2-binary: the base image does NOT bundle a Postgres driver by default --
# required here because our metadata DB *and* examples DB both live on Postgres.
# trino[sqlalchemy]: lets Superset query the pipeline's Iceberg tables through Trino -- registered automatically by
# superset_bootstrap/bootstrap_dashboard.py to power the usage and reliability dashboards.
RUN pip install --no-cache-dir psycopg2-binary==2.9.9 "trino[sqlalchemy]==0.337.0"

COPY superset_config.py /app/pythonpath/superset_config.py
COPY gunicorn_config.py /app/pythonpath/gunicorn_config.py

USER superset

FROM apache/superset:4.1.4

USER root

# psycopg2-binary: the base image does NOT bundle a Postgres driver by default --
# required here because our metadata DB *and* examples DB both live on Postgres.
# pymysql: not needed for Superset's own metadata DB, but required so Superset can
# connect to the pipeline's MySQL analytics DB -- registered automatically by
# superset_bootstrap/bootstrap_dashboard.py to power the native "Superset Usage
# Analytics" dashboard (see access-log-pipeline/superset_bootstrap/).
RUN pip install --no-cache-dir psycopg2-binary==2.9.9 pymysql==1.1.1

COPY superset_config.py /app/pythonpath/superset_config.py
COPY gunicorn_config.py /app/pythonpath/gunicorn_config.py

USER superset

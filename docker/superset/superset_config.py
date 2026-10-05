import os

import pymysql
from superset.utils.log import DBEventLogger

# Superset's MySQL engine spec imports the `MySQLdb` (mysqlclient) module directly
# for a few code paths (notably virtual/SQL-defined dataset column introspection)
# regardless of which driver the connection's SQLAlchemy URI actually uses. We
# connect with pymysql (no C build deps, already installed -- see Dockerfile), so
# register it as a drop-in `MySQLdb` to satisfy those `import MySQLdb` calls
# instead of adding the heavier mysqlclient build toolchain to the image.
pymysql.install_as_MySQLdb()

# --- Metadata DB (Superset's own state: users, dashboards, charts, the `logs` table) ---
SQLALCHEMY_DATABASE_URI = (
    f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:"
    f"{os.environ['POSTGRES_PASSWORD']}@"
    f"{os.environ.get('POSTGRES_HOST', 'postgres')}:5432/"
    f"{os.environ['POSTGRES_DB']}"
)

# `superset load_examples` loads its sample dashboards/charts into whatever this URI
# points at. We keep it on the SAME Postgres instance as the metadata DB (a separate
# logical database) so the "examples" data lives next to Superset's own state -- our
# MySQL instance is reserved entirely for the *pipeline's* cleaned-up output, never
# used as a Superset-registered source. This keeps the two concerns (Superset's
# internal state vs. our analytics destination) unambiguously separate.
SQLALCHEMY_EXAMPLES_URI = (
    f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:"
    f"{os.environ['POSTGRES_PASSWORD']}@"
    f"{os.environ.get('POSTGRES_HOST', 'postgres')}:5432/"
    f"{os.environ['SUPERSET_EXAMPLES_DB_NAME']}"
)

# --- Celery / Redis (async chart queries, thumbnails, alerts) ---
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = os.environ.get("REDIS_PORT", "6379")


class CeleryConfig:
    broker_url = f"redis://{REDIS_HOST}:{REDIS_PORT}/0"
    imports = ("superset.sql_lab",)
    result_backend = f"redis://{REDIS_HOST}:{REDIS_PORT}/1"
    worker_prefetch_multiplier = 1
    task_acks_late = False


CELERY_CONFIG = CeleryConfig

CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    # seconds a cached chart result is reused; set CACHE_TIMEOUT_SECONDS in .env
    "CACHE_DEFAULT_TIMEOUT": int(os.environ.get("CACHE_TIMEOUT_SECONDS", "300")),
    "CACHE_KEY_PREFIX": "superset_",
    "CACHE_REDIS_HOST": REDIS_HOST,
    "CACHE_REDIS_PORT": REDIS_PORT,
    "CACHE_REDIS_DB": 2,
}
DATA_CACHE_CONFIG = CACHE_CONFIG

# --- Event logging (TASK 1: this is the source our ETL pulls from via /api/v1/log/) ---
# DBEventLogger is Superset's built-in logger that writes every logged action
# (dashboard_load, chart_data, log, etc.) as a row in the metadata DB's `logs` table.
# It is Superset's default EVENT_LOGGER already, but we set it explicitly here so
# it's documented and doesn't silently depend on the framework default.
EVENT_LOGGER = DBEventLogger()

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]

FEATURE_FLAGS = {
    "DASHBOARD_RBAC": False,
}

# --- TASK 1: lets superset_bootstrap/bootstrap_dashboard.py register the MySQL
# pipeline DB as a Superset connection ---
# Superset's SIP-15 "unsafe DB connection" guard (on by default) rejects adding a
# database connection whose host resolves to a private/link-local IP -- which is
# exactly what the `mysql` compose service's Docker-network hostname does. Safe to
# disable here since this is a closed local lab network, not a multi-tenant deploy.
PREVENT_UNSAFE_DB_CONNECTIONS = False

# --- TASK 2: webserver/query timeouts ---
# Deliberately short in the "before" state to reproduce the silent-kill bug; the
# fix scenario (see intermittent-error/fix/) raises SUPERSET_WEBSERVER_TIMEOUT and,
# more importantly, adds a DB-side statement_timeout so slow queries fail with a
# real logged exception instead of the OS killing the whole worker process.
SUPERSET_WEBSERVER_TIMEOUT = int(os.environ.get("SUPERSET_WEBSERVER_TIMEOUT", "15"))
SQLLAB_TIMEOUT = int(os.environ.get("SQLLAB_TIMEOUT", "15"))
SUPERSET_WEBSERVER_TIMEOUT_PATH = None


# --- Request trace: lets us say WHO/WHAT a gunicorn "WORKER TIMEOUT" belonged to ---
# gunicorn's own timeout line only carries the worker pid and a time. Each worker
# therefore logs one line per request as it starts: its pid, the user, and the URL
# (the chart-data URL already contains slice_id and dashboard_id). The ETL
# (etl/ingest_container_errors.py) joins the timeout line's pid to the last trace
# line of that same pid. Logged on request START on purpose: a killed request never
# reaches any "end of request" hook.
def FLASK_APP_MUTATOR(app):
    import logging
    import sys
    import time

    from flask import request
    from flask_login import current_user

    tracer = logging.getLogger("reqtrace")
    tracer.setLevel(logging.INFO)
    tracer.propagate = False
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("%(message)s"))
    tracer.addHandler(handler)

    skip = ("/health", "/static/", "/superset/log", "/api/v1/me", "/api/v1/log")

    @app.before_request
    def _trace_request():
        try:
            if request.path.startswith(skip):
                return
            user = current_user.get_id() if current_user.is_authenticated else "-"
            tracer.info(
                "REQTRACE ts=%s pid=%s user=%s method=%s url=%s",
                time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
                os.getpid(), user, request.method, request.full_path,
            )
        except Exception:  # tracing must never break a request
            pass

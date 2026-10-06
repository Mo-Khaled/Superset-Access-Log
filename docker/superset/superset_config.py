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
    # Metadata cache (includes the page bootstrap data, which carries the browser's
    # request timeout). The no-limit demo server sets its own CACHE_KEY_PREFIX so it
    # does not read the main server's cached 15 s timeout.
    "CACHE_KEY_PREFIX": os.environ.get("CACHE_KEY_PREFIX", "superset_"),
    "CACHE_REDIS_HOST": REDIS_HOST,
    "CACHE_REDIS_PORT": REDIS_PORT,
    "CACHE_REDIS_DB": 2,
}
# Chart results keep one fixed prefix so every server shares them.
DATA_CACHE_CONFIG = {**CACHE_CONFIG, "CACHE_KEY_PREFIX": "superset_data_"}

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


# --- Request trace: one log line per request event, so ANY failure can be explained, whatever its cause ---
# We do not know in advance which errors production will produce, so nothing here is specific to one cause.
# Each Superset worker writes these lines to stderr (the container log); etl/ingest_container_errors.py reads them:
#
#   REQTRACE  every request, when it STARTS (pid, user, id, url). A request killed by gunicorn never reaches the
#             end-of-request hook, so this is the only record of who/what it was; the ETL joins gunicorn's
#             "WORKER TIMEOUT (pid:N)" line to the last REQTRACE of that pid.
#   REQEND    only requests that ended with an HTTP status >= 400 on a dashboard/chart path: status, duration and the
#             error message from the response body. This catches query errors, permission errors, bad dashboards,
#             bad datasets, anything Superset turns into an error response.
#   REQEXC    unhandled exceptions: the exception class and message, tied to the request by its id.
#
# ERROR_TRACE_PREFIXES (comma separated) sets which paths REQEND watches; the default covers dashboards and charts.
def FLASK_APP_MUTATOR(app):
    import logging
    import re
    import sys
    import time
    import uuid

    from flask import g, got_request_exception, request
    from flask_login import current_user

    tracer = logging.getLogger("reqtrace")
    tracer.setLevel(logging.INFO)
    tracer.propagate = False
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("%(message)s"))
    tracer.addHandler(handler)

    skip = ("/health", "/static/", "/superset/log", "/api/v1/me", "/api/v1/log")
    watch = tuple(
        x.strip() for x in os.environ.get(
            "ERROR_TRACE_PREFIXES",
            "/api/v1/chart,/api/v1/dashboard,/api/v1/explore,/api/v1/dataset,/api/v1/sqllab,"
            "/superset/dashboard,/superset/explore_json",
        ).split(",") if x.strip()
    )
    space = re.compile(r"\s+")

    def one_line(text, limit=300):
        return space.sub(" ", str(text or "")).strip()[:limit]

    def now():
        return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())

    def user_id():
        return current_user.get_id() if current_user.is_authenticated else "-"

    @app.before_request
    def _trace_request():
        try:
            g._rid = uuid.uuid4().hex[:12]
            g._t0 = time.time()
            if request.path.startswith(skip):
                return
            tracer.info(
                "REQTRACE ts=%s pid=%s user=%s method=%s rid=%s url=%s",
                now(), os.getpid(), user_id(), request.method, g._rid, request.full_path,
            )
        except Exception:  # tracing must never break a request
            pass

    @app.after_request
    def _trace_request_end(response):
        try:
            if response.status_code >= 400 and request.path.startswith(watch):
                message = ""
                body = response.get_json(silent=True) if response.is_json else None
                if isinstance(body, dict):
                    message = body.get("message") or body.get("error") or ""
                    if not message and body.get("errors"):
                        first = body["errors"][0]
                        message = first.get("message") if isinstance(first, dict) else first
                tracer.info(
                    "REQEND ts=%s pid=%s user=%s method=%s rid=%s status=%s dur=%d url=%s msg=%s",
                    now(), os.getpid(), user_id(), request.method, getattr(g, "_rid", "-"),
                    response.status_code, int((time.time() - getattr(g, "_t0", time.time())) * 1000),
                    request.full_path, one_line(message),
                )
        except Exception:
            pass
        return response

    def _trace_exception(sender, exception, **extra):
        try:
            tracer.info(
                "REQEXC ts=%s pid=%s rid=%s exc=%s msg=%s",
                now(), os.getpid(), getattr(g, "_rid", "-"), type(exception).__name__, one_line(exception),
            )
        except Exception:
            pass

    got_request_exception.connect(_trace_exception, app)

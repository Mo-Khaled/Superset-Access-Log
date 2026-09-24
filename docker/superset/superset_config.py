import os

from superset.utils.log import DBEventLogger

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
    "CACHE_DEFAULT_TIMEOUT": 300,
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

# --- TASK 2: webserver/query timeouts ---
# Deliberately short in the "before" state to reproduce the silent-kill bug; the
# fix scenario (see intermittent-error/fix/) raises SUPERSET_WEBSERVER_TIMEOUT and,
# more importantly, adds a DB-side statement_timeout so slow queries fail with a
# real logged exception instead of the OS killing the whole worker process.
SUPERSET_WEBSERVER_TIMEOUT = int(os.environ.get("SUPERSET_WEBSERVER_TIMEOUT", "15"))
SQLLAB_TIMEOUT = int(os.environ.get("SQLLAB_TIMEOUT", "15"))
SUPERSET_WEBSERVER_TIMEOUT_PATH = None

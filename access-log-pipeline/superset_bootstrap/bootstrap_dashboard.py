"""
Idempotent bootstrap for the native Superset "Superset Usage Analytics" dashboard.

Runs once per `docker compose up` (as the one-shot `superset-bootstrap` service)
after `superset-init` has created the admin user and the webserver is up. It:

  1. Registers a Superset database connection pointing at the pipeline's own
     MySQL database (the same one `etl/sync_logs.py` writes to -- no schema
     changes, no new data processing logic).
  2. Registers the tables the usage dashboard needs as Superset datasets --
     three physical (`fact_access_events`, `dim_dashboards`, `dim_users`) and
     three "virtual" (SQL-defined) ones that port the join/aggregation logic
     that used to live in `access-log-pipeline/dashboard/queries.py` 1:1, since
     Superset's chart builder can't express a join against a single dataset.
  3. Creates the "Superset Usage Analytics" dashboard and the 7 required charts,
     wiring each chart to that dashboard.

Safe to re-run: every step is get-or-create (by name), matching the same
idempotency philosophy as `etl/sync_logs.py` -- re-running this after the stack
is already provisioned just confirms everything still exists and leaves it
alone (or updates a chart's definition in place if it already exists).

Run standalone for debugging: `python superset_bootstrap/bootstrap_dashboard.py`
(needs the same env vars as the ETL: SUPERSET_BASE_URL/SUPERSET_ADMIN_* and
MYSQL_*; see .env / docker-compose.yml for the `superset-bootstrap` service).
"""
import json
import os
import sys
import time

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from superset_client import SupersetClient  # noqa: E402

# Deliberately does NOT contain "examples" -- intermittent-error/fix/apply_fix.py
# looks up Superset's own "examples" database by a `database_name contains
# 'examples'` filter, and this name must never collide with that lookup.
DB_CONNECTION_NAME = "Access Log Pipeline (MySQL)"
DASHBOARD_TITLE = "Superset Usage Analytics"

MYSQL_HOST = os.environ["MYSQL_HOST"]
MYSQL_PORT = os.environ.get("MYSQL_PORT", "3306")
MYSQL_DATABASE = os.environ["MYSQL_DATABASE"]
MYSQL_USER = os.environ["MYSQL_USER"]
MYSQL_PASSWORD = os.environ["MYSQL_PASSWORD"]

SQLALCHEMY_URI = (
    f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@"
    f"{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
)

SUPERSET_BASE_URL = os.environ.get("SUPERSET_BASE_URL", "http://superset:8088").rstrip("/")

# --- Virtual dataset SQL -- ported from access-log-pipeline/dashboard/queries.py ---
# (all-time grain; the per-window 7d/30d/90d/all selector from the old standalone
# dashboard is superseded by Superset's own native dashboard-level time range
# filter on the charts built against the physical fact_access_events table)

MOST_VIEWED_DASHBOARDS_SQL = """
SELECT
    f.dashboard_id AS dashboard_id,
    COALESCE(d.title, CONCAT('Dashboard ', f.dashboard_id)) AS title,
    COUNT(*) AS view_count,
    COUNT(DISTINCT f.user_id) AS unique_viewers
FROM fact_access_events f
LEFT JOIN dim_dashboards d ON d.dashboard_id = f.dashboard_id
WHERE f.action = 'dashboard_view'
GROUP BY f.dashboard_id, d.title
ORDER BY view_count DESC
""".strip()

USER_ACTIVITY_SQL = """
SELECT
    f.user_id AS user_id,
    COALESCE(u.username, CONCAT('user_', f.user_id)) AS username,
    COUNT(*) AS event_count,
    SUM(CASE WHEN f.action = 'dashboard_view' THEN 1 ELSE 0 END) AS dashboard_views,
    SUM(CASE WHEN f.action = 'chart_view' THEN 1 ELSE 0 END) AS chart_views,
    MAX(f.event_ts) AS last_active
FROM fact_access_events f
LEFT JOIN dim_users u ON u.user_id = f.user_id
GROUP BY f.user_id, u.username
ORDER BY event_count DESC
""".strip()

ZERO_VIEW_DASHBOARDS_SQL = """
SELECT
    d.dashboard_id AS dashboard_id,
    d.title AS title,
    d.owner_names AS owner_names,
    COALESCE(v.view_count, 0) AS view_count
FROM dim_dashboards d
LEFT JOIN (
    SELECT dashboard_id, COUNT(*) AS view_count
    FROM fact_access_events
    WHERE action = 'dashboard_view'
    GROUP BY dashboard_id
) v ON v.dashboard_id = d.dashboard_id
WHERE COALESCE(v.view_count, 0) <= 1
ORDER BY view_count ASC, d.title ASC
""".strip()

# Explicit column fallback for the virtual datasets above -- see
# _ensure_virtual_dataset_columns() for why this is needed (Superset's virtual
# dataset column introspection silently returns no columns when the probe query
# happens to return zero rows, which `vw_zero_view_dashboards` can easily do).
# Tuples are (column_name, sql type, is_dttm).
MOST_VIEWED_DASHBOARDS_COLUMNS = [
    ("dashboard_id", "BIGINT", False),
    ("title", "VARCHAR(500)", False),
    ("view_count", "BIGINT", False),
    ("unique_viewers", "BIGINT", False),
]
USER_ACTIVITY_COLUMNS = [
    ("user_id", "INT", False),
    ("username", "VARCHAR(255)", False),
    ("event_count", "BIGINT", False),
    ("dashboard_views", "BIGINT", False),
    ("chart_views", "BIGINT", False),
    ("last_active", "DATETIME", True),
]
ZERO_VIEW_DASHBOARDS_COLUMNS = [
    ("dashboard_id", "INT", False),
    ("title", "VARCHAR(500)", False),
    ("owner_names", "VARCHAR(500)", False),
    ("view_count", "BIGINT", False),
]


def wait_for_superset(max_wait_seconds=300, interval=5):
    """Superset has no compose healthcheck today, so poll /health ourselves
    instead of requiring depends_on: service_healthy."""
    deadline = time.time() + max_wait_seconds
    last_exc = None
    while time.time() < deadline:
        try:
            resp = requests.get(f"{SUPERSET_BASE_URL}/health", timeout=5)
            if resp.status_code == 200:
                return
        except requests.RequestException as exc:
            last_exc = exc
        time.sleep(interval)
    raise RuntimeError(
        f"Superset at {SUPERSET_BASE_URL} did not become ready within "
        f"{max_wait_seconds}s: {last_exc}"
    )


def get_one(client, path, rison_filter):
    resp = client.get(path, params={"q": rison_filter})
    result = resp.json().get("result", [])
    return result[0] if result else None


def get_or_create_database(client):
    existing = get_one(
        client,
        "/api/v1/database/",
        f"(filters:!((col:database_name,opr:eq,value:'{DB_CONNECTION_NAME}')))",
    )
    if existing:
        print(f"database connection '{DB_CONNECTION_NAME}' already exists (id={existing['id']})")
        return existing["id"]
    payload = {
        "database_name": DB_CONNECTION_NAME,
        "sqlalchemy_uri": SQLALCHEMY_URI,
        "expose_in_sqllab": True,
    }
    resp = client.post("/api/v1/database/", json=payload)
    db_id = resp.json()["id"]
    print(f"created database connection '{DB_CONNECTION_NAME}' (id={db_id})")
    return db_id


def _dataset_columns(client, dataset_id):
    resp = client.get(f"/api/v1/dataset/{dataset_id}")
    return resp.json()["result"].get("columns", [])


def _refresh_dataset_columns(client, dataset_id):
    # Virtual (SQL-defined) datasets sometimes come back from creation with an
    # empty column list -- explicitly refresh so charts built against this
    # dataset can resolve its columns instead of failing with "Columns missing
    # in dataset". Idempotent/cheap enough to call on every run, including
    # against an already-existing dataset, so a previous partial run can't
    # leave it permanently columnless.
    client.session.put(
        f"{client.base_url}/api/v1/dataset/{dataset_id}/refresh", timeout=60
    ).raise_for_status()


def _ensure_virtual_dataset_columns(client, dataset_id, table_name, column_defs):
    """Fallback for a genuine Superset quirk: `SupersetResultSet` (used by the
    dataset-refresh column introspection) discards column names entirely
    whenever the probe query happens to return zero rows at introspection time
    (see superset/result_set.py -- `if not pa_data: column_names = []`). Our
    `vw_zero_view_dashboards` dataset can easily hit exactly this at bootstrap
    time (e.g. if every dashboard already has > 1 view), so refreshing alone
    isn't reliable for it. If refresh left the dataset columnless, explicitly
    define its columns instead of depending on live query results.
    """
    if _dataset_columns(client, dataset_id):
        return
    payload = {
        "columns": [
            {
                "column_name": name,
                "type": col_type,
                "is_dttm": is_dttm,
                "groupby": True,
                "filterable": True,
            }
            for name, col_type, is_dttm in column_defs
        ]
    }
    client.session.put(
        f"{client.base_url}/api/v1/dataset/{dataset_id}", json=payload, timeout=60
    ).raise_for_status()
    print(f"explicitly set columns for '{table_name}' (id={dataset_id}) -- refresh returned none")


def get_or_create_dataset(client, db_id, table_name, sql=None, column_defs=None):
    existing = get_one(
        client,
        "/api/v1/dataset/",
        f"(filters:!((col:table_name,opr:eq,value:'{table_name}')))",
    )
    if existing:
        dataset_id = existing["id"]
        if sql:
            _refresh_dataset_columns(client, dataset_id)
            if column_defs:
                _ensure_virtual_dataset_columns(client, dataset_id, table_name, column_defs)
        print(f"dataset '{table_name}' already exists (id={dataset_id})")
        return dataset_id
    payload = {
        "database": db_id,
        "schema": MYSQL_DATABASE,
        "table_name": table_name,
    }
    if sql:
        payload["sql"] = sql
    resp = client.post("/api/v1/dataset/", json=payload)
    dataset_id = resp.json()["id"]
    if sql:
        _refresh_dataset_columns(client, dataset_id)
        if column_defs:
            _ensure_virtual_dataset_columns(client, dataset_id, table_name, column_defs)
    print(f"created dataset '{table_name}' (id={dataset_id}, virtual={bool(sql)})")
    return dataset_id


def get_or_create_dashboard(client, title):
    existing = get_one(
        client,
        "/api/v1/dashboard/",
        f"(filters:!((col:dashboard_title,opr:eq,value:'{title}')))",
    )
    if existing:
        print(f"dashboard '{title}' already exists (id={existing['id']})")
        return existing["id"]
    resp = client.post(
        "/api/v1/dashboard/", json={"dashboard_title": title, "published": True}
    )
    dashboard_id = resp.json()["id"]
    print(f"created dashboard '{title}' (id={dashboard_id})")
    return dashboard_id


def get_or_create_chart(client, slice_name, dataset_id, viz_type, params, dashboard_id):
    payload = {
        "slice_name": slice_name,
        "viz_type": viz_type,
        "datasource_id": dataset_id,
        "datasource_type": "table",
        "params": json.dumps(params),
        "dashboards": [dashboard_id],
    }
    existing = get_one(
        client,
        "/api/v1/chart/",
        f"(filters:!((col:slice_name,opr:eq,value:'{slice_name}')))",
    )
    if existing:
        chart_id = existing["id"]
        resp = client.session.put(
            f"{client.base_url}/api/v1/chart/{chart_id}", json=payload, timeout=60
        )
        resp.raise_for_status()
        print(f"updated chart '{slice_name}' (id={chart_id})")
        return chart_id
    resp = client.post("/api/v1/chart/", json=payload)
    chart_id = resp.json()["id"]
    print(f"created chart '{slice_name}' (id={chart_id})")
    return chart_id


def _base_params(dataset_id, viz_type):
    return {"datasource": f"{dataset_id}__table", "viz_type": viz_type}


def _action_filter(action):
    return {
        "clause": "WHERE",
        "subject": "action",
        "operator": "==",
        "comparator": action,
        "expressionType": "SIMPLE",
    }


def total_views_params(dataset_id):
    p = _base_params(dataset_id, "big_number_total")
    p.update(
        {
            "metric": {
                "expressionType": "SQL",
                "sqlExpression": "COUNT(*)",
                "label": "Total Dashboard Views",
            },
            "adhoc_filters": [_action_filter("dashboard_view")],
            "header_font_size": 0.4,
            "subheader_font_size": 0.15,
            "y_axis_format": "SMART_NUMBER",
            "time_range": "No filter",
        }
    )
    return p


def active_users_params(dataset_id):
    p = _base_params(dataset_id, "big_number_total")
    p.update(
        {
            "metric": {
                "expressionType": "SQL",
                "sqlExpression": "COUNT(DISTINCT user_id)",
                "label": "Active Users",
            },
            "adhoc_filters": [],
            "header_font_size": 0.4,
            "subheader_font_size": 0.15,
            "y_axis_format": "SMART_NUMBER",
            "time_range": "No filter",
        }
    )
    return p


def most_viewed_dashboards_params(dataset_id):
    p = _base_params(dataset_id, "table")
    p.update(
        {
            "query_mode": "raw",
            "all_columns": ["title", "view_count", "unique_viewers"],
            "order_by_cols": ['["view_count", false]'],
            "row_limit": 25,
            "server_pagination": False,
            "time_range": "No filter",
        }
    )
    return p


def views_over_time_params(dataset_id):
    p = _base_params(dataset_id, "echarts_timeseries_line")
    p.update(
        {
            "x_axis": "event_ts",
            "time_grain_sqla": "P1D",
            "metrics": [
                {
                    "expressionType": "SQL",
                    "sqlExpression": "COUNT(*)",
                    "label": "Dashboard Views",
                }
            ],
            "groupby": [],
            "adhoc_filters": [_action_filter("dashboard_view")],
            "row_limit": 10000,
            "time_range": "No filter",
            "show_legend": True,
            "rich_tooltip": True,
            "truncate_metric": True,
            "orientation": "vertical",
        }
    )
    return p


def user_activity_params(dataset_id):
    p = _base_params(dataset_id, "table")
    p.update(
        {
            "query_mode": "raw",
            "all_columns": [
                "username",
                "event_count",
                "dashboard_views",
                "chart_views",
                "last_active",
            ],
            "order_by_cols": ['["event_count", false]'],
            "row_limit": 50,
            "server_pagination": False,
            "time_range": "No filter",
        }
    )
    return p


def zero_view_dashboards_params(dataset_id):
    p = _base_params(dataset_id, "table")
    p.update(
        {
            "query_mode": "raw",
            "all_columns": ["title", "owner_names", "view_count"],
            "order_by_cols": ['["view_count", true]'],
            "row_limit": 50,
            "server_pagination": False,
            "time_range": "No filter",
        }
    )
    return p


def export_activity_params(dataset_id):
    p = _base_params(dataset_id, "table")
    p.update(
        {
            "query_mode": "aggregate",
            "groupby": ["action"],
            "metrics": [
                {
                    "expressionType": "SQL",
                    "sqlExpression": "COUNT(*)",
                    "label": "Export Count",
                }
            ],
            "adhoc_filters": [
                {
                    "clause": "WHERE",
                    "subject": "action",
                    "operator": "IN",
                    "comparator": ["export_csv", "export_excel"],
                    "expressionType": "SIMPLE",
                }
            ],
            "row_limit": 100,
            "server_pagination": False,
            "time_range": "No filter",
        }
    )
    return p


def main():
    wait_for_superset()
    client = SupersetClient()

    db_id = get_or_create_database(client)

    fact_id = get_or_create_dataset(client, db_id, "fact_access_events")
    get_or_create_dataset(client, db_id, "dim_dashboards")
    get_or_create_dataset(client, db_id, "dim_users")

    most_viewed_id = get_or_create_dataset(
        client, db_id, "vw_most_viewed_dashboards",
        sql=MOST_VIEWED_DASHBOARDS_SQL, column_defs=MOST_VIEWED_DASHBOARDS_COLUMNS,
    )
    user_activity_id = get_or_create_dataset(
        client, db_id, "vw_user_activity",
        sql=USER_ACTIVITY_SQL, column_defs=USER_ACTIVITY_COLUMNS,
    )
    zero_view_id = get_or_create_dataset(
        client, db_id, "vw_zero_view_dashboards",
        sql=ZERO_VIEW_DASHBOARDS_SQL, column_defs=ZERO_VIEW_DASHBOARDS_COLUMNS,
    )

    dashboard_id = get_or_create_dashboard(client, DASHBOARD_TITLE)

    get_or_create_chart(
        client, "Total Dashboard Views", fact_id, "big_number_total",
        total_views_params(fact_id), dashboard_id,
    )
    get_or_create_chart(
        client, "Active Users", fact_id, "big_number_total",
        active_users_params(fact_id), dashboard_id,
    )
    get_or_create_chart(
        client, "Most Viewed Dashboards", most_viewed_id, "table",
        most_viewed_dashboards_params(most_viewed_id), dashboard_id,
    )
    get_or_create_chart(
        client, "Dashboard Views Over Time", fact_id, "echarts_timeseries_line",
        views_over_time_params(fact_id), dashboard_id,
    )
    get_or_create_chart(
        client, "User Activity", user_activity_id, "table",
        user_activity_params(user_activity_id), dashboard_id,
    )
    get_or_create_chart(
        client, "Zero-View Dashboards", zero_view_id, "table",
        zero_view_dashboards_params(zero_view_id), dashboard_id,
    )
    get_or_create_chart(
        client, "Export Activity", fact_id, "table",
        export_activity_params(fact_id), dashboard_id,
    )

    print(
        f"Superset bootstrap complete -- open '{DASHBOARD_TITLE}' at "
        f"{SUPERSET_BASE_URL.replace('superset:8088', 'localhost:8089')}/dashboard/list/"
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Superset bootstrap failed: {exc}", file=sys.stderr)
        raise

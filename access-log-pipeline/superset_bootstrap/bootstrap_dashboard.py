"""
Idempotent bootstrap for the native Superset "Superset Usage Analytics" dashboard.

Runs once per `docker compose up` (as the one-shot `superset-bootstrap` service)
after `superset-init` has created the admin user and the webserver is up. It:

  1. Registers the Superset database connection to Trino (catalog `iceberg`), the
     pipeline's only store (the same tables `etl/sync_logs.py` writes to).
  2. Registers the tables the usage dashboard needs as Superset datasets --
     three physical (`fact_access_events`, `dim_dashboards`, `dim_users`) and
     three "virtual" (SQL-defined) ones that hold the join/aggregation logic,
     since Superset's chart builder can't express a join against a single dataset.
  3. Creates the "Superset Usage Analytics" dashboard and its 7 charts,
     wiring each chart to that dashboard.
  4. Builds the "Dashboard Reliability and Speed" dashboard (bootstrap_iceberg.py).

Safe to re-run: every step is get-or-create (by name) -- re-running this after the stack
is already provisioned just confirms everything still exists and leaves it
alone (or updates a chart's definition in place if it already exists). An install made
when the pipeline still used MySQL is migrated: its datasets are pointed at Trino and the
old MySQL connection is removed.

Run standalone for debugging: `python superset_bootstrap/bootstrap_dashboard.py`
(needs SUPERSET_BASE_URL/SUPERSET_ADMIN_* and TRINO_*; see .env / docker-compose.yml
for the `superset-bootstrap` service).
"""
import json
import os
import sys
import time

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from superset_client import SupersetClient  # noqa: E402
import bootstrap_iceberg as ice  # noqa: E402

LEGACY_DB_CONNECTION_NAME = "Access Log Pipeline (MySQL)"  # removed by migrate_legacy_database()
CATALOG, SCHEMA = ice.CATALOG, ice.SCHEMA
T = f"{CATALOG}.{SCHEMA}"  # tables are fully qualified in the virtual datasets' SQL
DASHBOARD_TITLE = "Superset Usage Analytics"

SUPERSET_BASE_URL = os.environ.get("SUPERSET_BASE_URL", "http://superset:8088").rstrip("/")

# --- Virtual dataset SQL (Trino). All-time grain; the per-window selector is Superset's own dashboard-level time
# range filter on the charts built against the physical fact_access_events table.

MOST_VIEWED_DASHBOARDS_SQL = f"""
SELECT
    f.dashboard_id AS dashboard_id,
    COALESCE(d.title, 'Dashboard ' || CAST(f.dashboard_id AS varchar)) AS title,
    COUNT(*) AS view_count,
    COUNT(DISTINCT f.user_id) AS unique_viewers
FROM {T}.fact_access_events f
LEFT JOIN {T}.dim_dashboards d ON d.dashboard_id = f.dashboard_id
WHERE f.action = 'dashboard_view'
GROUP BY f.dashboard_id, d.title
""".strip()

USER_ACTIVITY_SQL = f"""
SELECT
    f.user_id AS user_id,
    COALESCE(u.username, 'user_' || CAST(f.user_id AS varchar)) AS username,
    COUNT(*) AS event_count,
    SUM(CASE WHEN f.action = 'dashboard_view' THEN 1 ELSE 0 END) AS dashboard_views,
    SUM(CASE WHEN f.action = 'chart_view' THEN 1 ELSE 0 END) AS chart_views,
    MAX(f.event_ts) AS last_active
FROM {T}.fact_access_events f
LEFT JOIN {T}.dim_users u ON u.user_id = f.user_id
GROUP BY f.user_id, u.username
""".strip()

ZERO_VIEW_DASHBOARDS_SQL = f"""
SELECT
    d.dashboard_id AS dashboard_id,
    d.title AS title,
    COALESCE(d.owner_names, '') AS owner_names,
    COALESCE(v.view_count, 0) AS view_count
FROM {T}.dim_dashboards d
LEFT JOIN (
    SELECT dashboard_id, COUNT(*) AS view_count
    FROM {T}.fact_access_events
    WHERE action = 'dashboard_view'
    GROUP BY dashboard_id
) v ON v.dashboard_id = d.dashboard_id
WHERE COALESCE(v.view_count, 0) <= 1
""".strip()

# Explicit column fallback for the virtual datasets above -- see
# _ensure_virtual_dataset_columns() for why this is needed (Superset's virtual
# dataset column introspection silently returns no columns when the probe query
# happens to return zero rows, which `vw_zero_view_dashboards` can easily do).
# Tuples are (column_name, sql type, is_dttm).
MOST_VIEWED_DASHBOARDS_COLUMNS = [
    ("dashboard_id", "INTEGER", False),
    ("title", "VARCHAR", False),
    ("view_count", "BIGINT", False),
    ("unique_viewers", "BIGINT", False),
]
USER_ACTIVITY_COLUMNS = [
    ("user_id", "INTEGER", False),
    ("username", "VARCHAR", False),
    ("event_count", "BIGINT", False),
    ("dashboard_views", "BIGINT", False),
    ("chart_views", "BIGINT", False),
    ("last_active", "TIMESTAMP(6)", True),
]
ZERO_VIEW_DASHBOARDS_COLUMNS = [
    ("dashboard_id", "INTEGER", False),
    ("title", "VARCHAR", False),
    ("owner_names", "VARCHAR", False),
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
    return ice.get_or_create_trino_database(client, sys.modules[__name__])


def migrate_legacy_database(client, new_db_id):
    """Installs made when the pipeline used MySQL still have datasets bound to that connection. Point them at Trino,
    then drop the old connection. Best effort: a failure here leaves the old connection in place, harmlessly."""
    old = get_one(client, "/api/v1/database/",
                  f"(filters:!((col:database_name,opr:eq,value:'{LEGACY_DB_CONNECTION_NAME}')))")
    if not old:
        return
    try:
        resp = client.get(f"/api/v1/dataset/?q=(filters:!((col:database,opr:rel_o_m,value:{old['id']})),page_size:100)")
        for d in resp.json().get("result", []):
            client.session.put(f"{client.base_url}/api/v1/dataset/{d['id']}",
                               json={"database_id": new_db_id, "schema": SCHEMA}, timeout=60).raise_for_status()
            print(f"moved dataset '{d['table_name']}' from the MySQL connection to Trino")
        client.session.delete(f"{client.base_url}/api/v1/database/{old['id']}", timeout=60).raise_for_status()
        print(f"removed the old '{LEGACY_DB_CONNECTION_NAME}' connection")
    except Exception as exc:  # noqa: BLE001
        print(f"WARNING: could not migrate the old MySQL connection ({exc}); remove it by hand in Superset")


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
        # keep the dataset's SQL and connection in step with this file (an older install pointed at MySQL)
        update = {"database_id": db_id, "schema": SCHEMA}
        if sql:
            update["sql"] = sql
        client.session.put(f"{client.base_url}/api/v1/dataset/{dataset_id}", json=update, timeout=60).raise_for_status()
        if sql:
            _refresh_dataset_columns(client, dataset_id)
            if column_defs:
                _ensure_virtual_dataset_columns(client, dataset_id, table_name, column_defs)
        print(f"dataset '{table_name}' already exists (id={dataset_id})")
        return dataset_id
    payload = {
        "database": db_id,
        "schema": SCHEMA,
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
    migrate_legacy_database(client, db_id)

    fact_id = get_or_create_dataset(client, db_id, "fact_access_events")
    ice.mark_time_column(client, sys.modules[__name__], fact_id, "event_ts")  # Superset offers day/week/month grains
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

    # failure rate, load time, errors by category: a second dashboard on the same Trino connection
    ice.main(client, sys.modules[__name__])

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

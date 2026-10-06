"""
Builds the "Dashboard Reliability and Speed" dashboard on top of the Iceberg tables (read through Trino).
Called by bootstrap_dashboard.py when ICEBERG_SINK_ENABLED is on; idempotent like the rest of the bootstrap.

  * registers a Superset database connection to Trino (catalog `iceberg`)
  * three virtual datasets, named so they cannot collide with the MySQL datasets of the usage dashboard
      ice_failure_rate  : the day-grain view vw_chart_failure_rate (chart loads, failed loads, server kills)
      ice_chart_loads   : chart views with load time, joined to dashboard titles
      ice_chart_errors  : the error rows, joined to dashboard titles
  * charts, all aggregated by Superset's own time grain (day, week, month):
      failure rate = SUM(failed_loads) / SUM(chart_loads)   (a ratio of sums, not an average of daily ratios)
      load time    = average and p95 of the browser-measured load time of successful chart loads
      slow rate    = share of successful loads slower than SLOW_LOAD_MS

The failure rate counts browser-reported failures only, so it is a LOWER BOUND (a tab closed during the timeout
reports nothing). The "server kills" column is gunicorn's own count of killed workers, shown beside it.
"""
import os

TRINO_DB_NAME = "Lakehouse (Trino Iceberg)"
TRINO_URI = "trino://%s@%s:%s/%s" % (
    os.environ.get("TRINO_USER", "pipeline"),
    os.environ.get("TRINO_HOST", "trino"),
    os.environ.get("TRINO_PORT", "8080"),
    os.environ.get("TRINO_CATALOG", "iceberg"),
)
SCHEMA = os.environ.get("TRINO_SCHEMA", "monitoring")
CATALOG = os.environ.get("TRINO_CATALOG", "iceberg")
SLOW_LOAD_MS = int(os.environ.get("SLOW_LOAD_MS", "5000"))
DASHBOARD_TITLE = "Dashboard Reliability and Speed"

FAILURE_RATE_SQL = f"SELECT * FROM {CATALOG}.{SCHEMA}.vw_chart_failure_rate"
FAILURE_RATE_COLUMNS = [
    ("day", "DATE", True), ("dashboard_id", "INTEGER", False), ("title", "VARCHAR", False),
    ("chart_loads", "BIGINT", False), ("failed_loads", "BIGINT", False), ("server_kills", "BIGINT", False),
    ("server_errors", "BIGINT", False),
]

CHART_LOADS_SQL = f"""
SELECT a.event_ts, a.dashboard_id, coalesce(d.title, 'Dashboard ' || cast(a.dashboard_id AS varchar)) AS title,
       a.chart_id, a.user_id, a.duration_ms, a.is_error
FROM {CATALOG}.{SCHEMA}.fact_access_events a
LEFT JOIN {CATALOG}.{SCHEMA}.dim_dashboards d ON d.dashboard_id = a.dashboard_id
WHERE a.action = 'chart_view' AND a.event_ts <= CAST(current_timestamp AT TIME ZONE 'UTC' AS timestamp(6))
""".strip()
CHART_LOADS_COLUMNS = [
    ("event_ts", "TIMESTAMP(6)", True), ("dashboard_id", "INTEGER", False), ("title", "VARCHAR", False),
    ("chart_id", "INTEGER", False), ("user_id", "INTEGER", False), ("duration_ms", "INTEGER", False),
    ("is_error", "BOOLEAN", False),
]

CHART_ERRORS_SQL = f"""
SELECT e.event_ts, e.source, coalesce(e.error_category, 'unclassified') AS error_category, e.error_type,
       coalesce(e.error_signature, e.error_type) AS error_signature,
       coalesce(e.error_message, e.reason) AS error_message, e.status_code, e.request_path,
       coalesce(d.title, '') AS dashboard, e.chart_id, e.user_id, e.duration_ms, e.reason
FROM {CATALOG}.{SCHEMA}.fact_error_events e
LEFT JOIN {CATALOG}.{SCHEMA}.dim_dashboards d ON d.dashboard_id = e.dashboard_id
""".strip()
CHART_ERRORS_COLUMNS = [
    ("event_ts", "TIMESTAMP(6)", True), ("source", "VARCHAR", False), ("error_category", "VARCHAR", False),
    ("error_type", "VARCHAR", False), ("error_signature", "VARCHAR", False), ("error_message", "VARCHAR", False),
    ("status_code", "INTEGER", False), ("request_path", "VARCHAR", False), ("dashboard", "VARCHAR", False),
    ("chart_id", "INTEGER", False), ("user_id", "INTEGER", False), ("duration_ms", "INTEGER", False),
    ("reason", "VARCHAR", False),
]

# ---- metrics (SQL expressions Superset wraps in its own time-grain GROUP BY) ----
FAILURE_RATE = {
    "expressionType": "SQL",
    "sqlExpression": "SUM(failed_loads) * 1.0 / NULLIF(SUM(chart_loads), 0)",
    "label": "Failure rate",
}
CHART_LOADS = {"expressionType": "SQL", "sqlExpression": "SUM(chart_loads)", "label": "Chart loads"}
FAILED_LOADS = {"expressionType": "SQL", "sqlExpression": "SUM(failed_loads)", "label": "Failed loads"}
SERVER_KILLS = {"expressionType": "SQL", "sqlExpression": "SUM(server_kills)", "label": "Server kills"}
SERVER_ERRORS = {"expressionType": "SQL", "sqlExpression": "SUM(server_errors)", "label": "Server errors"}
# error discovery: group by what the error SAYS, so an unknown cause shows up as a growing signature
ERRORS = {"expressionType": "SQL", "sqlExpression": "COUNT(*)", "label": "Errors"}
OCCURRENCES = {"expressionType": "SQL", "sqlExpression": "COUNT(*)", "label": "Occurrences"}
LAST_SEEN = {"expressionType": "SQL", "sqlExpression": "MAX(event_ts)", "label": "Last seen"}
EXAMPLE = {"expressionType": "SQL", "sqlExpression": "MAX(error_message)", "label": "Example message"}
AVG_LOAD = {"expressionType": "SQL", "sqlExpression": "AVG(duration_ms)", "label": "Average load (ms)"}
P95_LOAD = {"expressionType": "SQL", "sqlExpression": "approx_percentile(duration_ms, 0.95)", "label": "p95 load (ms)"}
SLOW_RATE = {
    "expressionType": "SQL",
    "sqlExpression": f"SUM(CASE WHEN duration_ms > {SLOW_LOAD_MS} THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0)",
    "label": f"Slow loads (> {SLOW_LOAD_MS} ms)",
}
LOADS_COUNT = {"expressionType": "SQL", "sqlExpression": "COUNT(*)", "label": "Successful loads"}
OK_LOADS_ONLY = {
    "clause": "WHERE", "expressionType": "SQL", "sqlExpression": "is_error = false AND duration_ms IS NOT NULL",
}


def _base(dataset_id, viz_type):
    return {"datasource": f"{dataset_id}__table", "viz_type": viz_type}


def _line(dataset_id, x_axis, grain, metrics, y_format, where=None):
    p = _base(dataset_id, "echarts_timeseries_line")
    p.update({
        "x_axis": x_axis, "time_grain_sqla": grain, "metrics": metrics, "groupby": [],
        "adhoc_filters": [where] if where else [], "row_limit": 10000, "time_range": "No filter",
        "show_legend": True, "rich_tooltip": True, "truncate_metric": True, "orientation": "vertical",
        "y_axis_format": y_format, "markerEnabled": True, "markerSize": 6,
    })
    return p


def _table(dataset_id, groupby, metrics, sort_metric, where=None, limit=50):
    p = _base(dataset_id, "table")
    p.update({
        "query_mode": "aggregate", "groupby": groupby, "metrics": metrics, "all_columns": [],
        "adhoc_filters": [where] if where else [], "order_desc": True, "timeseries_limit_metric": sort_metric,
        "row_limit": limit, "server_pagination": False, "time_range": "No filter",
    })
    return p


def _raw_table(dataset_id, columns, order_col, limit=100):
    p = _base(dataset_id, "table")
    p.update({
        "query_mode": "raw", "all_columns": columns, "order_by_cols": [f'["{order_col}", false]'],
        "row_limit": limit, "server_pagination": False, "time_range": "No filter", "adhoc_filters": [],
    })
    return p


def _bars(dataset_id, x_axis, grain, metrics, groupby=None, stacked=False):
    p = _line(dataset_id, x_axis, grain, metrics, "SMART_NUMBER")
    p["viz_type"] = "echarts_timeseries_bar"
    if groupby:
        p["groupby"] = groupby
    if stacked:
        p["stack"] = "Stack"
    return p


def get_or_create_trino_database(client, h):
    existing = h.get_one(
        client, "/api/v1/database/",
        f"(filters:!((col:database_name,opr:eq,value:'{TRINO_DB_NAME}')))",
    )
    if existing:
        print(f"database connection '{TRINO_DB_NAME}' already exists (id={existing['id']})")
        return existing["id"]
    resp = client.post("/api/v1/database/", json={
        "database_name": TRINO_DB_NAME, "sqlalchemy_uri": TRINO_URI, "expose_in_sqllab": True,
    })
    db_id = resp.json()["id"]
    print(f"created database connection '{TRINO_DB_NAME}' (id={db_id})")
    return db_id


def mark_time_column(client, h, dataset_id, column):
    """Superset only offers time grains (day/week/month) when the dataset has a time column. A virtual dataset over
    Trino does not get one automatically, so set it explicitly. Idempotent."""
    cols = h._dataset_columns(client, dataset_id)
    payload = {
        "main_dttm_col": column,
        "columns": [
            {"id": c["id"], "column_name": c["column_name"], "type": c.get("type"),
             "is_dttm": c["column_name"] == column, "groupby": True, "filterable": True}
            for c in cols
        ],
    }
    client.session.put(f"{client.base_url}/api/v1/dataset/{dataset_id}", json=payload, timeout=60).raise_for_status()


def get_or_create_virtual_dataset(client, h, db_id, name, sql, column_defs, time_column=None):
    existing = h.get_one(
        client, "/api/v1/dataset/",
        f"(filters:!((col:table_name,opr:eq,value:'{name}')))",
    )
    if existing:
        dataset_id = existing["id"]
        # keep the dataset's SQL in step with this file, so a new column added here reaches an existing install
        client.session.put(f"{client.base_url}/api/v1/dataset/{dataset_id}", json={"sql": sql}, timeout=60).raise_for_status()
        h._refresh_dataset_columns(client, dataset_id)
        h._ensure_virtual_dataset_columns(client, dataset_id, name, column_defs)
        if time_column:
            mark_time_column(client, h, dataset_id, time_column)
        print(f"dataset '{name}' already exists (id={dataset_id})")
        return dataset_id
    resp = client.post("/api/v1/dataset/", json={
        "database": db_id, "schema": SCHEMA, "table_name": name, "sql": sql,
    })
    dataset_id = resp.json()["id"]
    h._refresh_dataset_columns(client, dataset_id)
    h._ensure_virtual_dataset_columns(client, dataset_id, name, column_defs)
    if time_column:
        mark_time_column(client, h, dataset_id, time_column)
    print(f"created dataset '{name}' (id={dataset_id})")
    return dataset_id


def main(client, h):
    """`h` is the bootstrap_dashboard module (its get_or_create_* helpers are reused, not copied)."""
    db_id = get_or_create_trino_database(client, h)
    rate_id = get_or_create_virtual_dataset(client, h, db_id, "ice_failure_rate", FAILURE_RATE_SQL, FAILURE_RATE_COLUMNS, "day")
    loads_id = get_or_create_virtual_dataset(client, h, db_id, "ice_chart_loads", CHART_LOADS_SQL, CHART_LOADS_COLUMNS, "event_ts")
    errors_id = get_or_create_virtual_dataset(client, h, db_id, "ice_chart_errors", CHART_ERRORS_SQL, CHART_ERRORS_COLUMNS, "event_ts")

    dash = h.get_or_create_dashboard(client, DASHBOARD_TITLE)
    chart = lambda name, ds, viz, params: h.get_or_create_chart(client, name, ds, viz, params, dash)  # noqa: E731

    # reliability: the same metric at three grains
    chart("Failure Rate (Daily)", rate_id, "echarts_timeseries_line", _line(rate_id, "day", "P1D", [FAILURE_RATE], ".2%"))
    chart("Failure Rate (Weekly)", rate_id, "echarts_timeseries_line", _line(rate_id, "day", "P1W", [FAILURE_RATE], ".2%"))
    chart("Failure Rate (Monthly)", rate_id, "echarts_timeseries_line", _line(rate_id, "day", "P1M", [FAILURE_RATE], ".2%"))
    chart("Failed vs Total Chart Loads (Daily)", rate_id, "echarts_timeseries_bar",
          _bars(rate_id, "day", "P1D", [CHART_LOADS, FAILED_LOADS, SERVER_KILLS]))
    chart("Failure Rate by Dashboard", rate_id, "table",
          _table(rate_id, ["title"], [FAILURE_RATE, CHART_LOADS, FAILED_LOADS, SERVER_ERRORS, SERVER_KILLS], FAILURE_RATE))

    # speed: successful loads only (a timeout always reads about 15 s and would hide real slowness)
    chart("Chart Load Time (Daily)", loads_id, "echarts_timeseries_line",
          _line(loads_id, "event_ts", "P1D", [AVG_LOAD, P95_LOAD], ",.0f", OK_LOADS_ONLY))
    chart("Chart Load Time (Weekly)", loads_id, "echarts_timeseries_line",
          _line(loads_id, "event_ts", "P1W", [AVG_LOAD, P95_LOAD], ",.0f", OK_LOADS_ONLY))
    chart("Slow Loads by Dashboard", loads_id, "table",
          _table(loads_id, ["title"], [SLOW_RATE, AVG_LOAD, P95_LOAD, LOADS_COUNT], SLOW_RATE, OK_LOADS_ONLY))

    # what is failing, whatever the cause: by kind over time, and by message to spot a new cause
    chart("Errors by Category (Weekly)", errors_id, "echarts_timeseries_bar",
          _bars(errors_id, "event_ts", "P1W", [ERRORS], groupby=["error_category"], stacked=True))
    chart("Top Error Messages", errors_id, "table",
          _table(errors_id, ["error_category", "error_signature"], [OCCURRENCES, LAST_SEEN, EXAMPLE], OCCURRENCES,
                 limit=30))

    # the individual failures, with their category and raw message (moved here from the usage dashboard)
    chart("Chart Load Errors", errors_id, "table", _raw_table(
        errors_id, ["event_ts", "source", "error_category", "status_code", "dashboard", "chart_id", "user_id",
                    "duration_ms", "error_message"],
        "event_ts"))
    print(f"Iceberg dashboard ready: '{DASHBOARD_TITLE}'")

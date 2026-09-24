"""
TASK 2 repro setup: create a virtual dataset whose query sleeps for longer than
the webserver/gunicorn timeout, a chart on it, and a dashboard containing that
chart -- so loading the dashboard reproduces "chart query slow enough to hit a
timeout, worker killed mid-request, nothing logged".

Run once against the running stack:
    docker compose run --rm pipeline python /repro/create_slow_dataset.py
(mounted read-only into the pipeline container -- see docker-compose.yml)
"""
import os
import sys

sys.path.insert(0, "/pipeline")  # reuse the same SupersetClient as the other scripts
from superset_client import SupersetClient  # noqa: E402

SLEEP_SECONDS = int(os.environ.get("SLOW_QUERY_SLEEP_SECONDS", "45"))


def find_examples_database(client: SupersetClient) -> int:
    resp = client.get(
        "/api/v1/database/?q=(filters:!((col:database_name,opr:ct,value:examples)))"
    )
    result = resp.json().get("result", [])
    if not result:
        raise RuntimeError(
            "No 'examples' database connection found -- has `superset load_examples` run yet?"
        )
    return result[0]["id"]


def delete_existing_dataset(client: SupersetClient, database_id: int) -> None:
    """Delete any leftover 'slow_query_demo' dataset from a prior run so re-running
    this script (e.g. after a client-side timeout on a previous attempt) doesn't hit
    a 422 on the table_name uniqueness constraint."""
    resp = client.get(
        "/api/v1/dataset/?q=(filters:!((col:table_name,opr:eq,value:slow_query_demo)))"
    )
    for existing in resp.json().get("result", []):
        if existing["database"]["id"] == database_id:
            client.delete(f"/api/v1/dataset/{existing['id']}")
            print(f"deleted leftover dataset id={existing['id']} from a prior run")


def create_dataset(client: SupersetClient, database_id: int) -> int:
    delete_existing_dataset(client, database_id)
    sql = f"SELECT pg_sleep({SLEEP_SECONDS}) AS slept, 1 AS metric"
    payload = {
        "database": database_id,
        "schema": "public",
        "table_name": "slow_query_demo",
        "sql": sql,
    }
    # Superset introspects the virtual dataset's columns by running the query at
    # creation time -- for a query built around pg_sleep(), that can take multiple
    # multiples of SLEEP_SECONDS (column inference plus internal retries), well past
    # the default 60s client timeout. Give it a generous margin.
    resp = client.post("/api/v1/dataset/", json=payload, timeout=max(60, SLEEP_SECONDS * 4))
    dataset_id = resp.json()["id"]
    print(f"created virtual dataset id={dataset_id} (sleeps {SLEEP_SECONDS}s per query)")
    return dataset_id


def create_dashboard(client: SupersetClient) -> int:
    resp = client.post(
        "/api/v1/dashboard/",
        json={"dashboard_title": "Intermittent Error Repro (slow chart)"},
    )
    dashboard_id = resp.json()["id"]
    print(f"created dashboard id={dashboard_id}")
    return dashboard_id


def create_chart(client: SupersetClient, dataset_id: int, dashboard_id: int) -> int:
    payload = {
        "slice_name": "Slow Chart (sleeps past webserver timeout)",
        "viz_type": "big_number_total",
        "datasource_id": dataset_id,
        "datasource_type": "table",
        "params": (
            '{"viz_type": "big_number_total", "metric": "count", '
            f'"datasource": "{dataset_id}__table", "adhoc_filters": []}}'
        ),
        "dashboards": [dashboard_id],
    }
    resp = client.post("/api/v1/chart/", json=payload)
    chart_id = resp.json()["id"]
    print(f"created chart id={chart_id}, attached to dashboard id={dashboard_id}")
    return chart_id


def main():
    client = SupersetClient()
    database_id = find_examples_database(client)
    dataset_id = create_dataset(client, database_id)
    dashboard_id = create_dashboard(client)
    chart_id = create_chart(client, dataset_id, dashboard_id)

    print("\n--- repro fixture ready ---")
    print(f"dashboard_id={dashboard_id} chart_id={chart_id} dataset_id={dataset_id}")
    print(f"dashboard URL: {client.base_url}/superset/dashboard/{dashboard_id}/")
    print("Next: run trigger_timeout.py to fire the slow chart query and capture logs.")

    with open("/repro/.repro_ids.env", "w") as f:
        f.write(f"REPRO_DASHBOARD_ID={dashboard_id}\n")
        f.write(f"REPRO_CHART_ID={chart_id}\n")
        f.write(f"REPRO_DATASET_ID={dataset_id}\n")


if __name__ == "__main__":
    main()

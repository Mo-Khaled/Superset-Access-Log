"""
Incremental ETL: /api/v1/log/ (Superset) -> raw_log_landing / dim_users /
dim_dashboards / fact_access_events (MySQL).

Run standalone: `python etl/sync_logs.py` (PYTHONPATH must include the pipeline
root -- see the Dockerfile / README). Safe to re-run: reads its high-water-mark
from etl_sync_state before pulling, and only advances it after a successful,
committed batch, so a re-run after a partial failure just re-pulls the same page
and upserts (idempotent on primary keys) rather than duplicating rows.

Scheduling (see README/WRITEUP for detail): designed to be invoked by an external
scheduler (cron in this sandbox; Airflow or a k8s CronJob in prod) rather than
looping internally -- each run does one bounded incremental sync and exits.
"""
import json
import os
import sys
from datetime import datetime, timezone

import pymysql
import pymysql.cursors

from superset_client import SupersetClient

PAGE_SIZE = int(os.environ.get("ETL_PAGE_SIZE", "100"))
SOURCE_NAME = os.environ.get("ETL_SOURCE_NAME", "local_superset")

# Actions we classify into the fact table; everything else from LogRestApi (e.g.
# generic 'log' rows for non-view interactions) is still staged in raw_log_landing
# for completeness, but excluded from fact_access_events since it's not a
# dashboard/chart/export "access" event that the usage dashboard should count.
ACTION_MAP = {
    "dashboard_load": "dashboard_view",
    "chart_data": "chart_view",
    "export_csv": "export_csv",
    "export_excel": "export_excel",
}


def mysql_conn():
    return pymysql.connect(
        host=os.environ["MYSQL_HOST"],
        port=int(os.environ.get("MYSQL_PORT", "3306")),
        user=os.environ["MYSQL_USER"],
        password=os.environ["MYSQL_PASSWORD"],
        database=os.environ["MYSQL_DATABASE"],
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
    )


def get_sync_state(conn):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT last_synced_log_id, last_synced_ts FROM etl_sync_state WHERE source_name = %s",
            (SOURCE_NAME,),
        )
        row = cur.fetchone()
        if row is None:
            return 0, None
        return row["last_synced_log_id"], row["last_synced_ts"]


def set_sync_state(conn, last_log_id, last_ts):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO etl_sync_state (source_name, last_synced_log_id, last_synced_ts)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
                last_synced_log_id = VALUES(last_synced_log_id),
                last_synced_ts = VALUES(last_synced_ts)
            """,
            (SOURCE_NAME, last_log_id, last_ts),
        )


def fetch_log_pages(client: SupersetClient, since_ts):
    """Yields pages of log rows with dttm > since_ts, ordered by dttm ascending.

    LogRestApi only allows filtering/ordering on columns in its declared
    list_columns (id is not one of them -- filtering or ordering by id returns
    a 400), so the incremental watermark here is timestamp-based instead. The
    API also returns each row's numeric id in a separate top-level `ids` array
    (parallel-indexed to `result`, not embedded in each row object), which we
    merge back in below.
    """
    page = 0
    since_str = since_ts.isoformat() if hasattr(since_ts, "isoformat") else (since_ts or "1970-01-01T00:00:00")
    while True:
        rison_filter = (
            f"(filters:!((col:dttm,opr:gt,value:'{since_str}')),"
            f"order_column:dttm,order_direction:asc,"
            f"page:{page},page_size:{PAGE_SIZE})"
        )
        resp = client.get(f"/api/v1/log/?q={rison_filter}")
        body = resp.json()
        result = body.get("result", [])
        ids = body.get("ids", [])
        if not result:
            return
        for row, row_id in zip(result, ids):
            row["id"] = row_id
        yield result
        if len(result) < PAGE_SIZE:
            return
        page += 1


def sync_all_dashboards(client: SupersetClient, conn):
    """Upserts every dashboard Superset currently knows about into dim_dashboards --
    not just ones referenced by a log row. A dashboard with zero views never
    appears in a log pull at all, so without this pass it could never show up
    in the "zero/near-zero views" report, which defeats the point of that report
    (the whole reason to run it is to find dashboards nobody's touching)."""
    page = 0
    with conn.cursor() as cur:
        while True:
            resp = client.get(f"/api/v1/dashboard/?q=(page:{page},page_size:100)")
            result = resp.json().get("result", [])
            if not result:
                break
            for d in result:
                owners = ", ".join(o.get("username", "") for o in d.get("owners", []))
                cur.execute(
                    """
                    INSERT INTO dim_dashboards (dashboard_id, title, owner_names, created_on, changed_on)
                    VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        title = VALUES(title), owner_names = VALUES(owner_names),
                        changed_on = VALUES(changed_on)
                    """,
                    (d["id"], d.get("dashboard_title", f"Dashboard {d['id']}"),
                     owners, d.get("created_on"), d.get("changed_on")),
                )
            if len(result) < 100:
                break
            page += 1
    conn.commit()


def upsert_dim_dashboard(client: SupersetClient, cache: dict, cur, dashboard_id):
    if dashboard_id is None or dashboard_id in cache:
        return
    try:
        resp = client.get(f"/api/v1/dashboard/{dashboard_id}")
        d = resp.json()["result"]
    except Exception:
        cache[dashboard_id] = True
        return
    owners = ", ".join(o.get("username", "") for o in d.get("owners", []))
    cur.execute(
        """
        INSERT INTO dim_dashboards (dashboard_id, title, owner_names, created_on, changed_on)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            title = VALUES(title), owner_names = VALUES(owner_names),
            changed_on = VALUES(changed_on)
        """,
        (dashboard_id, d.get("dashboard_title", f"Dashboard {dashboard_id}"),
         owners, d.get("created_on"), d.get("changed_on")),
    )
    cache[dashboard_id] = True


def upsert_dim_user(client: SupersetClient, cache: dict, cur, user_id, username_hint):
    if user_id is None or user_id in cache:
        return
    try:
        resp = client.get(f"/api/v1/user/{user_id}")
        u = resp.json().get("result", {})
    except Exception:
        u = {}
    cur.execute(
        """
        INSERT INTO dim_users (user_id, username, first_name, last_name, email, is_active)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            username = VALUES(username), first_name = VALUES(first_name),
            last_name = VALUES(last_name), email = VALUES(email),
            is_active = VALUES(is_active)
        """,
        (user_id, u.get("username", username_hint or f"user_{user_id}"),
         u.get("first_name"), u.get("last_name"), u.get("email"),
         u.get("active", True)),
    )
    cache[user_id] = True


def run():
    client = SupersetClient()
    conn = mysql_conn()
    sync_all_dashboards(client, conn)
    last_log_id, last_ts = get_sync_state(conn)

    dashboard_cache, user_cache = {}, {}
    total_raw, total_fact = 0, 0
    max_log_id = last_log_id
    max_ts = last_ts

    for page in fetch_log_pages(client, last_ts):
        with conn.cursor() as cur:
            for row in page:
                log_id = row["id"]
                max_log_id = max(max_log_id, log_id)

                cur.execute(
                    """
                    INSERT IGNORE INTO raw_log_landing (source_log_id, payload)
                    VALUES (%s, %s)
                    """,
                    (log_id, json.dumps(row, default=str)),
                )
                total_raw += 1

                action = row.get("action")
                fact_action = ACTION_MAP.get(action)
                dttm = row.get("dttm")
                if dttm:
                    max_ts = dttm

                user_id = row.get("user_id")
                username_hint = (row.get("user") or {}).get("username") if isinstance(row.get("user"), dict) else None
                upsert_dim_user(client, user_cache, cur, user_id, username_hint)

                dashboard_id = row.get("dashboard_id")
                upsert_dim_dashboard(client, dashboard_cache, cur, dashboard_id)

                if fact_action is None:
                    continue  # staged in raw_log_landing only; not an access event we rank on

                cur.execute(
                    """
                    INSERT IGNORE INTO fact_access_events
                        (event_id, event_ts, user_id, dashboard_id, chart_id, action, source_ip, raw_log_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (log_id, dttm, user_id, dashboard_id, row.get("slice_id"),
                     fact_action, None, log_id),
                    # source_ip is always NULL: LogRestApi's Log model does not
                    # capture client IP at all (see WRITEUP.md "gaps" section).
                )
                total_fact += 1

        conn.commit()

    if total_raw > 0:
        with conn.cursor() as cur:
            set_sync_state(conn, max_log_id, max_ts)
        conn.commit()

    print(f"synced through log id {max_log_id}: {total_raw} raw rows staged, "
          f"{total_fact} fact rows inserted (dupes ignored on re-run)")
    conn.close()


if __name__ == "__main__":
    try:
        run()
    except Exception as exc:
        print(f"ETL run failed: {exc}", file=sys.stderr)
        raise

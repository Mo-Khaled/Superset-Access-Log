"""Parameterized SQL for the standalone usage dashboard. Everything here reads
only from MySQL -- no calls back to Superset -- so the dashboard stays up even if
the source Superset instance is degraded or down."""
import os

import pymysql
import pymysql.cursors

WINDOW_DAYS = {"7d": 7, "30d": 30, "90d": 90, "all": None}


def get_conn():
    return pymysql.connect(
        host=os.environ["MYSQL_HOST"],
        port=int(os.environ.get("MYSQL_PORT", "3306")),
        user=os.environ["MYSQL_USER"],
        password=os.environ["MYSQL_PASSWORD"],
        database=os.environ["MYSQL_DATABASE"],
        cursorclass=pymysql.cursors.DictCursor,
    )


def _window_clause(window: str) -> str:
    days = WINDOW_DAYS.get(window, 30)
    if days is None:
        return "1=1"
    return f"event_ts >= DATE_SUB(NOW(), INTERVAL {int(days)} DAY)"


def top_dashboards(window: str, limit: int = 10):
    clause = _window_clause(window)
    sql = f"""
        SELECT
            f.dashboard_id,
            COALESCE(d.title, CONCAT('Dashboard ', f.dashboard_id)) AS title,
            COUNT(*) AS view_count,
            COUNT(DISTINCT f.user_id) AS unique_viewers
        FROM fact_access_events f
        LEFT JOIN dim_dashboards d ON d.dashboard_id = f.dashboard_id
        WHERE f.action = 'dashboard_view' AND {clause}
        GROUP BY f.dashboard_id, d.title
        ORDER BY view_count DESC
        LIMIT %s
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (limit,))
        return cur.fetchall()


def trend_for_top_dashboards(window: str, top_n: int = 5):
    clause = _window_clause(window)
    top_ids_sql = f"""
        SELECT dashboard_id FROM fact_access_events
        WHERE action = 'dashboard_view' AND {clause}
        GROUP BY dashboard_id ORDER BY COUNT(*) DESC LIMIT %s
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(top_ids_sql, (top_n,))
        top_ids = [r["dashboard_id"] for r in cur.fetchall()]
        if not top_ids:
            return {"dashboards": [], "series": []}

        fmt_ids = ",".join(str(i) for i in top_ids)
        sql = f"""
            SELECT
                f.dashboard_id,
                COALESCE(d.title, CONCAT('Dashboard ', f.dashboard_id)) AS title,
                DATE(f.event_ts) AS day,
                COUNT(*) AS view_count
            FROM fact_access_events f
            LEFT JOIN dim_dashboards d ON d.dashboard_id = f.dashboard_id
            WHERE f.action = 'dashboard_view' AND f.dashboard_id IN ({fmt_ids}) AND {clause}
            GROUP BY f.dashboard_id, d.title, DATE(f.event_ts)
            ORDER BY day ASC
        """
        cur.execute(sql)
        rows = cur.fetchall()

    titles = {}
    series = {}
    for r in rows:
        titles[r["dashboard_id"]] = r["title"]
        series.setdefault(r["dashboard_id"], []).append(
            {"day": r["day"].isoformat(), "view_count": r["view_count"]}
        )
    return {
        "dashboards": [{"dashboard_id": did, "title": titles[did]} for did in top_ids],
        "series": series,
    }


def zero_view_dashboards(window: str):
    clause = _window_clause(window)
    sql = f"""
        SELECT d.dashboard_id, d.title, d.owner_names,
               COALESCE(v.view_count, 0) AS view_count
        FROM dim_dashboards d
        LEFT JOIN (
            SELECT dashboard_id, COUNT(*) AS view_count
            FROM fact_access_events
            WHERE action = 'dashboard_view' AND {clause}
            GROUP BY dashboard_id
        ) v ON v.dashboard_id = d.dashboard_id
        WHERE COALESCE(v.view_count, 0) <= 1
        ORDER BY view_count ASC, d.title ASC
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()

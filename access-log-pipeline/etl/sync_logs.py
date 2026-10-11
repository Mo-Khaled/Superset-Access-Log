"""
Incremental ETL: /api/v1/log/ (Superset) -> raw_log_landing / dim_users /
dim_dashboards / fact_access_events (MySQL), and, when ICEBERG_SINK_ENABLED is on,
fact_access_events / fact_error_events / dim_dashboards in Iceberg through Trino.

Run standalone: `python etl/sync_logs.py [--since TIMESTAMP | --full]` (PYTHONPATH must include
the pipeline root -- see the Dockerfile / README).

Safe to re-run, and built so past-dated rows cannot be silently skipped:
  * LogRestApi cannot filter by id, so the pull filters on `dttm > watermark`. A row inserted later with an
    OLDER dttm (late browser flush, clock skew, downtime, a backfill) would never be seen. Three guards:
      1. lookback: every pull starts at `watermark - ETL_LOOKBACK_HOURS` (default 24)
      2. reconciliation: after the pull, the API total is compared with raw_log_landing; if the API has more
         rows than we hold, the whole log is re-pulled once and a WARNING is printed
      3. `--since TIMESTAMP` / `--full` for a deliberate backfill
  * Re-reading is always safe: MySQL uses INSERT IGNORE, Iceberg uses MERGE.
  * The watermark moves only after every write (including Iceberg) succeeded, so a failed run is simply retried.
  * Single writer: a MySQL advisory lock stops two runs from writing at once (Iceberg MERGEs would conflict).

Scheduling (see README/WRITEUP for detail): designed to be invoked by an external
scheduler (cron in this sandbox; Airflow or a k8s CronJob in prod) rather than
looping internally -- each run does one bounded incremental sync and exits.
"""
import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone

import pymysql
import pymysql.cursors

import iceberg_sink
from error_classify import one_line, row_category_and_signature
from superset_client import SupersetClient

PAGE_SIZE = int(os.environ.get("ETL_PAGE_SIZE", "100"))
SOURCE_NAME = os.environ.get("ETL_SOURCE_NAME", "local_superset")
LOOKBACK_HOURS = float(os.environ.get("ETL_LOOKBACK_HOURS", "24"))
LOCK_NAME = "etl_sync_logs"
EPOCH = datetime(1970, 1, 1)

# Actions we classify into the fact table; everything else from LogRestApi (e.g.
# generic 'log' rows for non-view interactions) is still staged in raw_log_landing
# for completeness, but excluded from fact_access_events since it's not a
# dashboard/chart/export "access" event that the usage dashboard should count.
#
# "dashboard_load"/"chart_data" match the shape seed/generate_activity.py writes
# directly into Postgres (mirroring what it documented as "exactly like the rows
# Superset itself would write"). In practice, Superset 4.1.4's real frontend
# logging pipeline does NOT write those as top-level `action` values for organic
# browser usage -- it posts a generic `action='log'` row and nests the actual
# event under `json.event_name` instead (see LOG_EVENT_NAME_MAP below). Both
# paths are kept so the dashboard counts real usage *and* the seeded demo data.
ACTION_MAP = {
    "dashboard_load": "dashboard_view",
    "chart_data": "chart_view",
    "export_csv": "export_csv",
    "export_excel": "export_excel",
}

# Maps the real event_name Superset's frontend nests inside a generic
# action='log' row's `json` payload to the same fact_access_events.action
# values as ACTION_MAP above.
LOG_EVENT_NAME_MAP = {
    "mount_dashboard": "dashboard_view",
    "load_chart": "chart_view",
    "force_refresh_chart": "chart_view",
}


def _classify_action(row):
    """Returns the fact_access_events action for a raw log row, checking both
    the top-level `action` column (seeded demo data) and, for generic 'log'
    rows, the nested `json.event_name` (real Superset frontend logging)."""
    action = row.get("action")
    mapped = ACTION_MAP.get(action)
    if mapped is not None:
        return mapped
    if action != "log":
        return None
    payload = row.get("json")
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except ValueError:
            return None
    if not isinstance(payload, dict):
        return None
    return LOG_EVENT_NAME_MAP.get(payload.get("event_name"))


def extract_browser_error(row):
    """Returns a fact_error_events dict for a browser-reported chart load failure
    (generic action='log' row, json.event_name='load_chart', json.has_err=true),
    else None. This runs *in addition to* the fact_access_events insert, which
    still counts the same row as a normal chart_view."""
    if row.get("action") != "log":
        return None
    payload = row.get("json")
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except ValueError:
            return None
    if not isinstance(payload, dict):
        return None
    if payload.get("event_name") != "load_chart" or not payload.get("has_err"):
        return None
    details = one_line(payload.get("error_details") or "unknown", 500)
    duration = payload.get("duration")
    reason = f"Browser reported chart load failure: {details}"
    if duration is not None:
        reason += f" after {int(duration)} ms"
    return {
        "error_type": "load_chart_error",
        "reason": reason[:500],
        "duration_ms": int(duration) if duration is not None else None,
        "chart_id": row.get("slice_id"),
        # cause-agnostic: whatever the browser reported, keep the raw message and derive a category and a signature
        "error_category": row_category_and_signature("browser", "load_chart_error", None, None, details)[0],
        "error_signature": row_category_and_signature("browser", "load_chart_error", None, None, details)[1],
        "error_message": details,
    }


def _payload(row):
    payload = row.get("json")
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except ValueError:
            return {}
    return payload if isinstance(payload, dict) else {}


def _load_metrics(row, action):
    """(duration_ms, is_error) for a chart view. Real browser load_chart rows carry the load time in
    json.duration; seeded rows carry it in the log row's own duration_ms. Dashboard views have none."""
    if action != "chart_view":
        return None, False
    if row.get("action") == "log":
        p = _payload(row)
        duration = p.get("duration")
        return (int(duration) if duration is not None else None), bool(p.get("has_err"))
    duration = row.get("duration_ms")
    return (int(duration) if duration is not None else None), False


def build_event_rows(row):
    """Turns one Superset log row into (access_row, error_row), either may be None. Shared by the ETL and the
    Iceberg backfill, so both classify a log row identically. Returns (None, None) when the row has no usable
    timestamp."""
    ts = _parse_dttm(row.get("dttm"))
    if ts is None:
        return None, None
    log_id = row["id"]
    access = error = None
    action = _classify_action(row)
    if action is not None:
        duration, is_error = _load_metrics(row, action)
        access = {
            "event_id": log_id, "event_ts": ts, "user_id": row.get("user_id"),
            "dashboard_id": row.get("dashboard_id"), "chart_id": row.get("slice_id"),
            "action": action, "raw_log_id": log_id, "duration_ms": duration, "is_error": is_error,
        }
    err = extract_browser_error(row)
    if err is not None:
        error = {
            "dedupe_key": f"browser:{log_id}", "event_ts": ts, "source": "browser",
            "error_type": err["error_type"], "reason": err["reason"], "user_id": row.get("user_id"),
            "dashboard_id": row.get("dashboard_id"), "chart_id": err["chart_id"],
            "duration_ms": err["duration_ms"], "raw_log_id": log_id,
            "error_category": err["error_category"], "error_signature": err["error_signature"],
            "error_message": err["error_message"], "status_code": None, "request_path": None, "request_id": None,
        }
    return access, error


def acquire_etl_lock(conn):
    """Single-writer guard. MySQL advisory locks belong to the connection and are released when it closes."""
    with conn.cursor() as cur:
        cur.execute("SELECT GET_LOCK(%s, 0) AS got", (LOCK_NAME,))
        return cur.fetchone()["got"] == 1


def insert_error_event(cur, row, err):
    cur.execute(
        """
        INSERT IGNORE INTO fact_error_events
            (event_ts, source, error_type, reason, user_id, dashboard_id,
             chart_id, duration_ms, raw_log_id, dedupe_key)
        VALUES (%s, 'browser', %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (row.get("dttm"), err["error_type"], err["reason"], row.get("user_id"),
         row.get("dashboard_id"), err["chart_id"], err["duration_ms"],
         row["id"], f"browser:{row['id']}"),
    )


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


def _parse_dttm(value):
    """Best-effort parse of a dttm value (already a datetime from MySQL, or an
    ISO-ish string as returned by LogRestApi JSON) into a naive UTC datetime
    for comparison. Returns None if it can't be parsed."""
    if isinstance(value, datetime):
        return value.replace(tzinfo=None) if value.tzinfo else value
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.replace(tzinfo=None) if parsed.tzinfo else parsed


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


def sync_all_dashboards(client: SupersetClient, conn, sink=None):
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
                if sink is not None:
                    sink.add_dashboard({"dashboard_id": d["id"],
                                        "title": d.get("dashboard_title", f"Dashboard {d['id']}")})
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


def ensure_landing_dttm(conn):
    """Migration for databases created before raw_log_landing.source_dttm existed (the MySQL init scripts only run
    on a fresh volume). Adds the column and fills it from the stored payloads. A no-op once done."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) AS n FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'raw_log_landing' AND COLUMN_NAME = 'source_dttm'"
        )
        if cur.fetchone()["n"] == 0:
            cur.execute("ALTER TABLE raw_log_landing ADD COLUMN source_dttm DATETIME(6) NULL, "
                        "ADD KEY idx_source_dttm (source_dttm)")
            conn.commit()
        while True:
            cur.execute("SELECT source_log_id, payload FROM raw_log_landing WHERE source_dttm IS NULL LIMIT 2000")
            rows = cur.fetchall()
            if not rows:
                break
            updates = []
            for r in rows:
                payload = json.loads(r["payload"]) if isinstance(r["payload"], str) else r["payload"]
                # a row with no usable timestamp gets the epoch so this loop always terminates
                updates.append((_parse_dttm(payload.get("dttm")) or EPOCH, r["source_log_id"]))
            cur.executemany("UPDATE raw_log_landing SET source_dttm = %s WHERE source_log_id = %s", updates)
            conn.commit()


def api_log_total(client: SupersetClient, until=None):
    """Number of rows Superset's log holds (the `count` field of the list endpoint), optionally only rows dated
    before `until`. Counting a fixed window on both sides is what makes the reconciliation reliable: the ETL's own
    API calls keep adding rows to Superset's log, which would hide a few missing rows in a total count."""
    # NOTE: this API silently IGNORES the operators lte/le/gte/ge (it returns the unfiltered count); only lt and gt
    # filter. Verified against Superset 4.1.4. Use lt/gt only.
    if until is None:
        q = "(page:0,page_size:1)"
    else:
        q = f"(filters:!((col:dttm,opr:lt,value:'{until.isoformat()}')),page:0,page_size:1)"
    return int(client.get(f"/api/v1/log/?q={q}").json().get("count", 0))


def landing_total(conn, until=None):
    with conn.cursor() as cur:
        if until is None:
            cur.execute("SELECT COUNT(*) AS n FROM raw_log_landing")
        else:
            cur.execute("SELECT COUNT(*) AS n FROM raw_log_landing WHERE source_dttm < %s", (until,))
        return cur.fetchone()["n"]


def pull(client, conn, since_ts, sink, stats, dashboard_cache, user_cache):
    """One pass over the Superset log from since_ts. Updates `stats`; returns (max_log_id, max_dttm).
    Everything written here is idempotent, so overlapping passes are harmless."""
    max_log_id, max_ts = 0, None
    for page in fetch_log_pages(client, since_ts):
        with conn.cursor() as cur:
            for row in page:
                log_id = row["id"]
                max_log_id = max(max_log_id, log_id)

                parsed = _parse_dttm(row.get("dttm"))
                cur.execute(
                    "INSERT IGNORE INTO raw_log_landing (source_log_id, payload, source_dttm) VALUES (%s, %s, %s)",
                    (log_id, json.dumps(row, default=str), parsed),
                )
                stats["raw_seen"] += 1
                stats["raw_new"] += cur.rowcount

                if parsed is not None and (max_ts is None or parsed > max_ts):
                    max_ts = parsed

                user_id = row.get("user_id")
                username_hint = (row.get("user") or {}).get("username") if isinstance(row.get("user"), dict) else None
                upsert_dim_user(client, user_cache, cur, user_id, username_hint)
                upsert_dim_dashboard(client, dashboard_cache, cur, row.get("dashboard_id"))

                access, error = build_event_rows(row)
                if error is not None:
                    stats["errors"] += 1
                    if sink is not None:
                        sink.add_error(error)
                    else:  # no Iceberg: keep the legacy MySQL error table so errors are not lost
                        insert_error_event(cur, row, extract_browser_error(row))
                if access is None:
                    continue  # staged in raw_log_landing only; not an access event we rank on

                cur.execute(
                    """
                    INSERT IGNORE INTO fact_access_events
                        (event_id, event_ts, user_id, dashboard_id, chart_id, action, source_ip, raw_log_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (log_id, row.get("dttm"), user_id, access["dashboard_id"], access["chart_id"],
                     access["action"], None, log_id),
                    # source_ip is always NULL: LogRestApi's Log model does not
                    # capture client IP at all (see WRITEUP.md "gaps" section).
                )
                stats["fact"] += 1
                if sink is not None:
                    sink.add_access(access)
        conn.commit()
    return max_log_id, max_ts


def run(since=None, full=False):
    client = SupersetClient()
    conn = mysql_conn()
    if not acquire_etl_lock(conn):
        print(f"another ETL run holds the lock '{LOCK_NAME}'; skipping this run (single writer)")
        conn.close()
        return 0

    ensure_landing_dttm(conn)
    sink = iceberg_sink.IcebergSink() if iceberg_sink.enabled() else None
    sync_all_dashboards(client, conn, sink)
    last_log_id, last_ts = get_sync_state(conn)
    watermark = _parse_dttm(last_ts)

    # Reconciliation window: everything Superset dated up to t0. After the pull we must hold every one of
    # those rows; if Superset has more than we do, the watermark filter skipped some.
    t0 = datetime.now(timezone.utc).replace(tzinfo=None)

    if full:
        start = EPOCH
    elif since is not None:
        start = since
    elif watermark is not None:
        start = watermark - timedelta(hours=LOOKBACK_HOURS)
    else:
        start = EPOCH

    stats = {"raw_seen": 0, "raw_new": 0, "fact": 0, "errors": 0}
    dashboard_cache, user_cache = {}, {}
    max_log_id, max_ts = pull(client, conn, start, sink, stats, dashboard_cache, user_cache)

    api_n, held = api_log_total(client, t0), landing_total(conn, t0)
    gap = api_n - held
    if gap > 0 and start > EPOCH:
        print(f"WARNING: Superset's log has {gap} rows dated up to {t0:%Y-%m-%d %H:%M:%S} that we do not hold "
              f"(api={api_n}, landing={held}); the watermark filter skipped them. "
              f"Re-pulling the whole log once (idempotent).")
        mid, mts = pull(client, conn, EPOCH, sink, stats, dashboard_cache, user_cache)
        max_log_id = max(max_log_id, mid)
        if mts is not None and (max_ts is None or mts > max_ts):
            max_ts = mts
        held = landing_total(conn, t0)
        gap = api_n - held
    if gap > 0:
        print(f"WARNING: still {gap} rows short after a full pull (api={api_n}, landing={held}); "
              f"a row may have been committed late. Not retrying; the next run will look again.")

    iceberg_written = sink.flush() if sink is not None else None  # must succeed before the watermark moves

    # Never move the watermark backwards, and never into the future: a future-dated row (bad clock, an old seed
    # script) would otherwise make every later real event look "already seen".
    now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
    new_ts = max([t for t in (max_ts, watermark) if t is not None], default=None)
    if new_ts is not None and new_ts > now_utc:
        new_ts = now_utc
    if new_ts is not None:
        set_sync_state(conn, max(last_log_id, max_log_id), new_ts)
        conn.commit()

    print(f"synced through log id {max(last_log_id, max_log_id)}: {stats['raw_seen']} log rows read "
          f"({stats['raw_new']} new in raw_log_landing), {stats['fact']} access events, "
          f"{stats['errors']} browser errors; pull started at {start:%Y-%m-%d %H:%M:%S}")
    if iceberg_written is not None:
        print(f"iceberg: sent {iceberg_written['access']} access rows, {iceberg_written['errors']} error rows, "
              f"{iceberg_written['dashboards']} dashboards to MERGE (rows already present are left untouched)")
    conn.close()
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Incremental Superset log ETL")
    ap.add_argument("--since", help="re-pull from this timestamp (ISO), for a known backfill")
    ap.add_argument("--full", action="store_true", help="re-pull the whole Superset log (idempotent)")
    args = ap.parse_args()
    try:
        sys.exit(run(since=_parse_dttm(args.since) if args.since else None, full=args.full))
    except Exception as exc:
        print(f"ETL run failed: {exc}", file=sys.stderr)
        raise

"""
Incremental ETL: Superset's log (/api/v1/log/) -> Iceberg, through Trino: raw_log_landing, dim_users, dim_dashboards,
fact_access_events and fact_error_events. Iceberg is the only store (there is no MySQL).

Run standalone: `python etl/sync_logs.py [--since TIMESTAMP | --full | --reprocess]` (PYTHONPATH must include the
pipeline root -- see the Dockerfile / README).

Safe to re-run, and built so past-dated rows cannot be silently skipped:
  * LogRestApi cannot filter by id, so the pull filters on `dttm > watermark`. A row inserted later with an
    OLDER dttm (late browser flush, clock skew, downtime, a backfill) would never be seen. Three guards:
      1. lookback: every pull starts at `watermark - ETL_LOOKBACK_HOURS` (default 24)
      2. reconciliation: after the pull, the API total is compared with raw_log_landing; if the API has more
         rows than we hold, the whole log is re-pulled once and a WARNING is printed
      3. `--since TIMESTAMP` / `--full` for a deliberate backfill
  * Re-reading is always safe: every write is an Iceberg MERGE. Rows already in raw_log_landing are skipped before
    anything is sent (they have all their facts, see below); `--reprocess` sends them again anyway.
  * The watermark is max(source_dttm) of raw_log_landing, derived from the data. Raw rows are written LAST, after
    the facts and dimensions they produced, so the watermark can only move once everything before it is stored. A run
    that fails halfway is simply repeated. There is no separate state to get out of step.
  * Single writer: a lease row in Iceberg (etl/iceberg_state.py) stops two runs from writing at once. Commit conflicts
    that still happen are retried (iceberg_sink.run_sql), and every write is idempotent.

Scheduling (see README/WRITEUP for detail): designed to be invoked by an external
scheduler (cron in this sandbox; Airflow or a k8s CronJob in prod) rather than
looping internally -- each run does one bounded incremental sync and exits.
"""
import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone

import iceberg_sink
import iceberg_state
from error_classify import one_line, row_category_and_signature
from superset_client import SupersetClient

PAGE_SIZE = int(os.environ.get("ETL_PAGE_SIZE", "100"))
LOOKBACK_HOURS = float(os.environ.get("ETL_LOOKBACK_HOURS", "24"))
LOCK_NAME = "etl"  # shared by sync_logs, ingest_container_errors, backfill_iceberg and iceberg_maintenance
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
    category, signature = row_category_and_signature("browser", "load_chart_error", None, None, details)
    return {
        "error_type": "load_chart_error",
        "reason": reason[:500],
        "duration_ms": int(duration) if duration is not None else None,
        "chart_id": row.get("slice_id"),
        # cause-agnostic: whatever the browser reported, keep the raw message and derive a category and a signature
        "error_category": category,
        "error_signature": signature,
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
    backfill, so both classify a log row identically. Returns (None, None) when the row has no usable timestamp."""
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


def _parse_dttm(value):
    """Best-effort parse of a dttm value (a datetime, or an ISO-ish string as returned by LogRestApi JSON) into a
    naive UTC datetime for comparison. Returns None if it can't be parsed."""
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


def _dashboard_row(d):
    owners = ", ".join(o.get("username", "") for o in d.get("owners", []))
    return {"dashboard_id": d["id"], "title": d.get("dashboard_title", f"Dashboard {d['id']}"),
            "owner_names": owners, "created_on": _parse_dttm(d.get("created_on")),
            "changed_on": _parse_dttm(d.get("changed_on"))}


def sync_all_dashboards(client: SupersetClient, sink):
    """Sends every dashboard Superset currently knows about to dim_dashboards -- not just ones referenced by a log
    row. A dashboard with zero views never appears in a log pull at all, so without this pass it could never show up
    in the "zero/near-zero views" report, which defeats the point of that report (the whole reason to run it is to
    find dashboards nobody's touching)."""
    page = 0
    while True:
        resp = client.get(f"/api/v1/dashboard/?q=(page:{page},page_size:100)")
        result = resp.json().get("result", [])
        if not result:
            break
        for d in result:
            sink.add_dashboard(_dashboard_row(d))
        if len(result) < 100:
            break
        page += 1


def enrich_dashboard(client: SupersetClient, cache: dict, sink, dashboard_id):
    """Covers a dashboard that is in the log but not in the listing (deleted, or created since the listing)."""
    if dashboard_id is None or dashboard_id in cache:
        return
    cache[dashboard_id] = True
    try:
        d = client.get(f"/api/v1/dashboard/{dashboard_id}").json()["result"]
    except Exception:  # noqa: BLE001
        return
    d["id"] = dashboard_id
    sink.add_dashboard(_dashboard_row(d))


def enrich_user(client: SupersetClient, cache: dict, sink, user_id, username_hint):
    if user_id is None or user_id in cache:
        return
    cache[user_id] = True
    try:
        u = client.get(f"/api/v1/user/{user_id}").json().get("result", {})
    except Exception:  # noqa: BLE001
        u = {}
    sink.add_user({"user_id": user_id, "username": u.get("username", username_hint or f"user_{user_id}"),
                   "first_name": u.get("first_name"), "last_name": u.get("last_name"), "email": u.get("email"),
                   "is_active": bool(u.get("active", True))})


def pull(client, sink, since_ts, known_ids, stats, dashboard_cache, user_cache):
    """One pass over the Superset log from since_ts. Updates `stats`. Rows whose id is in `known_ids` are already fully
    stored and are skipped. Everything sent is idempotent, so overlapping passes are harmless."""
    for page in fetch_log_pages(client, since_ts):
        for row in page:
            log_id = row["id"]
            stats["raw_seen"] += 1
            if log_id in known_ids:
                continue
            parsed = _parse_dttm(row.get("dttm"))
            if parsed is None:
                stats["no_timestamp"] += 1  # cannot be keyed or filtered by time; the API would not return it again either
                continue
            stats["raw_new"] += 1

            user_id = row.get("user_id")
            username_hint = (row.get("user") or {}).get("username") if isinstance(row.get("user"), dict) else None
            enrich_user(client, user_cache, sink, user_id, username_hint)
            enrich_dashboard(client, dashboard_cache, sink, row.get("dashboard_id"))

            access, error = build_event_rows(row)
            if error is not None:
                stats["errors"] += 1
                sink.add_error(error)
            if access is not None:  # other rows are staged in raw_log_landing only: not an access event we rank on
                stats["fact"] += 1
                sink.add_access(access)
            # last: the sink writes raw rows only after the facts and dimensions collected so far
            sink.add_raw({"source_log_id": log_id, "source_dttm": parsed, "payload": json.dumps(row, default=str)})


def run(since=None, full=False, reprocess=False):
    client = SupersetClient()
    conn = iceberg_sink.trino_conn()
    lease = iceberg_state.Lease(conn, LOCK_NAME)
    if not lease.acquire():
        print(f"another run holds the single-writer lease '{LOCK_NAME}'; skipping this run")
        return 0
    stats = {"raw_seen": 0, "raw_new": 0, "fact": 0, "errors": 0, "no_timestamp": 0}
    try:
        code = _run_locked(client, conn, lease, stats, since, full, reprocess)
        iceberg_state.record_run(conn, "sync", "ok", stats["raw_seen"], stats["raw_new"],
                                 f"access={stats['fact']} browser_errors={stats['errors']}")
        return code
    except Exception as exc:  # noqa: BLE001
        iceberg_state.record_run(conn, "sync", "failed", stats["raw_seen"], stats["raw_new"], str(exc))
        raise
    finally:
        lease.release()


def _run_locked(client, conn, lease, stats, since, full, reprocess):
    sink = iceberg_sink.IcebergSink(on_progress=lease.renew)
    sync_all_dashboards(client, sink)
    watermark, last_log_id = iceberg_state.get_watermark(conn)

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

    def known(frm):
        return set() if reprocess else iceberg_state.known_raw_ids(conn, None if frm <= EPOCH else frm)

    dashboard_cache, user_cache = {}, {}
    pull(client, sink, start, known(start), stats, dashboard_cache, user_cache)
    written = sink.flush()  # raw rows go last inside flush(); the watermark is read from them

    api_n, held = api_log_total(client, t0), iceberg_state.landing_total(conn, t0)
    gap = api_n - held
    if gap > 0 and start > EPOCH:
        print(f"WARNING: Superset's log has {gap} rows dated up to {t0:%Y-%m-%d %H:%M:%S} that we do not hold "
              f"(api={api_n}, landing={held}); the watermark filter skipped them. "
              f"Re-pulling the whole log once (idempotent).")
        pull(client, sink, EPOCH, known(EPOCH), stats, dashboard_cache, user_cache)
        written = sink.flush()
        held = iceberg_state.landing_total(conn, t0)
        gap = api_n - held
    if gap > 0:
        print(f"WARNING: still {gap} rows short after a full pull (api={api_n}, landing={held}); "
              f"a row may have been committed late. Not retrying; the next run will look again.")
    if stats["no_timestamp"]:
        print(f"WARNING: {stats['no_timestamp']} log rows had no usable timestamp and were not stored")

    new_watermark, new_id = iceberg_state.get_watermark(conn)
    already = stats["raw_seen"] - stats["raw_new"] - stats["no_timestamp"]
    print(f"synced through log id {max(last_log_id, new_id)}, watermark {new_watermark}: {stats['raw_seen']} log rows read "
          f"({stats['raw_new']} new, {already} already stored), "
          f"{stats['fact']} access events, {stats['errors']} browser errors; pull started at {start:%Y-%m-%d %H:%M:%S}")
    print(f"iceberg: sent {written['access']} access rows, {written['errors']} error rows, {written['raw']} raw rows, "
          f"{written['dashboards']} dashboards, {written['users']} users to MERGE (rows already present are left untouched)")
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Incremental Superset log ETL")
    ap.add_argument("--since", help="re-pull from this timestamp (ISO), for a known backfill")
    ap.add_argument("--full", action="store_true", help="re-pull the whole Superset log (idempotent)")
    ap.add_argument("--reprocess", action="store_true",
                    help="also re-send rows already in raw_log_landing (e.g. after changing how rows are classified)")
    args = ap.parse_args()
    try:
        sys.exit(run(since=_parse_dttm(args.since) if args.since else None, full=args.full, reprocess=args.reprocess))
    except Exception as exc:
        print(f"ETL run failed: {exc}", file=sys.stderr)
        raise

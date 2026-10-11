"""
Unit checks for the Iceberg-only pipeline, using a fake Trino connection (no services needed):

  * commit conflicts are retried, other errors are not
  * the single-writer lease: acquired, refused, and the "lost the commit race" case
  * write ordering: facts and dimensions are always stored before the raw rows the watermark is read from
  * raw payloads are chunked by size, so a statement stays under Trino's query length limit
  * the ETL skips rows that are already stored, and rows with no timestamp

    docker compose exec -T pipeline python tests/test_iceberg_state.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "etl"))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import iceberg_sink  # noqa: E402
import iceberg_state  # noqa: E402
import sync_logs  # noqa: E402

iceberg_sink.time.sleep = lambda _s: None  # no real backoff in tests
iceberg_state.time.sleep = lambda _s: None
failures = []


def check(name, ok, detail=""):
    print(("PASS  " if ok else "FAIL  ") + name + (f"  ({detail})" if detail else ""))
    if not ok:
        failures.append(name)


class FakeCursor:
    def __init__(self, conn):
        self.conn, self.rows = conn, []

    def execute(self, sql):
        self.conn.sql.append(sql)
        self.rows = self.conn.handler(sql)

    def fetchall(self):
        return self.rows

    def fetchmany(self, n):
        out, self.rows = self.rows[:n], self.rows[n:]
        return out


class FakeConn:
    def __init__(self, handler=lambda sql: []):
        self.sql, self.handler = [], handler

    def cursor(self):
        return FakeCursor(self)


def target(sql):
    """The table a statement writes to, or None."""
    for kw in ("MERGE INTO ", "INSERT INTO ", "UPDATE "):
        if sql.startswith(kw):
            return sql[len(kw):].split()[0]
    return None


# ---------- retry ----------
calls = []


def flaky(sql):
    calls.append(sql)
    if len(calls) < 3:
        raise RuntimeError("Failed to commit Iceberg update to table: monitoring.fact_access_events")
    return [(1,)]


check("a commit conflict is retried until it succeeds", iceberg_sink.run_sql(FakeConn(flaky), "SELECT 1") == [(1,)]
      and len(calls) == 3)

calls.clear()


def broken(sql):
    calls.append(sql)
    raise RuntimeError("line 1:8: Column 'x' cannot be resolved")


try:
    iceberg_sink.run_sql(FakeConn(broken), "SELECT x")
    check("a non-conflict error is raised at once", False)
except RuntimeError:
    check("a non-conflict error is raised at once", len(calls) == 1)

calls.clear()


def always_conflict(sql):
    calls.append(sql)
    raise RuntimeError("Failed to commit Iceberg update")


try:
    iceberg_sink.run_sql(FakeConn(always_conflict), "MERGE ...", retries=2)
    check("retries are bounded", False)
except RuntimeError:
    check("retries are bounded", len(calls) == 3)

# ---------- lease ----------
state = {}


def lease_handler(sql):
    if sql.startswith("MERGE INTO etl_lock"):
        state["merged"] = True
        return []
    if sql.startswith("SELECT owner"):
        return [(state["owner"], state.get("live", True))]
    return []


conn = FakeConn(lease_handler)
lease = iceberg_state.Lease(conn, "etl")
state["owner"] = lease.owner
check("lease acquired when the stored owner is this run", lease.acquire())
state["owner"] = "someone-else"
check("lease refused when another run holds it", not lease.acquire())


def race_handler(sql):
    if sql.startswith("MERGE INTO etl_lock"):
        raise RuntimeError("Failed to commit Iceberg update to table: monitoring.etl_lock")
    return [("someone-else", True)]


check("lease: losing a commit race to a live lease means 'not acquired', not a crash",
      not iceberg_state.Lease(FakeConn(race_handler), "etl").acquire())

# the catalog fails our commit while the lease is free: retry, then succeed
attempts = {"n": 0}
holder = iceberg_state.Lease(FakeConn(), "etl")


def hiccup(sql):
    if sql.startswith("MERGE INTO etl_lock"):
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise RuntimeError("Failed to commit the transaction during write: Service failed: 500")
        return []
    return [(holder.owner, True)] if attempts["n"] >= 3 else [("stale-owner", False)]


holder.conn.handler = hiccup
check("lease: a free lease is retried when our own commit fails", holder.acquire() and attempts["n"] == 3, str(attempts))

stuck = FakeConn(lambda sql: (_ for _ in ()).throw(RuntimeError("Failed to commit"))
                 if sql.startswith("MERGE") else [("stale-owner", False)])
check("lease: gives up after a bounded number of tries", not iceberg_state.Lease(stuck, "etl").acquire(attempts=3)
      and sum(1 for q in stuck.sql if q.startswith("MERGE")) == 3)


def other_error(sql):
    raise RuntimeError("Table 'etl_lock' does not exist")


try:
    iceberg_state.Lease(FakeConn(other_error), "etl").acquire()
    check("lease: a real error is not swallowed", False)
except RuntimeError:
    check("lease: a real error is not swallowed", True)

check("lease uses the server clock, not the client's",
      "current_timestamp" in iceberg_state.Lease(FakeConn(lease_handler), "etl")._expiry())

# ---------- watermark / counts ----------
from datetime import datetime  # noqa: E402

wm = FakeConn(lambda sql: [(datetime(2026, 10, 5, 12, 0, 0), 77)])
check("watermark is read from raw_log_landing", iceberg_state.get_watermark(wm) == (datetime(2026, 10, 5, 12, 0, 0), 77))
check("the watermark query ignores future-dated rows", "source_dttm <=" in wm.sql[0])
check("an empty table gives no watermark", iceberg_state.get_watermark(FakeConn(lambda sql: [(None, None)])) == (None, 0))
check("known ids come back as a set", iceberg_state.known_raw_ids(FakeConn(lambda sql: [(1,), (2,), (2,)])) == {1, 2})

# ---------- write ordering ----------
conn = FakeConn()
sink = iceberg_sink.IcebergSink(conn)
ts = datetime(2026, 10, 5, 12, 0, 0)
sink.add_raw({"source_log_id": 1, "source_dttm": ts, "payload": "{}"})  # added FIRST on purpose
sink.add_access({"event_id": 1, "event_ts": ts, "action": "chart_view"})
sink.add_error({"dedupe_key": "browser:1", "event_ts": ts, "source": "browser"})
sink.add_dashboard({"dashboard_id": 3, "title": "d"})
sink.add_user({"user_id": 1, "username": "u"})
sink.flush()
order = [target(s) for s in conn.sql]
check("raw rows are written last", order[-1] == "raw_log_landing" and order.index("fact_access_events") < order.index("raw_log_landing"),
      str(order))
check("dimensions are written before raw rows", order.index("dim_dashboards") < order.index("raw_log_landing")
      and order.index("dim_users") < order.index("raw_log_landing"), str(order))

# a raw chunk that fills up mid-run must first store the facts collected so far
iceberg_sink.RAW_BATCH_ROWS = 2
conn = FakeConn()
sink = iceberg_sink.IcebergSink(conn)
for i in (1, 2):
    sink.add_access({"event_id": i, "event_ts": ts, "action": "chart_view"})
    sink.add_raw({"source_log_id": i, "source_dttm": ts, "payload": "{}"})
order = [target(s) for s in conn.sql]
check("a full raw chunk stores the facts first", order == ["fact_access_events", "raw_log_landing"], str(order))
iceberg_sink.RAW_BATCH_ROWS = 300

# raw chunking by size
iceberg_sink.RAW_BATCH_CHARS = 1000
conn = FakeConn()
sink = iceberg_sink.IcebergSink(conn)
for i in range(5):
    sink.add_raw({"source_log_id": i, "source_dttm": ts, "payload": "x" * 600})
sink.flush()
raw_stmts = [s for s in conn.sql if target(s) == "raw_log_landing"]
check("raw payloads are chunked by size", len(raw_stmts) >= 3 and all(len(s) < 3000 for s in raw_stmts),
      f"{len(raw_stmts)} statements, longest {max(len(s) for s in raw_stmts)}")
iceberg_sink.RAW_BATCH_CHARS = 400000

check("a quote in a payload is escaped", "it''s" in iceberg_sink.merge_raw_sql(
    [{"source_log_id": 1, "source_dttm": ts, "payload": "it's"}]))
check("raw merge stamps ingested_at on the server", "ingested_at" in iceberg_sink.merge_raw_sql(
    [{"source_log_id": 1, "source_dttm": ts, "payload": "{}"}]))

# progress callback runs after every statement (it renews the lease)
ticks = []
sink = iceberg_sink.IcebergSink(FakeConn(), on_progress=lambda: ticks.append(1))
sink.add_access({"event_id": 1, "event_ts": ts, "action": "chart_view"})
sink.add_raw({"source_log_id": 1, "source_dttm": ts, "payload": "{}"})
sink.flush()
check("the lease is renewed after every statement", len(ticks) == 2, str(len(ticks)))


# ---------- the ETL pull ----------
class FakeResp:
    def __init__(self, body):
        self.body = body

    def json(self):
        return self.body


class FakeClient:
    def __init__(self, rows):
        self.rows = rows

    def get(self, path):
        if path.startswith("/api/v1/log/"):
            page = self.rows if "page:0," in path else []
            return FakeResp({"result": page, "ids": [r["id"] for r in page]})
        if path.startswith("/api/v1/user/"):
            return FakeResp({"result": {"username": "alice"}})
        return FakeResp({"result": {"dashboard_title": "Sales", "owners": []}})


class RecordingSink:
    def __init__(self):
        self.events = []

    def __getattr__(self, name):
        if name.startswith("add_"):
            return lambda row: self.events.append((name[4:], row))
        raise AttributeError(name)


rows = [
    {"id": 10, "dttm": "2026-10-05T12:00:00", "action": "chart_data", "user_id": 1, "dashboard_id": 3, "slice_id": 7, "duration_ms": 100},
    {"id": 11, "dttm": "2026-10-05T12:00:01", "action": "chart_data", "user_id": 1, "dashboard_id": 3, "slice_id": 7, "duration_ms": 100},
    {"id": 12, "dttm": None, "action": "chart_data", "user_id": 1, "dashboard_id": 3, "slice_id": 7},
]
stats = {"raw_seen": 0, "raw_new": 0, "fact": 0, "errors": 0, "no_timestamp": 0}
rs = RecordingSink()
sync_logs.pull(FakeClient(rows), rs, sync_logs.EPOCH, {10}, stats, {}, {})
raw_ids = [r["source_log_id"] for kind, r in rs.events if kind == "raw"]
check("a row that is already stored is skipped", raw_ids == [11] and stats["raw_seen"] == 3, str(stats))
check("a row with no timestamp is counted, not stored", stats["no_timestamp"] == 1)
kinds = [k for k, _ in rs.events if k in ("access", "raw")]
check("per row, the access fact is added before its raw row", kinds == ["access", "raw"], str(kinds))
check("users and dashboards seen in the log are added to the dimensions",
      {k for k, _ in rs.events} >= {"user", "dashboard"})

print()
print("ALL PASSED" if not failures else "FAILED: " + "; ".join(failures))
sys.exit(0 if not failures else 1)

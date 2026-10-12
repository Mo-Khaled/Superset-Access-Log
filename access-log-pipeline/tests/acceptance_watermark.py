"""
Acceptance test for the ETL watermark: past-dated and future-dated log rows must never be silently skipped
or silently counted.

Run inside the pipeline container (it needs Postgres, the Superset API and Trino):
    docker compose exec -T pipeline python tests/acceptance_watermark.py

It inserts clearly labelled rows (referrer 'acceptance-test') straight into Superset's `logs` table, the same way
the seed script does, runs the real ETL after each step and checks where the rows ended up:

  A  a row dated 3 hours ago, inserted AFTER the watermark moved  -> found within one run (lookback)
  B  a row dated 10 days ago                                        -> found by the reconciliation re-pull (WARNING printed)
  C  a row dated tomorrow                                           -> stored, but excluded from vw_chart_failure_rate
  D  a run with no new data                                         -> changes no Iceberg row, prints no warning
  E  Superset's rows before a cutoff <= raw_log_landing             -> nothing is missing
  F  a second run started while one holds the lease                 -> skips, writes nothing

Note: every API call the ETL makes is itself logged by Superset, so a run always finds a few NEW log rows
(its own footprint). 'No new data' therefore means no new *fact* rows, not no new log rows.

The inserted rows stay in the lab data (labelled), so run it on a lab stack.
"""
import os
import subprocess
import sys
from datetime import datetime, timedelta

import psycopg2

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "etl"))  # etl modules live there
import iceberg_sink  # noqa: E402
from superset_client import SupersetClient
import iceberg_state  # noqa: E402
from sync_logs import LOCK_NAME, api_log_total

LABEL = "acceptance-test"
failures = []


def check(name, ok, detail=""):
    print(("PASS  " if ok else "FAIL  ") + name + (f"  ({detail})" if detail else ""))
    if not ok:
        failures.append(name)


def pg():
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"], dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"], password=os.environ["POSTGRES_PASSWORD"],
    )


def insert_row(dttm, dashboard_id, chart_id, duration_ms=1234):
    conn = pg()
    with conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO logs (user_id, action, json, dashboard_id, slice_id, dttm, duration_ms, referrer) "
            "VALUES (1, 'chart_data', '{}', %s, %s, %s, %s, %s) RETURNING id",
            (dashboard_id, chart_id, dttm, duration_ms, LABEL),
        )
        log_id = cur.fetchone()[0]
    conn.close()
    return log_id


def run_etl():
    p = subprocess.run([sys.executable, "etl/sync_logs.py"], capture_output=True, text=True)
    out = p.stdout + p.stderr
    print("      " + out.strip().replace("\n", "\n      "))
    if p.returncode != 0:
        failures.append("ETL run failed")
    return out


def iceberg_has(conn, log_id):
    cur = conn.cursor()
    cur.execute(f"SELECT count(*) FROM fact_access_events WHERE event_id = {int(log_id)}")
    return cur.fetchall()[0][0]


def iceberg_total(conn, table):
    cur = conn.cursor()
    cur.execute(f"SELECT count(*) FROM {table}")
    return cur.fetchall()[0][0]


def main():
    client = SupersetClient()
    tr = iceberg_sink.trino_conn()

    # a real dashboard and chart to hang the test rows on
    dash = iceberg_sink.run_sql(tr, "SELECT entity_id FROM dim_entities WHERE entity_type = 'dashboard' ORDER BY entity_id LIMIT 1")[0][0]
    chart = 1

    print("0. baseline run so the watermark is current")
    run_etl()

    now = datetime.utcnow()

    print("A. row dated 3 hours ago, inserted after the watermark moved")
    a = insert_row(now - timedelta(hours=3), dash, chart)
    out = run_etl()
    check("A: found in Iceberg within one run", iceberg_has(tr, a) == 1, f"log id {a}")

    print("B. row dated 10 days ago")
    b = insert_row(now - timedelta(days=10), dash, chart)
    out = run_etl()
    check("B: found in Iceberg", iceberg_has(tr, b) == 1, f"log id {b}")
    check("B: reconciliation warned and re-pulled", "Re-pulling the whole log" in out)

    print("C. row dated tomorrow")
    c = insert_row(now + timedelta(days=1), dash, chart)
    run_etl()
    check("C: stored in the fact table", iceberg_has(tr, c) == 1, f"log id {c}")
    cur = tr.cursor()
    tomorrow = (now + timedelta(days=1)).strftime("%Y-%m-%d")
    cur.execute(f"SELECT count(*) FROM vw_chart_failure_rate WHERE day = DATE '{tomorrow}'")
    check("C: excluded from vw_chart_failure_rate", cur.fetchall()[0][0] == 0)

    print("D. run again with no new data")
    before = (iceberg_total(tr, "fact_access_events"), iceberg_total(tr, "fact_error_events"))
    out = run_etl()
    after = (iceberg_total(tr, "fact_access_events"), iceberg_total(tr, "fact_error_events"))
    check("D: no warning", "WARNING" not in out)
    # the ETL's own API calls add a handful of chart_data-less log rows, never fact rows
    check("D: Iceberg counts unchanged", before == after, f"{before} -> {after}")

    print("E. nothing is missing")
    # Fix a cutoff first, then run the ETL: every row Superset dated before the cutoff must be held afterwards.
    # (Measuring after the run would race with the log rows the ETL's own last API calls create.)
    until = datetime.utcnow()
    run_etl()
    api, held = api_log_total(client, until), iceberg_state.landing_total(tr, until)
    check("E: every Superset row dated before the cutoff is held", api <= held, f"api={api}, landing={held}")

    print("F. a run started while another holds the lease")
    holder = iceberg_state.Lease(tr, LOCK_NAME)
    check("F: the first run takes the lease", holder.acquire())
    before = iceberg_total(tr, "raw_log_landing")
    out = run_etl()
    check("F: the second run skips", "holds the single-writer lease" in out)
    check("F: and wrote nothing", before == iceberg_total(tr, "raw_log_landing"))
    holder.release()
    out = run_etl()
    check("F: once released, a run proceeds", "synced through" in out)

    print()
    print("ALL PASSED" if not failures else "FAILED: " + "; ".join(failures))
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main())

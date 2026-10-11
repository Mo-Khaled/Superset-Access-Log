"""
End-to-end test with a REAL browser (Chrome driven by Playwright), not API calls. It checks that a failure which the
browser itself experiences and reports reaches Iceberg, and that a failure seen by BOTH the browser and the server is
counted once in the failure rate.

Runs on the HOST (it needs Chrome and Playwright), against a running lab stack:
    pip install playwright            # uses the installed Chrome, no browser download
    python access-log-pipeline/tests/browser_failure_test.py

What it does
  1. creates a dashboard "Browser failure test" holding one deliberately broken chart (SUM of a column that does not exist)
  2. opens the slow-chart dashboard and the broken-chart dashboard in Chrome, logged in as admin, and waits for the
     browser to report the failures to Superset
  3. checks Superset's own `logs` table has the browser's failure events (action 'log', load_chart, has_err)
  4. runs one ETL cycle and checks Iceberg:
       - the browser rows exist, with the right user, dashboard, chart and category
       - the server side saw the same failures (a gunicorn kill for the timeout, an HTTP 400 for the broken chart)
       - failure rate: failed_loads rose by exactly 1 per dashboard (browser only) while server_errors rose by 1
         separately, so nothing was counted twice
       - the error dataset marks the server rows as also_in_browser, so the error charts count each failure once
  5. deletes the test chart and dashboard (the failure rows stay in the lab data, labelled by their dashboard title)
"""
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone

import requests
from playwright.sync_api import sync_playwright

ROOT = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
BASE = os.environ.get("SUPERSET_URL", "http://localhost:8089")
SLOW_DASHBOARD = int(os.environ.get("SLOW_DASHBOARD_ID", "13"))
SHOTS = os.path.join(os.environ.get("TEMP", "/tmp"), "browser_failure_shots")
failures = []


def check(name, ok, detail=""):
    print(("PASS  " if ok else "FAIL  ") + name + (f"  ({detail})" if detail else ""))
    if not ok:
        failures.append(name)


def sh(args, **kw):
    return subprocess.run(args, cwd=ROOT, capture_output=True, text=True, **kw)


def run_cycle():
    """One ETL cycle, the same three steps as access-log-pipeline/run_etl.sh, run with docker directly so the test does
    not depend on which `bash` is first on the PATH."""
    for script in ("etl/iceberg_init.py", "etl/sync_logs.py"):
        p = sh(["docker", "compose", "exec", "-T", "pipeline", "python", script])
        if p.returncode != 0:
            raise RuntimeError(f"{script} failed:\n{p.stdout}\n{p.stderr}")
    logs = sh(["docker", "compose", "logs", "--no-log-prefix", "--since", "30m", "superset", "superset-worker"]).stdout
    p = subprocess.run(["docker", "compose", "exec", "-T", "pipeline", "python", "etl/ingest_container_errors.py"],
                       cwd=ROOT, input=logs, capture_output=True, text=True)
    if p.returncode != 0:
        raise RuntimeError(f"ingest failed:\n{p.stdout}\n{p.stderr}")
    return p.stdout.strip()


def trino(sql):
    p = sh(["docker", "compose", "-f", "lakehouse/docker-compose.yml", "exec", "-T", "trino",
            "trino", "--output-format", "TSV", "--execute", sql])
    if p.returncode != 0:
        raise RuntimeError(p.stdout + p.stderr)
    return [line.split("\t") for line in p.stdout.strip().splitlines() if line.strip()]


def pg(sql):
    p = sh(["docker", "compose", "exec", "-T", "postgres", "psql", "-U", "superset", "-d", "superset", "-At", "-F", "\t", "-c", sql])
    return [line.split("\t") for line in p.stdout.strip().splitlines() if line.strip()]


def api():
    s = requests.Session()
    r = s.post(f"{BASE}/api/v1/security/login", json={"username": "admin", "password": "admin", "provider": "db"})
    r.raise_for_status()
    s.headers["Authorization"] = "Bearer " + r.json()["access_token"]
    # Superset requires a CSRF token (and a Referer) on every write
    s.headers["X-CSRFToken"] = s.get(f"{BASE}/api/v1/security/csrf_token/").json()["result"]
    s.headers["Referer"] = BASE
    return s


def view_row(dashboard_id):
    """failed_loads / server_errors / chart_loads for a dashboard, summed over all days (from the real view)."""
    rows = trino("select coalesce(sum(chart_loads),0), coalesce(sum(rated_loads),0), coalesce(sum(failed_loads),0), "
                 "coalesce(sum(server_errors),0) from iceberg.monitoring.vw_chart_failure_rate "
                 f"where dashboard_id = {dashboard_id}")
    return [int(float(x)) for x in rows[0]]


def main():
    os.makedirs(SHOTS, exist_ok=True)
    s = api()

    # 1. a dashboard with one broken chart
    ds = s.get(f"{BASE}/api/v1/dataset/", params={"q": "(filters:!((col:table_name,opr:eq,value:'fact_access_events')))"}).json()["result"][0]["id"]
    dash = s.post(f"{BASE}/api/v1/dashboard/", json={"dashboard_title": "Browser failure test", "published": True}).json()["id"]
    params = {"datasource": f"{ds}__table", "viz_type": "big_number_total", "adhoc_filters": [], "time_range": "No filter",
              "metric": {"expressionType": "SQL", "sqlExpression": "SUM(no_such_column_xyz)", "label": "broken"}}
    chart = s.post(f"{BASE}/api/v1/chart/", json={"slice_name": "Broken chart (browser test)", "viz_type": "big_number_total",
                   "datasource_id": ds, "datasource_type": "table", "params": json.dumps(params), "dashboards": [dash]}).json()["id"]
    print(f"test dashboard {dash} with broken chart {chart}")

    try:
        # baseline, so the checks measure what THIS test caused
        sh(["docker", "compose", "exec", "-T", "redis", "redis-cli", "-n", "2", "FLUSHDB"])  # cold cache: the slow chart must run
        run_cycle()
        base_slow, base_broken = view_row(SLOW_DASHBOARD), view_row(dash)
        slow_chart = int(trino("select min(chart_id) from iceberg.monitoring.fact_access_events "
                               f"where dashboard_id = {SLOW_DASHBOARD} and chart_id is not null")[0][0])
        start = datetime.now(timezone.utc).replace(tzinfo=None)
        start_s = start.strftime("%Y-%m-%d %H:%M:%S")

        # 2. a real browser
        with sync_playwright() as pw:
            browser = pw.chromium.launch(channel="chrome", headless=True)
            page = browser.new_context(viewport={"width": 1400, "height": 900}).new_page()
            page.goto(f"{BASE}/login/")
            page.fill("#username", "admin")
            page.fill("#password", "admin")
            page.click("input[type=submit]")
            page.wait_for_url("**/superset/welcome/**", timeout=30000)

            print("opening the slow dashboard in Chrome (the chart times out after about 15 s)...")
            page.goto(f"{BASE}/superset/dashboard/{SLOW_DASHBOARD}/")
            page.wait_for_timeout(24000)
            page.screenshot(path=os.path.join(SHOTS, "slow_dashboard.png"))

            print("opening the broken-chart dashboard in Chrome...")
            page.goto(f"{BASE}/superset/dashboard/{dash}/")
            page.wait_for_timeout(9000)
            page.screenshot(path=os.path.join(SHOTS, "broken_dashboard.png"))

            page.goto(f"{BASE}/superset/welcome/")   # leaving the page also flushes any queued log events
            page.wait_for_timeout(12000)
            browser.close()

        # 3. Superset's own log has what the browser reported
        events = pg("select dttm, user_id, dashboard_id, slice_id, json::json->>'error_details' from logs "
                    "where action = 'log' and json::json->>'event_name' = 'load_chart' and json::json->>'has_err' = 'true' "
                    f"and dttm >= '{start_s}' order by dttm")
        print("browser-reported failures in Superset's logs table:")
        for e in events:
            print("   ", e)
        slow_evt = [e for e in events if e[3] == str(slow_chart)]
        broken_evt = [e for e in events if e[3] == str(chart)]
        check("browser reported the timeout (slow chart)", len(slow_evt) == 1, f"{len(slow_evt)} event(s)")
        check("browser reported the query error (broken chart)", len(broken_evt) == 1, f"{len(broken_evt)} event(s)")

        # 4. one ETL cycle, then look in Iceberg
        print(run_cycle())
        rows = trino("select source, error_category, error_type, user_id, dashboard_id, chart_id, status_code, "
                     "substr(error_message, 1, 70) from iceberg.monitoring.fact_error_events "
                     f"where event_ts >= TIMESTAMP '{start_s}' and dashboard_id in ({SLOW_DASHBOARD}, {dash}) order by event_ts")
        print("rows in Iceberg for this test:")
        for r in rows:
            print("   ", r)
        by = lambda src, d: [r for r in rows if r[0] == src and r[4] == str(d)]
        b_slow, b_broken = by("browser", SLOW_DASHBOARD), by("browser", dash)
        check("browser timeout reached Iceberg, user + chart + category correct",
              len(b_slow) == 1 and b_slow[0][1] == "timeout" and b_slow[0][3] == "1" and b_slow[0][5] == str(slow_chart), str(b_slow))
        check("browser query error reached Iceberg, user + chart + category correct",
              len(b_broken) == 1 and b_broken[0][1] == "query_error" and b_broken[0][3] == "1" and b_broken[0][5] == str(chart), str(b_broken))
        g = by("gunicorn", SLOW_DASHBOARD)
        # Chrome retries a chart request once when the connection dies, so ONE browser-reported failure is normally TWO
        # server-side kills. That is exactly why server counts must not be added to the browser count.
        check("server side saw the timeout: gunicorn kills tied to the same chart and user",
              len(g) >= 1 and all(r[1] == "timeout" and r[5] == str(slow_chart) and r[3] == "1" for r in g),
              f"{len(g)} kill(s) for 1 browser failure")
        sv = by("server", dash)
        check("server side saw the query error: HTTP 400 tied to the same chart",
              len(sv) == 1 and sv[0][6] in ("400", "422", "500") and sv[0][1] == "query_error" and sv[0][5] == str(chart), str(sv))

        # failure rate: the same failure seen by browser AND server must count once
        after_slow, after_broken = view_row(SLOW_DASHBOARD), view_row(dash)
        d_slow = [a - b for a, b in zip(after_slow, base_slow)]       # chart_loads, rated_loads, failed_loads, server_errors
        d_broken = [a - b for a, b in zip(after_broken, base_broken)]
        print(f"view deltas  slow dashboard   (loads, rated, failed, server_errors): {d_slow}")
        print(f"view deltas  broken dashboard (loads, rated, failed, server_errors): {d_broken}")
        check("slow dashboard: failed_loads rose by exactly 1 (the browser's one report), not by the server kills",
              d_slow[2] == 1 and d_slow[3] >= 1, f"failed_loads +{d_slow[2]}, server_errors +{d_slow[3]} (separate column)")
        check("broken dashboard: failed_loads rose by exactly 1 although the server also logged the HTTP 400",
              d_broken[2] == 1 and d_broken[3] == 1, f"failed_loads +{d_broken[2]}, server_errors +{d_broken[3]} (separate column)")
        check("the failed load is also in the denominator (one load, one failure)", d_broken[0] == 1 and d_broken[1] == 1, str(d_broken))

        # the error charts count each failure once: the server rows are flagged as already reported by the browser
        sys.path.insert(0, os.path.join(ROOT, "access-log-pipeline", "superset_bootstrap"))
        os.environ.setdefault("TRINO_HOST", "trino")
        import bootstrap_iceberg  # noqa: E402
        sql = re.sub(r"--[^\n]*", "", bootstrap_iceberg.CHART_ERRORS_SQL).replace("\n", " ")
        flagged = trino(f"select source, also_in_browser from ({sql}) where event_ts >= TIMESTAMP '{start_s}' and chart_id in ({slow_chart}, {chart})")
        srv = [r for r in flagged if r[0] != "browser"]
        check("every server-side row for these failures is marked also_in_browser",
              len(srv) >= 2 and all(r[1] == "true" for r in srv), f"{len(srv)} server-side rows")
        distinct = [r for r in flagged if r[1] != "true"]
        check("error charts count each of the 2 failures once", len(distinct) == 2, f"{len(distinct)} distinct of {len(flagged)} rows")
    finally:
        s.delete(f"{BASE}/api/v1/chart/{chart}")
        s.delete(f"{BASE}/api/v1/dashboard/{dash}")

    print()
    print("ALL PASSED" if not failures else "FAILED: " + "; ".join(failures))
    print("screenshots:", SHOTS)
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main())

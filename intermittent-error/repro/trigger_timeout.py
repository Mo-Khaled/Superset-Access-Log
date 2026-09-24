"""
Host-side script: fires the slow chart's query against the running Superset
instance (as a real browser tab loading the repro dashboard would), times how
long the client waits and what it sees, then captures `docker compose logs
superset` from just before the request to just after -- so we can inspect
exactly what Superset's application logs did or didn't record.

Usage (run from the repo root, on the HOST -- not inside a container, so it can
shell out to `docker compose logs`):
    python intermittent-error/repro/trigger_timeout.py before   # pre-fix repro
    python intermittent-error/repro/trigger_timeout.py after    # post-fix proof

Reads dashboard_id/chart_id/dataset_id written by create_slow_dataset.py.
"""
import os
import subprocess
import sys
import time
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[2]
IDS_FILE = ROOT / "intermittent-error" / "repro" / ".repro_ids.env"

SUPERSET_BASE_URL = os.environ.get("SUPERSET_BASE_URL", "http://localhost:8089")
ADMIN_USERNAME = os.environ.get("SUPERSET_ADMIN_USERNAME", "admin")
ADMIN_PASSWORD = os.environ.get("SUPERSET_ADMIN_PASSWORD", "admin")


def load_ids():
    ids = {}
    for line in IDS_FILE.read_text().splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            ids[k.strip()] = v.strip()
    return ids


def login():
    session = requests.Session()
    resp = session.post(
        f"{SUPERSET_BASE_URL}/api/v1/security/login",
        json={"username": ADMIN_USERNAME, "password": ADMIN_PASSWORD,
              "provider": "db", "refresh": True},
        timeout=30,
    )
    resp.raise_for_status()
    token = resp.json()["access_token"]
    session.headers.update({"Authorization": f"Bearer {token}"})
    csrf = session.get(f"{SUPERSET_BASE_URL}/api/v1/security/csrf_token/", timeout=30)
    csrf.raise_for_status()
    session.headers.update({
        "X-CSRFToken": csrf.json()["result"],
        "Referer": SUPERSET_BASE_URL,
    })
    return session


def fire_slow_chart(session, dataset_id, dashboard_id):
    payload = {
        "datasource": {"id": int(dataset_id), "type": "table"},
        "queries": [{"metrics": ["count"], "row_limit": 1}],
        "form_data": {
            "viz_type": "big_number_total",
            "datasource": f"{dataset_id}__table",
            "metric": "count",
            "dashboardId": int(dashboard_id),
        },
    }
    started = time.time()
    print(f"[{time.strftime('%H:%M:%S')}] POST /api/v1/chart/data ...")
    try:
        resp = session.post(
            f"{SUPERSET_BASE_URL}/api/v1/chart/data", json=payload, timeout=120
        )
        elapsed = time.time() - started
        print(f"[{time.strftime('%H:%M:%S')}] client received HTTP {resp.status_code} "
              f"after {elapsed:.1f}s")
        print(f"body (truncated): {resp.text[:500]}")
    except requests.exceptions.RequestException as exc:
        elapsed = time.time() - started
        print(f"[{time.strftime('%H:%M:%S')}] client-side exception after {elapsed:.1f}s: {exc}")
    return started


def capture_superset_logs(since_epoch, out_path: Path):
    since_arg = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(since_epoch - 2))
    result = subprocess.run(
        ["docker", "compose", "logs", "--no-color", "--since", since_arg, "superset"],
        cwd=str(ROOT), capture_output=True, text=True,
    )
    out_path.write_text(result.stdout + result.stderr)
    print(f"captured superset container logs -> {out_path}")


def main():
    if len(sys.argv) != 2 or sys.argv[1] not in ("before", "after"):
        print("usage: trigger_timeout.py [before|after]")
        sys.exit(1)
    phase = sys.argv[1]
    out_dir = ROOT / "intermittent-error" / f"logs_{phase}"
    out_dir.mkdir(exist_ok=True)

    ids = load_ids()
    session = login()
    started = fire_slow_chart(session, ids["REPRO_DATASET_ID"], ids["REPRO_DASHBOARD_ID"])

    time.sleep(3)  # let log lines flush
    capture_superset_logs(started, out_dir / "superset_container_logs.txt")

    print(f"\nDone. Inspect {out_dir}/superset_container_logs.txt for whether a real "
          f"exception/error was logged, vs. just a worker timeout / nothing at all.")


if __name__ == "__main__":
    main()

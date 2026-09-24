"""
Simulate realistic, skewed dashboard traffic against the local Superset instance.

Why this writes to Superset's `logs` table directly (via Postgres) instead of only
calling Superset's REST/UI endpoints: reproducing genuine browser-driven activity
would mean headlessly driving Superset's React frontend (which is what actually
fires `dashboard_load` / `chart_data` log events, via calls the frontend batches to
`/api/v1/log/`). That's out of scope for a backend lab whose goal is realistic *log
data*, not re-rendering charts. Instead we:
  1. Create real Superset user accounts directly in the metadata DB (see
     pbkdf2_password_hash below), so /api/v1/user/ + dim_users resolve real names.
  2. Read real dashboards/charts already created by `superset load_examples`.
  3. Insert log rows directly into Superset's `logs` table, shaped exactly like the
     rows Superset itself would write (same columns, same action names), with a
     deliberately skewed distribution across dashboards, users, and time.
The ETL script then pulls these rows back out through the real `/api/v1/log/` REST
API exactly as it would pull genuine traffic -- the simulation only replaces how the
log rows are produced, not how they're consumed.
"""
import hashlib
import os
import random
import secrets
from datetime import datetime, timedelta

import psycopg2
import psycopg2.extras

from superset_client import SupersetClient

FAKE_USER_COUNT = int(os.environ.get("SEED_FAKE_USER_COUNT", "12"))
DAYS_OF_HISTORY = int(os.environ.get("SEED_DAYS_OF_HISTORY", "30"))
SESSIONS_PER_DAY = int(os.environ.get("SEED_SESSIONS_PER_DAY", "40"))
FAKE_USER_PASSWORD = os.environ.get("SEED_FAKE_USER_PASSWORD", "Sim3ulated!Pass")

FIRST_NAMES = [
    "Alex", "Sam", "Jordan", "Taylor", "Morgan", "Riley", "Casey", "Drew",
    "Avery", "Quinn", "Reese", "Jamie", "Rowan", "Skyler", "Parker", "Emerson",
]
LAST_NAMES = [
    "Chen", "Patel", "Garcia", "Kim", "Nguyen", "Smith", "Johansson", "Rossi",
    "Silva", "Kowalski", "Abara", "Haddad", "Ibrahim", "Larsen", "Novak", "Costa",
]


def pg_conn():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=5432,
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def pbkdf2_password_hash(password: str, iterations: int = 260000) -> str:
    """Produces a password hash in werkzeug's `pbkdf2:sha256:<iterations>$<salt>$
    <hexhash>` format using only the stdlib -- werkzeug's check_password_hash (what
    Flask-AppBuilder calls at login) parses the method/iteration count from the
    string itself, so this verifies correctly without needing werkzeug installed
    in this lightweight pipeline image."""
    salt = secrets.token_hex(8)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), iterations)
    return f"pbkdf2:sha256:{iterations}${salt}${dk.hex()}"


def create_fake_users(client: SupersetClient, n: int) -> list[int]:
    """Create n Gamma-role Superset users. Returns their user ids. Idempotent:
    skips users that already exist (safe to re-run the seed script).

    Superset's own REST API does not expose user management (no
    /api/v1/security/users/, no /api/v1/security/roles/ in this version --
    only login/csrf/guest-token live under /api/v1/security/). So we create
    users directly in the metadata DB, hashing the password in werkzeug's
    `pbkdf2:sha256:<iterations>$<salt>$<hexhash>` format -- the format
    Flask-AppBuilder's `check_password_hash` (itself werkzeug's) parses
    generically from the embedded method/iteration count, so this verifies
    correctly at Superset login without needing werkzeug installed here.
    """
    conn = pg_conn()
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("SELECT id FROM ab_role WHERE name = 'Gamma'")
    row = cur.fetchone()
    if not row:
        raise RuntimeError("Gamma role not found -- has `superset init` run yet?")
    gamma_role_id = row[0]

    user_ids = []
    for i in range(n):
        username = f"sim_user_{i:03d}"
        cur.execute("SELECT id FROM ab_user WHERE username = %s", (username,))
        row = cur.fetchone()
        if row:
            user_ids.append(row[0])
            continue

        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)
        password_hash = pbkdf2_password_hash(FAKE_USER_PASSWORD)
        # ab_user.id has no DB-level sequence/default in this schema (Alembic
        # created it as a plain integer PK) -- Superset's own ORM layer supplies
        # ids via SQLAlchemy's identity map, so raw SQL inserts must assign one.
        cur.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM ab_user")
        next_id = cur.fetchone()[0]
        cur.execute(
            """
            INSERT INTO ab_user (id, first_name, last_name, username, password, active, email)
            VALUES (%s, %s, %s, %s, %s, TRUE, %s)
            RETURNING id
            """,
            (next_id, first, last, username, password_hash, f"{username}@example.com"),
        )
        user_id = cur.fetchone()[0]
        cur.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM ab_user_role")
        next_role_link_id = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO ab_user_role (id, user_id, role_id) VALUES (%s, %s, %s)",
            (next_role_link_id, user_id, gamma_role_id),
        )
        user_ids.append(user_id)
        print(f"created user {username} (id={user_id})")

    cur.close()
    conn.close()
    return user_ids


def fetch_dashboards(client: SupersetClient) -> list[dict]:
    resp = client.get("/api/v1/dashboard/?q=(page_size:100)")
    return resp.json()["result"]


def fetch_dashboard_chart_ids(conn, dashboard_id: int) -> list[int]:
    cur = conn.cursor()
    cur.execute(
        "SELECT slice_id FROM dashboard_slices WHERE dashboard_id = %s", (dashboard_id,)
    )
    ids = [r[0] for r in cur.fetchall()]
    cur.close()
    return ids


def zipf_weights(n: int, s: float = 1.3) -> list[float]:
    """Zipf-like skew: first item gets the most weight, tapering off fast, so a
    handful of dashboards dominate traffic and a tail gets near-zero views."""
    weights = [1.0 / ((i + 1) ** s) for i in range(n)]
    total = sum(weights)
    return [w / total for w in weights]


def random_timestamp(days_back: int) -> datetime:
    day_offset = random.uniform(0, days_back)
    dt = datetime.utcnow() - timedelta(days=day_offset)
    # bias towards business hours, Mon-Fri, for realism
    hour = int(random.triangular(7, 20, 13))
    return dt.replace(hour=min(hour, 23), minute=random.randint(0, 59), second=random.randint(0, 59))


def main():
    client = SupersetClient()
    user_ids = create_fake_users(client, FAKE_USER_COUNT)
    dashboards = fetch_dashboards(client)
    if not dashboards:
        raise RuntimeError("No dashboards found -- run `superset load_examples` first.")

    dashboards.sort(key=lambda d: d["id"])
    dash_weights = zipf_weights(len(dashboards))
    # a handful of users are "power users" who generate most of the traffic --
    # mirrors real orgs where a few analysts drive most dashboard opens.
    user_weights = zipf_weights(len(user_ids), s=0.9)

    conn = pg_conn()
    conn.autocommit = False
    cur = conn.cursor()

    dashboard_charts = {
        d["id"]: fetch_dashboard_chart_ids(conn, d["id"]) for d in dashboards
    }

    total_sessions = DAYS_OF_HISTORY * SESSIONS_PER_DAY
    rows = []
    for _ in range(total_sessions):
        dashboard = random.choices(dashboards, weights=dash_weights, k=1)[0]
        user_id = random.choices(user_ids, weights=user_weights, k=1)[0]
        ts = random_timestamp(DAYS_OF_HISTORY)
        referrer = f"/superset/dashboard/{dashboard['id']}/"

        rows.append(
            (user_id, "dashboard_load", "{}", dashboard["id"], None, ts,
             random.randint(150, 2500), referrer)
        )

        chart_ids = dashboard_charts.get(dashboard["id"], [])
        if chart_ids:
            viewed_charts = random.sample(
                chart_ids, k=min(len(chart_ids), random.randint(1, len(chart_ids)))
            )
            for chart_id in viewed_charts:
                chart_ts = ts + timedelta(milliseconds=random.randint(50, 800))
                rows.append(
                    (user_id, "chart_data", "{}", dashboard["id"], chart_id, chart_ts,
                     random.randint(80, 4000), referrer)
                )

        # occasional export action, for the "export vs view" distinction in docs
        if random.random() < 0.03 and chart_ids:
            export_ts = ts + timedelta(seconds=random.randint(5, 120))
            rows.append(
                (user_id, "export_csv", "{}", dashboard["id"],
                 random.choice(chart_ids), export_ts, None, referrer)
            )

    psycopg2.extras.execute_values(
        cur,
        """
        INSERT INTO logs (user_id, action, json, dashboard_id, slice_id, dttm, duration_ms, referrer)
        VALUES %s
        """,
        rows,
    )
    conn.commit()
    print(f"inserted {len(rows)} simulated log rows across {len(dashboards)} dashboards "
          f"and {len(user_ids)} users over {DAYS_OF_HISTORY} days")
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()

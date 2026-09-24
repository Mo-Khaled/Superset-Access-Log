"""
Applies fix #1 (timeout alignment) from the plan: sets a Postgres statement_timeout
on the 'examples' database connection that Superset uses for the repro dataset.

Why this is "the fix": with no statement_timeout, a slow query just runs until the
gunicorn *webserver* timeout kills the whole worker process (SIGKILL) -- which
happens *outside* Superset's own code, so no exception handler, no
logging.exception(), no EVENT_LOGGER call ever runs. By making the *database*
cancel the query first (well before the webserver timeout), Postgres raises a
normal QueryCanceled error inside the request, which Superset's chart-data view
DOES catch and log -- turning a silent process kill into a real, logged,
user-facing error.

Run once, AFTER bumping SUPERSET_WEBSERVER_TIMEOUT in .env and recreating the
superset container (see README) so the DB timeout fires well before the (now
longer) webserver timeout would:
    docker compose run --rm pipeline python /fix/apply_fix.py
"""
import os
import sys

sys.path.insert(0, "/pipeline")
from superset_client import SupersetClient  # noqa: E402

STATEMENT_TIMEOUT_MS = int(os.environ.get("FIX_STATEMENT_TIMEOUT_MS", "20000"))


def main():
    client = SupersetClient()
    resp = client.get(
        "/api/v1/database/?q=(filters:!((col:database_name,opr:ct,value:examples)))"
    )
    result = resp.json().get("result", [])
    if not result:
        raise RuntimeError("examples database not found")
    db_id = result[0]["id"]

    extra = {
        "engine_params": {
            "connect_args": {
                "options": f"-c statement_timeout={STATEMENT_TIMEOUT_MS}"
            }
        }
    }
    put_resp = client.session.put(
        f"{client.base_url}/api/v1/database/{db_id}",
        json={"extra": str(extra).replace("'", '"')},
        timeout=30,
    )
    put_resp.raise_for_status()
    print(
        f"applied statement_timeout={STATEMENT_TIMEOUT_MS}ms to database id={db_id} "
        f"(examples). Make sure SUPERSET_WEBSERVER_TIMEOUT in .env is greater than "
        f"{STATEMENT_TIMEOUT_MS / 1000:.0f}s and the superset container has been "
        f"recreated with that value."
    )


if __name__ == "__main__":
    main()

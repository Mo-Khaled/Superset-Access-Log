"""One-off: derive fact_error_events (source='browser') from rows already staged in
raw_log_landing, for failures ingested before extract_browser_error existed.
Idempotent (dedupe_key)."""
import json

from sync_logs import extract_browser_error, insert_error_event, mysql_conn


def main():
    conn = mysql_conn()
    found = 0
    with conn.cursor() as cur:
        cur.execute("SELECT source_log_id, payload FROM raw_log_landing")
        for r in cur.fetchall():
            row = json.loads(r["payload"]) if isinstance(r["payload"], str) else r["payload"]
            row["id"] = r["source_log_id"]
            err = extract_browser_error(row)
            if err is not None:
                insert_error_event(cur, row, err)
                found += 1
    conn.commit()
    conn.close()
    print(f"browser errors found in raw_log_landing: {found}")


if __name__ == "__main__":
    main()

"""
ETL state, kept in Iceberg tables so Trino is the only dependency:

  * Lease       the single-writer guard: a row in `etl_lock` with an expiry.
  * Watermark   max(source_dttm) of raw_log_landing. Derived from the data, not stored beside it,
                so it can never disagree with what was actually written.
  * Run log     one row per run in `etl_run_log`, for the "pipeline is stale" alert.

How the lease stays safe without locks: Iceberg commits are atomic and serializable per table. Two runs that try to
take the lock at the same moment issue the same MERGE; one commit wins, the other fails with a commit conflict and is
retried. After every attempt the owner is read back, and only the run whose id is stored proceeds; a run that sees a live
lease owned by someone else skips, one that sees a free lease (its own commit failed) tries again a few times.
The lease expires on its own (ETL_LOCK_TTL_MINUTES), so a crashed run cannot block the pipeline forever; a long run
renews it after every statement. If two runs ever do overlap anyway (a lease that expired mid-run, a scheduler that
ignores its own concurrency policy), the data is still correct: every write is an idempotent MERGE and commit
conflicts are retried (iceberg_sink.run_sql). The lease only avoids wasted work.
"""
import os
import random
import time
import uuid

from iceberg_sink import is_commit_conflict, run_sql

LOCK_TTL_MINUTES = int(os.environ.get("ETL_LOCK_TTL_MINUTES", "30"))
NOW_UTC = "CAST(current_timestamp AT TIME ZONE 'UTC' AS timestamp(6))"


def _q(value):
    return "'%s'" % str(value).replace("'", "''")


class Lease:
    def __init__(self, conn, name, ttl_minutes=LOCK_TTL_MINUTES):
        self.conn, self.name, self.ttl = conn, name, int(ttl_minutes)
        self.owner = uuid.uuid4().hex
        self._renewed_at = 0.0

    def _expiry(self):
        return "%s + INTERVAL '%d' MINUTE" % (NOW_UTC, self.ttl)

    def acquire(self, attempts=4):
        """True when this run now holds the lease. A refusal means another run holds an UNEXPIRED lease. If our own
        commit failed while the lease was free (a catalog hiccup, or a commit race both sides lost), try again."""
        sql = (
            "MERGE INTO etl_lock t USING (SELECT %s AS lock_name, %s AS owner, %s AS acquired_at, %s AS expires_at) s\n"
            "ON t.lock_name = s.lock_name\n"
            "WHEN MATCHED AND t.expires_at < s.acquired_at THEN UPDATE SET\n"
            "    owner = s.owner, acquired_at = s.acquired_at, expires_at = s.expires_at\n"
            "WHEN NOT MATCHED THEN INSERT (lock_name, owner, acquired_at, expires_at)\n"
            "    VALUES (s.lock_name, s.owner, s.acquired_at, s.expires_at)"
            % (_q(self.name), _q(self.owner), NOW_UTC, self._expiry())
        )
        for attempt in range(attempts):
            try:
                run_sql(self.conn, sql, retries=0)
            except Exception as exc:  # noqa: BLE001
                if not is_commit_conflict(exc):
                    raise
                # the commit failed; the read-back below says whether someone else holds the lease
            rows = run_sql(self.conn, "SELECT owner, expires_at > %s FROM etl_lock WHERE lock_name = %s"
                           % (NOW_UTC, _q(self.name)))
            if rows and rows[0][0] == self.owner:
                self._renewed_at = time.monotonic()
                return True
            if rows and rows[0][1]:
                return False  # another run holds a live lease
            time.sleep((1 + attempt) * (0.5 + random.random()))  # free, but our commit lost: try again
        return False

    def renew(self, force=False):
        """Pushes the expiry forward. Called after every statement; throttled to one UPDATE a minute."""
        if not force and time.monotonic() - self._renewed_at < 60:
            return
        self._renewed_at = time.monotonic()
        try:
            run_sql(self.conn, "UPDATE etl_lock SET expires_at = %s WHERE lock_name = %s AND owner = %s"
                    % (self._expiry(), _q(self.name), _q(self.owner)), retries=0)
        except Exception as exc:  # noqa: BLE001
            if not is_commit_conflict(exc):
                raise  # a conflict only means someone else touched the row; the lease stays advisory

    def release(self):
        try:
            run_sql(self.conn, "UPDATE etl_lock SET expires_at = %s WHERE lock_name = %s AND owner = %s"
                    % (NOW_UTC, _q(self.name), _q(self.owner)), retries=2)
        except Exception:  # noqa: BLE001
            pass  # it expires on its own


def get_watermark(conn):
    """(max source_dttm, max source_log_id) over raw_log_landing; (None, 0) when empty. Rows dated in the future are
    ignored, so a bad clock or an old seed script cannot make every later real event look 'already seen'."""
    rows = run_sql(conn, "SELECT max(source_dttm), max(source_log_id) FROM raw_log_landing WHERE source_dttm <= %s" % NOW_UTC)
    ts, log_id = rows[0] if rows else (None, None)
    return ts, int(log_id or 0)


def landing_total(conn, until=None):
    """Rows held in raw_log_landing, optionally only those dated before `until` (naive UTC datetime)."""
    where = "" if until is None else " WHERE source_dttm < TIMESTAMP '%s'" % until.strftime("%Y-%m-%d %H:%M:%S.%f")
    return int(run_sql(conn, "SELECT count(*) FROM raw_log_landing" + where)[0][0])


def known_raw_ids(conn, since=None):
    """Ids of raw_log_landing rows dated at or after `since` (all rows when None). The ETL skips these: raw is written
    after the facts, so a row held here has all of its facts too."""
    where = "" if since is None else " WHERE source_dttm >= TIMESTAMP '%s'" % since.strftime("%Y-%m-%d %H:%M:%S.%f")
    cur = conn.cursor()
    cur.execute("SELECT source_log_id FROM raw_log_landing" + where)
    ids = set()
    while True:
        batch = cur.fetchmany(10000)
        if not batch:
            return ids
        ids.update(r[0] for r in batch)


def record_run(conn, kind, status, rows_read=0, rows_new=0, detail=""):
    """Appends one etl_run_log row. Never raises: losing a log row must not fail the run."""
    try:
        run_sql(conn, "INSERT INTO etl_run_log (run_ts, kind, status, rows_read, rows_new, detail) VALUES (%s, %s, %s, %d, %d, %s)"
                % (NOW_UTC, _q(kind), _q(status), int(rows_read), int(rows_new), _q(detail[:500])), retries=2)
    except Exception:  # noqa: BLE001
        pass

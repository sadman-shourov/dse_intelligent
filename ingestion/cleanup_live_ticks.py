from __future__ import annotations

import json
import os
from datetime import date
from pathlib import Path

import psycopg2
from dotenv import load_dotenv


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def get_db_date(conn) -> date:
    cur = conn.cursor()
    try:
        cur.execute("SELECT CURRENT_DATE")
        row = cur.fetchone()
        if not row or row[0] is None:
            raise RuntimeError("SELECT CURRENT_DATE returned no date")
        return row[0]
    finally:
        cur.close()


def cleanup_live_ticks() -> dict:
    root = _project_root()
    load_dotenv(root / ".env")
    database_url = os.environ.get("DATABASE_URL")
    if not database_url or not database_url.strip():
        raise RuntimeError("DATABASE_URL is missing or empty in .env")

    conn = psycopg2.connect(database_url)
    conn.autocommit = False
    target_date = get_db_date(conn)
    target_date_str = target_date.strftime("%Y-%m-%d")
    cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(*) FROM live_ticks WHERE date = %s", (target_date_str,))
        source_count = cur.fetchone()[0]
        if source_count == 0:
            conn.rollback()
            return {
                "status": "skipped",
                "message": "No live ticks found for today. Nothing to archive.",
                "rows_affected": 0,
                "details": {"date": target_date_str},
            }

        cur.execute("SELECT COUNT(*) FROM live_ticks_archive WHERE date = %s", (target_date_str,))
        archive_before = cur.fetchone()[0]

        cur.execute(
            """
            INSERT INTO live_ticks_archive
                (symbol, date, session_no, fetched_at, ltp, high, low,
                 close, ycp, change_val, trade, value, volume)
            SELECT
                symbol, date, session_no, fetched_at, ltp, high, low,
                close, ycp, change_val, trade, value, volume
            FROM live_ticks
            WHERE date = %s
            """,
            (target_date_str,),
        )
        archived_rows = cur.rowcount

        cur.execute("SELECT COUNT(*) FROM live_ticks_archive WHERE date = %s", (target_date_str,))
        archive_after = cur.fetchone()[0]
        if archive_after != archive_before + source_count or archived_rows != source_count:
            raise RuntimeError("Archive verification failed")

        cur.execute("DELETE FROM live_ticks WHERE date = %s", (target_date_str,))
        deleted_rows = cur.rowcount
        if deleted_rows != source_count:
            raise RuntimeError("Delete verification failed")

        cur.execute("SELECT COUNT(*) FROM live_ticks WHERE date = %s", (target_date_str,))
        remaining = cur.fetchone()[0]
        if remaining != 0:
            raise RuntimeError("Post-delete verification failed")

        conn.commit()
        return {
            "status": "ok",
            "message": "Live Ticks EOD Cleanup Complete",
            "rows_affected": deleted_rows,
            "details": {
                "date": target_date_str,
                "rows_archived": archived_rows,
                "rows_deleted": deleted_rows,
                "live_ticks_remaining": remaining,
            },
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    result = {"status": "error", "message": "Unknown error", "rows_affected": 0, "details": {}}
    try:
        result = cleanup_live_ticks()
    except Exception as e:
        result = {"status": "error", "message": str(e), "rows_affected": 0, "details": {}}
    print(json.dumps(result, indent=2, default=str))
    if result.get("status") == "error":
        raise SystemExit(1)

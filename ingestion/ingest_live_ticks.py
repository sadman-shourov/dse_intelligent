from __future__ import annotations

import json
import os
from datetime import date, datetime, time, timedelta
from pathlib import Path

import psycopg2
import pytz
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


def _compute_session_info(now_dhaka: datetime) -> tuple[int | None, str | None]:
    # Sessions: 10:15=1 … 14:45=27 (10-minute slots; window 10:15–14:45 Asia/Dhaka)
    market_open = time(10, 15)
    market_close = time(14, 45)
    t = now_dhaka.time()
    if t < market_open or t > market_close:
        return None, None
    start_dt = now_dhaka.replace(hour=10, minute=15, second=0, microsecond=0)
    elapsed_min = int((now_dhaka - start_dt).total_seconds() // 60)
    session_no = elapsed_min // 10 + 1
    if session_no < 1 or session_no > 27:
        return None, None
    session_start = start_dt + timedelta(minutes=(session_no - 1) * 10)
    session_time = session_start.strftime("%-I:%M%p").lower()
    return session_no, session_time


def ingest_live_ticks() -> dict:
    root = _project_root()
    load_dotenv(root / ".env")
    database_url = os.environ.get("DATABASE_URL")
    if not database_url or not database_url.strip():
        raise RuntimeError("DATABASE_URL is missing or empty in .env")

    tz = pytz.timezone("Asia/Dhaka")
    now_dhaka = datetime.now(tz)
    session_no, session_label = _compute_session_info(now_dhaka)
    if session_no is None:
        return {
            "status": "skipped",
            "message": "Outside market session window (10:15-14:45 Asia/Dhaka).",
            "rows_affected": 0,
            "details": {},
        }

    import bdshare  # type: ignore
    trade_data = bdshare.get_current_trade_data()
    if trade_data is None or (hasattr(trade_data, "empty") and trade_data.empty):
        raise RuntimeError("bdshare total failure: get_current_trade_data() returned empty.")
    if hasattr(trade_data, "reset_index"):
        try:
            trade_data = trade_data.reset_index(drop=True)
        except Exception:
            pass
    if not hasattr(trade_data, "to_dict"):
        raise RuntimeError("bdshare total failure: unexpected response type.")

    rows = trade_data.to_dict(orient="records")
    if not rows:
        raise RuntimeError("bdshare total failure: no rows returned.")

    def _ltp_value(row: dict) -> float:
        try:
            return float(row.get("ltp"))
        except Exception:
            return 0.0

    if all(_ltp_value(r) == 0.0 for r in rows):
        return {
            "status": "skipped",
            "message": "Market closed or no trading activity. Exiting.",
            "rows_affected": 0,
            "details": {},
        }

    conn = psycopg2.connect(database_url)
    conn.autocommit = True
    target_date = get_db_date(conn)
    cur = conn.cursor()
    try:
        insert_sql = """
            INSERT INTO live_ticks
                (symbol, date, session_no, fetched_at, ltp, high, low,
                 close, ycp, change_val, trade, value, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, date, session_no) DO NOTHING;
        """.strip()
        rows_written = 0
        rows_skipped_zero = 0
        row_errors = 0
        for row in rows:
            symbol = row.get("symbol")
            ltp = _ltp_value(row)
            if ltp == 0.0:
                rows_skipped_zero += 1
                continue
            try:
                cur.execute(
                    insert_sql,
                    (
                        symbol,
                        target_date,
                        session_no,
                        now_dhaka,
                        row.get("ltp"),
                        row.get("high"),
                        row.get("low"),
                        row.get("close"),
                        row.get("ycp"),
                        row.get("change"),
                        row.get("trade"),
                        row.get("value"),
                        row.get("volume"),
                    ),
                )
                if cur.rowcount == 1:
                    rows_written += 1
            except Exception:
                row_errors += 1

        return {
            "status": "ok",
            "message": "Live Tick Ingest Complete",
            "rows_affected": rows_written,
            "details": {
                "date": target_date.isoformat(),
                "session": f"{session_no} ({session_label})",
                "rows_written": rows_written,
                "rows_skipped_zero": rows_skipped_zero,
                "row_errors": row_errors,
            },
        }
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    result = {"status": "error", "message": "Unknown error", "rows_affected": 0, "details": {}}
    try:
        result = ingest_live_ticks()
    except Exception as e:
        result = {"status": "error", "message": str(e), "rows_affected": 0, "details": {}}
    print(json.dumps(result, indent=2, default=str))
    if result.get("status") == "error":
        raise SystemExit(1)

from __future__ import annotations

import json
import os
import time
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


def _all_ltp_zero(rows: list[dict]) -> bool:
    if not rows:
        return False
    for row in rows:
        try:
            if row.get("ltp") is None or float(row.get("ltp")) != 0.0:
                return False
        except Exception:
            return False
    return True


def _to_rows(df) -> list[dict]:
    if df is None or (hasattr(df, "empty") and df.empty):
        return []
    if hasattr(df, "reset_index"):
        try:
            df = df.reset_index()
        except Exception:
            pass
    if not hasattr(df, "to_dict"):
        return []
    return df.to_dict(orient="records")


def append_price_history() -> dict:
    root = _project_root()
    load_dotenv(root / ".env")
    database_url = os.environ.get("DATABASE_URL")
    if not database_url or not database_url.strip():
        raise RuntimeError("DATABASE_URL is missing or empty in .env")

    import bdshare  # type: ignore

    conn = psycopg2.connect(database_url)
    conn.autocommit = True
    target_date = get_db_date(conn)
    target_date_str = target_date.strftime("%Y-%m-%d")
    cur = conn.cursor()

    try:
        cur.execute("SELECT symbol FROM stocks_master WHERE is_active = TRUE ORDER BY symbol ASC;")
        symbols = [r[0] for r in cur.fetchall()]

        upsert_sql = """
            INSERT INTO price_history
                (symbol, date, open, high, low, close, ltp, ycp, trade, value, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, date) DO UPDATE SET
                open   = EXCLUDED.open,
                high   = EXCLUDED.high,
                low    = EXCLUDED.low,
                close  = EXCLUDED.close,
                ltp    = EXCLUDED.ltp,
                ycp    = EXCLUDED.ycp,
                trade  = EXCLUDED.trade,
                value  = EXCLUDED.value,
                volume = EXCLUDED.volume;
        """.strip()

        first_batch_cache: dict[str, list[dict]] = {}
        first_batch_all_zero = True
        for sym in symbols[:5]:
            try:
                rows = _to_rows(bdshare.get_historical_data(start=target_date_str, end=target_date_str, code=sym))
                first_batch_cache[sym] = rows
                if not rows or not _all_ltp_zero(rows):
                    first_batch_all_zero = False
            except Exception:
                first_batch_all_zero = False

        if first_batch_all_zero:
            return {
                "status": "skipped",
                "message": "Market appears closed today. Exiting.",
                "rows_affected": 0,
                "details": {"date": target_date_str},
            }

        symbols_done = 0
        symbols_failed = 0
        rows_upserted = 0
        failed: dict[str, str] = {}

        for symbol in symbols:
            try:
                rows = first_batch_cache.get(symbol)
                if rows is None:
                    rows = _to_rows(bdshare.get_historical_data(start=target_date_str, end=target_date_str, code=symbol))
                if not rows:
                    symbols_failed += 1
                    failed[symbol] = "No data returned"
                    time.sleep(0.3)
                    continue
                if _all_ltp_zero(rows):
                    symbols_failed += 1
                    failed[symbol] = "Market closed/holiday data (ltp=0), skipped"
                    time.sleep(0.3)
                    continue

                upserts_for_symbol = 0
                for row in rows:
                    dt = row.get("date")
                    if dt is None:
                        continue
                    cur.execute(
                        upsert_sql,
                        (
                            symbol,
                            dt,
                            row.get("open"),
                            row.get("high"),
                            row.get("low"),
                            row.get("close"),
                            row.get("ltp"),
                            row.get("ycp"),
                            row.get("trade"),
                            row.get("value"),
                            row.get("volume"),
                        ),
                    )
                    upserts_for_symbol += 1

                rows_upserted += upserts_for_symbol
                symbols_done += 1
            except Exception as e:
                symbols_failed += 1
                failed[symbol] = f"{type(e).__name__}: {e}"
            time.sleep(0.3)

        return {
            "status": "ok",
            "message": "Daily Price History Append Complete",
            "rows_affected": rows_upserted,
            "details": {
                "date": target_date_str,
                "symbols_done": symbols_done,
                "symbols_failed": symbols_failed,
                "rows_upserted": rows_upserted,
                "failed_symbols": failed,
            },
        }
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    result = {"status": "error", "message": "Unknown error", "rows_affected": 0, "details": {}}
    try:
        result = append_price_history()
    except Exception as e:
        result = {"status": "error", "message": str(e), "rows_affected": 0, "details": {}}
    print(json.dumps(result, indent=2, default=str))
    if result.get("status") == "error":
        raise SystemExit(1)

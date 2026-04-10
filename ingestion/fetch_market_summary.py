from __future__ import annotations

import json
import os
from datetime import date, datetime
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


def _normalize_col(name: str) -> str:
    return " ".join(str(name).strip().lower().replace("_", " ").replace(".", " ").split())


def _pick_value(row: dict, candidates: list[str]):
    normalized = {_normalize_col(str(k)): v for k, v in row.items()}
    for c in candidates:
        v = normalized.get(_normalize_col(c))
        if v is not None:
            return v
    return None


def fetch_market_summary() -> dict:
    root = _project_root()
    load_dotenv(root / ".env")
    database_url = os.environ.get("DATABASE_URL")
    if not database_url or not database_url.strip():
        raise RuntimeError("DATABASE_URL is missing or empty in .env")

    conn = psycopg2.connect(database_url)
    conn.autocommit = True
    target_date = get_db_date(conn)

    import bdshare  # type: ignore
    df = bdshare.get_market_info()
    if df is None or (hasattr(df, "empty") and df.empty):
        conn.close()
        raise RuntimeError("bdshare total failure: get_market_info() returned empty.")
    if not hasattr(df, "columns") or not hasattr(df, "head") or not hasattr(df, "to_dict"):
        conn.close()
        raise RuntimeError("bdshare total failure: unexpected response type.")

    print("Raw columns:", list(df.columns))
    print("First 3 rows:")
    print(df.head(3))

    rows = df.to_dict(orient="records")
    if not rows:
        conn.close()
        raise RuntimeError("bdshare total failure: no rows returned.")

    today_row = None
    for row in rows:
        raw_date = _pick_value(row, ["Date", "date", "Trade Date", "trade_date"])
        if raw_date is None:
            continue
        raw_date_str = str(raw_date).strip()
        parsed_date = None
        for fmt in ("%d-%m-%Y", "%Y-%m-%d"):
            try:
                parsed_date = datetime.strptime(raw_date_str[:10], fmt).date()
                break
            except ValueError:
                continue
        if parsed_date is not None and parsed_date == target_date:
            today_row = row

    today_str = target_date.isoformat()

    if today_row is None:
        conn.close()
        return {
            "status": "skipped",
            "message": "Today's market summary not found in DataFrame.",
            "rows_affected": 0,
            "details": {"date": today_str},
        }

    total_trade = _pick_value(today_row, ["Total Trade", "trade", "total_trade"])
    total_volume = _pick_value(today_row, ["Total Volume", "volume", "total_volume"])
    total_value_mn = _pick_value(today_row, ["Total Value (mn)", "Total Value", "value", "total_value_mn"])
    total_mcap_mn = _pick_value(today_row, ["Total Market Cap. (mn)", "Total Market Cap (mn)", "Total Market Cap", "total_mcap_mn"])
    dsex_index = _pick_value(today_row, ["DSEX Index", "DSEX", "dsex_index"])
    dses_index = _pick_value(today_row, ["DSES Index", "DSES", "dses_index"])
    ds30_index = _pick_value(today_row, ["DS30", "DS30 Index", "ds30_index", "DGEN Index", "dgen_index"])

    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'market_summary';
            """
        )
        ms_cols = {r[0] for r in cur.fetchall()}

        if "date" in ms_cols:
            date_col = "date"
            mcap_col = "total_mcap_mn"
            ds30_col = "ds30_index"
        else:
            date_col = "trade_date"
            mcap_col = "total_market_cap_mn" if "total_market_cap_mn" in ms_cols else "total_mcap_mn"
            ds30_col = "dgen_index" if "dgen_index" in ms_cols else "ds30_index"

        upsert_sql = f"""
            INSERT INTO market_summary
                ({date_col}, total_trade, total_volume, total_value_mn,
                 {mcap_col}, dsex_index, dses_index, {ds30_col})
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT ({date_col}) DO UPDATE SET
                total_trade    = EXCLUDED.total_trade,
                total_volume   = EXCLUDED.total_volume,
                total_value_mn = EXCLUDED.total_value_mn,
                {mcap_col}     = EXCLUDED.{mcap_col},
                dsex_index     = EXCLUDED.dsex_index,
                dses_index     = EXCLUDED.dses_index,
                {ds30_col}     = EXCLUDED.{ds30_col};
        """.strip()

        cur.execute(
            upsert_sql,
            (today_str, total_trade, total_volume, total_value_mn, total_mcap_mn, dsex_index, dses_index, ds30_index),
        )
    finally:
        cur.close()
        conn.close()

    return {
        "status": "ok",
        "message": "Market Summary Fetch Complete",
        "rows_affected": 1,
        "details": {
            "date": today_str,
            "dsex": dsex_index,
            "dses": dses_index,
            "total_volume": total_volume,
            "total_value_mn": total_value_mn,
        },
    }


if __name__ == "__main__":
    result = {"status": "error", "message": "Unknown error", "rows_affected": 0, "details": {}}
    try:
        result = fetch_market_summary()
    except Exception as e:
        result = {"status": "error", "message": str(e), "rows_affected": 0, "details": {}}
    print(json.dumps(result, indent=2, default=str))
    if result.get("status") == "error":
        raise SystemExit(1)

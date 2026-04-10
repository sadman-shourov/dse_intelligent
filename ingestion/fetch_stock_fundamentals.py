from __future__ import annotations

import json
import os
from decimal import Decimal, InvalidOperation
from pathlib import Path

import psycopg2
from dotenv import load_dotenv


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _to_decimal_or_none(value):
    if value is None:
        return None
    s = str(value).strip()
    if s == "" or s.lower() in {"nan", "none", "null", "n/a", "-"}:
        return None
    s = s.replace(",", "")
    try:
        return Decimal(s)
    except (InvalidOperation, ValueError):
        return None


def safe_numeric(value) -> float | None:
    d = _to_decimal_or_none(value)
    if d is None:
        return None
    return float(d)


def fetch_stock_fundamentals() -> dict:
    root = _project_root()
    load_dotenv(root / ".env")
    database_url = os.environ.get("DATABASE_URL")
    if not database_url or not database_url.strip():
        raise RuntimeError("DATABASE_URL is missing or empty in .env")

    conn = psycopg2.connect(database_url)
    conn.autocommit = True
    cur = conn.cursor()
    try:
        cur.execute("SELECT CURRENT_DATE")
        fetched_at = cur.fetchone()[0]

        import bdshare  # type: ignore
        df = bdshare.get_latest_pe()
        if df is None or (hasattr(df, "empty") and df.empty):
            raise RuntimeError("bdshare total failure: get_latest_pe() returned empty.")
        if not hasattr(df, "columns") or not hasattr(df, "head") or not hasattr(df, "to_dict"):
            raise RuntimeError("bdshare total failure: unexpected response type.")

        print("Raw columns:", list(df.columns))
        print("First 5 rows:")
        print(df.head(5))

        rows = df.to_dict(orient="records")
        total_fetched = len(rows)

        cur.execute("SELECT symbol FROM stocks_master")
        allowed_symbols = {r[0] for r in cur.fetchall()}

        insert_sql = """
            INSERT INTO stock_fundamentals (symbol, pe_ratio, eps, fetched_at)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (symbol, fetched_at) DO NOTHING;
        """.strip()

        matched_to_db = 0
        inserted = 0
        skipped = 0
        row_errors = 0

        for row in rows:
            try:
                symbol_raw = row.get(0)
                symbol = str(symbol_raw).strip() if symbol_raw is not None else ""
                if not symbol or symbol not in allowed_symbols:
                    skipped += 1
                    continue

                matched_to_db += 1
                eps = _to_decimal_or_none(row.get(3))

                # Try bdshare P/E first
                pe = safe_numeric(row.get(5))

                # If P/E is None, calculate from price and EPS
                if pe is None:
                    ltp = safe_numeric(row.get(1))  # col 1 = current price
                    eps_f = safe_numeric(row.get(3))  # col 3 = EPS
                    if ltp and eps_f and eps_f > 0:
                        pe = round(ltp / eps_f, 2)

                # If EPS is negative, P/E is negative (loss making)
                if pe is None:
                    eps_f = safe_numeric(row.get(3))
                    ltp = safe_numeric(row.get(1))
                    if ltp and eps_f and eps_f < 0:
                        pe = round(ltp / eps_f, 2)

                if pe is not None and pe < 0.5 and pe >= 0:
                    pe = None  # not meaningful, likely data error

                pe_ratio = _to_decimal_or_none(pe) if pe is not None else None
                cur.execute(insert_sql, (symbol, pe_ratio, eps, fetched_at))
                if cur.rowcount == 1:
                    inserted += 1
            except Exception:
                row_errors += 1

        return {
            "status": "ok",
            "message": "Stock Fundamentals Fetch Complete",
            "rows_affected": inserted,
            "details": {
                "fetched_date": fetched_at,
                "total_fetched": total_fetched,
                "matched_to_db": matched_to_db,
                "inserted": inserted,
                "skipped": skipped,
                "row_errors": row_errors,
            },
        }
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    result = {"status": "error", "message": "Unknown error", "rows_affected": 0, "details": {}}
    try:
        result = fetch_stock_fundamentals()
    except Exception as e:
        result = {"status": "error", "message": str(e), "rows_affected": 0, "details": {}}
    print(json.dumps(result, indent=2, default=str))
    if result.get("status") == "error":
        raise SystemExit(1)

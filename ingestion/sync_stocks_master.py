from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Callable, Iterable, TypeVar

import psycopg2
from dotenv import load_dotenv

T = TypeVar("T")


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _call_with_retries(label: str, fn: Callable[[], T], attempts: int = 3, sleep_s: float = 2.0) -> T:
    last_err: Exception | None = None
    for i in range(1, attempts + 1):
        try:
            return fn()
        except Exception as e:
            last_err = e
            if i < attempts:
                time.sleep(sleep_s)
    assert last_err is not None
    raise RuntimeError(f"{label} failed after {attempts} attempts: {last_err}")


def _normalize_symbols(raw: Iterable) -> list[str]:
    out: list[str] = []
    for s in raw:
        if s is None:
            continue
        sym = str(s).strip()
        if sym:
            out.append(sym)
    return out


def _extract_symbols_from_dataframe(df, preferred_cols: tuple[str, ...]) -> list[str]:
    try:
        cols = list(getattr(df, "columns"))
    except Exception:
        cols = []

    col = None
    for c in preferred_cols:
        if c in cols:
            col = c
            break
    if col is None and len(cols) == 1:
        col = cols[0]
    if col is None:
        return []

    try:
        series = df[col]
        values = series.tolist() if hasattr(series, "tolist") else list(series)
        return _normalize_symbols(values)
    except Exception:
        return []


def _extract_all_symbols(all_symbols_raw) -> list[str]:
    if hasattr(all_symbols_raw, "columns") and hasattr(all_symbols_raw, "__getitem__"):
        extracted = _extract_symbols_from_dataframe(all_symbols_raw, ("symbol", "trading_code", "code"))
        if extracted:
            return extracted
    if isinstance(all_symbols_raw, (list, tuple, set)):
        return _normalize_symbols(all_symbols_raw)
    if isinstance(all_symbols_raw, dict):
        for key in ("symbol", "trading_code", "code"):
            if key in all_symbols_raw and isinstance(all_symbols_raw[key], (list, tuple)):
                return _normalize_symbols(all_symbols_raw[key])
    try:
        return _normalize_symbols(list(all_symbols_raw))
    except Exception:
        return []


def _extract_dsex_symbols(dsex_data) -> set[str]:
    if dsex_data is None:
        return set()
    if hasattr(dsex_data, "columns") and hasattr(dsex_data, "__getitem__"):
        return set(_extract_symbols_from_dataframe(dsex_data, ("symbol", "trading_code", "code")))
    if isinstance(dsex_data, (list, tuple)):
        out: set[str] = set()
        for item in dsex_data:
            if isinstance(item, dict):
                val = item.get("symbol") or item.get("trading_code") or item.get("code")
                if val:
                    out.add(str(val).strip())
        return out
    if isinstance(dsex_data, dict):
        for key in ("symbol", "trading_code", "code"):
            if key in dsex_data and isinstance(dsex_data[key], (list, tuple)):
                return set(_normalize_symbols(dsex_data[key]))
    return set()


def sync_stocks_master() -> dict:
    root = _project_root()
    load_dotenv(root / ".env")
    database_url = os.environ.get("DATABASE_URL")
    if not database_url or not database_url.strip():
        raise RuntimeError("DATABASE_URL is missing or empty in .env")

    import bdshare  # type: ignore

    all_symbols_raw = _call_with_retries("bdshare.get_current_trading_code()", lambda: bdshare.get_current_trading_code())
    all_symbols = _extract_all_symbols(all_symbols_raw)
    if not all_symbols:
        raise RuntimeError("bdshare.get_current_trading_code() returned 0 symbols")

    dsex_data = _call_with_retries("bdshare.get_dsex_data()", lambda: bdshare.get_dsex_data())
    dsex_symbols = _extract_dsex_symbols(dsex_data)

    inserted = 0
    updated = 0

    conn = psycopg2.connect(database_url)
    conn.autocommit = True
    cur = conn.cursor()
    try:
        upsert_sql = """
            INSERT INTO stocks_master (symbol, is_dsex, is_active, created_at, updated_at)
            VALUES (%s, %s, TRUE, NOW(), NOW())
            ON CONFLICT (symbol) DO UPDATE SET
                is_dsex = EXCLUDED.is_dsex,
                is_active = TRUE,
                updated_at = NOW();
        """.strip()
        exists_sql = "SELECT 1 FROM stocks_master WHERE symbol = %s LIMIT 1;"

        for symbol in all_symbols:
            cur.execute(exists_sql, (symbol,))
            existed = cur.fetchone() is not None
            cur.execute(upsert_sql, (symbol, symbol in dsex_symbols))
            if existed:
                updated += 1
            else:
                inserted += 1
    finally:
        cur.close()
        conn.close()

    return {
        "status": "ok",
        "message": "Stocks master sync complete",
        "rows_affected": inserted + updated,
        "details": {
            "total_symbols": len(all_symbols),
            "dsex_listed": len(set(all_symbols) & dsex_symbols),
            "new_inserted": inserted,
            "updated": updated,
        },
    }


if __name__ == "__main__":
    result = {"status": "error", "message": "Unknown error", "rows_affected": 0, "details": {}}
    try:
        result = sync_stocks_master()
    except Exception as e:
        result = {"status": "error", "message": str(e), "rows_affected": 0, "details": {}}
    print(json.dumps(result, indent=2, default=str))
    if result.get("status") == "error":
        raise SystemExit(1)

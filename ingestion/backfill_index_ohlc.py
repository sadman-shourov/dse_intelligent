"""One-time backfill of index_ohlc from stocknow's recorded index series.

DSE does not publish OHLC for its indices, so the per-stock day-end archive
(used by backfill_price_history) has no index candles. stocknow.com.bd records
the live index value intraday and exposes daily OHLC through its private chart
datafeed. We pull the full paginated history for each index once.

Endpoint (per index code):
    GET https://stocknow.com.bd/api/v1/instruments/{CODE}/history?resolution=D&skip=N

Response is 6 parallel arrays: [open, high, low, close, volume, unix_seconds].
Timestamps are midnight Asia/Dhaka, so we add 6h before taking the date. The
`open` column is the prior day's close carried forward, not a true session open.

Idempotent: INSERT ... ON CONFLICT (index_code, date) DO NOTHING.
"""

from __future__ import annotations

import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg2
import requests
from dotenv import load_dotenv

INDEX_CODES = ("DSEX", "DS30", "DSES")
BASE_URL = "https://stocknow.com.bd/api/v1/instruments/{code}/history"
PAGE_SIZE = 400  # stocknow returns 400 bars per page
DHAKA = timezone(timedelta(hours=6))

_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://stocknow.com.bd/chart",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36",
}


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _fetch_page(code: str, skip: int, retries: int = 4) -> list[dict]:
    """Fetch one page of daily candles for `code` at offset `skip`."""
    url = BASE_URL.format(code=code)
    params = {"resolution": "D", "skip": skip}
    last_exc: Exception | None = None

    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, params=params, headers=_HEADERS, timeout=20)
            resp.raise_for_status()
            cols = resp.json()
            if not isinstance(cols, list) or len(cols) < 6:
                raise ValueError(f"Unexpected payload shape: {type(cols)}")
            o, h, l, c, v, t = cols[0], cols[1], cols[2], cols[3], cols[4], cols[5]
            rows: list[dict] = []
            for i in range(len(t)):
                d = datetime.fromtimestamp(t[i], DHAKA).date()
                rows.append(
                    {
                        "date": d,
                        "open": o[i],
                        "high": h[i],
                        "low": l[i],
                        "close": c[i],
                        "volume": v[i],
                    }
                )
            return rows
        except (requests.RequestException, ValueError) as exc:
            last_exc = exc
            if attempt < retries:
                time.sleep(0.5 * (2 ** (attempt - 1)))

    raise RuntimeError(f"Failed to fetch {code} skip={skip}: {last_exc}")


def _fetch_all(code: str) -> list[dict]:
    """Page backward (skip += 400) until an empty page; dedupe by date."""
    seen: dict = {}
    skip = 0
    while True:
        rows = _fetch_page(code, skip)
        if not rows:
            break
        new = 0
        for r in rows:
            if r["date"] not in seen:
                seen[r["date"]] = r
                new += 1
        print(f"  {code} skip={skip}: {len(rows)} bars ({new} new)")
        # Stop if the page added nothing new (reached the archive floor).
        if new == 0:
            break
        skip += PAGE_SIZE
        time.sleep(0.4)
    return [seen[d] for d in sorted(seen)]


def backfill_index_ohlc() -> None:
    root = _project_root()
    load_dotenv(root / ".env")

    database_url = os.environ.get("DATABASE_URL")
    if not database_url or not database_url.strip():
        print("DATABASE_URL is missing or empty in .env", file=sys.stderr)
        sys.exit(1)

    insert_sql = (
        """
        INSERT INTO index_ohlc
            (index_code, date, open, high, low, close, volume, source)
        VALUES (%s, %s, %s, %s, %s, %s, %s, 'stocknow')
        ON CONFLICT (index_code, date) DO NOTHING;
        """
    ).strip()

    total_inserted = 0
    summary: dict[str, dict] = {}

    for code in INDEX_CODES:
        print(f"Fetching {code} ...")
        try:
            rows = _fetch_all(code)
        except Exception as exc:  # noqa: BLE001 - continue with other indices
            print(f"⚠ {code} - fetch failed: {exc}", file=sys.stderr)
            summary[code] = {"fetched": 0, "inserted": 0, "error": str(exc)}
            continue

        conn = psycopg2.connect(database_url)
        try:
            conn.autocommit = False
            cur = conn.cursor()
            inserted = 0
            for r in rows:
                cur.execute(
                    insert_sql,
                    (
                        code,
                        r["date"],
                        r["open"],
                        r["high"],
                        r["low"],
                        r["close"],
                        r["volume"],
                    ),
                )
                if cur.rowcount == 1:
                    inserted += 1
            conn.commit()
            cur.close()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

        total_inserted += inserted
        span = f"{rows[0]['date']} → {rows[-1]['date']}" if rows else "-"
        summary[code] = {"fetched": len(rows), "inserted": inserted, "span": span}
        print(f"✓ {code} - {inserted} inserted of {len(rows)} ({span})")

    line = "━" * 34
    print(line)
    print("Index OHLC Backfill Complete")
    for code in INDEX_CODES:
        s = summary.get(code, {})
        if "error" in s:
            print(f"{code:5} : FAILED - {s['error']}")
        else:
            print(
                f"{code:5} : {s.get('inserted', 0):>5} inserted / "
                f"{s.get('fetched', 0):>5} fetched   {s.get('span', '-')}"
            )
    print(f"Total inserted: {total_inserted:,}")
    print(line)


if __name__ == "__main__":
    backfill_index_ohlc()

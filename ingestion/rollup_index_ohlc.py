"""End-of-day rollup of index_ticks into self-generated index_ohlc candles.

During market hours fetch_market_summary appends the live DSEX/DS30/DSES value
to index_ticks on every run. After close, this job reads today's samples and
builds one OHLC candle per index:

    open  = first sample of the day (earliest fetched_at)
    high  = max sample
    low   = min sample
    close = last sample (latest fetched_at)

Volume is taken from market_summary.total_volume for the day (an index has no
share count of its own). Rows are written with source='self', marking them as
generated from our own pipeline rather than scraped.

Intended to run once in the EOD chain, right after the Fetch Market Summary
node. Idempotent: re-running the same day recomputes and upserts the candle.

Guard: a candle is only written when the day has enough samples spread across
the session (see _MIN_SAMPLES / _MIN_SPAN_MINUTES). Too few (market closed, an
off-hours manual test, or a day the ticks job barely ran) returns 'skipped' and
writes nothing, so a flat/near-flat artifact never overwrites a real candle.
"""

from __future__ import annotations

import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

# Map index_ohlc.index_code -> index_ticks column.
_INDEX_COLUMNS = {"DSEX": "dsex", "DS30": "ds30", "DSES": "dses"}

# A candle is only meaningful if built from enough samples spread across the
# session. Below these thresholds the data is a flat/near-flat artifact (e.g. an
# off-hours manual test recording one tick, or a day the market-hours chain
# barely ran), so we skip and write nothing rather than overwrite a real candle
# with garbage. A normal trading day clears both easily.
_MIN_SAMPLES = 4
_MIN_SPAN_MINUTES = 30

_UPSERT_SQL = (
    """
    INSERT INTO index_ohlc
        (index_code, date, open, high, low, close, volume, source)
    VALUES (%s, %s, %s, %s, %s, %s, %s, 'self')
    ON CONFLICT (index_code, date) DO UPDATE SET
        open   = EXCLUDED.open,
        high   = EXCLUDED.high,
        low    = EXCLUDED.low,
        close  = EXCLUDED.close,
        volume = EXCLUDED.volume,
        source = EXCLUDED.source;
    """
).strip()


def rollup_index_ohlc() -> dict:
    """Build today's index candles from index_ticks. Returns a status summary."""
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
    database_url = os.environ.get("DATABASE_URL")
    if not database_url or not database_url.strip():
        return {"status": "error", "message": "DATABASE_URL is missing or empty in .env"}

    conn = psycopg2.connect(database_url)
    conn.autocommit = True
    try:
        cur = conn.cursor()
        cur.execute("SELECT CURRENT_DATE")
        today = cur.fetchone()[0]

        # Sample count and the time span they cover. A real session produces
        # many samples over hours; an artifact produces one or two close together.
        cur.execute(
            """
            SELECT COUNT(*),
                   EXTRACT(EPOCH FROM (MAX(fetched_at) - MIN(fetched_at))) / 60.0
            FROM index_ticks WHERE date = %s
            """,
            (today,),
        )
        sample_count, span_minutes = cur.fetchone()
        span_minutes = float(span_minutes or 0.0)

        if sample_count < _MIN_SAMPLES or span_minutes < _MIN_SPAN_MINUTES:
            cur.close()
            return {
                "status": "skipped",
                "message": (
                    f"Not enough samples for a real candle "
                    f"({sample_count} samples over {span_minutes:.0f} min; "
                    f"need >={_MIN_SAMPLES} over >={_MIN_SPAN_MINUTES} min). "
                    f"Nothing written."
                ),
                "date": today.isoformat(),
                "samples_today": int(sample_count),
                "span_minutes": round(span_minutes, 1),
            }

        # Total market volume for the day (index candle 'volume').
        day_volume = None
        for date_col in ("date", "trade_date"):
            try:
                cur.execute(
                    f"SELECT total_volume FROM market_summary WHERE {date_col} = %s",
                    (today,),
                )
                row = cur.fetchone()
                if row is not None:
                    day_volume = row[0]
                break
            except psycopg2.errors.UndefinedColumn:
                conn.rollback()  # try the other column name
                continue

        written: dict[str, dict] = {}
        for code, col in _INDEX_COLUMNS.items():
            cur.execute(
                f"""
                SELECT
                    (array_agg({col} ORDER BY fetched_at ASC))[1]  AS open,
                    MAX({col})                                      AS high,
                    MIN({col})                                      AS low,
                    (array_agg({col} ORDER BY fetched_at DESC))[1] AS close,
                    COUNT({col})                                    AS n
                FROM index_ticks
                WHERE date = %s AND {col} IS NOT NULL
                """,
                (today,),
            )
            o, h, l, c, n = cur.fetchone()
            if n == 0 or o is None:
                written[code] = {"status": "skipped", "reason": "no non-null samples"}
                continue

            cur.execute(_UPSERT_SQL, (code, today, o, h, l, c, day_volume))
            written[code] = {
                "status": "ok",
                "samples": int(n),
                "open": float(o),
                "high": float(h),
                "low": float(l),
                "close": float(c),
            }

        cur.close()
        ok = [k for k, v in written.items() if v.get("status") == "ok"]
        return {
            "status": "ok" if ok else "skipped",
            "date": today.isoformat(),
            "samples_today": int(sample_count),
            "indices_written": ok,
            "indices": written,
        }
    finally:
        conn.close()


if __name__ == "__main__":
    import json

    print(json.dumps(rollup_index_ohlc(), indent=2))

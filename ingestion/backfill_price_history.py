"""One-time backfill of price_history from bdshare historical data.

- Fetches OHLCV history for all active symbols in stocks_master
- Inserts into price_history with ON CONFLICT DO NOTHING (idempotent)
- Continues on per-symbol failures
- One DB transaction per symbol (commit once) + fresh connection per attempt
- Resume support: if /tmp/dse_backfill.log exists, skip symbols already marked
  as completed (✓) or failed/ignored (⚠) in that log.

Expected runtime: ~5–10 minutes (396 sequential HTTP calls + inserts).
"""

from __future__ import annotations

import os
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Callable, TypeVar

import psycopg2
from dotenv import load_dotenv

T = TypeVar("T")

DEFAULT_LOG_PATH = Path("/tmp/dse_backfill.log")


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _yesterday_str() -> str:
    return (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")


def _fetch_symbols(database_url: str) -> list[str]:
    conn = psycopg2.connect(database_url)
    conn.autocommit = True
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT symbol
            FROM stocks_master
            WHERE is_active = TRUE
            ORDER BY symbol ASC;
            """.strip()
        )
        return [r[0] for r in cur.fetchall()]
    finally:
        cur.close()
        conn.close()


def _processed_symbols_from_log(log_path: Path) -> set[str]:
    """Parse symbols that were already handled in prior run(s)."""
    processed: set[str] = set()
    if not log_path.is_file():
        return processed

    try:
        for line in log_path.read_text(errors="ignore").splitlines():
            # Formats we print:
            #   ✓ SYMBOL — ...
            #   ⚠ SYMBOL — ...
            if line.startswith("✓ ") or line.startswith("⚠ "):
                rest = line[2:].strip()
                sym = rest.split(" — ", 1)[0].strip()
                if sym:
                    processed.add(sym)
    except Exception:
        return processed

    return processed


def _with_db_retry(database_url: str, fn: Callable[[psycopg2.extensions.connection], T], attempts: int = 3) -> T:
    last: Exception | None = None
    for i in range(attempts):
        conn = None
        try:
            conn = psycopg2.connect(database_url)
            return fn(conn)
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            last = e
            if conn is not None and not conn.closed:
                try:
                    conn.rollback()
                except Exception:
                    pass
            if i < attempts - 1:
                time.sleep(1.0 * (i + 1))
        except Exception:
            if conn is not None and not conn.closed:
                try:
                    conn.rollback()
                except Exception:
                    pass
            raise
        finally:
            if conn is not None and not conn.closed:
                conn.close()
    assert last is not None
    raise last


def backfill_price_history() -> None:
    start_date = "2024-01-01"
    end_date = _yesterday_str()

    try:
        root = _project_root()
        load_dotenv(root / ".env")

        database_url = os.environ.get("DATABASE_URL")
        if not database_url or not database_url.strip():
            raise RuntimeError("DATABASE_URL is missing or empty in .env")

        try:
            import bdshare  # type: ignore
        except Exception as e:
            raise RuntimeError("bdshare is not installed. Run: pip install -r requirements.txt") from e

        try:
            symbols = _fetch_symbols(database_url)
        except Exception as e:
            print(f"Failed to read stocks_master symbols: {e}", file=sys.stderr)
            sys.exit(1)

        processed = _processed_symbols_from_log(DEFAULT_LOG_PATH)

        total_symbols = len(symbols)
        symbols_done = 0
        symbols_failed = 0
        total_rows_inserted = 0
        failed: dict[str, str] = {}

        insert_sql = (
            """
            INSERT INTO price_history
                (symbol, date, open, high, low, close, ltp, ycp,
                 trade, value, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, date) DO NOTHING;
            """
        ).strip()

        for idx, symbol in enumerate(symbols, start=1):
            if symbol in processed:
                # Already handled in a previous run; skip without touching bdshare/DB.
                continue

            print(f"[{idx}/{total_symbols}] Processing {symbol}...")

            try:
                df = bdshare.get_historical_data(start=start_date, end=end_date, code=symbol)

                if df is None or (hasattr(df, "empty") and df.empty):
                    msg = "No data returned"
                    print(f"⚠ {symbol} — {msg}")
                    symbols_failed += 1
                    failed[symbol] = msg
                    time.sleep(0.5)
                    continue

                if hasattr(df, "reset_index"):
                    try:
                        df = df.reset_index()
                    except Exception:
                        pass

                if not hasattr(df, "to_dict"):
                    msg = f"Unexpected data type: {type(df)}"
                    print(f"⚠ {symbol} — {msg}")
                    symbols_failed += 1
                    failed[symbol] = msg
                    time.sleep(0.5)
                    continue

                rows = df.to_dict(orient="records")
                if not rows:
                    msg = "No data returned"
                    print(f"⚠ {symbol} — {msg}")
                    symbols_failed += 1
                    failed[symbol] = msg
                    time.sleep(0.5)
                    continue

                all_ltp_zero = True
                for r in rows:
                    ltp = r.get("ltp")
                    try:
                        if ltp is None:
                            all_ltp_zero = False
                            break
                        if float(ltp) != 0.0:
                            all_ltp_zero = False
                            break
                    except Exception:
                        all_ltp_zero = False
                        break

                if all_ltp_zero:
                    print(f"⚠ {symbol} — Market closed (ltp=0 for all rows), skipped")
                    symbols_done += 1
                    time.sleep(0.5)
                    continue

                rows_ref = rows

                def _insert_all(c: psycopg2.extensions.connection) -> int:
                    c.autocommit = False
                    cur = c.cursor()
                    n = 0
                    try:
                        for r in rows_ref:
                            dt = r.get("date")
                            if dt is None:
                                raise ValueError(
                                    "Historical data row missing 'date' after reset_index()"
                                )
                            cur.execute(
                                insert_sql,
                                (
                                    symbol,
                                    dt,
                                    r.get("open"),
                                    r.get("high"),
                                    r.get("low"),
                                    r.get("close"),
                                    r.get("ltp"),
                                    r.get("ycp"),
                                    r.get("trade"),
                                    r.get("value"),
                                    r.get("volume"),
                                ),
                            )
                            if cur.rowcount == 1:
                                n += 1
                        c.commit()
                        return n
                    except Exception:
                        c.rollback()
                        raise
                    finally:
                        cur.close()

                inserted = _with_db_retry(database_url, _insert_all)

                skipped = len(rows_ref) - inserted
                total_rows_inserted += inserted
                symbols_done += 1

                print(f"✓ {symbol} — {inserted} rows inserted (skipped {skipped})")

            except Exception as e:
                symbols_failed += 1
                failed[symbol] = f"{type(e).__name__}: {e}"
                print(f"⚠ {symbol} — {type(e).__name__}: {e}")

            time.sleep(0.5)

        line = "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        print(line)
        print("Price History Backfill Complete")
        print(f"Date range    : {start_date} to {end_date}")
        print(f"Symbols done  : {symbols_done}")
        print(f"Symbols failed: {symbols_failed}")
        print(f"Total rows    : {total_rows_inserted:,}")
        print(line)

        if failed:
            print("Failed symbols:")
            for sym in sorted(failed.keys()):
                print(f"  {sym} — {failed[sym]}")

    except Exception as e:
        print(f"Error running backfill_price_history: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    backfill_price_history()

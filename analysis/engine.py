from __future__ import annotations

import json
import os
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from datetime import date, datetime
from functools import partial
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from ta.momentum import RSIIndicator
from ta.trend import MACD


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def get_db_connection():
    root = _project_root()
    load_dotenv(root / ".env")
    database_url = os.environ.get("DATABASE_URL")
    if not database_url or not database_url.strip():
        raise RuntimeError("DATABASE_URL is missing or empty in .env")
    return psycopg2.connect(database_url)


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


def get_price_history(symbol: str, days: int = 90) -> pd.DataFrame:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT date, open, high, low, close, ltp, ycp, volume, trade, value
            FROM price_history
            WHERE symbol = %s
            ORDER BY date DESC
            LIMIT %s
            """,
            (symbol, days),
        )
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
    finally:
        cur.close()
        conn.close()

    if not rows:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "ltp", "ycp", "volume", "trade", "value"])

    df = pd.DataFrame(rows, columns=cols)
    return df.sort_values("date").reset_index(drop=True)


def get_latest_live_tick(symbol: str, as_of_date: date | None = None) -> list[dict]:
    """Return all live tick sessions today for a symbol, sorted by session_no ASC."""
    conn = get_db_connection()
    try:
        tick_date = as_of_date if as_of_date is not None else get_db_date(conn)
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT session_no, ltp, high, low, volume,
                       close, ycp, change_val, trade, value
                FROM live_ticks
                WHERE symbol = %s AND date = %s AND ltp > 0
                ORDER BY session_no ASC
                """,
                (symbol, tick_date),
            )
            rows = cur.fetchall()
        finally:
            cur.close()
    finally:
        conn.close()

    return [
        {
            "session_no": r[0],
            "ltp": float(r[1]) if r[1] is not None else None,
            "high": float(r[2]) if r[2] is not None else None,
            "low": float(r[3]) if r[3] is not None else None,
            "volume": int(r[4]) if r[4] is not None else 0,
            "close": float(r[5]) if r[5] is not None else None,
            "ycp": float(r[6]) if r[6] is not None else None,
            "change_val": float(r[7]) if r[7] is not None else None,
            "trade": int(r[8]) if r[8] is not None else 0,
            "value": float(r[9]) if r[9] is not None else None,
        }
        for r in rows
    ]


def get_pe_ratio(symbol: str) -> float | None:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT pe_ratio
            FROM stock_fundamentals
            WHERE symbol = %s
            ORDER BY fetched_at DESC
            LIMIT 1
            """,
            (symbol,),
        )
        row = cur.fetchone()
    finally:
        cur.close()
        conn.close()

    if not row or row[0] is None:
        return None
    try:
        return float(row[0])
    except Exception:
        return None


def get_eps(symbol: str) -> float | None:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT eps
            FROM stock_fundamentals
            WHERE symbol = %s
            ORDER BY fetched_at DESC
            LIMIT 1
            """,
            (symbol,),
        )
        row = cur.fetchone()
    finally:
        cur.close()
        conn.close()

    if not row or row[0] is None:
        return None
    try:
        return float(row[0])
    except Exception:
        return None


def get_market_summary(dt: str | None = None) -> dict | None:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        if dt is not None:
            target = dt
        else:
            target = get_db_date(conn).isoformat()
        cur.execute(
            """
            SELECT dsex_index, dses_index, total_volume, total_value_mn, total_trade, trade_date
            FROM market_summary
            WHERE trade_date = %s
            LIMIT 1
            """,
            (target,),
        )
        row = cur.fetchone()
    finally:
        cur.close()
        conn.close()

    if not row:
        return None
    return {
        "dsex_index": row[0],
        "dses_index": row[1],
        "total_volume": row[2],
        "total_value_mn": row[3],
        "total_trade": row[4],
        "trade_date": row[5].isoformat() if row[5] is not None else target,
    }


def get_previous_market_summary(dt: str | None = None) -> dict | None:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        if dt is not None:
            target = dt
        else:
            target = get_db_date(conn).isoformat()
        cur.execute(
            """
            SELECT dsex_index, dses_index, total_volume, total_value_mn, total_trade, trade_date
            FROM market_summary
            WHERE trade_date < %s
            ORDER BY trade_date DESC
            LIMIT 1
            """,
            (target,),
        )
        row = cur.fetchone()
    finally:
        cur.close()
        conn.close()

    if not row:
        return None
    return {
        "dsex_index": row[0],
        "dses_index": row[1],
        "total_volume": row[2],
        "total_value_mn": row[3],
        "total_trade": row[4],
        "trade_date": row[5].isoformat() if row[5] is not None else None,
    }


def bulk_fetch_price_history(days: int = 90) -> dict[str, pd.DataFrame]:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # pass days as integer multiplier to interval
        cur.execute(
            """
            SELECT symbol, date, open, high, low, close, ltp, ycp, volume, trade, value
            FROM price_history
            WHERE date >= CURRENT_DATE - (%s * INTERVAL '1 day')
            ORDER BY symbol ASC, date ASC
            """,
            (days,),
        )
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
    finally:
        cur.close()
        conn.close()

    if not rows:
        return {}

    df = pd.DataFrame(rows, columns=cols)
    out: dict[str, pd.DataFrame] = {}
    for symbol, grp in df.groupby("symbol", sort=False):
        out[str(symbol)] = grp.drop(columns=["symbol"]).reset_index(drop=True)
    return out


def bulk_fetch_live_ticks() -> dict[str, list[dict]]:
    """Return all sessions today per symbol, sorted by session_no ASC."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT symbol, session_no, fetched_at,
                   ltp, high, low, close, ycp,
                   change_val, trade, value, volume
            FROM live_ticks
            WHERE date = CURRENT_DATE AND ltp > 0
            ORDER BY symbol ASC, session_no ASC
            """
        )
        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    out: dict[str, list[dict]] = {}
    for r in rows:
        sym = r[0]
        entry = {
            "session_no": r[1],
            "fetched_at": r[2],
            "ltp": float(r[3]) if r[3] is not None else None,
            "high": float(r[4]) if r[4] is not None else None,
            "low": float(r[5]) if r[5] is not None else None,
            "close": float(r[6]) if r[6] is not None else None,
            "ycp": float(r[7]) if r[7] is not None else None,
            "change_val": float(r[8]) if r[8] is not None else None,
            "trade": int(r[9]) if r[9] is not None else 0,
            "value": float(r[10]) if r[10] is not None else None,
            "volume": int(r[11]) if r[11] is not None else 0,
        }
        out.setdefault(sym, []).append(entry)
    return out


def bulk_fetch_pe_ratios() -> dict[str, float | None]:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT DISTINCT ON (symbol) symbol, pe_ratio
            FROM stock_fundamentals
            ORDER BY symbol, fetched_at DESC
            """
        )
        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    out: dict[str, float | None] = {}
    for sym, pe in rows:
        if pe is None:
            out[sym] = None
        else:
            try:
                out[sym] = float(pe)
            except Exception:
                out[sym] = None
    return out


def bulk_fetch_eps() -> dict[str, float | None]:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT DISTINCT ON (symbol) symbol, eps
            FROM stock_fundamentals
            ORDER BY symbol, fetched_at DESC
            """
        )
        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    out: dict[str, float | None] = {}
    for sym, eps in rows:
        if eps is None:
            out[sym] = None
        else:
            try:
                out[sym] = float(eps)
            except Exception:
                out[sym] = None
    return out


def bulk_fetch_fundamentals() -> tuple[dict[str, float | None], dict[str, float | None]]:
    """Fetch latest PE and EPS per symbol in one query."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT DISTINCT ON (symbol) symbol, pe_ratio, eps
            FROM stock_fundamentals
            ORDER BY symbol, fetched_at DESC
            """
        )
        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    pe_map: dict[str, float | None] = {}
    eps_map: dict[str, float | None] = {}
    for sym, pe, eps in rows:
        try:
            pe_map[sym] = float(pe) if pe is not None else None
        except Exception:
            pe_map[sym] = None
        try:
            eps_map[sym] = float(eps) if eps is not None else None
        except Exception:
            eps_map[sym] = None
    return pe_map, eps_map


def bulk_fetch_stock_metadata() -> dict[str, dict]:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT symbol, is_dsex
            FROM stocks_master
            WHERE is_active = TRUE
            """
        )
        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    return {str(r[0]): {"is_dsex": bool(r[1]) if r[1] is not None else False} for r in rows}


def get_is_dsex(symbol: str) -> bool:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT is_dsex
            FROM stocks_master
            WHERE symbol = %s AND is_active = TRUE
            LIMIT 1
            """,
            (symbol,),
        )
        row = cur.fetchone()
    finally:
        cur.close()
        conn.close()

    if not row or row[0] is None:
        return False
    return bool(row[0])


def bulk_fetch_market_summary() -> tuple[dict | None, dict | None]:
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT trade_date, dsex_index, dses_index, total_volume, total_value_mn, total_trade
            FROM market_summary
            ORDER BY trade_date DESC
            LIMIT 2
            """
        )
        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    def to_dict(r):
        return {
            "trade_date": r[0].isoformat() if r[0] is not None else None,
            "dsex_index": r[1],
            "dses_index": r[2],
            "total_volume": r[3],
            "total_value_mn": r[4],
            "total_trade": r[5],
        }

    today_summary = to_dict(rows[0]) if len(rows) >= 1 else None
    yesterday_summary = to_dict(rows[1]) if len(rows) >= 2 else None
    return today_summary, yesterday_summary


def bulk_fetch_market_context(target_date: date) -> dict:
    """Fetch today + yesterday market summary, compute dsex_change_pct."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT trade_date, dsex_index, dses_index, total_volume, total_value_mn, total_trade
            FROM market_summary
            WHERE trade_date <= %s
            ORDER BY trade_date DESC
            LIMIT 2
            """,
            (target_date,),
        )
        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    if not rows:
        return {}

    r = rows[0]
    ctx: dict = {
        "trade_date": r[0].isoformat() if r[0] is not None else None,
        "dsex_index": float(r[1]) if r[1] is not None else None,
        "dses_index": float(r[2]) if r[2] is not None else None,
        "total_volume": int(r[3]) if r[3] is not None else None,
        "total_value_mn": float(r[4]) if r[4] is not None else None,
        "total_trade": int(r[5]) if r[5] is not None else None,
        "dsex_change_pct": None,
    }

    if len(rows) >= 2:
        prev_dsex = float(rows[1][1]) if rows[1][1] is not None else None
        today_dsex = ctx["dsex_index"]
        if prev_dsex and today_dsex and prev_dsex != 0:
            ctx["dsex_change_pct"] = round((today_dsex - prev_dsex) / prev_dsex * 100, 2)

    return ctx


def detect_market_trend(conn) -> dict:
    """Fetch last 5 trading days of market_summary to detect uptrend / downtrend."""
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT trade_date, dsex_index
            FROM market_summary
            WHERE trade_date <= CURRENT_DATE
            ORDER BY trade_date DESC
            LIMIT 5
            """
        )
        rows = cur.fetchall()
    finally:
        cur.close()

    empty = {"trend": "unknown", "consecutive_down_days": 0, "consecutive_up_days": 0, "dsex_5d_change_pct": None}
    if len(rows) < 2:
        return empty

    # Filter out None dsex values
    rows = [(r[0], float(r[1])) for r in rows if r[1] is not None]
    if len(rows) < 2:
        return empty

    consecutive_down = 0
    for i in range(len(rows) - 1):
        if rows[i][1] < rows[i + 1][1]:
            if consecutive_down == 0 or i == consecutive_down:
                consecutive_down += 1
        else:
            break

    consecutive_up = 0
    for i in range(len(rows) - 1):
        if rows[i][1] > rows[i + 1][1]:
            if consecutive_up == 0 or i == consecutive_up:
                consecutive_up += 1
        else:
            break

    latest_dsex = rows[0][1]
    oldest_dsex = rows[-1][1]
    dsex_5d_change = ((latest_dsex - oldest_dsex) / oldest_dsex * 100) if oldest_dsex else 0

    if consecutive_down >= 3:
        trend = "downtrend"
    elif consecutive_up >= 3:
        trend = "uptrend"
    elif dsex_5d_change < -2:
        trend = "weak"
    elif dsex_5d_change > 2:
        trend = "strong"
    else:
        trend = "neutral"

    return {
        "trend": trend,
        "consecutive_down_days": consecutive_down,
        "consecutive_up_days": consecutive_up,
        "dsex_5d_change_pct": round(dsex_5d_change, 2),
    }


def bulk_fetch_market_trend() -> dict:
    """Called once in analyse_all_symbols() and passed to all symbols."""
    conn = get_db_connection()
    try:
        return detect_market_trend(conn)
    finally:
        conn.close()


def _current_price(df: pd.DataFrame) -> float | None:
    if df.empty:
        return None
    last = df.iloc[-1]
    for col in ("ltp", "close", "ycp"):
        val = last.get(col)
        if pd.notna(val):
            try:
                return float(val)
            except Exception:
                pass
    return None


def validate_price_row(row: dict, symbol: str) -> bool:
    """Return True if price row passes sanity checks."""
    close = row.get("close") or row.get("ltp")
    high = row.get("high")
    low = row.get("low")
    ycp = row.get("ycp")
    volume = row.get("volume")
    open_ = row.get("open")

    if not close or float(close) <= 0:
        print(f"VALIDATION FAILED {symbol}: close={close} must be > 0")
        return False
    close = float(close)
    if high is not None and float(high) < close:
        print(f"VALIDATION FAILED {symbol}: high={high} < close={close}")
        return False
    if low is not None and float(low) > close:
        print(f"VALIDATION FAILED {symbol}: low={low} > close={close}")
        return False
    if high is not None and low is not None and float(high) < float(low):
        print(f"VALIDATION FAILED {symbol}: high={high} < low={low}")
        return False
    if volume is not None and float(volume) < 0:
        print(f"VALIDATION FAILED {symbol}: volume={volume} < 0")
        return False
    if open_ is not None and float(open_) <= 0:
        print(f"VALIDATION FAILED {symbol}: open={open_} must be > 0")
        return False
    if ycp is not None and float(ycp) > 0:
        pct_diff = abs(close - float(ycp)) / float(ycp) * 100
        if pct_diff > 50:
            print(f"VALIDATION FAILED {symbol}: close={close} is {pct_diff:.1f}% from ycp={ycp}")
            return False
    return True


def build_intraday_series(live_ticks_today: list[dict] | None) -> pd.DataFrame | None:
    """Build per-session OHLCV DataFrame from live tick list. Returns None if < 2 sessions."""
    if not live_ticks_today or len(live_ticks_today) < 2:
        return None

    rows = []
    prev_ltp = None
    for tick in sorted(live_ticks_today, key=lambda t: t.get("session_no", 0)):
        ltp = tick.get("ltp")
        if ltp is None:
            continue
        open_ = prev_ltp if prev_ltp is not None else ltp
        rows.append({
            "session_no": tick.get("session_no"),
            "open": open_,
            "high": tick.get("high") or ltp,
            "low": tick.get("low") or ltp,
            "close": ltp,
            "volume": tick.get("volume") or 0,
            "value": tick.get("value") or 0,
            "trade": tick.get("trade") or 0,
            "change_val": tick.get("change_val") or 0,
        })
        prev_ltp = ltp

    if len(rows) < 2:
        return None
    return pd.DataFrame(rows).sort_values("session_no").reset_index(drop=True)


def calculate_intraday_momentum(intraday_df: pd.DataFrame | None) -> dict:
    """Compute intraday trend and momentum quality from session series."""
    unavailable = {
        "available": False,
        "trend": "unknown",
        "momentum": "unknown",
        "sessions_counted": 0,
        "price_change_pct": None,
        "volume_acceleration": None,
    }
    if intraday_df is None or len(intraday_df) < 2:
        return unavailable

    try:
        prices = intraday_df["close"].tolist()
        first_price = prices[0]
        last_price = prices[-1]
        if not first_price:
            return unavailable

        price_change_pct = (last_price - first_price) / first_price * 100
        ups = sum(1 for i in range(1, len(prices)) if prices[i] > prices[i - 1])
        downs = sum(1 for i in range(1, len(prices)) if prices[i] < prices[i - 1])

        if ups > downs * 1.5:
            trend = "uptrend"
        elif downs > ups * 1.5:
            trend = "downtrend"
        else:
            trend = "sideways"

        vols = intraday_df["volume"].tolist()
        if len(vols) >= 4:
            half = len(vols) // 2
            early_avg = sum(vols[:half]) / half
            late_avg = sum(vols[half:]) / (len(vols) - half)
            vol_accel = (late_avg - early_avg) / early_avg * 100 if early_avg > 0 else 0.0
        else:
            vol_accel = 0.0

        if trend == "uptrend" and vol_accel > 20:
            momentum = "strong_bullish"
        elif trend == "uptrend" and vol_accel >= 0:
            momentum = "weak_bullish"
        elif trend == "downtrend" and vol_accel > 20:
            momentum = "strong_bearish"
        elif trend == "downtrend":
            momentum = "weak_bearish"
        else:
            momentum = "neutral"

        return {
            "available": True,
            "trend": trend,
            "momentum": momentum,
            "sessions_counted": len(intraday_df),
            "price_change_pct": round(price_change_pct, 2),
            "volume_acceleration": round(vol_accel, 2),
            "session_highs": intraday_df["high"].tolist(),
            "session_lows": intraday_df["low"].tolist(),
            "session_volumes": intraday_df["volume"].tolist(),
        }
    except Exception:
        return unavailable


def calculate_relative_strength(
    symbol_change_pct: float | None,
    market_context: dict | None,
) -> dict:
    """Compare stock intraday change vs DSEX change."""
    unavailable = {"available": False, "rs_signal": "unknown"}
    if symbol_change_pct is None or not market_context:
        return unavailable

    dsex_change = market_context.get("dsex_change_pct")
    if dsex_change is None:
        return unavailable

    rs = symbol_change_pct - dsex_change
    if rs > 2:
        rs_signal = "strong_outperform"
    elif rs > 0.5:
        rs_signal = "outperform"
    elif rs < -2:
        rs_signal = "strong_underperform"
    elif rs < -0.5:
        rs_signal = "underperform"
    else:
        rs_signal = "inline"

    return {
        "available": True,
        "stock_change_pct": round(symbol_change_pct, 2),
        "dsex_change_pct": round(dsex_change, 2),
        "relative_strength": round(rs, 2),
        "rs_signal": rs_signal,
    }


def calculate_support_resistance(df: pd.DataFrame) -> dict:
    try:
        if len(df) < 11:
            return {"status": "skipped", "support": [], "resistance": [], "current_price": _current_price(df)}

        close = df["close"].astype(float).values
        series = pd.Series(close)
        roll_min = series.rolling(window=11, center=True).min().values
        roll_max = series.rolling(window=11, center=True).max().values
        mins: list[float] = [float(close[i]) for i in range(5, len(close) - 5) if close[i] == roll_min[i]]
        maxs: list[float] = [float(close[i]) for i in range(5, len(close) - 5) if close[i] == roll_max[i]]

        def cluster(levels: list[float]) -> list[float]:
            if not levels:
                return []
            levels = sorted(levels)
            groups: list[list[float]] = [[levels[0]]]
            for lv in levels[1:]:
                pivot = float(np.mean(groups[-1]))
                if pivot != 0 and abs(lv - pivot) / abs(pivot) <= 0.015:
                    groups[-1].append(lv)
                else:
                    groups.append([lv])
            return [float(np.mean(g)) for g in groups]

        cp = _current_price(df)

        support_clustered = sorted(cluster(mins), reverse=True)[:2]
        resistance_clustered = sorted(cluster(maxs))[:2]

        if cp is not None:
            # Use a 0.1% tolerance so levels at exactly current price are excluded
            tol = cp * 0.001
            support_clustered = [s for s in support_clustered if s < cp - tol]
            resistance_clustered = [r for r in resistance_clustered if r > cp + tol]

        return {
            "status": "ok",
            "support": [round(x, 2) for x in support_clustered],
            "resistance": [round(x, 2) for x in resistance_clustered],
            "current_price": cp,
        }
    except Exception as e:
        return {"status": "error", "support": [], "resistance": [], "current_price": _current_price(df), "error": str(e)}


def detect_breakout(df: pd.DataFrame, resistance_levels: list) -> dict:
    try:
        cp = _current_price(df)
        if cp is None:
            return {
                "status": "skipped",
                "breakout": False,
                "breakout_level": None,
                "volume_confirmed": False,
                "current_volume": None,
                "avg_volume_20d": None,
            }

        vol_series = df["volume"].astype(float)
        avg_vol = float(vol_series.tail(20).mean()) if len(vol_series) >= 1 else 0.0
        curr_vol = float(vol_series.iloc[-1]) if len(vol_series) >= 1 else 0.0
        vol_confirmed = avg_vol > 0 and curr_vol > 1.5 * avg_vol

        breached = [float(r) for r in resistance_levels if cp > float(r)]
        breakout_level = max(breached) if breached else None

        breakout = breakout_level is not None and vol_confirmed
        return {
            "status": "ok",
            "breakout": breakout,
            "breakout_level": breakout_level,
            "volume_confirmed": vol_confirmed,
            "current_volume": int(curr_vol),
            "avg_volume_20d": int(avg_vol),
        }
    except Exception as e:
        return {
            "status": "error",
            "breakout": False,
            "breakout_level": None,
            "volume_confirmed": False,
            "current_volume": None,
            "avg_volume_20d": None,
            "error": str(e),
        }


def calculate_fibonacci(df: pd.DataFrame) -> dict:
    try:
        if df.empty:
            return {"status": "skipped", "high_90d": None, "low_90d": None, "levels": {}, "current_price": None, "current_zone": None}

        h = float(df["high"].astype(float).max())
        l = float(df["low"].astype(float).min())
        diff = h - l
        cp = _current_price(df)

        levels = {
            "0": h,
            "23.6": h - 0.236 * diff,
            "38.2": h - 0.382 * diff,
            "50": h - 0.500 * diff,
            "61.8": h - 0.618 * diff,
            "78.6": h - 0.786 * diff,
            "100": l,
        }

        zone = None
        if cp is not None:
            ordered = [levels["0"], levels["23.6"], levels["38.2"], levels["50"], levels["61.8"], levels["78.6"], levels["100"]]
            names = ["0-23.6", "23.6-38.2", "38.2-50", "50-61.8", "61.8-78.6", "78.6-100"]
            for i in range(len(ordered) - 1):
                top, bot = ordered[i], ordered[i + 1]
                if bot <= cp <= top:
                    zone = names[i]
                    break

        return {
            "status": "ok",
            "high_90d": round(h, 2),
            "low_90d": round(l, 2),
            "levels": {k: round(v, 2) for k, v in levels.items()},
            "current_price": cp,
            "current_zone": zone,
        }
    except Exception as e:
        return {"status": "error", "high_90d": None, "low_90d": None, "levels": {}, "current_price": _current_price(df), "current_zone": None, "error": str(e)}


def calculate_rsi(df: pd.DataFrame, period: int = 14) -> dict:
    try:
        if len(df) < period + 1:
            return {
                "status": "skipped",
                "rsi": None,
                "signal": "neutral",
                "oversold": False,
                "overbought": False,
                "rsi_direction": "unknown",
                "averaging_zone": None,
            }

        close = df["close"].astype(float)
        rsi_series = RSIIndicator(close=close, window=period).rsi()
        rsi_val = float(rsi_series.iloc[-1])
        oversold = rsi_val < 30
        overbought = rsi_val > 70
        signal = "oversold" if oversold else "overbought" if overbought else "neutral"

        # RSI direction: is momentum improving or worsening?
        rsi_direction = "unknown"
        if len(close) >= period + 4:
            rsi_series_full = RSIIndicator(
                close=close, window=period
            ).rsi().dropna()
            if len(rsi_series_full) >= 3:
                rsi_prev2 = float(rsi_series_full.iloc[-3])
                rsi_prev1 = float(rsi_series_full.iloc[-2])
                rsi_curr = float(rsi_series_full.iloc[-1])

                if rsi_curr > rsi_prev1 > rsi_prev2:
                    rsi_direction = "rising"
                elif rsi_curr < rsi_prev1 < rsi_prev2:
                    rsi_direction = "falling"
                elif abs(rsi_curr - rsi_prev1) < 2:
                    rsi_direction = "stabilizing"
                else:
                    rsi_direction = "mixed"

        if rsi_val < 30:
            averaging_zone = "strong"  # oversold
        elif rsi_val < 40 and rsi_direction in ("stabilizing", "rising"):
            averaging_zone = "acceptable"
        elif rsi_val < 40 and rsi_direction == "falling":
            averaging_zone = "wait"  # still falling
        elif rsi_val >= 50:
            averaging_zone = "avoid"
        else:
            averaging_zone = "neutral"

        return {
            "status": "ok",
            "rsi": round(rsi_val, 2),
            "signal": signal,
            "oversold": oversold,
            "overbought": overbought,
            "rsi_direction": rsi_direction,
            "averaging_zone": averaging_zone,
        }
    except Exception as e:
        return {
            "status": "error",
            "rsi": None,
            "signal": "neutral",
            "oversold": False,
            "overbought": False,
            "rsi_direction": "unknown",
            "averaging_zone": None,
            "error": str(e),
        }


def calculate_macd(df: pd.DataFrame) -> dict:
    try:
        if len(df) < 35:
            return {"status": "skipped", "macd_line": None, "signal_line": None, "histogram": None, "signal": "neutral"}

        close = df["close"].astype(float)
        m = MACD(close=close, window_fast=12, window_slow=26, window_sign=9)
        macd_line = m.macd()
        signal_line = m.macd_signal()
        hist = m.macd_diff()

        curr_macd, prev_macd = float(macd_line.iloc[-1]), float(macd_line.iloc[-2])
        curr_sig, prev_sig = float(signal_line.iloc[-1]), float(signal_line.iloc[-2])

        if prev_macd <= prev_sig and curr_macd > curr_sig:
            sig = "bullish_crossover"
        elif prev_macd >= prev_sig and curr_macd < curr_sig:
            sig = "bearish_crossover"
        else:
            sig = "neutral"

        return {
            "status": "ok",
            "macd_line": round(curr_macd, 4),
            "signal_line": round(curr_sig, 4),
            "histogram": round(float(hist.iloc[-1]), 4),
            "signal": sig,
        }
    except Exception as e:
        return {"status": "error", "macd_line": None, "signal_line": None, "histogram": None, "signal": "neutral", "error": str(e)}


def calculate_volume_profile(df: pd.DataFrame) -> dict:
    try:
        if df.empty:
            return {
                "status": "skipped",
                "current_volume": None,
                "avg_volume_20d": None,
                "pct_vs_avg": None,
                "signal": "normal",
                "volume_price_pattern": "unknown",
                "averaging_signal": "neutral",
            }

        vol = df["volume"].astype(float)
        curr_vol = float(vol.iloc[-1]) if len(vol) else 0.0
        avg_vol20 = float(vol.tail(20).mean()) if len(vol) else 0.0

        # Use BDT value where available and non-zero
        use_value = False
        if "value" in df.columns:
            val_series = df["value"].astype(float)
            curr_val = float(val_series.iloc[-1]) if len(val_series) else 0.0
            avg_val20 = float(val_series.tail(20).mean()) if len(val_series) else 0.0
            if avg_val20 > 0 and curr_val > 0:
                use_value = True

        # Detect volume+price relationship pattern
        # Requires last 3 sessions of data
        volume_price_pattern = "unknown"
        if len(df) >= 3:
            closes = df["close"].astype(float).tail(3).values
            vols = df["volume"].astype(float).tail(3).values

            price_falling = closes[-1] < closes[-2] < closes[-3]
            price_rising = closes[-1] > closes[-2] > closes[-3]
            price_stable = (
                abs(closes[-1] - closes[-2]) / closes[-2] < 0.01
                if closes[-2] != 0
                else False
            )

            vol_falling = vols[-1] < vols[-2] * 0.85
            vol_rising = vols[-1] > vols[-2] * 1.15
            vol_stable = not vol_falling and not vol_rising

            if price_falling and vol_falling:
                volume_price_pattern = "sellers_exhausted"
                # Good for averaging — selling pressure dying
            elif price_stable and vol_rising:
                volume_price_pattern = "accumulation"
                # Strong averaging signal — buyers entering quietly
            elif price_rising and vol_rising:
                volume_price_pattern = "bullish_momentum"
                # Trend continuation — not averaging, this is breakout
            elif price_falling and vol_rising:
                volume_price_pattern = "distribution"
                # DANGER — institutions dumping, do NOT average
            elif price_falling and vol_stable:
                volume_price_pattern = "weak_selloff"
                # Caution — no conviction either way
            elif price_rising and vol_falling:
                volume_price_pattern = "weak_rally"
                # Rally without volume — unsustainable
            else:
                volume_price_pattern = "mixed"

        averaging_signal = (
            "strong"
            if volume_price_pattern == "accumulation"
            else "good"
            if volume_price_pattern == "sellers_exhausted"
            else "avoid"
            if volume_price_pattern == "distribution"
            else "neutral"
        )

        if use_value:
            pct = (curr_val - avg_val20) / avg_val20 * 100 if avg_val20 > 0 else 0.0
            if avg_val20 > 0 and curr_val > 1.5 * avg_val20:
                sig = "high"
            elif avg_val20 > 0 and curr_val < 0.5 * avg_val20:
                sig = "low"
            else:
                sig = "normal"
            return {
                "status": "ok",
                "current_volume": int(curr_vol),
                "avg_volume_20d": int(avg_vol20),
                "current_value_mn": round(curr_val / 1_000_000, 4) if curr_val else None,
                "avg_value_20d_mn": round(avg_val20 / 1_000_000, 4) if avg_val20 else None,
                "pct_vs_avg": round(pct, 2),
                "signal": sig,
                "based_on": "value",
                "volume_price_pattern": volume_price_pattern,
                "averaging_signal": averaging_signal,
            }

        # Fall back to raw volume
        pct = ((curr_vol - avg_vol20) / avg_vol20) * 100 if avg_vol20 > 0 else 0.0
        if avg_vol20 > 0 and curr_vol > 1.5 * avg_vol20:
            sig = "high"
        elif avg_vol20 > 0 and curr_vol < 0.5 * avg_vol20:
            sig = "low"
        else:
            sig = "normal"

        return {
            "status": "ok",
            "current_volume": int(curr_vol),
            "avg_volume_20d": int(avg_vol20),
            "pct_vs_avg": round(pct, 2),
            "signal": sig,
            "based_on": "volume",
            "volume_price_pattern": volume_price_pattern,
            "averaging_signal": averaging_signal,
        }
    except Exception as e:
        return {
            "status": "error",
            "current_volume": None,
            "avg_volume_20d": None,
            "pct_vs_avg": None,
            "signal": "normal",
            "volume_price_pattern": "unknown",
            "averaging_signal": "neutral",
            "error": str(e),
        }


def calculate_moving_averages(df: pd.DataFrame) -> dict:
    try:
        if len(df) < 20:
            return {
                "status": "insufficient_data",
                "ma20": None, "ma50": None, "ma200": None,
                "above_ma20": None, "above_ma50": None,
                "above_ma200": None,
                "price_vs_ma20_pct": None,
                "price_vs_ma50_pct": None,
                "price_vs_ma200_pct": None,
                "trend": "unknown"
            }

        close = df["close"].astype(float)
        current = float(close.iloc[-1])

        ma20 = round(float(close.tail(20).mean()), 2)
        ma50 = round(float(close.tail(50).mean()), 2) if len(df) >= 50 else None
        ma200 = round(float(close.tail(200).mean()), 2) if len(df) >= 200 else None

        above_ma20 = current > ma20 if ma20 else None
        above_ma50 = current > ma50 if ma50 else None
        above_ma200 = current > ma200 if ma200 else None

        pct_vs_ma20 = round((current - ma20) / ma20 * 100, 2) if ma20 else None
        pct_vs_ma50 = round((current - ma50) / ma50 * 100, 2) if ma50 else None
        pct_vs_ma200 = round((current - ma200) / ma200 * 100, 2) if ma200 else None

        # Trend assessment
        if ma50 and ma200:
            if current > ma50 and current > ma200:
                trend = "strong_uptrend"
            elif current > ma50 and current < ma200:
                trend = "recovering"
            elif current < ma50 and current > ma200:
                trend = "pullback_in_uptrend"
            elif current < ma50 and current < ma200:
                trend = "downtrend"
            else:
                trend = "neutral"
        elif ma50:
            if current > ma50:
                trend = "above_ma50"
            else:
                trend = "below_ma50"
        else:
            trend = "insufficient_data"

        return {
            "status": "ok",
            "ma20": ma20,
            "ma50": ma50,
            "ma200": ma200,
            "above_ma20": above_ma20,
            "above_ma50": above_ma50,
            "above_ma200": above_ma200,
            "price_vs_ma20_pct": pct_vs_ma20,
            "price_vs_ma50_pct": pct_vs_ma50,
            "price_vs_ma200_pct": pct_vs_ma200,
            "trend": trend
        }
    except Exception as e:
        return {"status": "error", "error": str(e),
                "ma20": None, "ma50": None, "ma200": None,
                "trend": "unknown"}


def is_mutual_fund(symbol: str) -> bool:
    return symbol.endswith("MF") or symbol.endswith("MF1") or symbol.endswith("MF2") or "FUND" in symbol.upper()


def is_new_listing(df: pd.DataFrame) -> bool:
    return len(df) < 30


def is_suspected_z_category(
    df: pd.DataFrame,
    pe_ratio: float | None,
    eps: float | None,
    is_dsex: bool,
) -> bool:
    if len(df) < 10:
        return False

    avg_volume = float(df["volume"].tail(20).mean())
    trading_days = int((df["volume"] > 0).sum())
    total_days = len(df)
    trading_consistency = trading_days / total_days if total_days else 0.0

    # Stock with extremely thin volume AND price never moves
    # (price_range < 0.5 means completely illiquid/halted)
    price_range = df['close'].max() - df['close'].min()
    no_price_movement = price_range < 0.5  # less than 0.5 BDT range in 90 days
    if no_price_movement and avg_volume < 1000:
        return True

    return all(
        [
            (eps is None or eps < 0),
            (pe_ratio is None or pe_ratio < 0),
            avg_volume < 15000,
            not is_dsex,
            trading_consistency < 0.40,
        ]
    )


def classify_stock(
    symbol: str,
    df: pd.DataFrame,
    pe_ratio: float | None,
    eps: float | None,
    is_dsex: bool,
) -> tuple[str, dict]:
    daily_returns = df["close"].astype(float).pct_change().dropna()
    volatility = float(daily_returns.std() * (252**0.5)) if len(daily_returns) else 0.0
    avg_volume = float(df["volume"].astype(float).tail(20).mean()) if len(df) else 0.0
    trading_days = int((df["volume"] > 0).sum())
    trading_consistency = trading_days / len(df) if len(df) else 0.0

    flags: dict = {
        "mutual_fund": False,
        "new_listing": False,
        "suspected_z_category": False,
        "is_dsex": is_dsex,
    }

    if is_mutual_fund(symbol):
        flags["mutual_fund"] = True
        return ("INVESTMENT", flags)

    if is_new_listing(df):
        flags["new_listing"] = True
        return ("TRADING", flags)

    if is_suspected_z_category(df, pe_ratio, eps, is_dsex):
        flags["suspected_z_category"] = True
        return ("GAMBLING", flags)

    is_investment = all(
        [
            pe_ratio is not None,
            pe_ratio is not None and 4 <= pe_ratio <= 30,
            eps is not None and eps > 0,
            volatility < 0.45,
            avg_volume > 30000,
            trading_consistency > 0.70,
            is_dsex,
        ]
    )
    if is_investment:
        return ("INVESTMENT", flags)

    is_gambling = any(
        [
            volatility > 0.80,
            avg_volume < 15000,
            trading_consistency < 0.40,
            all([(eps is None or eps < 0), (pe_ratio is None or pe_ratio < 0), not is_dsex]),
        ]
    )
    if is_gambling:
        return ("GAMBLING", flags)

    return ("TRADING", flags)


def determine_overall_signal(
    sr: dict,
    breakout: dict,
    fib: dict,
    rsi: dict,
    macd: dict,
    volume: dict,
    stock_class: str,
    market_summary: dict | None = None,
    previous_market_summary: dict | None = None,
    class_flags: dict | None = None,
    intraday: dict | None = None,
    rs: dict | None = None,
    market_context: dict | None = None,
    market_trend: dict | None = None,
    history_len: int = 0,
) -> tuple[str, float, int, dict]:
    score = 50

    cp = sr.get("current_price") or fib.get("current_price")
    supports = sr.get("support", []) or []
    resistances = sr.get("resistance", []) or []

    score_breakdown = {
        "base": 50,
        "breakout": 0,
        "rsi_oversold": 0,
        "macd_bullish": 0,
        "volume_high": 0,
        "near_support": 0,
        "dsex_bonus": 0,
        "gambling_penalty": 0,
        "rsi_overbought": 0,
        "macd_bearish": 0,
        "near_resistance": 0,
        "market_bonus": 0,
        "total": 0,
    }
    confirming_signals = 0

    if breakout.get("breakout"):
        score += 15
        score_breakdown["breakout"] += 15
        confirming_signals += 1
    if rsi.get("oversold"):
        score += 10
        score_breakdown["rsi_oversold"] += 10
        confirming_signals += 1
    if macd.get("signal") == "bullish_crossover":
        score += 10
        score_breakdown["macd_bullish"] += 10
        confirming_signals += 1
    if volume.get("signal") == "high":
        score += 10
        score_breakdown["volume_high"] += 10
        confirming_signals += 1

    near_support = False
    if cp is not None and supports:
        nearest_support = min(supports, key=lambda x: abs(float(cp) - float(x)))
        if nearest_support and abs(float(cp) - float(nearest_support)) / float(nearest_support) <= 0.03:
            score += 5
            score_breakdown["near_support"] += 5
            near_support = True
    if near_support:
        confirming_signals += 1

    if rsi.get("overbought"):
        score -= 15
        score_breakdown["rsi_overbought"] -= 15
    if macd.get("signal") == "bearish_crossover":
        score -= 10
        score_breakdown["macd_bearish"] -= 10

    if cp is not None and resistances:
        nearest_res = min(resistances, key=lambda x: abs(float(cp) - float(x)))
        if nearest_res and abs(float(cp) - float(nearest_res)) / float(nearest_res) <= 0.01:
            score -= 10
            score_breakdown["near_resistance"] -= 10

    if stock_class == "GAMBLING":
        score -= 20
        score_breakdown["gambling_penalty"] -= 20

    # Intraday momentum scoring
    intraday = intraday or {}
    if intraday.get("available"):
        mom = intraday.get("momentum", "")
        if mom == "strong_bullish":
            score += 12
            score_breakdown["market_bonus"] += 12
        elif mom == "weak_bullish":
            score += 6
            score_breakdown["market_bonus"] += 6
        elif mom == "strong_bearish":
            score -= 12
            score_breakdown["market_bonus"] -= 12
        elif mom == "weak_bearish":
            score -= 6
            score_breakdown["market_bonus"] -= 6

    # Relative strength scoring
    rs = rs or {}
    if rs.get("available"):
        rs_sig = rs.get("rs_signal", "")
        if rs_sig == "strong_outperform":
            score += 10
            score_breakdown["market_bonus"] += 10
        elif rs_sig == "outperform":
            score += 5
            score_breakdown["market_bonus"] += 5
        elif rs_sig == "strong_underperform":
            score -= 10
            score_breakdown["market_bonus"] -= 10
        elif rs_sig == "underperform":
            score -= 5
            score_breakdown["market_bonus"] -= 5

    # DSEX daily direction (from market_context if available, else fall back to summary pair)
    dsex_chg = None
    if market_context and market_context.get("dsex_change_pct") is not None:
        dsex_chg = market_context["dsex_change_pct"]
    elif market_summary and previous_market_summary:
        try:
            today_dsex = float(market_summary.get("dsex_index") or 0)
            prev_dsex = float(previous_market_summary.get("dsex_index") or 0)
            if prev_dsex:
                dsex_chg = (today_dsex - prev_dsex) / prev_dsex * 100.0
        except Exception:
            pass

    dsex_chg = dsex_chg or 0
    if dsex_chg > 2:
        score += 8    # strong up day
        score_breakdown["dsex_bonus"] += 8
    elif dsex_chg > 1:
        score += 4    # mild up day
        score_breakdown["dsex_bonus"] += 4
    elif dsex_chg < -2:
        score -= 15   # strong down day — very cautious
        score_breakdown["dsex_bonus"] -= 15
    elif dsex_chg < -1:
        score -= 10   # mild down day — cautious
        score_breakdown["dsex_bonus"] -= 10

    # Multi-day market trend scoring
    mt = market_trend or {}
    trend = mt.get("trend", "unknown")
    consecutive_down = mt.get("consecutive_down_days", 0)
    dsex_5d = mt.get("dsex_5d_change_pct", 0) or 0

    if trend == "downtrend":
        score -= 20   # 3+ consecutive down days
        score_breakdown["market_bonus"] -= 20
        score = min(score, 64)   # never generate BUY in downtrend
    elif trend == "weak":
        score -= 12   # 5-day decline > 2%
        score_breakdown["market_bonus"] -= 12
    elif trend == "uptrend":
        score += 10   # 3+ consecutive up days
        score_breakdown["market_bonus"] += 10
    elif trend == "strong":
        score += 6    # 5-day gain > 2%
        score_breakdown["market_bonus"] += 6

    if consecutive_down == 2:
        score -= 8
        score_breakdown["market_bonus"] -= 8
    elif consecutive_down >= 3:
        score -= 20   # already captured in downtrend but stack it
        score_breakdown["market_bonus"] -= 20

    flags = class_flags or {}
    if flags.get("mutual_fund"):
        score = min(score, 64)
    if flags.get("new_listing"):
        score = min(score, 64)
    if flags.get("suspected_z_category"):
        score = min(score, 35)
    if flags.get("is_dsex"):
        score += 5
        score_breakdown["dsex_bonus"] += 5

    score = max(0, min(100, score))

    # Hard BUY blockers (override score only for BUY assignment)
    rsi_value = rsi.get("rsi")
    try:
        rsi_float = float(rsi_value) if rsi_value is not None else None
    except (TypeError, ValueError):
        rsi_float = None

    hard_buy_blocked = False
    if stock_class == "GAMBLING":
        hard_buy_blocked = True
    if flags.get("new_listing") or flags.get("suspected_z_category") or flags.get("mutual_fund"):
        hard_buy_blocked = True
    if history_len < 60:
        hard_buy_blocked = True
    if rsi_float is not None and rsi_float > 70:
        hard_buy_blocked = True
    if cp is not None and resistances:
        try:
            r0 = float(resistances[0])
            if r0 > 0 and float(cp) > r0 * 1.05:
                hard_buy_blocked = True
        except (TypeError, ValueError):
            pass

    # Stricter thresholds + confirming-signal requirement for BUY
    if score >= 80 and confirming_signals >= 2 and not hard_buy_blocked:
        signal = "BUY"
    elif score >= 60:
        signal = "WATCH"
    elif score >= 35:
        signal = "HOLD"
    else:
        signal = "EXIT"

    score_breakdown["total"] = int(round(score))
    return signal, round(score / 100.0, 2), int(confirming_signals), score_breakdown


def generate_trade_setup(
    symbol: str,
    df: pd.DataFrame,
    sr: dict,
    breakout: dict,
    fib: dict,
    rsi: dict,
    macd: dict,
    volume: dict,
    intraday: dict,
    rs: dict,
    stock_class: str,
    class_flags: dict,
    market_context: dict,
    current_price: float,
    session_no: int | None,
) -> dict:
    """
    Generates a precise trade setup with entry, targets, stop loss and risk/reward.
    Only returns a valid setup if conditions are met.
    """
    _ = symbol  # reserved for logging / future use
    class_flags = class_flags or {}
    intraday = intraday or {}
    rs = rs or {}
    market_context = market_context or {}

    rsi_val = rsi.get("rsi")
    try:
        rsi_f = float(rsi_val) if rsi_val is not None else None
    except (TypeError, ValueError):
        rsi_f = None

    setup_type: str | None = None

    # Step 1 — Detect setup type
    if (
        bool(breakout.get("breakout"))
        and bool(breakout.get("volume_confirmed"))
        and rsi_f is not None
        and 40 <= rsi_f <= 75
        and stock_class != "GAMBLING"
        and not class_flags.get("suspected_z_category")
        and current_price > 0
    ):
        setup_type = "BREAKOUT"

    supports = sr.get("support") or []
    lowest_support = min(supports) if supports else None
    if setup_type is None and supports and lowest_support and rsi_f is not None and stock_class != "GAMBLING":
        try:
            ls = float(lowest_support)
            if ls > 0 and current_price >= ls and (current_price - ls) / ls <= 0.02:
                if rsi_f < 42:
                    tail5 = df["close"].astype(float).tail(5)
                    if len(tail5) >= 2 and not tail5.is_monotonic_decreasing:
                        setup_type = "SUPPORT_BOUNCE"
        except (TypeError, ValueError):
            pass

    if setup_type is None and rsi_f is not None:
        if (
            rsi_f < 32
            and macd.get("signal") == "bullish_crossover"
            and stock_class in ("TRADING", "INVESTMENT")
            and volume.get("signal") in ("normal", "high")
        ):
            setup_type = "OVERSOLD_REVERSAL"

    if setup_type is None and rsi_f is not None:
        closes = df["close"].astype(float)
        if len(closes) >= 6:
            last3 = closes.tail(3).values
            prev3 = closes.iloc[-6:-3].values
            step_up = all(last3[i] > prev3[i] for i in range(3))
            if (
                step_up
                and 50 <= rsi_f <= 68
                and volume.get("signal") == "high"
                and intraday.get("momentum") in ("strong_bullish", "weak_bullish")
                and rs.get("rs_signal") in ("outperform", "strong_outperform")
            ):
                setup_type = "MOMENTUM_CONTINUATION"

    if setup_type is None:
        return {"has_setup": False}

    resistance_levels = list(sr.get("resistance") or [])
    fib_levels = fib.get("levels") or {}
    if not isinstance(fib_levels, dict):
        fib_levels = {}

    def _fib_float(key: str) -> float | None:
        v = fib_levels.get(key)
        try:
            return float(v) if v is not None else None
        except (TypeError, ValueError):
            return None

    entry: float
    entry_zone: list[float]
    support: float | None = None

    if setup_type == "BREAKOUT":
        entry = float(current_price)
        entry_zone = [current_price * 0.99, current_price * 1.005]

    elif setup_type == "SUPPORT_BOUNCE":
        support_list = sr.get("support") or []
        support = float(support_list[0]) if support_list else None
        if not support:
            return {"has_setup": False}
        entry = support * 1.005
        entry_zone = [support * 0.995, support * 1.01]

    elif setup_type == "OVERSOLD_REVERSAL":
        entry = float(current_price)
        entry_zone = [current_price * 0.995, current_price * 1.01]

    else:  # MOMENTUM_CONTINUATION
        entry = float(current_price) * 0.99
        entry_zone = [current_price * 0.985, current_price * 0.995]

    target_1: float
    if resistance_levels:
        target_1 = float(resistance_levels[0])
    else:
        target_1 = entry * 1.07

    target_2: float
    if len(resistance_levels) >= 2:
        target_2 = float(resistance_levels[1])
    else:
        target_2 = entry * 1.15

    # Fib refinements: only override if level lies strictly between entry and cap resistance
    cap_r = float(resistance_levels[0]) if resistance_levels else None
    near_tol = 0.015
    cp = float(current_price)
    l618 = _fib_float("61.8")
    l382 = _fib_float("38.2")
    l50 = _fib_float("50")
    l236 = _fib_float("23.6")
    fib_candidate: float | None = None
    if l618 and cp > 0 and abs(cp - l618) / l618 <= near_tol and l382:
        fib_candidate = l382
    elif l50 and cp > 0 and abs(cp - l50) / l50 <= near_tol and l236:
        fib_candidate = l236
    if fib_candidate and fib_candidate > entry:
        if cap_r is None:
            target_1 = fib_candidate
        elif entry < fib_candidate < cap_r:
            target_1 = fib_candidate

    # Step 5 — Stop loss
    if setup_type == "BREAKOUT":
        stop_loss = entry * 0.92
    elif setup_type == "SUPPORT_BOUNCE" and support:
        stop_loss = support * 0.97
    elif setup_type == "OVERSOLD_REVERSAL":
        recent_low = float(df["low"].astype(float).tail(5).min())
        stop_loss = min(recent_low * 0.98, entry * 0.92)
    else:
        stop_loss = entry * 0.94

    risk = entry - stop_loss
    reward_1 = target_1 - entry
    reward_2 = target_2 - entry
    rr_1 = reward_1 / risk if risk > 0 else 0.0
    rr_2 = reward_2 / risk if risk > 0 else 0.0

    if rr_1 < 1.5:
        return {"has_setup": False}

    if setup_type == "BREAKOUT":
        urgency = "NOW"
    elif setup_type == "SUPPORT_BOUNCE":
        urgency = "NOW" if current_price <= entry_zone[1] else "WATCH"
    elif setup_type == "OVERSOLD_REVERSAL":
        urgency = "WATCH"
    else:
        urgency = "FORMING"

    reasons: list[str] = []
    if setup_type == "BREAKOUT":
        bl = breakout.get("breakout_level")
        if bl is not None:
            try:
                reasons.append(f"Broke {float(bl):.1f} resistance")
            except (TypeError, ValueError):
                reasons.append("Broke resistance")
        avg_v = breakout.get("avg_volume_20d") or 0
        cur_v = breakout.get("current_volume") or 0
        try:
            if float(avg_v) > 0:
                reasons.append(f"Volume {float(cur_v) / float(avg_v):.1f}x average")
        except (TypeError, ValueError):
            pass
    if rsi_f is not None and rsi_f < 40:
        reasons.append(f"RSI {rsi_f:.0f} — not overbought, room to run")
    if rs.get("available") and rs.get("rs_signal") in ("outperform", "strong_outperform"):
        rs_val = rs.get("relative_strength") or 0.0
        try:
            reasons.append(f"Outperforming DSEX by {float(rs_val):.1f}%")
        except (TypeError, ValueError):
            reasons.append("Outperforming DSEX")
    if macd.get("signal") == "bullish_crossover":
        reasons.append("MACD bullish crossover")
    if intraday.get("momentum") == "strong_bullish":
        reasons.append("Strong intraday uptrend")
    dsex_chg = market_context.get("dsex_change_pct")
    try:
        if dsex_chg is not None and float(dsex_chg) > 0.5:
            reasons.append(f"Market supportive (DSEX +{float(dsex_chg):.1f}%)")
    except (TypeError, ValueError):
        pass

    reasons = reasons[:3]

    return {
        "has_setup": True,
        "setup_type": setup_type,
        "urgency": urgency,
        "entry": round(entry, 2),
        "entry_zone": [round(entry_zone[0], 2), round(entry_zone[1], 2)],
        "target_1": round(target_1, 2),
        "target_2": round(target_2, 2),
        "stop_loss": round(stop_loss, 2),
        "rr_1": round(rr_1, 2),
        "rr_2": round(rr_2, 2),
        "pct_to_target_1": round((target_1 - entry) / entry * 100, 1) if entry else 0.0,
        "pct_to_target_2": round((target_2 - entry) / entry * 100, 1) if entry else 0.0,
        "pct_to_stop": round((stop_loss - entry) / entry * 100, 1) if entry else 0.0,
        "reasons": reasons,
        "setup_detected_session": session_no,
        "current_price": round(float(current_price), 2),
    }


def build_signal_reason(
    breakout: dict,
    rsi: dict,
    macd: dict,
    volume: dict,
    sr: dict,
    current_price: float | None,
    class_flags: dict | None = None,
    intraday: dict | None = None,
    rs: dict | None = None,
) -> str:
    triggers: list[tuple[int, str]] = []

    # Priority 1: strong intraday momentum + outperform
    intraday = intraday or {}
    rs = rs or {}
    if intraday.get("available") and rs.get("available"):
        mom = intraday.get("momentum", "")
        rs_sig = rs.get("rs_signal", "")
        if mom == "strong_bullish" and rs_sig in ("outperform", "strong_outperform"):
            triggers.append((20, "Strong intraday momentum + outperforming DSEX"))
        elif mom == "strong_bearish" and rs_sig in ("underperform", "strong_underperform"):
            triggers.append((20, "Strong intraday selling + underperforming DSEX"))

    # Priority 2: breakout + volume
    if breakout.get("breakout"):
        lvl = breakout.get("breakout_level")
        avg = breakout.get("avg_volume_20d") or 0
        cur = breakout.get("current_volume") or 0
        mult = (float(cur) / float(avg)) if avg else 0.0
        triggers.append((15, f"Breakout above {lvl} + volume {mult:.1f}x avg"))

    # Priority 3: RSI
    rsi_val = rsi.get("rsi")
    if rsi.get("oversold") and rsi_val is not None:
        triggers.append((10, f"RSI oversold ({rsi_val})"))
    elif rsi.get("overbought") and rsi_val is not None:
        triggers.append((10, f"RSI overbought ({rsi_val})"))

    # Priority 3: MACD
    if macd.get("signal") == "bullish_crossover":
        triggers.append((10, "MACD bullish crossover"))
    elif macd.get("signal") == "bearish_crossover":
        triggers.append((10, "MACD bearish crossover"))

    # Priority 4: relative strength alone
    if rs.get("available"):
        rs_sig = rs.get("rs_signal", "")
        rs_val = rs.get("relative_strength", 0)
        if rs_sig == "strong_outperform":
            triggers.append((9, f"Outperforming DSEX by {rs_val:.1f}%"))
        elif rs_sig == "strong_underperform":
            triggers.append((9, f"Underperforming DSEX by {abs(rs_val):.1f}%"))

    if volume.get("signal") == "high":
        triggers.append((8, "High volume"))
    elif volume.get("signal") == "low":
        triggers.append((6, "Low volume"))

    if current_price is not None and sr.get("resistance"):
        nr = min(sr["resistance"], key=lambda x: abs(float(current_price) - float(x)))
        if nr and abs(float(current_price) - float(nr)) / float(nr) <= 0.01:
            triggers.append((9, f"Near resistance {nr}"))

    if current_price is not None and sr.get("support"):
        ns = min(sr["support"], key=lambda x: abs(float(current_price) - float(x)))
        if ns and abs(float(current_price) - float(ns)) / float(ns) <= 0.03:
            triggers.append((7, f"Near support {ns}"))

    if not triggers:
        base = "Technical setup mixed"
    else:
        triggers.sort(key=lambda x: x[0], reverse=True)
        top = [t[1] for t in triggers[:2]]
        base = (" + ".join(top))[:100]

    flags = class_flags or {}
    extras: list[str] = []
    if flags.get("mutual_fund"):
        extras.append("(MF)")
    if flags.get("new_listing"):
        extras.append("(new listing)")
    if flags.get("suspected_z_category"):
        extras.append("(Z-cat suspected)")
    if extras:
        return (base + " " + " ".join(extras))[:130]
    return base


def analyse_symbol(
    symbol: str,
    price_df: pd.DataFrame = None,
    live_tick: dict = None,            # kept for back-compat; ignored if live_ticks_today provided
    pe_ratio: float = None,
    eps: float = None,
    is_dsex: bool = False,
    today_market: dict = None,
    prev_market: dict = None,
    target_date: date | None = None,
    live_ticks_today: list[dict] | None = None,  # full session list (CHANGE 2/3)
    market_context: dict | None = None,           # pre-computed context with dsex_change_pct
    market_trend: dict | None = None,             # multi-day trend from bulk_fetch_market_trend
    prev_signals: dict[str, str] | None = None,
) -> dict:
    try:
        df = price_df.copy() if price_df is not None else get_price_history(symbol, days=90)

        if len(df) < 20:
            return {"status": "skipped", "symbol": symbol, "reason": "insufficient data"}

        # Filter out rows where both ltp and volume are zero
        # These are non-trading days that corrupt indicators
        df = df[(df['ltp'] > 0) | (df['volume'] > 0)].copy()
        df = df.reset_index(drop=True)

        # Re-check minimum rows after filtering
        if len(df) < 20:
            return {"status": "skipped", "symbol": symbol,
                    "reason": "insufficient clean data after filtering"}

        if target_date is None:
            _conn_td = get_db_connection()
            try:
                target_date = get_db_date(_conn_td)
            finally:
                _conn_td.close()

        market_summary = today_market if today_market is not None else get_market_summary()
        previous_market_summary = prev_market if prev_market is not None else get_previous_market_summary()

        # Resolve live tick list — prefer explicit list, fall back to single-tick compat
        if live_ticks_today is None:
            if live_tick is not None:
                live_ticks_today = [live_tick] if isinstance(live_tick, dict) else []
            else:
                live_ticks_today = get_latest_live_tick(symbol, as_of_date=target_date)

        session_no = None

        if live_ticks_today:
            latest = live_ticks_today[-1]
            session_no = latest.get("session_no")

            # Build intraday-enhanced synthetic row
            valid_highs = [t["high"] for t in live_ticks_today if t.get("high")]
            valid_lows = [t["low"] for t in live_ticks_today if t.get("low")]
            today_high = max(valid_highs) if valid_highs else latest.get("ltp")
            today_low = min(valid_lows) if valid_lows else latest.get("ltp")
            today_ltp = latest.get("ltp")
            today_vol = latest.get("volume") or 0
            today_val = latest.get("value")
            today_trade = latest.get("trade") or 0
            today_ycp = latest.get("ycp")
            first_ltp = live_ticks_today[0].get("ltp") or today_ltp

            if today_ltp:
                synthetic_row: dict = {
                    "date": target_date,
                    "open": first_ltp,
                    "high": today_high,
                    "low": today_low,
                    "close": today_ltp,
                    "ltp": today_ltp,
                    "ycp": today_ycp if today_ycp else (float(df["close"].iloc[-1]) if not df.empty else None),
                    "volume": today_vol,
                    "value": today_val,
                    "trade": today_trade,
                }
                if validate_price_row(synthetic_row, symbol):
                    df = df[df["date"] != target_date]
                    synthetic_df = pd.DataFrame([synthetic_row])
                    df = pd.concat([df, synthetic_df], ignore_index=True)
                    df = df.sort_values("date").reset_index(drop=True)

            intraday_df = build_intraday_series(live_ticks_today)
        else:
            intraday_df = None

        pe = pe_ratio if pe_ratio is not None else get_pe_ratio(symbol)
        eps_val = eps if eps is not None else get_eps(symbol)

        # In bulk mode is_dsex is pre-fetched; single-symbol mode can fetch on demand.
        if price_df is None and is_dsex is False:
            is_dsex_b = get_is_dsex(symbol)
        else:
            is_dsex_b = bool(is_dsex)

        sr = calculate_support_resistance(df)
        breakout = detect_breakout(df, sr.get("resistance", []))
        fib = calculate_fibonacci(df)
        rsi = calculate_rsi(df)
        macd = calculate_macd(df)
        volume = calculate_volume_profile(df)
        moving_averages = calculate_moving_averages(df)

        # New signals
        intraday = calculate_intraday_momentum(intraday_df)
        symbol_change_pct = None
        if live_ticks_today:
            latest = live_ticks_today[-1]
            chg = latest.get("change_val")
            ycp = latest.get("ycp")
            if chg is not None and ycp and float(ycp) != 0:
                symbol_change_pct = float(chg) / float(ycp) * 100
            elif intraday.get("price_change_pct") is not None:
                symbol_change_pct = intraday["price_change_pct"]

        rs = calculate_relative_strength(symbol_change_pct, market_context)

        stock_class, class_flags = classify_stock(symbol, df, pe, eps_val, is_dsex_b)
        signal, confidence, confirming_signals, score_breakdown = determine_overall_signal(
            sr,
            breakout,
            fib,
            rsi,
            macd,
            volume,
            stock_class,
            market_summary,
            previous_market_summary,
            class_flags,
            intraday=intraday,
            rs=rs,
            market_context=market_context,
            market_trend=market_trend,
            history_len=len(df),
        )

        # Stability filter for intraday session-to-session transitions.
        prev_map = prev_signals or {}
        if session_no is not None and session_no >= 2 and prev_map:
            prev = prev_map.get(symbol)
            if prev == 'BUY' and signal == 'EXIT':
                signal = 'WATCH'
                confidence = min(float(confidence or 0.0), 0.49)
            if prev == 'EXIT' and signal == 'BUY':
                signal = 'WATCH'
                confidence = min(float(confidence or 0.0), 0.64)
            if signal == 'BUY' and prev not in ('BUY', 'WATCH'):
                signal = 'WATCH'
                confidence = min(float(confidence or 0.0), 0.64)

        analysis_date = target_date.isoformat()
        current_price = _current_price(df)
        mc_for_setup = market_context if market_context is not None else {
            "today": market_summary,
            "previous": previous_market_summary,
        }
        cp_setup = float(current_price) if current_price is not None else 0.0
        trade_setup = generate_trade_setup(
            symbol,
            df,
            sr,
            breakout,
            fib,
            rsi,
            macd,
            volume,
            intraday,
            rs,
            stock_class,
            class_flags,
            mc_for_setup,
            cp_setup,
            session_no,
        )
        reason = build_signal_reason(
            breakout, rsi, macd, volume, sr, current_price, class_flags,
            intraday=intraday, rs=rs,
        )

        payload = {
            "symbol": symbol,
            "analysis_date": analysis_date,
            "session_no": session_no,
            "support_levels": sr.get("support", []),
            "resistance_levels": sr.get("resistance", []),
            "breakout_signal": "BREAKOUT" if breakout.get("breakout") else "NONE",
            "fib_levels": fib.get("levels", {}),
            "rsi": rsi.get("rsi"),
            "macd": {
                "macd_line": macd.get("macd_line"),
                "signal_line": macd.get("signal_line"),
                "histogram": macd.get("histogram"),
                "signal": macd.get("signal"),
            },
            "volume_signal": volume.get("signal"),
            "stock_class": stock_class,
            "ma20": moving_averages.get("ma20"),
            "ma50": moving_averages.get("ma50"),
            "ma200": moving_averages.get("ma200"),
            "above_ma50": moving_averages.get("above_ma50"),
            "above_ma200": moving_averages.get("above_ma200"),
            "ma_trend": moving_averages.get("trend"),
            "overall_signal": signal,
            "confidence_score": confidence,
            "raw_output": {
                "current_price": float(df["close"].iloc[-1])
                if len(df) > 0
                else None,
                "current_ltp": float(df["ltp"].iloc[-1])
                if "ltp" in df.columns and len(df) > 0
                else None,
                "sr": sr,
                "breakout": breakout,
                "fib": fib,
                "rsi": rsi,
                "rsi_direction": rsi.get("rsi_direction"),
                "averaging_zone": rsi.get("averaging_zone"),
                "macd": macd,
                "volume": volume,
                "pe_ratio": pe,
                "eps": eps_val,
                "class_flags": class_flags,
                "market_context": market_context or {"today": market_summary, "previous": previous_market_summary},
                "intraday_momentum": intraday,
                "relative_strength": rs,
                "volume_profile": volume,
                "volume_price_pattern": volume.get("volume_price_pattern"),
                "averaging_signal": volume.get("averaging_signal"),
                "moving_averages": moving_averages,
                "signal_reason": reason,
                "market_trend": market_trend or {},
                "trade_setup": trade_setup,
                "confirming_signals": confirming_signals,
                "score_breakdown": score_breakdown,
            },
            "signal_reason": reason,
            "price_at_signal": current_price,
        }

        return {"status": "ok", **payload}
    except Exception as e:
        return {"status": "error", "symbol": symbol, "reason": str(e)}


def analyse_symbol_with_data(
    symbol: str,
    price_history_map: dict[str, pd.DataFrame],
    live_ticks_map: dict[str, list[dict]],
    pe_ratios_map: dict[str, float | None],
    eps_map: dict[str, float | None],
    metadata_map: dict[str, dict],
    today_market: dict | None,
    prev_market: dict | None,
    target_date: date,
    market_context: dict | None = None,
    market_trend: dict | None = None,
    prev_signals: dict[str, str] | None = None,
) -> dict:
    live_ticks_today = live_ticks_map.get(symbol, [])
    return analyse_symbol(
        symbol,
        price_df=price_history_map.get(symbol),
        pe_ratio=pe_ratios_map.get(symbol),
        eps=eps_map.get(symbol),
        is_dsex=bool(metadata_map.get(symbol, {}).get("is_dsex", False)),
        today_market=today_market,
        prev_market=prev_market,
        target_date=target_date,
        live_ticks_today=live_ticks_today,
        market_context=market_context,
        market_trend=market_trend,
        prev_signals=prev_signals,
    )


def batch_upsert_analysis_results(results: list[dict]) -> None:
    if not results:
        return
    conn = get_db_connection()
    conn.autocommit = True
    cur = conn.cursor()
    try:
        sql = """
            INSERT INTO analysis_results (
                symbol, analysis_date, session_no,
                support_levels, resistance_levels,
                breakout_signal, fib_levels, rsi,
                macd, volume_signal, stock_class,
                overall_signal, confidence_score, raw_output
            ) VALUES (%s, %s, %s, %s::jsonb, %s::jsonb, %s, %s::jsonb, %s, %s::jsonb, %s, %s, %s, %s, %s::jsonb)
            ON CONFLICT (symbol, analysis_date, session_no)
            DO UPDATE SET
                support_levels = EXCLUDED.support_levels,
                resistance_levels = EXCLUDED.resistance_levels,
                breakout_signal = EXCLUDED.breakout_signal,
                fib_levels = EXCLUDED.fib_levels,
                rsi = EXCLUDED.rsi,
                macd = EXCLUDED.macd,
                volume_signal = EXCLUDED.volume_signal,
                stock_class = EXCLUDED.stock_class,
                overall_signal = EXCLUDED.overall_signal,
                confidence_score = EXCLUDED.confidence_score,
                raw_output = EXCLUDED.raw_output;
        """
        params = [
            (
                r["symbol"],
                r["analysis_date"],
                r.get("session_no"),
                json.dumps(r.get("support_levels", [])),
                json.dumps(r.get("resistance_levels", [])),
                r.get("breakout_signal"),
                json.dumps(r.get("fib_levels", {})),
                r.get("rsi"),
                json.dumps(r.get("macd", {})),
                r.get("volume_signal"),
                r.get("stock_class"),
                r.get("overall_signal"),
                r.get("confidence_score"),
                json.dumps(r.get("raw_output", {})),
            )
            for r in results
        ]
        psycopg2.extras.execute_batch(cur, sql, params, page_size=100)
    finally:
        cur.close()
        conn.close()


# Cached schema flags — detected once on first call, reused thereafter.
_signals_has_trader_id: bool | None = None
_traders_has_is_active: bool | None = None


def _detect_signals_schema(cur) -> tuple[bool, bool]:
    global _signals_has_trader_id, _traders_has_is_active
    if _signals_has_trader_id is None:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='public' AND table_name='signals'"
        )
        signal_cols = {r[0] for r in cur.fetchall()}
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='public' AND table_name='traders'"
        )
        trader_cols = {r[0] for r in cur.fetchall()}
        _signals_has_trader_id = "trader_id" in signal_cols
        _traders_has_is_active = "is_active" in trader_cols
    return _signals_has_trader_id, _traders_has_is_active


def batch_upsert_signals(signals: list[dict]) -> None:
    if not signals:
        return

    conn = get_db_connection()
    conn.autocommit = True
    cur = conn.cursor()
    try:
        has_trader_id, has_is_active = _detect_signals_schema(cur)

        params = [
            (
                s["symbol"],
                s["signal_type"],
                s["signal_date"],
                s.get("price_at_signal"),
                s.get("reason"),
            )
            for s in signals
        ]

        if has_trader_id:
            trader_filter = "WHERE t.is_active = TRUE" if has_is_active else ""
            sql = f"""
                INSERT INTO signals (
                    trader_id, symbol, signal_type, signal_date,
                    price_at_signal, reason, is_active
                )
                SELECT t.id, %s, %s, %s, %s, %s, TRUE
                FROM traders t
                {trader_filter}
                ON CONFLICT (trader_id, symbol, signal_date, signal_type) DO UPDATE SET
                    price_at_signal = EXCLUDED.price_at_signal,
                    reason = EXCLUDED.reason,
                    is_active = EXCLUDED.is_active
            """
        else:
            sql = """
                INSERT INTO signals (
                    symbol, signal_type, signal_date,
                    price_at_signal, reason, is_active
                )
                VALUES (%s, %s, %s, %s, %s, TRUE)
                ON CONFLICT (symbol, signal_date, signal_type) DO UPDATE SET
                    price_at_signal = EXCLUDED.price_at_signal,
                    reason = EXCLUDED.reason,
                    is_active = EXCLUDED.is_active
            """
        psycopg2.extras.execute_batch(cur, sql, params, page_size=100)
    finally:
        cur.close()
        conn.close()


def detect_extreme_moves(
    live_ticks_map: dict[str, list[dict]],
    threshold_pct: float = 5.0,
) -> list[dict]:
    """Return symbols whose latest-session % change 
    exceeds threshold_pct."""
    extreme: list[dict] = []
    for symbol, sessions in live_ticks_map.items():
        if not sessions:
            continue
        latest = sessions[-1]
        
        ltp = latest.get("ltp")
        ycp = latest.get("ycp")
        
        # Calculate real percentage change
        if ltp is None or ycp is None:
            continue
        try:
            ltp_f = float(ltp)
            ycp_f = float(ycp)
        except (TypeError, ValueError):
            continue
        
        if ycp_f == 0:
            continue
            
        change_pct = round((ltp_f - ycp_f) / ycp_f * 100, 2)
        change_val = round(ltp_f - ycp_f, 2)
        
        if abs(change_pct) >= threshold_pct:
            extreme.append({
                "symbol": symbol,
                "change_pct": change_pct,
                "change_val": change_val,
                "direction": "up" if change_pct > 0 else "down",
                "current_price": ltp_f,
                "ycp": ycp_f,
                "session_no": latest.get("session_no"),
                "volume": latest.get("volume"),
            })
    return sorted(
        extreme, 
        key=lambda x: abs(x["change_pct"]), 
        reverse=True
    )


def _compute_session_no() -> int:
    """Return session 1-10 during market hours, 0 outside."""
    import pytz
    dhaka = pytz.timezone("Asia/Dhaka")
    now = datetime.now(dhaka)
    if now.hour == 14 and now.minute <= 30:
        in_session = True
    elif 10 <= now.hour < 14:
        in_session = True
    else:
        in_session = False
    if not in_session:
        return 0
    session_no = ((now.hour - 10) * 2 + now.minute // 30) + 1
    return max(1, min(10, session_no))


def bulk_fetch_previous_signals(conn, target_date: date, session_no: int) -> dict[str, str]:
    """Fetch previous session overall_signal per symbol for stability filtering."""
    if session_no <= 1:
        return {}

    prev_session = session_no - 1
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT DISTINCT ON (symbol)
                symbol, overall_signal
            FROM analysis_results
            WHERE analysis_date = %s
              AND session_no = %s
            ORDER BY symbol, id DESC
            """,
            (target_date, prev_session),
        )
        return {r[0]: r[1] for r in cur.fetchall() if r[0] and r[1]}
    finally:
        cur.close()


def _worker_analyse(args: tuple) -> dict:
    """Top-level picklable worker for ProcessPoolExecutor.

    Accepts primitives only (no DataFrames) so it can be pickled across processes.
    Reconstructs DataFrames from records inside the worker process.
    """
    (
        symbol,
        price_records,       # list[dict] — JSON-safe records from df.to_dict('records')
        price_cols,          # column order for DataFrame reconstruction
        live_ticks_today,
        pe_ratio,
        eps,
        is_dsex,
        today_market,
        prev_market,
        target_date_iso,     # str ISO date
        market_context,
        market_trend,
        prev_signals,
    ) = args

    target_date = date.fromisoformat(target_date_iso)
    if price_records:
        price_df = pd.DataFrame(price_records, columns=price_cols)
        # Restore date column type
        if "date" in price_df.columns:
            price_df["date"] = pd.to_datetime(price_df["date"]).dt.date
    else:
        price_df = pd.DataFrame()

    return analyse_symbol(
        symbol,
        price_df=price_df,
        pe_ratio=pe_ratio,
        eps=eps,
        is_dsex=is_dsex,
        today_market=today_market,
        prev_market=prev_market,
        target_date=target_date,
        live_ticks_today=live_ticks_today,
        market_context=market_context,
        market_trend=market_trend,
        prev_signals=prev_signals,
    )


def analyse_all_symbols() -> dict:
    start = time.time()
    run_session_no = _compute_session_no()

    conn = get_db_connection()
    conn.autocommit = True
    target_date = get_db_date(conn)
    cur = conn.cursor()
    try:
        # Root-cause cleanup: remove today's analysis rows for active symbols.
        cur.execute(
            """
            DELETE FROM analysis_results
            WHERE analysis_date = %s
              AND symbol IN (
                  SELECT symbol FROM stocks_master WHERE is_active = TRUE
              )
            """,
            (target_date,),
        )
        # Deactivate today's signals so fresh run is the source of truth.
        cur.execute(
            """
            UPDATE signals SET is_active = FALSE
            WHERE signal_date = %s
            """,
            (target_date,),
        )
        cur.execute("DELETE FROM signals WHERE reason = 'auto-hold' OR reason IS NULL")
        cur.execute(
            "UPDATE signals SET is_active = FALSE WHERE signal_date < %s AND is_active = TRUE",
            (target_date,),
        )
        cur.execute("SELECT symbol FROM stocks_master WHERE is_active = TRUE ORDER BY symbol ASC")
        symbols = [r[0] for r in cur.fetchall()]
    finally:
        cur.close()
        conn.close()

    t0 = time.time()
    with ThreadPoolExecutor(max_workers=6) as _loader:
        f_price   = _loader.submit(bulk_fetch_price_history, 90)
        f_ticks   = _loader.submit(bulk_fetch_live_ticks)
        f_fund    = _loader.submit(bulk_fetch_fundamentals)
        f_meta    = _loader.submit(bulk_fetch_stock_metadata)
        f_mkt     = _loader.submit(bulk_fetch_market_summary)
        f_ctx     = _loader.submit(bulk_fetch_market_context, target_date)

        price_history_map       = f_price.result()
        live_ticks_map          = f_ticks.result()
        pe_ratios_map, eps_map  = f_fund.result()
        metadata_map            = f_meta.result()
        today_mkt, prev_mkt     = f_mkt.result()
        market_ctx              = f_ctx.result()

    print(f"All bulk data loaded in {time.time()-t0:.1f}s "
          f"(price={len(price_history_map)}, ticks={len(live_ticks_map)}, "
          f"fund={len(pe_ratios_map)}, meta={len(metadata_map)})")

    market_trend = bulk_fetch_market_trend()
    conn_prev = get_db_connection()
    try:
        prev_signals = bulk_fetch_previous_signals(conn_prev, target_date, run_session_no)
    finally:
        conn_prev.close()
    print(f"Market trend: {market_trend['trend']} | 5d change: {market_trend['dsex_5d_change_pct']}%")

    symbols = [s for s in symbols if s in price_history_map]
    print(f"Data loaded. Running analysis on {len(symbols)} symbols...")

    total = len(symbols)
    done = 0
    skipped = 0
    failed = 0
    buy_signals = 0
    watch_signals = 0
    exit_signals = 0
    failed_symbols: dict[str, str] = {}

    ok_results: list[dict] = []
    signal_rows: list[dict] = []

    analyse_fn = partial(
        analyse_symbol_with_data,
        price_history_map=price_history_map,
        live_ticks_map=live_ticks_map,
        pe_ratios_map=pe_ratios_map,
        eps_map=eps_map,
        metadata_map=metadata_map,
        today_market=today_mkt,
        prev_market=prev_mkt,
        target_date=target_date,
        market_context=market_ctx,
        market_trend=market_trend,
        prev_signals=prev_signals,
    )

    processed = 0
    with ThreadPoolExecutor(max_workers=min(32, (os.cpu_count() or 4) * 4)) as executor:
        futures = {executor.submit(analyse_fn, symbol): symbol for symbol in symbols}
        for future in as_completed(futures):
            symbol = futures[future]
            processed += 1
            try:
                result = future.result()
                status = result.get("status")
                if status == "ok":
                    done += 1
                    ok_results.append(result)
                    sig = result.get("overall_signal")
                    if sig == "BUY":
                        buy_signals += 1
                    if sig == "WATCH":
                        watch_signals += 1
                    if sig == "EXIT":
                        exit_signals += 1
                    if sig in ("BUY", "WATCH", "EXIT"):
                        signal_rows.append(
                            {
                                "symbol": symbol,
                                "signal_type": sig,
                                "signal_date": result.get("analysis_date"),
                                "price_at_signal": result.get("price_at_signal"),
                                "reason": result.get("signal_reason"),
                            }
                        )
                elif status == "skipped":
                    skipped += 1
                else:
                    failed += 1
                    failed_symbols[symbol] = result.get("reason", "unknown error")
            except Exception as e:
                failed += 1
                failed_symbols[symbol] = str(e)

            if processed % 50 == 0:
                print(f"[{processed}/{total}] ...")

    # Enforce session_no=0 for off-hours runs so the unique constraint
    # (symbol, analysis_date, session_no) prevents duplicate EOD rows.
    for r in ok_results:
        if r.get("session_no") is None:
            r["session_no"] = run_session_no

    # Hard daily BUY cap: keep top 20 BUYs by confidence, downgrade the rest to WATCH.
    buy_results = sorted(
        [r for r in ok_results if r.get("overall_signal") == "BUY"],
        key=lambda x: float(x.get("confidence_score") or 0.0),
        reverse=True,
    )
    print(f"BUY signals before cap: {len(buy_results)}")
    if len(buy_results) > 20:
        for r in buy_results[20:]:
            r["overall_signal"] = "WATCH"
        print(f"BUY cap: kept 20, downgraded {len(buy_results) - 20}")
    else:
        print(f"BUY cap: {len(buy_results)} signals, no cap needed")

    # Rebuild counters and signal rows after BUY cap adjustments.
    buy_signals = sum(1 for r in ok_results if r.get("overall_signal") == "BUY")
    watch_signals = sum(1 for r in ok_results if r.get("overall_signal") == "WATCH")
    exit_signals = sum(1 for r in ok_results if r.get("overall_signal") == "EXIT")
    signal_rows = [
        {
            "symbol": r.get("symbol"),
            "signal_type": r.get("overall_signal"),
            "signal_date": r.get("analysis_date"),
            "price_at_signal": r.get("price_at_signal"),
            "reason": r.get("signal_reason"),
        }
        for r in ok_results
        if r.get("overall_signal") in ("BUY", "WATCH", "EXIT")
    ]

    batch_upsert_analysis_results(ok_results)
    batch_upsert_signals(signal_rows)

    elapsed = time.time() - start
    print(f"Completed in {elapsed:.1f}s")

    return {
        "status": "ok",
        "total": total,
        "done": done,
        "skipped": skipped,
        "failed": failed,
        "buy_signals": buy_signals,
        "watch_signals": watch_signals,
        "exit_signals": exit_signals,
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "elapsed_seconds": round(elapsed, 1),
        "failed_symbols": failed_symbols,
    }


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        result = analyse_symbol(sys.argv[1])
        if result.get("status") == "ok":
            batch_upsert_analysis_results([result])
        print(json.dumps(result, indent=2, default=str))
    else:
        result = analyse_all_symbols()
        print(json.dumps(result, indent=2, default=str))

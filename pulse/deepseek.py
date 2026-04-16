from __future__ import annotations

import html
import json
import logging
import os
import re
import sys
import traceback
from collections import defaultdict
from datetime import date, datetime, time, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg2
import pytz
from dotenv import load_dotenv

logger = logging.getLogger(__name__)


SYSTEM_PROMPT = """
You are NexTrade, a proactive DSE trading intelligence system.
You write like a knowledgeable friend who trades — direct,
warm, and honest. No jargon. No bullet points. Plain English.

When writing any pulse message:

STOCK ANALYSIS FORMAT:
For every stock mentioned, include these 4 layers:
1. Price context: "trading at X on [date], [up/down] Y%"
2. Trend context: "above/below its 50-day average of Z" 
   — this tells the trader if the trend is with them or against them
3. Momentum context: use RSI direction — 
   "RSI stabilizing at 34 — selling pressure slowing" or
   "RSI falling at 28 — still losing momentum, wait"
4. Volume context: use volume_price_pattern —
   "sellers are exhausting" (sellers_exhausted) or
   "buyers stepping in quietly" (accumulation) or
   "institutions appear to be selling" (distribution — warn strongly)

BUY SIGNAL FORMAT (mandatory fields):
- Entry level and zone
- First target with % upside
- Stop loss at -8% with BDT amount
- MA context: is price above or below MA50?
- Volume confirmation: is volume pattern supportive?
- One clear urgency: "Act now" / "Watch for confirmation" / "Wait"

PORTFOLIO POSITION FORMAT:
For each position show:
- Current P&L with context ("down 6% — approaching stop loss zone")
- Key level to watch ("holding above support at 211 — critical")
- MA context ("below its 50-day average — trend working against you")
- Clear action: hold / exit on bounce / exit now

MARKET CONTEXT:
- DSEX: Only mention if explicitly provided in the data below.
  If not provided or marked "not available", do NOT mention 
  DSEX at all — not even approximately or historically.
- State market trend only if data supports it
- State session context: "Session X — [what this means]"

TONE:
- Short paragraphs, never bullet points
- Say "buy", "exit", "wait" — never "consider" or "might"
- Be honest — if market is weak, say it clearly
- Maximum 500 words
- Plain text only, no markdown
- Use CAPS for urgency when needed
"""


PREMARKET_INSTRUCTIONS = """
For pre-market briefing:

OPENING: One sentence on market mood based on yesterday's close
and DSEX direction. Be direct — "market closed weak" or 
"market closed with quiet strength."

WATCHLIST REVIEW: For each watchlist stock:
- Price as of last close with date
- MA context: above or below MA50 — one sentence
- RSI zone: oversold / neutral / overbought
- Volume pattern from last session
- One clear action for today's open: 
  "Watch for bounce off [support] with volume"
  or "Avoid — trend is down, wait for stabilization"
  or "Near target [price] — consider partial exit"

PORTFOLIO REVIEW: For each position:
- P&L with context
- Key support to watch
- If down >5%: flag explicitly with stop loss reminder
- If near target: suggest partial profit taking

MARKET SETUP: 
- Any BUY signals from yesterday's analysis?
- Key DSEX level to watch today
- Overall posture: aggressive / selective / defensive

ONE FOCUS: End with exactly one thing to watch today.
Make it specific — stock name, price level, condition.

TONE: Like a coach giving the team talk before the game.
Confident, prepared, honest about risks.
"""

EOD_INSTRUCTIONS = """
For EOD messages:
- Open with honest one-liner about the day
- Open with how the day went in one line
- Use section headers:
  <b>📊 How today went:</b>
  <b>💼 Portfolio update:</b>
  <b>👀 Tomorrow's setup:</b>
- Highlight what worked and what didn't
- Update portfolio with final day P&L
- Preview tomorrow's setups if any
- End with: one thing to prepare for tomorrow
- End with forward-looking closer
- Tone: like a coach reviewing the game after
  it ends — honest, constructive, forward-looking
"""

_root = Path(__file__).resolve().parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))


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


def _table_exists(cur, name: str) -> bool:
    cur.execute(
        """
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = %s
        """,
        (name,),
    )
    return cur.fetchone() is not None


def _columns(cur, table: str) -> set[str]:
    cur.execute(
        """
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        """,
        (table,),
    )
    return {r[0] for r in cur.fetchall()}


def get_market_context(conn, target_date: date) -> dict:
    cur = conn.cursor()
    try:
        cols = _columns(cur, "market_summary")
        has_td = "trade_date" in cols
        has_d = "date" in cols
        if has_td and has_d:
            sort_expr = "COALESCE(trade_date, date)"
        elif has_td:
            sort_expr = "trade_date"
        else:
            sort_expr = "date"

        row = None
        if has_td and has_d:
            cur.execute(
                f"""
                SELECT
                    COALESCE(dsex_index, 0),
                    COALESCE(dses_index, 0),
                    COALESCE(total_volume, 0),
                    COALESCE(total_value_mn, 0),
                    {sort_expr} AS row_date
                FROM market_summary
                WHERE trade_date = %s OR date = %s
                LIMIT 1
                """,
                (target_date, target_date),
            )
            row = cur.fetchone()
        elif has_td:
            cur.execute(
                f"""
                SELECT
                    COALESCE(dsex_index, 0),
                    COALESCE(dses_index, 0),
                    COALESCE(total_volume, 0),
                    COALESCE(total_value_mn, 0),
                    trade_date AS row_date
                FROM market_summary
                WHERE trade_date = %s
                LIMIT 1
                """,
                (target_date,),
            )
            row = cur.fetchone()
        else:
            cur.execute(
                f"""
                SELECT
                    COALESCE(dsex_index, 0),
                    COALESCE(dses_index, 0),
                    COALESCE(total_volume, 0),
                    COALESCE(total_value_mn, 0),
                    date AS row_date
                FROM market_summary
                WHERE date = %s
                LIMIT 1
                """,
                (target_date,),
            )
            row = cur.fetchone()

        if not row:
            cur.execute(
                f"""
                SELECT
                    COALESCE(dsex_index, 0),
                    COALESCE(dses_index, 0),
                    COALESCE(total_volume, 0),
                    COALESCE(total_value_mn, 0),
                    {sort_expr} AS row_date
                FROM market_summary
                ORDER BY {sort_expr} DESC NULLS LAST
                LIMIT 1
                """
            )
            row = cur.fetchone()

        if not row:
            return {
                "dsex_index": 0.0,
                "dses_index": 0.0,
                "total_volume": 0,
                "total_value_mn": 0.0,
                "data_date": None,
            }

        dsex, dses, vol, tvm, row_date = row
        data_date = row_date if isinstance(row_date, date) else None
        data_date_str = row_date.isoformat() if hasattr(row_date, "isoformat") else str(row_date)

        if data_date is not None:
            from datetime import timedelta
            days_old = (target_date - data_date).days
            if days_old > 7:
                return {
                    "dsex_index": None,
                    "dses_index": None,
                    "total_volume": None,
                    "total_value_mn": None,
                    "data_date": None,
                    "is_stale": True,
                }

        return {
            "dsex_index": float(dsex) if dsex is not None else 0.0,
            "dses_index": float(dses) if dses is not None else 0.0,
            "total_volume": int(vol) if vol is not None else 0,
            "total_value_mn": float(tvm) if tvm is not None else 0.0,
            "data_date": data_date_str,
        }
    finally:
        cur.close()


def _float_or_none(x: Any) -> float | None:
    if x is None:
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _parse_json_dict(val: Any) -> dict:
    if val is None:
        return {}
    if isinstance(val, dict):
        return val
    if isinstance(val, str):
        try:
            return json.loads(val)
        except json.JSONDecodeError:
            return {}
    return {}


def _json_list(val: Any) -> list:
    if val is None:
        return []
    if isinstance(val, list):
        return val
    if isinstance(val, str):
        try:
            j = json.loads(val)
            return j if isinstance(j, list) else []
        except json.JSONDecodeError:
            return []
    return []


def get_analysis_summary(conn, target_date: date) -> dict:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT DISTINCT ON (symbol)
                symbol,
                overall_signal,
                confidence_score,
                stock_class,
                support_levels,
                resistance_levels,
                rsi,
                raw_output
            FROM analysis_results
            WHERE analysis_date = %s
            ORDER BY symbol, analysis_date DESC, session_no DESC NULLS LAST, confidence_score DESC NULLS LAST, id DESC
            """,
            (target_date,),
        )
        rows = cur.fetchall()

        # If today has no BUY signals, fall back to the most recent date that does
        has_buys = any(r[1] == "BUY" for r in rows)
        if not has_buys:
            cur.execute(
                """
                SELECT MAX(analysis_date) FROM analysis_results
                WHERE overall_signal = 'BUY'
                  AND analysis_date < %s
                """,
                (target_date,),
            )
            fallback_row = cur.fetchone()
            fallback_date = fallback_row[0] if fallback_row else None
            if fallback_date:
                cur.execute(
                    """
                    SELECT DISTINCT ON (symbol)
                        symbol,
                        overall_signal,
                        confidence_score,
                        stock_class,
                        support_levels,
                        resistance_levels,
                        rsi,
                        raw_output
                    FROM analysis_results
                    WHERE analysis_date = %s
                    ORDER BY symbol, analysis_date DESC, session_no DESC NULLS LAST, confidence_score DESC NULLS LAST, id DESC
                    """,
                    (fallback_date,),
                )
                rows = cur.fetchall()
                target_date = fallback_date

        cur.execute(
            """
            SELECT symbol, signal_type, reason, id
            FROM signals
            WHERE signal_date = %s AND is_active = TRUE
            ORDER BY symbol, id DESC
            """,
            (target_date,),
        )
        sig_by_symbol: dict[str, list[tuple[str, str]]] = defaultdict(list)
        for sym, st, rea, _i in cur.fetchall():
            sig_by_symbol[str(sym)].append((st or "", (rea or "").strip()))

        def pick_signal_reason(symbol: str, overall: str) -> str:
            lst = sig_by_symbol.get(symbol, [])
            for st, rea in lst:
                if st == overall and rea:
                    return rea
            for _st, rea in lst:
                if rea:
                    return rea
            return ""

        def reason_from_breakout(raw: dict) -> str:
            bo = raw.get("breakout")
            if isinstance(bo, dict) and bo.get("breakout_level") is not None:
                return f"Breakout level {bo.get('breakout_level')}"
            return ""

        def current_price_from_raw(raw: dict) -> float | None:
            v = _float_or_none(raw.get("current_price"))
            if v is not None:
                return v
            sr = raw.get("sr") if isinstance(raw.get("sr"), dict) else {}
            return _float_or_none(sr.get("current_price"))

        buy_signals: list[dict] = []
        watch_signals: list[dict] = []
        exit_signals: list[dict] = []

        for (
            symbol,
            overall,
            conf,
            stock_class,
            sup,
            res,
            rsi,
            raw_out,
        ) in rows:
            raw = _parse_json_dict(raw_out)
            current_price = current_price_from_raw(raw)
            cf = raw.get("class_flags")
            class_flags = cf if isinstance(cf, dict) else {}
            confidence = _float_or_none(conf) or 0.0
            ov = overall or ""
            db_reason = pick_signal_reason(symbol, ov)
            reason = db_reason or reason_from_breakout(raw)
            ts = raw.get("trade_setup")
            trade_setup = ts if isinstance(ts, dict) else {}
            entry = {
                "symbol": symbol,
                "confidence": confidence,
                "stock_class": stock_class or "",
                "reason": reason,
                "support": _json_list(sup),
                "resistance": _json_list(res),
                "rsi": _float_or_none(rsi),
                "current_price": current_price,
                "class_flags": class_flags,
                "trade_setup": trade_setup,
            }
            if overall == "BUY":
                buy_signals.append(entry)
            elif overall == "WATCH":
                watch_signals.append(entry)
            elif overall == "EXIT":
                exit_signals.append(entry)

        buy_signals.sort(key=lambda x: x["confidence"], reverse=True)
        watch_signals.sort(key=lambda x: x["confidence"], reverse=True)
        buy_total = len(buy_signals)
        watch_total = len(watch_signals)
        buy_signals = buy_signals[:10]
        watch_signals = watch_signals[:10]

        return {
            "total_analysed": len(rows),
            "buy_signals": buy_signals,
            "buy_signal_total": buy_total,
            "watch_signals": watch_signals,
            "watch_signal_total": watch_total,
            "exit_signals": exit_signals,
            "market_context": {},
        }
    finally:
        cur.close()


def _enrich_position_row(cur, symbol: str, quantity: int, avg_buy_price: float) -> dict | None:
    if quantity <= 0 or avg_buy_price <= 0:
        return None
    cur.execute(
        """
        SELECT DISTINCT ON (symbol)
            overall_signal, raw_output
        FROM analysis_results
        WHERE symbol = %s AND analysis_date = CURRENT_DATE
        ORDER BY symbol, session_no DESC NULLS LAST, id DESC
        """,
        (symbol,),
    )
    ar = cur.fetchone()
    signal = "HOLD"
    reason = ""
    current_price = None
    if ar:
        signal = ar[0] or "HOLD"
        raw = _parse_json_dict(ar[1])
        sr = raw.get("sr") or {}
        current_price = _float_or_none(raw.get("current_price"))
        if current_price is None:
            current_price = _float_or_none(sr.get("current_price"))
    cur.execute(
        """
        SELECT reason FROM signals
        WHERE symbol = %s AND signal_date = CURRENT_DATE AND is_active = TRUE
        ORDER BY id DESC LIMIT 1
        """,
        (symbol,),
    )
    sr2 = cur.fetchone()
    if sr2 and sr2[0]:
        reason = sr2[0]

    if current_price is None:
        cur.execute(
            """
            SELECT ltp, close FROM price_history
            WHERE symbol = %s ORDER BY date DESC LIMIT 1
            """,
            (symbol,),
        )
        pr = cur.fetchone()
        if pr:
            current_price = _float_or_none(pr[0]) or _float_or_none(pr[1])

    if current_price is None:
        current_price = avg_buy_price

    pnl_pct = ((current_price - avg_buy_price) / avg_buy_price) * 100.0 if avg_buy_price else 0.0
    pnl_value = (current_price - avg_buy_price) * quantity

    return {
        "symbol": symbol,
        "quantity": quantity,
        "avg_buy_price": avg_buy_price,
        "current_price": current_price,
        "pnl_pct": round(pnl_pct, 2),
        "pnl_value": round(pnl_value, 2),
        "signal": signal,
        "reason": reason or "",
    }


def get_trader_portfolio(conn, trader_id: int) -> list[dict]:
    cur = conn.cursor()
    try:
        if _table_exists(cur, "portfolio_holdings"):
            ph_cols = _columns(cur, "portfolio_holdings")
            if "trader_id" in ph_cols and "is_open" in ph_cols:
                cur.execute(
                    """
                    SELECT symbol, quantity, avg_buy_price
                    FROM portfolio_holdings
                    WHERE trader_id = %s AND is_open = TRUE
                    """,
                    (trader_id,),
                )
                out: list[dict] = []
                for symbol, qty, avg_px in cur.fetchall():
                    row = _enrich_position_row(cur, symbol, int(qty or 0), float(avg_px or 0))
                    if row:
                        out.append(row)
                return out

        pos_cols = _columns(cur, "positions")
        if "trader_id" in pos_cols:
            cur.execute(
                """
                SELECT symbol, quantity, avg_buy_price
                FROM positions
                WHERE trader_id = %s AND is_open = TRUE
                """,
                (trader_id,),
            )
            out2: list[dict] = []
            for symbol, qty, avg_px in cur.fetchall():
                row = _enrich_position_row(cur, symbol, int(qty or 0), float(avg_px or 0))
                if row:
                    out2.append(row)
            return out2
        return []
    finally:
        cur.close()


def get_trader_watchlist(conn, trader_id: int) -> list[dict]:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT tw.symbol, tw.target_price
            FROM trader_watchlist tw
            WHERE tw.trader_id = %s AND tw.is_active = TRUE
            ORDER BY tw.symbol
            """,
            (trader_id,),
        )
        rows = cur.fetchall()
        out: list[dict] = []
        for symbol, target_price in rows:
            cur.execute(
                """
                SELECT DISTINCT ON (symbol)
                    overall_signal, raw_output
                FROM analysis_results
                WHERE symbol = %s AND analysis_date = CURRENT_DATE
                ORDER BY symbol, session_no DESC NULLS LAST, id DESC
                """,
                (symbol,),
            )
            ar = cur.fetchone()
            signal = "HOLD"
            current_price = None
            reason = ""
            if ar:
                signal = ar[0] or "HOLD"
                raw = _parse_json_dict(ar[1])
                sr = raw.get("sr") or {}
                current_price = _float_or_none(sr.get("current_price"))
            cur.execute(
                """
                SELECT reason FROM signals
                WHERE symbol = %s AND signal_date = CURRENT_DATE AND is_active = TRUE
                ORDER BY id DESC LIMIT 1
                """,
                (symbol,),
            )
            sr2 = cur.fetchone()
            if sr2 and sr2[0]:
                reason = sr2[0]
            if current_price is None:
                cur.execute(
                    "SELECT ltp, close FROM price_history WHERE symbol = %s ORDER BY date DESC LIMIT 1",
                    (symbol,),
                )
                pr = cur.fetchone()
                if pr:
                    current_price = _float_or_none(pr[0]) or _float_or_none(pr[1])
            out.append(
                {
                    "symbol": symbol,
                    "target_price": float(target_price) if target_price is not None else None,
                    "current_price": current_price,
                    "signal": signal,
                    "reason": reason or "",
                }
            )
        return out
    finally:
        cur.close()


def get_top_movers_today(conn, session_no: int, limit: int = 3) -> list[dict]:
    if conn is None or session_no <= 0:
        return []
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT symbol, change_pct, overall_signal
            FROM (
                SELECT
                    symbol,
                    NULLIF(trim(raw_output->'relative_strength'->>'stock_change_pct'), '')::numeric AS change_pct,
                    overall_signal,
                    ROW_NUMBER() OVER (
                        PARTITION BY symbol
                        ORDER BY ABS(NULLIF(trim(raw_output->'relative_strength'->>'stock_change_pct'), '')::numeric) DESC,
                                 id DESC
                    ) AS rn
                FROM analysis_results
                WHERE analysis_date = CURRENT_DATE
                  AND session_no = %s
                  AND NULLIF(trim(raw_output->'relative_strength'->>'stock_change_pct'), '') IS NOT NULL
            ) q
            WHERE rn = 1
            ORDER BY ABS(change_pct) DESC, symbol ASC
            LIMIT %s
            """,
            (session_no, limit),
        )
        out = []
        for sym, chg, sig in cur.fetchall():
            out.append({
                "symbol": sym,
                "change_pct": float(chg) if chg is not None else None,
                "signal": sig,
            })
        return out
    except Exception:
        return []
    finally:
        cur.close()


def get_previous_session_counts(conn, target_date: date, session_no: int) -> dict:
    if conn is None or session_no <= 1:
        return {"buy": 0, "watch": 0, "exit": 0}
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
                COUNT(CASE WHEN overall_signal = 'BUY' THEN 1 END),
                COUNT(CASE WHEN overall_signal = 'WATCH' THEN 1 END),
                COUNT(CASE WHEN overall_signal = 'EXIT' THEN 1 END)
            FROM analysis_results
            WHERE analysis_date = %s
              AND session_no = %s
            """,
            (target_date, session_no - 1),
        )
        row = cur.fetchone() or (0, 0, 0)
        return {"buy": int(row[0] or 0), "watch": int(row[1] or 0), "exit": int(row[2] or 0)}
    except Exception:
        return {"buy": 0, "watch": 0, "exit": 0}
    finally:
        cur.close()


def get_position_alert_count(conn, trader_id: int, symbol: str, target_date: date) -> int:
    if conn is None:
        return 0
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT COUNT(*)
            FROM analysis_results ar
            JOIN pulse_log pl ON pl.pulse_date = ar.analysis_date
            WHERE pl.trader_id = %s
              AND ar.symbol = %s
              AND ar.analysis_date = %s
              AND ar.overall_signal = 'EXIT'
              AND pl.telegram_sent = TRUE
            """,
            (trader_id, symbol, target_date),
        )
        row = cur.fetchone()
        return int(row[0] or 0) if row else 0
    except Exception:
        return 0
    finally:
        cur.close()


def calculate_win_rate(conn, trader_id: int) -> dict:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT s.id, s.symbol, s.price_at_signal, phc.close
            FROM signals s
            JOIN (
                SELECT DISTINCT ON (symbol) symbol, close
                FROM price_history
                ORDER BY symbol, date DESC
            ) phc ON s.symbol = phc.symbol
            WHERE s.signal_type = 'BUY'
              AND s.is_active = FALSE
            ORDER BY s.id DESC
            """
        )
        rows = cur.fetchall()

        wins = losses = neutrals = 0
        resolved_pnls: list[float] = []
        for _id, _sym, entry, current in rows:
            e = _float_or_none(entry)
            c = _float_or_none(current)
            if not e or not c:
                continue
            pnl = ((c - e) / e) * 100.0
            if pnl > 2.0:
                wins += 1
                resolved_pnls.append(pnl)
            elif pnl < -2.0:
                losses += 1
                resolved_pnls.append(pnl)
            else:
                neutrals += 1

        cur.execute(
            """
            SELECT COUNT(*) FROM signals
            WHERE signal_type = 'BUY' AND is_active = TRUE
            """
        )
        unresolved = int((cur.fetchone() or [0])[0] or 0)

        sample_size = wins + losses
        if sample_size < 10:
            return {
                "win_rate": None,
                "wins": wins,
                "losses": losses,
                "unresolved": unresolved,
                "avg_pnl_on_resolved": round(sum(resolved_pnls) / len(resolved_pnls), 2) if resolved_pnls else 0.0,
                "sample_size": sample_size,
                "message": "Building accuracy baseline — insufficient data (<10 resolved signals)",
                "neutrals": neutrals,
                "total_evaluated": wins + losses + neutrals,
            }

        return {
            "win_rate": round((wins / sample_size) * 100, 1),
            "wins": wins,
            "losses": losses,
            "unresolved": unresolved,
            "avg_pnl_on_resolved": round(sum(resolved_pnls) / len(resolved_pnls), 2) if resolved_pnls else 0.0,
            "sample_size": sample_size,
            "neutrals": neutrals,
            "total_evaluated": wins + losses + neutrals,
            "message": "",
        }
    finally:
        cur.close()


def get_scorecard_data(conn, trader_id: int) -> dict:
    base = {
        "win_rate": None,
        "wins": 0,
        "losses": 0,
        "neutrals": 0,
        "unresolved": 0,
        "avg_pnl_on_resolved": 0.0,
        "sample_size": 0,
        "message": "Building accuracy baseline — insufficient data (<10 resolved signals)",
        "top_wins": [],
        "recent_losses": [],
        "total_evaluated": 0,
    }

    wr = calculate_win_rate(conn, trader_id)
    base.update(wr)

    cur = conn.cursor()
    try:
        # Recent realized losses from sells (deterministic order)
        losses_rows = []
        try:
            cur.execute(
                """
                SELECT
                    tt.symbol,
                    tt.price AS sell_price,
                    ph.avg_buy_price,
                    ((tt.price - ph.avg_buy_price) / NULLIF(ph.avg_buy_price, 0) * 100) AS pnl_pct,
                    tt.transaction_date,
                    tt.id
                FROM trade_transactions tt
                JOIN portfolio_holdings ph ON
                    tt.trader_id = ph.trader_id AND tt.symbol = ph.symbol
                WHERE tt.trader_id = %s
                  AND tt.transaction_type = 'SELL'
                ORDER BY tt.transaction_date DESC, tt.id DESC
                LIMIT 5
                """,
                (trader_id,),
            )
            losses_rows = cur.fetchall()
        except Exception:
            losses_rows = []

        if losses_rows:
            base["recent_losses"] = [
                {
                    "symbol": r[0],
                    "signal_type": "SELL",
                    "price_at_signal": _float_or_none(r[2]),
                    "price_at_eval": _float_or_none(r[1]),
                    "pnl_pct": round(_float_or_none(r[3]) or 0.0, 2),
                    "transaction_date": r[4].isoformat() if hasattr(r[4], "isoformat") else str(r[4]),
                }
                for r in losses_rows
                if (_float_or_none(r[3]) or 0.0) < 0
            ]
        else:
            cur.execute(
                """
                SELECT s.symbol, s.signal_type, s.price_at_signal,
                       ph_current.close AS current_price,
                       ((ph_current.close - s.price_at_signal) / NULLIF(s.price_at_signal, 0) * 100) AS pnl_pct,
                       s.id
                FROM signals s
                JOIN (
                    SELECT DISTINCT ON (symbol) symbol, close
                    FROM price_history
                    ORDER BY symbol, date DESC
                ) ph_current ON s.symbol = ph_current.symbol
                WHERE s.signal_type = 'BUY'
                  AND s.is_active = FALSE
                  AND ((ph_current.close - s.price_at_signal) / NULLIF(s.price_at_signal, 0) * 100) < 0
                ORDER BY pnl_pct ASC, s.id DESC
                LIMIT 5
                """
            )
            base["recent_losses"] = [
                {
                    "symbol": r[0],
                    "signal_type": r[1],
                    "price_at_signal": _float_or_none(r[2]),
                    "price_at_eval": _float_or_none(r[3]),
                    "pnl_pct": round(_float_or_none(r[4]) or 0.0, 2),
                }
                for r in cur.fetchall()
            ]

        # Deterministic top wins from inactive BUY signals
        cur.execute(
            """
            SELECT s.symbol, s.signal_type, s.price_at_signal,
                   ph_current.close AS current_price,
                   ((ph_current.close - s.price_at_signal) / NULLIF(s.price_at_signal, 0) * 100) AS pnl_pct,
                   s.id
            FROM signals s
            JOIN (
                SELECT DISTINCT ON (symbol) symbol, close
                FROM price_history
                ORDER BY symbol, date DESC
            ) ph_current ON s.symbol = ph_current.symbol
            WHERE s.signal_type = 'BUY'
              AND s.is_active = FALSE
              AND ((ph_current.close - s.price_at_signal) / NULLIF(s.price_at_signal, 0) * 100) > 0
            ORDER BY pnl_pct DESC, s.id DESC
            LIMIT 5
            """
        )
        base["top_wins"] = [
            {
                "symbol": r[0],
                "signal_type": r[1],
                "price_at_signal": _float_or_none(r[2]),
                "price_at_eval": _float_or_none(r[3]),
                "pnl_pct": round(_float_or_none(r[4]) or 0.0, 2),
            }
            for r in cur.fetchall()
        ]

        # Compatibility keys used in prompt/formatter
        base["top_losses"] = list(base.get("recent_losses") or [])
        base["avg_pnl_pct"] = base.get("avg_pnl_on_resolved", 0.0)
    finally:
        cur.close()

    return base


def _compute_dsex_change_pct(conn, target_date: date) -> float | None:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT dsex_index FROM market_summary
            WHERE trade_date <= %s
            ORDER BY trade_date DESC
            LIMIT 2
            """,
            (target_date,),
        )
        rows = cur.fetchall()
        if len(rows) < 2:
            return None
        t0 = _float_or_none(rows[0][0])
        t1 = _float_or_none(rows[1][0])
        if t1 and t1 != 0 and t0 is not None:
            return round((t0 - t1) / t1 * 100.0, 2)
        return None
    finally:
        cur.close()


def enrich_market_context_pulse(conn, target_date: date, base: dict | None) -> dict:
    out = dict(base or {})
    chg = _compute_dsex_change_pct(conn, target_date)
    if chg is not None:
        out["dsex_change_pct"] = chg
    return out


def _fetch_buy_setups_for_session(
    conn, target_date: date, session_no: int, urgency: str
) -> list[dict]:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT symbol, raw_output->'trade_setup' AS setup
            FROM analysis_results
            WHERE analysis_date = %s
              AND session_no = %s
              AND overall_signal = 'BUY'
              AND COALESCE(raw_output->'trade_setup'->>'has_setup', '') IN ('true', 'True')
              AND raw_output->'trade_setup'->>'urgency' = %s
              AND (raw_output->'trade_setup'->>'rr_1')::numeric >= 1.5
            """,
            (target_date, session_no, urgency),
        )
        out: list[dict] = []
        for sym, setup in cur.fetchall():
            s = setup if isinstance(setup, dict) else _parse_json_dict(setup)
            out.append({"symbol": sym, "setup": s})
        return out
    finally:
        cur.close()


def _setup_symbols(rows: list[dict]) -> set[str]:
    return {str(r["symbol"]) for r in rows if r.get("symbol")}


def _latest_price_for_symbol(cur, symbol: str) -> float | None:
    cur.execute(
        """
        SELECT ltp, close FROM price_history
        WHERE symbol = %s
        ORDER BY date DESC
        LIMIT 1
        """,
        (symbol,),
    )
    row = cur.fetchone()
    if not row:
        return None
    return _float_or_none(row[0]) or _float_or_none(row[1])


def _latest_overall_signal(conn, symbol: str, analysis_date: date) -> str | None:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT overall_signal
            FROM analysis_results
            WHERE symbol = %s AND analysis_date = %s
            ORDER BY session_no DESC NULLS LAST, id DESC
            LIMIT 1
            """,
            (symbol, analysis_date),
        )
        row = cur.fetchone()
        return (row[0] or None) if row else None
    finally:
        cur.close()


def _trader_intent_targets(cur, trader_id: int, symbol: str) -> tuple[float | None, float | None]:
    if not _table_exists(cur, "trader_stock_intents"):
        return None, None
    cur.execute(
        """
        SELECT target_price, stop_price
        FROM trader_stock_intents
        WHERE trader_id = %s AND symbol = %s AND is_active = TRUE
        ORDER BY updated_at DESC NULLS LAST
        LIMIT 1
        """,
        (trader_id, symbol),
    )
    row = cur.fetchone()
    if not row:
        return None, None
    return _float_or_none(row[0]), _float_or_none(row[1])


def should_send_pulse(
    conn,
    trader_id: int,
    target_date,
    session_no: int,
    analysis_summary: dict,
    portfolio: list,
    market_context: dict,
) -> tuple[bool, str, dict]:
    """Returns (should_send, reason, meta) for proactive pulses."""
    meta: dict[str, Any] = {}

    # --- Check 1: portfolio urgency ---
    cur = conn.cursor()
    try:
        for p in portfolio or []:
            sym = str(p.get("symbol") or "")
            if not sym:
                continue
            pnl = float(p.get("pnl_pct") or 0.0)
            if pnl <= -7.5:
                meta.update({"symbol": sym, "pnl_pct": pnl, "current_price": p.get("current_price")})
                # Only fire stop loss alert once per 3 sessions
                # to avoid spamming the same message repeatedly
                cur_sl = conn.cursor()
                cur_sl.execute("""
                    SELECT MAX(session_no) FROM pulse_log
                    WHERE trader_id = %s
                      AND pulse_date = %s
                      AND deepseek_input::text LIKE '%portfolio_stop_loss_alert%'
                      AND telegram_sent = TRUE
                """, (trader_id, target_date))
                last_sl_session = cur_sl.fetchone()
                cur_sl.close()
                last_sl = int(last_sl_session[0]) if last_sl_session and last_sl_session[0] else 0
                
                # Fire immediately if never sent today
                # After that, only re-fire every 3 sessions
                if last_sl > 0 and (session_no - last_sl) < 3:
                    # Skip — too soon since last stop loss alert
                    pass
                else:
                    tgt, stp = _trader_intent_targets(cur, trader_id, sym)
                    meta["stop_price"] = stp
                    return True, "portfolio_stop_loss_alert", meta

            sig = _latest_overall_signal(conn, sym, target_date)
            if sig == "EXIT":
                meta.update({"symbol": sym, "signal": sig})
                return True, "portfolio_exit_signal", meta

            latest_px = _latest_price_for_symbol(cur, sym)
            if latest_px is None:
                latest_px = _float_or_none(p.get("current_price"))
            target_px, _stp = _trader_intent_targets(cur, trader_id, sym)
            if target_px is not None and latest_px is not None and latest_px >= target_px:
                meta.update(
                    {
                        "symbol": sym,
                        "target_price": target_px,
                        "pnl_pct": pnl,
                        "current_price": latest_px,
                    }
                )
                return True, "portfolio_target_hit", meta
    finally:
        cur.close()

    # --- Check 2: new high-quality NOW setups vs previous session ---
    if session_no is not None and session_no > 0:
        now_rows = _fetch_buy_setups_for_session(conn, target_date, session_no, "NOW")
        now_syms = _setup_symbols(now_rows)
        prev_syms: set[str] = set()
        if session_no > 1:
            prev_rows = _fetch_buy_setups_for_session(conn, target_date, session_no - 1, "NOW")
            prev_syms = _setup_symbols(prev_rows)
        truly_new = [s for s in sorted(now_syms) if s not in prev_syms]
        if truly_new:
            meta["new_setup_symbols"] = truly_new
            meta["now_setups"] = now_rows
            return True, "new_trade_setup", meta

    # --- Check 3: WATCH -> NOW confirmation ---
    if session_no is not None and session_no > 1:
        prev_watch = _fetch_buy_setups_for_session(conn, target_date, session_no - 1, "WATCH")
        watch_syms = _setup_symbols(prev_watch)
        now_rows2 = _fetch_buy_setups_for_session(conn, target_date, session_no, "NOW")
        now_syms2 = _setup_symbols(now_rows2)
        confirmed = sorted(watch_syms & now_syms2)
        if confirmed:
            meta["confirmed_symbols"] = confirmed
            meta["now_setups"] = now_rows2
            return True, "setup_confirmed", meta

    # --- Check 4: significant DSEX move since last delivered pulse ---
    mc = market_context or {}
    cur_chg = mc.get("dsex_change_pct")
    try:
        cur_f = float(cur_chg) if cur_chg is not None else None
    except (TypeError, ValueError):
        cur_f = None
    if cur_f is not None:
        cur2 = conn.cursor()
        try:
            cur2.execute(
                """
                SELECT deepseek_input
                FROM pulse_log
                WHERE trader_id = %s AND telegram_sent = TRUE
                ORDER BY id DESC
                LIMIT 1
                """,
                (trader_id,),
            )
            row = cur2.fetchone()
        finally:
            cur2.close()
        prev_snap = None
        if row and row[0] is not None:
            inp = row[0]
            if isinstance(inp, str):
                try:
                    inp = json.loads(inp)
                except json.JSONDecodeError:
                    inp = {}
            if isinstance(inp, dict):
                prev_snap = inp.get("market_snapshot") or {}
        prev_chg = prev_snap.get("dsex_change_pct") if isinstance(prev_snap, dict) else None
        try:
            prev_f = float(prev_chg) if prev_chg is not None else None
        except (TypeError, ValueError):
            prev_f = None
        if prev_f is None:
            if abs(cur_f) > 1.5:
                meta["dsex_change_pct"] = cur_f
                return True, "market_significant_move", meta
        elif abs(cur_f - prev_f) > 1.5:
            meta["dsex_change_pct"] = cur_f
            meta["prev_dsex_change_pct"] = prev_f
            return True, "market_significant_move", meta

    # --- Check 5: first session of the day ---
    if session_no == 1:
        return True, "first_session", meta

    # ── FORMING SETUPS CHECK ─────────────────────────────
    if session_no is not None and session_no > 2:
        try:
            from analysis.engine import detect_forming_setups
            current_setups = detect_forming_setups(
                conn, target_date, session_no
            )
            prev_setups = detect_forming_setups(
                conn, target_date, max(0, session_no - 1)
            )
            current_high = {
                s["symbol"] for s in current_setups 
                if s["score"] >= 5
            }
            prev_high = {
                s["symbol"] for s in prev_setups 
                if s["score"] >= 5
            }
            new_forming = [
                s for s in current_setups
                if s["symbol"] not in prev_high 
                and s["score"] >= 5
            ]
            improved = [
                s for s in current_setups
                if s["symbol"] in prev_high 
                and s["score"] >= 6
            ]
            if new_forming or improved:
                meta["forming_setups"] = current_setups
                meta["new_forming"] = new_forming
                meta["improved_setups"] = improved
                return True, "setup_forming", meta
        except Exception as e:
            logger.warning("forming setups check failed: %s", e)

    # Check: periodic market update even when quiet
    if session_no is not None and session_no > 0:
        if session_no % 3 == 0:
            cur = conn.cursor()
            try:
                cur.execute(
                    """
                    SELECT sent_at FROM pulse_log
                    WHERE trader_id = %s
                      AND pulse_date = %s
                      AND telegram_sent = TRUE
                    ORDER BY id DESC LIMIT 1
                    """,
                    (trader_id, target_date),
                )
                row = cur.fetchone()
            finally:
                cur.close()

            if row is None:
                return True, "market_update", meta

            last_sent = row[0]
            if last_sent:
                now_utc = datetime.now(timezone.utc)
                if last_sent.tzinfo is None:
                    last_sent = last_sent.replace(tzinfo=timezone.utc)
                mins_since = (now_utc - last_sent).total_seconds() / 60
                if mins_since >= 90:
                    return True, "market_update", meta

    return False, "nothing_actionable", meta


def calculate_position_size(
    entry_price: float,
    stop_loss: float,
    portfolio_size: float,
    risk_pct: float = 2.0,
) -> dict:
    risk_amount = portfolio_size * (risk_pct / 100.0)
    if entry_price <= 0:
        return {
            "risk_amount_bdt": round(risk_amount, 0),
            "max_position_bdt": 0.0,
            "suggested_shares": 0,
            "risk_pct": risk_pct,
        }
    stop_distance_pct = (entry_price - stop_loss) / entry_price
    if stop_distance_pct <= 0:
        return {
            "risk_amount_bdt": round(risk_amount, 0),
            "max_position_bdt": 0.0,
            "suggested_shares": 0,
            "risk_pct": risk_pct,
        }
    max_position_value = risk_amount / stop_distance_pct
    max_shares = int(max_position_value / entry_price)
    return {
        "risk_amount_bdt": round(risk_amount, 0),
        "max_position_bdt": round(max_position_value, 0),
        "suggested_shares": max_shares,
        "risk_pct": risk_pct,
    }


def _load_trader_preferences_json(conn, trader_id: int) -> dict:
    cur = conn.cursor()
    try:
        cur.execute("SELECT preferences FROM traders WHERE id = %s", (trader_id,))
        row = cur.fetchone()
        if not row or row[0] is None:
            return {}
        p = row[0]
        if isinstance(p, dict):
            return p
        if isinstance(p, str):
            try:
                return json.loads(p)
            except json.JSONDecodeError:
                return {}
        return {}
    except Exception:
        return {}
    finally:
        cur.close()


def build_proactive_pulse(
    analysis_summary: dict,
    portfolio: list,
    market_context: dict,
    session_no: int,
    send_reason: str,
    target_date: date,
    conn,
    trader_id: int,
    meta: dict | None = None,
) -> tuple[str, str]:
    """Build system + user message for DeepSeek focused on the pulse trigger."""
    meta = meta or {}
    prefs = _load_trader_preferences_json(conn, trader_id)
    try:
        portfolio_size = float(prefs.get("portfolio_size") or 0)
    except (TypeError, ValueError):
        portfolio_size = 0.0
    try:
        risk_pct = float(prefs["risk_pct"]) if prefs.get("risk_pct") is not None else 2.0
    except (TypeError, ValueError, KeyError):
        risk_pct = 2.0

    system_prompt = SYSTEM_PROMPT.strip()
    if session_no == 0:
        system_prompt = system_prompt + "\n\n" + EOD_INSTRUCTIONS.strip()

    dsex = _float_or_none(market_context.get("dsex_index"))
    dsex_ch = _float_or_none(market_context.get("dsex_change_pct"))

    dhaka = pytz.timezone("Asia/Dhaka")
    now = datetime.now(dhaka)
    market_open = now.replace(hour=10, minute=15, second=0, microsecond=0)
    market_close = now.replace(hour=14, minute=30, second=0, microsecond=0)
    is_market_hours = market_open <= now <= market_close

    data_date_raw = market_context.get("data_date")
    data_date_obj = None
    if isinstance(data_date_raw, date):
        data_date_obj = data_date_raw
    elif isinstance(data_date_raw, str) and data_date_raw.strip():
        try:
            data_date_obj = datetime.fromisoformat(data_date_raw).date()
        except ValueError:
            data_date_obj = None

    today_dhaka = now.date()
    yesterday_dhaka = today_dhaka - timedelta(days=1)
    has_fresh_dsex = (
        (not is_market_hours)
        and dsex is not None
        and data_date_obj in (today_dhaka, yesterday_dhaka)
    )

    if is_market_hours:
        dsex_str = "DSEX: Not available during market hours"
        ch_str = ""
        dsex_context_line = (
            "DSEX: Updates after market close. "
            "Do not mention DSEX value in this message."
        )
    elif not has_fresh_dsex:
        dsex_str = "DSEX: Data not yet available"
        ch_str = ""
        dsex_context_line = (
            "DSEX: Not yet updated. "
            "Do not mention or invent any DSEX value."
        )
    else:
        dsex_str = f"{dsex:.2f}"
        ch_str = f"{dsex_ch:+.1f}%" if dsex_ch is not None else ""
        if ch_str:
            dsex_context_line = f"DSEX: {dsex_str} ({ch_str})"
        else:
            dsex_context_line = f"DSEX: {dsex_str}"

    def _fmt_setup_block(rows: list[dict], *, now_sizing: bool = False) -> str:
        lines: list[str] = []
        for r in rows:
            sym = r.get("symbol")
            s = r.get("setup") or {}
            if not isinstance(s, dict):
                continue
            block = (
                f"{sym} — {s.get('setup_type', '')}\n"
                f"Current price: {s.get('current_price', '')}\n"
                f"ENTRY: {s.get('entry', '')} (zone: {s.get('entry_zone', ['',''])[0]}-{s.get('entry_zone', ['',''])[1]})\n"
                f"TARGET 1: {s.get('target_1', '')} (+{s.get('pct_to_target_1', '')}%) — sell 50% here\n"
                f"TARGET 2: {s.get('target_2', '')} (+{s.get('pct_to_target_2', '')}%) — sell rest here\n"
                f"STOP LOSS: {s.get('stop_loss', '')} ({s.get('pct_to_stop', '')}%) — hard stop\n"
                f"RISK/REWARD: 1:{s.get('rr_1', '')}\n"
                f"WHY: {', '.join(s.get('reasons') or [])}\n"
            )
            if now_sizing:
                entry = _float_or_none(s.get("entry"))
                stop = _float_or_none(s.get("stop_loss"))
                if portfolio_size > 0 and entry and stop and entry > 0 and entry > stop:
                    sizing = calculate_position_size(entry, stop, portfolio_size, risk_pct)
                    block += (
                        f"\nPOSITION SIZING (your {risk_pct}% risk rule):\n"
                        f"Portfolio: {portfolio_size:,.0f} BDT\n"
                        f"Max risk: {sizing['risk_amount_bdt']:,.0f} BDT\n"
                        f"Suggested: {sizing['suggested_shares']} shares "
                        f"({sizing['max_position_bdt']:,.0f} BDT)\n"
                    )
                elif portfolio_size > 0:
                    block += (
                        "\nPOSITION SIZING: need valid entry/stop to size this trade.\n"
                    )
                else:
                    block += (
                        "\nSet your portfolio size to get position sizing: "
                        "reply with your total BDT\n"
                    )
            lines.append(block)
        return "\n".join(lines).strip()


    def _portfolio_lines() -> str:
        lines = []
        for p in portfolio or []:
            lines.append(
                f"- {p.get('symbol')}: qty {p.get('quantity')} @ {p.get('avg_buy_price')} | "
                f"now {p.get('current_price')} | P&L {float(p.get('pnl_pct') or 0):+.1f}%"
            )
        return "\n".join(lines) if lines else "(no open positions)"

    now_rows = meta.get("now_setups")
    if not isinstance(now_rows, list) or not now_rows:
        now_rows = _fetch_buy_setups_for_session(conn, target_date, session_no, "NOW")
    watch_rows = _fetch_buy_setups_for_session(conn, target_date, session_no, "WATCH")

    user_msg = ""

    if send_reason in ("new_trade_setup", "setup_confirmed"):
        user_msg = (
            f"TRADE ALERT — Session {session_no}\n"
            f"Date: {target_date.isoformat()}\n"
            f"{dsex_context_line}\n\n"
            f"NEW SETUP(S) CONFIRMED:\n"
            f"{_fmt_setup_block(now_rows, now_sizing=True)}\n\n"
            f"TRADER PORTFOLIO:\n{_portfolio_lines()}\n\n"
            f"WATCH LIST (forming setups):\n{_fmt_setup_block(watch_rows, now_sizing=False)}\n"
        )

    elif send_reason == "portfolio_stop_loss_alert":
        sym = meta.get("symbol", "")

        # Fetch analysis data for all portfolio positions
        analysis_by_symbol = {}
        if portfolio:
            symbols = [p['symbol'] for p in portfolio]
            cur_a = conn.cursor()
            cur_a.execute("""
                SELECT DISTINCT ON (symbol)
                    symbol,
                    rsi,
                    raw_output->>'volume_price_pattern' as vp,
                    raw_output->'moving_averages'->>'trend' as ma_trend,
                    raw_output->'moving_averages'->>'above_ma50' as above_ma50,
                    raw_output->>'rsi_direction' as rsi_dir,
                    support_levels,
                    resistance_levels
                FROM analysis_results
                WHERE symbol = ANY(%s)
                  AND analysis_date = %s
                ORDER BY symbol, session_no DESC NULLS LAST, id DESC
            """, (symbols, target_date))
            for row in cur_a.fetchall():
                analysis_by_symbol[row[0]] = {
                    'rsi': float(row[1]) if row[1] else None,
                    'vp': row[2] or 'unknown',
                    'ma_trend': row[3] or 'unknown',
                    'above_ma50': row[4],
                    'rsi_dir': row[5] or 'unknown',
                    'support': row[6] or [],
                    'resistance': row[7] or []
                }
            cur_a.close()

        # Build critical positions block
        critical_lines = []
        for p in portfolio:
            sym = p['symbol']
            pnl = float(p.get('pnl_pct') or 0)
            price = p.get('current_price')
            ar = analysis_by_symbol.get(sym, {})
            rsi = ar.get('rsi')
            vp = ar.get('vp', 'unknown')
            ma_trend = ar.get('ma_trend', 'unknown')
            rsi_dir = ar.get('rsi_dir', 'unknown')
            sup = ar.get('support', [])
            sup_str = str(sup[0]) if sup else 'none'
            
            vp_note = {
                'distribution': 'DISTRIBUTION — institutions selling',
                'sellers_exhausted': 'sellers exhausting — possible stabilization',
                'accumulation': 'accumulation — buyers stepping in',
                'weak_rally': 'weak rally — no conviction',
                'mixed': 'mixed — unclear'
            }.get(vp, vp)
            
            if pnl <= -10:
                action = 'EXIT NOW'
            elif pnl <= -8:
                action = 'EXIT ON NEXT BOUNCE'
            elif pnl <= -7.5:
                action = 'AT STOP LOSS ZONE — decide now'
            else:
                action = 'HOLD — watch key levels'
            
            rsi_str = f"{rsi:.1f} ({rsi_dir})" if rsi else 'n/a'
            
            critical_lines.append(
                f"{sym}: {pnl:+.1f}% @ {price}\n"
                f"  RSI: {rsi_str}\n"
                f"  Volume: {vp_note}\n"
                f"  MA trend: {ma_trend}\n"
                f"  Support: {sup_str}\n"
                f"  Action: {action}"
            )
        
        critical_block = "\n\n".join(critical_lines)
        
        # Count how many times stop loss alert fired today
        cur_c = conn.cursor()
        cur_c.execute("""
            SELECT COUNT(*) FROM pulse_log
            WHERE trader_id = %s
              AND pulse_date = %s
              AND deepseek_input::text LIKE '%portfolio_stop_loss_alert%'
              AND telegram_sent = TRUE
        """, (trader_id, target_date))
        alert_count = int((cur_c.fetchone() or [0])[0] or 0)
        cur_c.close()
        
        if alert_count == 0:
            urgency = "POSITION ALERT"
        elif alert_count == 1:
            urgency = "SECOND ALERT — Still holding?"
        elif alert_count == 2:
            urgency = "THIRD ALERT — Loss compounding"
        else:
            urgency = f"ALERT #{alert_count + 1} — URGENT ACTION NEEDED"

        user_msg = (
            f"{urgency}\n"
            f"Session: {session_no} | Date: {target_date}\n"
            f"{dsex_context_line}\n\n"
            f"PORTFOLIO POSITIONS:\n\n"
            f"{critical_block}\n\n"
            f"Stop loss rule: Exit any position beyond -8%. No exceptions.\n"
        )

    elif send_reason == "portfolio_target_hit":
        sym = meta.get("symbol", "")
        user_msg = (
            f"TARGET HIT — {sym}\n\n"
            f"{sym} reached {meta.get('target_price', '')}.\n"
            f"You are up {float(meta.get('pnl_pct') or 0):.1f}% on this position.\n\n"
            f"PORTFOLIO:\n{_portfolio_lines()}\n\n"
            f"Consider: Take partial or full profit.\n"
        )

    elif send_reason == "portfolio_exit_signal":
        sym = meta.get("symbol", "")
        user_msg = (
            f"EXIT SIGNAL — {sym}\n\n"
            f"Latest model signal for {sym} is EXIT.\n\n"
            f"PORTFOLIO:\n{_portfolio_lines()}\n\n"
            f"Action required: Decide whether to exit {sym} now.\n"
        )

    elif send_reason == "market_significant_move":
        if has_fresh_dsex and dsex_ch is not None:
            direction = "up" if dsex_ch >= 0 else "down"
            dsex_move_line = f"DSEX {direction} {abs(dsex_ch):.1f}% (session context)."
        else:
            dsex_move_line = dsex_context_line
        user_msg = (
            f"MARKET MOVE ALERT\n\n"
            f"{dsex_move_line}\n"
            f"Impact on your portfolio:\n{_portfolio_lines()}\n\n"
            f"Active setups (NOW):\n{_fmt_setup_block(now_rows, now_sizing=True)}\n"
        )

    elif send_reason == "first_session":
        prev_dsex = None
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT dsex_index FROM market_summary
                WHERE trade_date < %s
                ORDER BY trade_date DESC
                LIMIT 1
                """,
                (target_date,),
            )
            pr = cur.fetchone()
            if pr:
                prev_dsex = _float_or_none(pr[0])
        finally:
            cur.close()
        prev_s = f"{prev_dsex:.2f}" if prev_dsex is not None else "n/a"
        user_msg = (
            f"MARKET OPEN — Session 1\n"
            f"{dsex_context_line}\nYesterday close: {prev_s}\n\n"
            f"ACTIVE SETUPS TODAY:\n"
            f"NOW:\n{_fmt_setup_block(now_rows, now_sizing=True)}\n\n"
            f"WATCH:\n{_fmt_setup_block(watch_rows, now_sizing=False)}\n\n"
            f"YOUR PORTFOLIO:\n{_portfolio_lines()}\n\n"
            f"Focus for today: prioritise risk, then highest conviction NOW setups.\n"
        )

    elif send_reason == "market_update":
        buy_count = len(analysis_summary.get("buy_signals", []))
        watch_count = analysis_summary.get("watch_signal_total", 0)
        user_msg = (
            f"MARKET UPDATE — Session {session_no}\n"
            f"Date: {target_date.isoformat()}\n"
            f"{dsex_context_line}\n\n"
            f"CURRENT SIGNALS:\n"
            f"BUY: {buy_count} | WATCH: {watch_count}\n\n"
            f"PORTFOLIO:\n{_portfolio_lines()}\n\n"
            f"WATCH LIST:\n"
        )
        for w in get_trader_watchlist(conn, trader_id):
            user_msg += (
                f"- {w['symbol']}: "
                f"{w.get('current_price', 'n/a')} | "
                f"{w.get('signal', 'n/a')}\n"
            )
        if has_fresh_dsex and dsex_ch is not None:
            user_msg += (
                f"\nNo major setups confirmed yet. "
                f"Market is {dsex_ch:+.1f}% "
                f"vs yesterday. Watching for opportunities.\n"
            )
        else:
            user_msg += (
                "\nNo major setups confirmed yet. "
                "DSEX updates after market close. Watching for opportunities.\n"
            )

    elif send_reason == "setup_forming":
        new_setups = meta.get("new_forming", [])
        improved = meta.get("improved_setups", [])
        all_setups = (new_setups + improved)[:3]

        setup_lines = []
        for s in all_setups:
            target_str = (
                f"Target: {s['first_target']}" 
                if s.get("first_target") 
                else "Target: calculating"
            )
            support_str = (
                f"Support: {s['nearest_support']} "
                f"({s['distance_to_support_pct']}% away)"
                if s.get("nearest_support")
                else "Support: none confirmed"
            )
            missing_str = (
                ", ".join(s["missing"]) 
                if s.get("missing") 
                else "all conditions near confirmation"
            )
            setup_lines.append(
                f"{s['symbol']} @ {s['current_price']}"
                f" | Score {s['score']}/8 "
                f"({s['confidence_pct']}%)\n"
                f"Setup: {s['setup_type']}\n"
                f"{support_str}\n"
                f"{target_str}\n"
                f"RSI: {s['rsi']} ({s['rsi_direction']})\n"
                f"Volume: {s['volume_pattern']}\n"
                f"MA trend: {s['ma_trend']}\n"
                f"Still needs: {missing_str}"
            )

        setups_block = "\n\n".join(setup_lines) or "None identified."

        user_msg = (
            f"SETUP FORMING ALERT\n"
            f"Session: {session_no}\n"
            f"Date: {target_date}\n"
            f"{dsex_context_line}\n\n"
            f"SETUPS BUILDING — NOT BUY SIGNALS YET:\n\n"
            f"{setups_block}\n\n"
            f"PORTFOLIO:\n{_portfolio_lines()}\n\n"
            f"These are pre-signals. Watch for confirmation "
            f"next session. If conditions complete, a BUY "
            f"signal will fire."
        )

    else:
        user_msg = (
            f"SESSION {session_no} — {target_date.isoformat()}\n"
            f"{dsex_context_line}\n"
            f"PORTFOLIO:\n{_portfolio_lines()}\n"
        )

    return system_prompt, user_msg




def call_deepseek(system_message: str, user_message: str, temperature: float = 0.3) -> str:
    from openai import OpenAI

    api_key = os.environ.get("DEEPSEEK_API_KEY")
    if not api_key or not api_key.strip():
        raise RuntimeError("DEEPSEEK_API_KEY is missing or empty in .env")

    client = OpenAI(api_key=api_key, base_url="https://api.deepseek.com")
    response = client.chat.completions.create(
        model="deepseek-chat",
        messages=[
            {"role": "system", "content": system_message},
            {"role": "user", "content": user_message},
        ],
        max_tokens=1500,
        temperature=temperature,
        stream=False,
    )
    return (response.choices[0].message.content or "").strip()


_TAG_RE = re.compile(r"<[^>]+>")


def _plain_text_for_telegram_body(model_text: str) -> str:
    """Strip HTML-like tags the model may emit before the body is inserted into the message."""
    if not model_text:
        return ""
    t = _TAG_RE.sub("", model_text)
    t = t.replace("&nbsp;", " ")
    return t.strip()


def get_session_label(session_no: int) -> str:
    if session_no == -1:
        return "Pre-Market Briefing"
    if session_no == 0:
        return "EOD Summary"
    if 1 <= session_no <= 10:
        return f"Intraday Pulse — Session {session_no} of 10"
    return "Market Update"


def format_telegram_message(
    deepseek_response: str,
    analysis: dict,
    portfolio: list,
    target_date: date,
    session_no: int = 0,
    send_reason: str = "nothing_actionable",
    **kwargs: Any,
) -> str:
    """Wrap DeepSeek plain-text body in Telegram HTML header/footer."""
    _ = analysis, portfolio
    pulse_type = kwargs.pop("pulse_type", None)
    kwargs.pop("scorecard", None)
    kwargs.pop("proactive_now_count", None)

    body_raw = _plain_text_for_telegram_body(deepseek_response or "")
    deepseek_response = re.sub(r"\*\*(.*?)\*\*", r"\1", body_raw)
    deepseek_response = re.sub(r"\*(.*?)\*", r"\1", deepseek_response)
    deepseek_response = re.sub(r"__(.*?)__", r"\1", deepseek_response)

    day_str = target_date.strftime("%a %d %b")
    if session_no == -1:
        session_str = "Pre-Market"
    elif session_no == 0:
        session_str = "End of Day"
    else:
        session_str = f"Session {session_no}"

    headers = {
        "new_trade_setup": "🚨 NexTrade — Live Trade Alert",
        "setup_confirmed": "⚡ NexTrade — Setup Confirmed",
        "setup_forming": "🔭 NexTrade — Setup Forming",
        "portfolio_stop_loss_alert": "⚠️ NexTrade — Position Alert",
        "portfolio_target_hit": "💰 NexTrade — Target Hit",
        "market_significant_move": "📊 NexTrade — Market Alert",
        "first_session": "🔔 NexTrade — Market Open",
        "portfolio_exit_signal": "🚨 NexTrade — Exit Alert",
        "nothing_actionable": "📊 NexTrade — Market Pulse",
    }
    header_title = headers.get(send_reason, "📊 NexTrade — Market Update")

    if pulse_type == "premarket":
        head_block = (
            f"<b>🌅 NexTrade — Pre-Market Brief</b>\n"
            f"📅 {day_str} · Market opens at 10:15am"
        )
    elif pulse_type == "eod":
        head_block = (
            f"<b>🌙 NexTrade — End of Day</b>\n"
            f"📅 {day_str} · Market closed"
        )
    else:
        head_block = (
            f"<b>{html.escape(header_title)}</b>\n"
            f"📅 {day_str} · {session_str}"
        )

    message = (
        f"{head_block}\n\n{deepseek_response}\n\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        "<i>🤖 Powered by NexTrade</i>"
    )

    if len(message) > 4000:
        cutoff = message[:3900].rfind("\n")
        if cutoff < 1:
            cutoff = 3900
        message = message[:cutoff]
        message += (
            "\n\n<i>Message trimmed. "
            "Full details in next update.</i>\n"
            "<i>🤖 Powered by NexTrade</i>"
        )

    return message


def _current_session_no_dhaka() -> int:
    tz = pytz.timezone("Asia/Dhaka")
    now_dhaka = datetime.now(tz)
    market_open = time(10, 15)
    market_close = time(14, 45)
    t = now_dhaka.time()
    if t < market_open or t > market_close:
        return 0
    start_dt = now_dhaka.replace(hour=10, minute=15, second=0, microsecond=0)
    elapsed_min = int((now_dhaka - start_dt).total_seconds() // 60)
    session_no = elapsed_min // 30 + 1
    if session_no < 1 or session_no > 10:
        return 0
    return session_no


def pulse_already_sent(conn, trader_id: int, target_date: date, session_no: int) -> bool:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT COUNT(*) FROM pulse_log
            WHERE trader_id = %s
              AND pulse_date = %s
              AND session_no = %s
              AND telegram_sent = TRUE
            """,
            (trader_id, target_date, session_no),
        )
        row = cur.fetchone()
        return bool(row and (row[0] or 0) > 0)
    finally:
        cur.close()


def _insert_pulse_log(
    conn,
    trader_id: int | None,
    target_date: date,
    session_no: int,
    deepseek_input: dict,
    deepseek_output: str,
) -> None:
    cur = conn.cursor()
    try:
        pl_cols = _columns(cur, "pulse_log")
        payload_in = json.dumps(deepseek_input, default=str)
        fields = ["pulse_date", "session_no", "deepseek_input", "deepseek_output", "telegram_sent", "sent_at"]
        values: list[Any] = [target_date, session_no, payload_in, deepseek_output, False, None]
        if "trader_id" in pl_cols and trader_id is not None:
            fields.insert(0, "trader_id")
            values.insert(0, trader_id)
        placeholders = []
        for f in fields:
            if f == "deepseek_input":
                placeholders.append("%s::jsonb")
            else:
                placeholders.append("%s")
        ph = ", ".join(placeholders)
        cols_sql = ", ".join(fields)
        cur.execute(
            f"INSERT INTO pulse_log ({cols_sql}) VALUES ({ph})",
            tuple(values),
        )
    finally:
        cur.close()


def generate_pulse(trader_id: int) -> dict:
    conn = get_db_connection()
    conn.autocommit = True
    session_no = _current_session_no_dhaka()
    dhaka = pytz.timezone("Asia/Dhaka")
    now = datetime.now(dhaka)
    is_market_hours = (
        now.weekday() < 5
        and (now.hour > 10 or (now.hour == 10 and now.minute >= 0))
        and (now.hour < 14 or (now.hour == 14 and now.minute <= 30))
    )
    pulse_type = "intraday" if is_market_hours else "eod"
    analysis: dict = {}
    portfolio: list = []

    try:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM traders WHERE id = %s", (trader_id,))
        if cur.fetchone() is None:
            cur.close()
            err = {"status": "error", "reason": "trader not found"}
            td = get_db_date(conn)
            try:
                _insert_pulse_log(conn, trader_id, td, session_no, err, json.dumps(err))
            except Exception:
                pass
            return err
        cur.close()

        actual_today = get_db_date(conn)
        target_date = actual_today

        if pulse_already_sent(conn, trader_id, target_date, session_no):
            return {
                "status": "skipped",
                "reason": "already_sent_this_session",
                "trader_id": trader_id,
                "session_no": session_no,
            }

        from api.health import check_data_freshness

        health = check_data_freshness(conn)
        data_warning = (
            "NOTE: " + "; ".join(health["warnings"])
            if health.get("warnings")
            else ""
        )
        if not health["healthy"]:
            from pulse.telegram import send_telegram_message

            alert_msg = (
                "⚠️ <b>NexTrade Data Alert</b>\n\n"
                "Pulse generation paused — data issues detected:\n"
            )
            for issue in health["issues"]:
                alert_msg += f"- {html.escape(issue)}\n"
            alert_msg += "\nWill retry at next scheduled run."

            skip = {
                "status": "skipped",
                "reason": "data_not_fresh",
                "issues": health["issues"],
                "warnings": health["warnings"],
            }
            try:
                _insert_pulse_log(conn, trader_id, target_date, session_no, skip, json.dumps(skip))
            except Exception:
                pass

            cur = conn.cursor()
            try:
                cur.execute(
                    "SELECT telegram_chat_id FROM traders WHERE id=%s",
                    (trader_id,),
                )
                row = cur.fetchone()
            finally:
                cur.close()
            if row and row[0]:
                try:
                    send_telegram_message(chat_id=str(row[0]), message=alert_msg)
                except Exception as tg_err:
                    logger.warning("Data alert Telegram send failed: %s", tg_err)

            return {
                "status": "skipped",
                "reason": "data_not_fresh",
                "issues": health["issues"],
                "warnings": health["warnings"],
            }

        analysis = get_analysis_summary(conn, target_date)
        analysis["market_context"] = enrich_market_context_pulse(
            conn, actual_today, get_market_context(conn, actual_today)
        )
        try:
            from analysis.engine import detect_market_trend
            analysis["market_trend"] = detect_market_trend(conn)
        except Exception:
            analysis["market_trend"] = {}

        portfolio = get_trader_portfolio(conn, trader_id)

        should_send, send_reason, meta = should_send_pulse(
            conn,
            trader_id,
            target_date,
            session_no,
            analysis,
            portfolio,
            analysis.get("market_context") or {},
        )

        if not should_send:
            skip = {
                "status": "skipped",
                "reason": "nothing_actionable",
                "detail": send_reason,
                "session_no": session_no,
                "trader_id": trader_id,
            }
            try:
                _insert_pulse_log(conn, trader_id, target_date, session_no, skip, json.dumps(skip))
            except Exception:
                pass
            return {
                "status": "skipped",
                "reason": "nothing_actionable",
                "session_no": session_no,
                "trader_id": trader_id,
            }

        scorecard: dict = {}
        try:
            from analysis.evaluator import (
                evaluate_past_signals,
                get_accuracy_context_for_deepseek,
            )
            evaluate_past_signals(days_back=7)
            scorecard = get_scorecard_data(conn, trader_id)
            _ = get_accuracy_context_for_deepseek()
        except Exception as ev_err:
            logger.warning("Evaluator error (non-fatal): %s", ev_err)

        system_msg, user_msg = build_proactive_pulse(
            analysis,
            portfolio,
            analysis.get("market_context") or {},
            session_no,
            send_reason,
            target_date,
            conn,
            trader_id,
            meta=meta,
        )
        mc = analysis.get("market_context") or {}
        market_snapshot = {
            "dsex_change_pct": mc.get("dsex_change_pct"),
            "dsex_index": mc.get("dsex_index"),
        }
        deepseek_input = {
            "system": system_msg,
            "user": user_msg,
            "proactive_reason": send_reason,
            "market_snapshot": market_snapshot,
        }

        n_now = len(_fetch_buy_setups_for_session(conn, target_date, session_no, "NOW"))

        try:
            deepseek_output = call_deepseek(system_msg, user_msg)
        except Exception as e:
            deepseek_output = f"DeepSeek API error: {e}\n{traceback.format_exc()}"
            telegram_message = format_telegram_message(
                deepseek_output,
                analysis,
                portfolio,
                target_date,
                session_no,
                send_reason,
                pulse_type=("eod" if pulse_type == "eod" else None),
            )
            try:
                _insert_pulse_log(conn, trader_id, target_date, session_no, deepseek_input, deepseek_output)
            except Exception:
                pass
            return {
                "status": "error",
                "message": str(e),
                "trader_id": trader_id,
                "pulse_date": target_date.isoformat(),
                "telegram_message": telegram_message,
                "send_reason": send_reason,
                "buy_signals": int(analysis.get("buy_signal_total") or len(analysis.get("buy_signals") or [])),
                "watch_signals": int(analysis.get("watch_signal_total") or len(analysis.get("watch_signals") or [])),
                "exit_signals": len(analysis.get("exit_signals") or []),
                "portfolio_positions": len(portfolio),
            }

        telegram_message = format_telegram_message(
            deepseek_output,
            analysis,
            portfolio,
            target_date,
            session_no,
            send_reason,
            pulse_type=("eod" if pulse_type == "eod" else None),
        )
        try:
            _insert_pulse_log(conn, trader_id, target_date, session_no, deepseek_input, deepseek_output)
        except Exception:
            pass

        return {
            "status": "ok",
            "trader_id": trader_id,
            "pulse_date": target_date.isoformat(),
            "telegram_message": telegram_message,
            "send_reason": send_reason,
            "buy_signals": int(analysis.get("buy_signal_total") or len(analysis.get("buy_signals") or [])),
            "watch_signals": int(analysis.get("watch_signal_total") or len(analysis.get("watch_signals") or [])),
            "exit_signals": len(analysis.get("exit_signals") or []),
            "portfolio_positions": len(portfolio),
        }
    except Exception as e:
        err_body = f"pulse error: {e}\n{traceback.format_exc()}"
        try:
            td = get_db_date(conn)
            _insert_pulse_log(
                conn,
                trader_id,
                td,
                session_no,
                {"error": str(e)},
                err_body,
            )
        except Exception:
            pass
        return {
            "status": "error",
            "message": str(e),
            "traceback": traceback.format_exc(),
        }
    finally:
        conn.close()



def generate_premarket_briefing(trader_id: int) -> dict:
    from datetime import timedelta

    conn = get_db_connection()
    conn.autocommit = True

    try:
        cur = conn.cursor()
        cur.execute("SELECT name FROM traders WHERE id = %s", (trader_id,))
        row = cur.fetchone()
        if row is None:
            cur.close()
            return {"status": "error", "reason": "trader not found"}
        trader_name = (row[0] or "").strip() or f"Trader {trader_id}"
        cur.close()

        target_date = get_db_date(conn)
        analysis_date = target_date - timedelta(days=1)
        session_no = -1

        if pulse_already_sent(conn, trader_id, target_date, session_no):
            return {
                "status": "skipped",
                "reason": "already_sent_this_session",
                "trader_id": trader_id,
                "session_no": session_no,
            }

        # --- Yesterday's market summary (last 2 rows to compute change) ---
        cur = conn.cursor()
        cur.execute(
            """
            SELECT trade_date, dsex_index, dses_index, total_volume, total_value_mn
            FROM market_summary
            ORDER BY trade_date DESC
            LIMIT 2
            """
        )
        ms_rows = cur.fetchall()
        cur.close()

        dsex = dses = volume = value_mn = None
        dsex_change = None
        ms_date = None
        if ms_rows:
            r = ms_rows[0]
            ms_date = r[0]
            dsex = _float_or_none(r[1])
            dses = _float_or_none(r[2])
            volume = int(r[3]) if r[3] is not None else None
            value_mn = _float_or_none(r[4])
            if len(ms_rows) >= 2 and dsex and ms_rows[1][1]:
                prev_dsex = _float_or_none(ms_rows[1][1])
                if prev_dsex and prev_dsex != 0:
                    dsex_change = round((dsex - prev_dsex) / prev_dsex * 100, 2)

        # --- Yesterday's top BUY signals ---
        yesterday = analysis_date
        cur = conn.cursor()
        cur.execute(
            """
            SELECT DISTINCT ON (symbol)
                symbol, confidence_score, support_levels,
                resistance_levels, raw_output
            FROM analysis_results
            WHERE analysis_date = %s AND overall_signal = 'BUY'
            ORDER BY symbol, confidence_score DESC NULLS LAST
            LIMIT 5
            """,
            (yesterday,),
        )
        buy_rows = cur.fetchall()
        cur.close()

        buy_signals: list[dict] = []
        for sym, conf, sup, res, raw in buy_rows:
            r = raw if isinstance(raw, dict) else {}
            sr = r.get("sr") or {}
            cp = _float_or_none(sr.get("current_price")) or _float_or_none(r.get("current_price"))
            reason = r.get("signal_reason") or ""
            buy_signals.append({
                "symbol": sym,
                "confidence": _float_or_none(conf) or 0.0,
                "current_price": cp,
                "support": sup if sup else [],
                "resistance": res if res else [],
                "reason": reason,
            })
        buy_signals.sort(key=lambda x: x["confidence"], reverse=True)

        analysis = get_analysis_summary(conn, analysis_date)

        # --- Portfolio and watchlist ---
        portfolio = get_trader_portfolio(conn, trader_id)
        watchlist = get_trader_watchlist(conn, trader_id)

        # For each portfolio position, get support from latest analysis
        cur = conn.cursor()
        support_by_sym: dict[str, list] = {}
        for pos in portfolio:
            sym = pos["symbol"]
            cur.execute(
                """
                SELECT DISTINCT ON (symbol) support_levels
                FROM analysis_results
                WHERE symbol = %s
                ORDER BY symbol, analysis_date DESC, session_no DESC NULLS LAST, id DESC
                """,
                (sym,),
            )
            ar = cur.fetchone()
            support_by_sym[sym] = ar[0] if ar and ar[0] else []
        cur.close()

        # --- Build prompts ---
        lines: list[str] = []
        lines.append(f"PRE-MARKET BRIEFING — {target_date.isoformat()}")
        lines.append("Market opens at 10:15am (Asia/Dhaka).")
        lines.append("")
        lines.append("YESTERDAY'S MARKET:")
        if dsex is not None:
            chg_str = f" ({dsex_change:+.2f}%)" if dsex_change is not None else ""
            lines.append(f"DSEX: {dsex}{chg_str}")
            lines.append(f"Volume: {volume:,} | Value: {value_mn}mn")
        else:
            lines.append("Market data unavailable.")
        lines.append("")
        lines.append(f"YESTERDAY'S TOP BUY SIGNALS ({len(buy_signals)}):")
        for b in buy_signals:
            cp = b["current_price"] or 0.0
            conf_pct = b["confidence"] * 100
            res = b["resistance"]
            sup = b["support"]
            lines.append(
                f"- {b['symbol']} @ {cp} | Confidence: {conf_pct:.0f}%\n"
                f"  Key resistance: {res[0] if res else 'N/A'} | "
                f"Key support: {sup[0] if sup else 'N/A'}"
            )
        lines.append("")
        if not portfolio:
            lines.append("TRADER PORTFOLIO: Empty. No open positions.")
        else:
            lines.append(f"TRADER PORTFOLIO ({len(portfolio)} positions):")
            for p in portfolio:
                sym = p["symbol"]
                sup = support_by_sym.get(sym, [])
                sup_str = str(sup[0]) if sup else "N/A"
                at_sl = " — at stop-loss threshold; review at open" if (p.get("pnl_pct") or 0) <= -8 else ""
                lines.append(
                    f"- {sym}: {p['quantity']} shares @ avg {p['avg_buy_price']}\n"
                    f"  Current: {p['current_price']} | P&L: {p.get('pnl_pct', 0):+.1f}%\n"
                    f"  Key support: {sup_str}{at_sl}"
                )
        lines.append("")
        if not watchlist:
            watchlist_lines = ["No watchlist stocks configured yet."]
            lines.append("WATCHLIST: empty — trader has not set up a watchlist yet.")
            lines.extend(watchlist_lines)
        else:
            watchlist_lines = [
                f"- {w['symbol']}: target {w.get('target_price')}, "
                f"signal {w.get('signal')}"
                for w in watchlist
            ]
            lines.append(f"WATCHLIST ({len(watchlist)} stocks):")
            lines.extend(watchlist_lines)

        user_msg = "\n".join(lines)

        system_msg = SYSTEM_PROMPT.strip() + "\n\n" + PREMARKET_INSTRUCTIONS.strip()

        deepseek_input = {"system": system_msg, "user": user_msg}

        try:
            deepseek_output = call_deepseek(system_msg, user_msg, temperature=0.2)
        except Exception as e:
            deepseek_output = f"DeepSeek API error: {e}"

        # --- Format Telegram message (shared header/footer with pulse formatter) ---
        premarket_analysis = {
            "buy_signal_total": int(analysis.get("buy_signal_total") or len(buy_signals)),
            "watch_signal_total": int(analysis.get("watch_signal_total") or 0),
            "exit_signals": analysis.get("exit_signals") or [],
            "total_analysed": int(analysis.get("total_analysed") or 0),
        }
        telegram_message = format_telegram_message(
            deepseek_output,
            premarket_analysis,
            portfolio,
            target_date,
            session_no,
            send_reason="nothing_actionable",
            pulse_type="premarket",
        )

        try:
            _insert_pulse_log(conn, trader_id, target_date, session_no, deepseek_input, deepseek_output)
        except Exception:
            pass

        return {
            "status": "ok",
            "type": "premarket",
            "trader_id": trader_id,
            "pulse_date": target_date.isoformat(),
            "telegram_message": telegram_message,
            "buy_signals_yesterday": len(buy_signals),
            "portfolio_positions": len(portfolio),
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "traceback": traceback.format_exc(),
        }
    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    tid = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    if len(sys.argv) > 2 and sys.argv[2] == "premarket":
        result = generate_premarket_briefing(tid)
        print(result.get("telegram_message", result))
    else:
        result = generate_pulse(tid)
        print("Telegram message preview:")
        print("=" * 50)
        print(result.get("telegram_message", result))
        print("=" * 50)
        if result.get("status") == "ok":
            print(
                f"Buy: {result['buy_signals']} | "
                f"Watch: {result['watch_signals']} | "
                f"Exit: {result['exit_signals']}"
            )

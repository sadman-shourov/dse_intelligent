from __future__ import annotations

import html
import json
import logging
import os
import re
import sys
import traceback
from collections import defaultdict
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg2
import pytz
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

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


def get_trader_preferences(conn, trader_id: int) -> dict:
    """Fetch trader preferences by joining traders + trader_preferences."""
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
                t.name,
                t.trading_style,
                t.holding_period,
                t.risk_tolerance,
                t.strategy_notes,
                t.onboarding_complete,
                tp.preferred_signals,
                tp.avoid_signals,
                tp.notes AS detailed_notes
            FROM traders t
            LEFT JOIN trader_preferences tp ON t.id = tp.trader_id
            WHERE t.id = %s
            """,
            (trader_id,),
        )
        row = cur.fetchone()
    finally:
        cur.close()

    if not row:
        return {}
    return {
        "name": row[0] or f"Trader {trader_id}",
        "trading_style": row[1] or "not specified",
        "holding_period": row[2] or "not specified",
        "risk_tolerance": row[3] or "moderate",
        "strategy_notes": row[4] or "",
        "onboarding_complete": bool(row[5]),
        "preferred_signals": row[6] or "",
        "avoid_signals": row[7] or "",
        "detailed_notes": row[8] or "",
    }


def get_trader_stock_intents(conn, trader_id: int) -> list:
    """Fetch active stock intents; price from today's analysis (live tick) with price_history fallback."""
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT DISTINCT ON (tsi.symbol)
                tsi.symbol,
                tsi.avg_buy_price,
                tsi.intent,
                tsi.target_price,
                tsi.stop_price,
                tsi.timeframe,
                tsi.notes,
                ar.overall_signal AS current_signal,
                ar.confidence_score AS confidence,
                COALESCE(
                    NULLIF(trim(ar.raw_output->>'current_price'), '')::numeric,
                    ph.ltp
                ) AS current_price
            FROM trader_stock_intents tsi
            LEFT JOIN (
                SELECT DISTINCT ON (symbol)
                    symbol, overall_signal, confidence_score, raw_output
                FROM analysis_results
                WHERE analysis_date = CURRENT_DATE
                ORDER BY symbol, session_no DESC NULLS LAST, id DESC
            ) ar ON tsi.symbol = ar.symbol
            LEFT JOIN (
                SELECT DISTINCT ON (symbol)
                    symbol, ltp
                FROM price_history
                ORDER BY symbol, date DESC
            ) ph ON tsi.symbol = ph.symbol
            WHERE tsi.trader_id = %s
              AND tsi.is_active = TRUE
            ORDER BY tsi.symbol, tsi.updated_at DESC
            """,
            (trader_id,),
        )
        rows = cur.fetchall()
    finally:
        cur.close()

    intents = []
    for r in rows:
        avg = float(r[1]) if r[1] else 0.0
        current = float(r[9]) if r[9] is not None else 0.0
        pnl = round(((current - avg) / avg * 100), 2) if avg > 0 else 0.0
        conf = r[8]
        intents.append({
            "symbol": r[0],
            "avg_buy_price": avg,
            "intent": r[2],
            "target_price": float(r[3]) if r[3] else None,
            "stop_price": float(r[4]) if r[4] else None,
            "timeframe": r[5] or "",
            "notes": r[6] or "",
            "current_signal": r[7] or "",
            "confidence": float(conf) if conf is not None else None,
            "current_price": current,
            "pnl_pct": pnl,
        })
    return intents


def build_trader_context(preferences: dict, stock_intents: list) -> str:
    """Build personalised trader context string for injection into DeepSeek prompt."""
    if not preferences:
        return ""

    context = (
        f"Trader profile:\n"
        f"Name: {preferences.get('name', 'Trader')}\n"
        f"Style: {preferences.get('trading_style', 'not specified')}\n"
        f"Holding period: {preferences.get('holding_period', 'not specified')}\n"
        f"Risk tolerance: {preferences.get('risk_tolerance', 'moderate')}\n"
        f"Strategy: {preferences.get('strategy_notes', 'none provided')}\n"
    )

    if stock_intents:
        context += "\nStock-specific intents:\n"
        for s in stock_intents:
            pnl_str = f"{s['pnl_pct']:+.1f}%" if s.get("pnl_pct") is not None else "N/A"
            sig = s.get("current_signal") or ""
            conf = s.get("confidence")
            conf_str = f"{conf:.2f}" if conf is not None else ""
            extra = ""
            if sig or conf_str:
                extra = f" | signal: {sig}" + (f" (conf {conf_str})" if conf_str else "")
            context += (
                f"- {s['symbol']}: avg {s['avg_buy_price']} | "
                f"now {s['current_price']} ({pnl_str}) | "
                f"intent: {s['intent']} | "
                f"target: {s['target_price']} | "
                f"timeframe: {s['timeframe']}{extra}\n"
            )

    return context


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


def build_system_prompt(preferences: dict) -> str:
    """Build system prompt dynamically from trader preferences stored in DB."""
    raw_style = preferences.get("trading_style") or "swing"
    trading_style = str(raw_style).strip().lower() if raw_style is not None else "swing"
    if trading_style in ("", "not specified", "none"):
        trading_style = "swing"

    raw_risk = preferences.get("risk_tolerance") or "moderate"
    risk_tolerance = str(raw_risk).strip().lower() if raw_risk is not None else "moderate"
    if risk_tolerance not in ("conservative", "moderate", "aggressive"):
        risk_tolerance = "moderate"

    holding_period = preferences.get("holding_period") or "days to weeks"
    holding_period = str(holding_period).strip() if holding_period is not None else "days to weeks"
    if holding_period.lower() in ("", "not specified", "none"):
        holding_period = "days to weeks"

    strategy_notes = str(preferences.get("strategy_notes") or "").strip()

    prompt = """You are NexTrade, a DSE trading intelligence assistant. Generate a sharp, personalised market pulse.

CRITICAL FORMATTING RULES:
- Plain text only
- No ALL CAPS anywhere
- No asterisks, no markdown, no bullet symbols
- Use plain dashes for lists
- Sentence case throughout
- Max 400 words
- Always use actual price levels
- Never use HTML or angle-bracket tags in your reply (no <b>, </b>, <i>, etc.)
- Write stock symbols and emphasis as plain words only; the app formats the outer Telegram message

"""

    if trading_style == "position":
        prompt += f"""TRADER IS A POSITION TRADER:
- Holding period: {holding_period}
- Never suggest scalps or intraday trades
- Never suggest "short-term bounce" plays
- Focus on multi-week/month setups only
- Exits should be at resistance zones, not arbitrary percentages
- Respect stated HOLD intents unless there is a clear structural breakdown (price closes below major support on high volume)
- Wider stop context — trader accepts drawdowns if thesis is intact

"""
    elif trading_style == "swing":
        prompt += f"""TRADER IS A SWING TRADER:
- Holding period: {holding_period}
- Focus on 5-15 day momentum setups
- Clear entry and exit levels required
- Tighter stops acceptable

"""
    elif trading_style == "intraday":
        prompt += """TRADER IS AN INTRADAY TRADER:
- Focus on today's volume spikes and breakouts
- Tight stops — preserve capital
- Only high conviction setups

"""

    if risk_tolerance == "conservative":
        prompt += """RISK PROFILE: CONSERVATIVE
- Always lead with downside risk
- Flag any position down more than 5% as requiring attention
- Prefer capital preservation over gains

"""
    elif risk_tolerance == "moderate":
        prompt += """RISK PROFILE: MODERATE
- Balance risk and reward in all advice
- Flag positions down more than 8% as urgent
- Can hold through moderate drawdowns

"""
    elif risk_tolerance == "aggressive":
        prompt += """RISK PROFILE: AGGRESSIVE
- Can highlight higher risk/reward setups
- Flag positions down more than 12% as urgent
- Trader accepts volatility for bigger gains

"""

    if strategy_notes:
        prompt += f"""TRADER'S OWN STRATEGY NOTES:
{strategy_notes}
Follow these rules when giving advice.

"""

    prompt += """STOCK INTENT RULES:
- For each stock in trader's intents section: give personalised advice aligned with their stated plan
- Never contradict a HOLD intent unless price has closed below major support
- For EXIT intents: flag when bounce or resistance levels are reached
- For WATCH intents: flag when entry conditions are met
- Always reference trader's avg buy price and show P&L clearly

CRITICAL: You must ONLY reference portfolio positions that are explicitly listed in the TRADER PORTFOLIO section below. Never invent, assume, or reference any position not in that list. If the portfolio section shows 0 positions, say the portfolio is empty. If it shows 2 positions, only discuss those 2. Fabricating positions is a serious error.

CRITICAL: Every pulse must open with a FRESH observation specific to THIS session. You are FORBIDDEN from using the same opening sentence as any previous session.

For each session observe:
- Is DSEX up or down vs yesterday's close?
- Is volume for this session higher or lower than the previous session?
- Which sector is leading today?
- What is the intraday trend so far (up/flat/down)?

Open with ONE of these observations — rotate them.
Never start two consecutive pulses the same way.

For positions flagged CRITICAL or URGENT, do NOT repeat the same advice from previous sessions. Instead ESCALATE — use stronger language each time. If a position has been flagged 5+ times, the trader is ignoring the advice. Address this directly and firmly.

RESPONSE STRUCTURE:
1. Market overview (2 sentences, specific levels)
2. Top 2-3 market opportunities aligned with trader's style
3. Portfolio review — every stock in their intents with personalised one-line advice
4. One key risk reminder

"""

    return prompt


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


def build_pulse_prompt(
    analysis: dict,
    portfolio: list[dict],
    watchlist: list[dict],
    trader_id: int,
    target_date: date,
    scorecard: dict | None = None,
    accuracy_context: str = "",
    trader_context: str = "",
    preferences: dict | None = None,
    stock_intents: list | None = None,
    data_warning: str = "",
    conn=None,
    session_no: int = 0,
) -> tuple[str, str]:
    # Base rules from DB preferences + trader profile / intents on the system message
    prefs = preferences or {}
    intents = stock_intents or []
    base_system = build_system_prompt(prefs)
    personalisation_rules = (trader_context or "").strip()
    if not personalisation_rules:
        personalisation_rules = build_trader_context(prefs, intents).strip()
    effective_system = base_system
    if personalisation_rules:
        effective_system = base_system + "\n\n" + personalisation_rules + "\n"

    mc = analysis.get("market_context") or {}
    dsex = mc.get("dsex_index")
    vol = mc.get("total_volume")
    tvm = mc.get("total_value_mn")

    buys = sorted(analysis.get("buy_signals") or [], key=lambda x: x["confidence"], reverse=True)[:5]
    watches = sorted(analysis.get("watch_signals") or [], key=lambda x: x["confidence"], reverse=True)[:5]
    exits = analysis.get("exit_signals") or []
    buy_total = int(analysis.get("buy_signal_total") or len(analysis.get("buy_signals") or []))

    if mc.get("is_stale") or dsex is None:
        market_line = "Market index data unavailable today."
    else:
        market_line = f"DSEX: {dsex} | Volume: {int(vol or 0):,} | Value: {tvm}mn"

    # Market trend context
    trend_data = analysis.get("market_trend") or {}
    if trend_data and trend_data.get("trend", "unknown") != "unknown":
        _td = str(trend_data.get("trend", "unknown")).replace("_", " ").strip()
        trend_direction = _td.title() if _td else "Unknown"
        trend_line = (
            f"Market trend:\n"
            f"Direction: {trend_direction}\n"
            f"Consecutive down days: {trend_data.get('consecutive_down_days', 0)}\n"
            f"5-day DSEX change: {trend_data.get('dsex_5d_change_pct', 0):+.1f}%"
        )
    else:
        trend_line = ""

    lines: list[str] = []
    lines.append(f"DATE: {target_date.isoformat()}")
    lines.append("")
    lines.append("Market overview:")
    lines.append(market_line)
    if trend_line:
        lines.append("")
        lines.append(trend_line)

    dsex_change_pct = 0.0
    if isinstance(mc.get("dsex_change_pct"), (int, float)):
        dsex_change_pct = float(mc.get("dsex_change_pct") or 0.0)
    else:
        dsex_now = _float_or_none(mc.get("dsex_index"))
        dsex_prev = _float_or_none((mc.get("previous") or {}).get("dsex_index"))
        if dsex_now is not None and dsex_prev not in (None, 0):
            dsex_change_pct = ((dsex_now - dsex_prev) / dsex_prev) * 100.0

    intraday_trend = str((analysis.get("market_trend") or {}).get("trend") or "unknown").replace("_", " ")
    vol_now = _float_or_none(mc.get("total_volume")) or 0.0
    vol_prev = _float_or_none((mc.get("previous") or {}).get("total_volume")) or 0.0
    if vol_prev > 0:
        vol_delta = ((vol_now - vol_prev) / vol_prev) * 100.0
        vol_comparison = f"{vol_delta:+.1f}% vs yesterday"
    else:
        vol_comparison = "n/a"

    movers = get_top_movers_today(conn, session_no, limit=3)
    movers_str = ", ".join(
        f"{m.get('symbol')} ({(m.get('change_pct') or 0):+.2f}%)" for m in movers
    ) if movers else "n/a"

    prev_counts = get_previous_session_counts(conn, target_date, session_no)

    lines.append("")
    lines.append("INTRADAY CONTEXT (use this for your opening):")
    lines.append(f"Session: {session_no} of 10")
    lines.append(f"Intraday trend so far: {intraday_trend}")
    lines.append(f"Volume vs yesterday: {vol_comparison}")
    lines.append(f"Top movers this session: {movers_str}")
    lines.append(f"DSEX change today: {dsex_change_pct:+.2f}%")
    lines.append(
        f"Previous session signal count: BUY {prev_counts['buy']} WATCH {prev_counts['watch']} EXIT {prev_counts['exit']}"
    )

    lines.append("")
    lines.append(f"Top buy signals today ({buy_total}):")
    for b in buys:
        cp = float(b.get("current_price") or 0.0)
        sl = cp * 0.92
        conf_pct = (b.get("confidence") or 0) * 100.0
        lines.append(
            f"- {b.get('symbol')} ({b.get('stock_class')}) @ {cp}\n"
            f"  Signal: {b.get('reason')}\n"
            f"  Support: {b.get('support')} | Resistance: {b.get('resistance')}\n"
            f"  RSI: {b.get('rsi')} | Confidence: {conf_pct:.0f}%\n"
            f"  Stop-loss: {sl:.2f}"
        )
    lines.append("")
    lines.append("Watch list signals (top 5):")
    for w in watches:
        lines.append(f"- {w.get('symbol')} @ {w.get('current_price')} — {w.get('reason')}")
    lines.append("")
    lines.append("Exit signals:")
    for e in exits:
        lines.append(f"- {e.get('symbol')} — {e.get('reason')}")
    lines.append("")
    lines.append(f"Trader portfolio ({len(portfolio)} positions):")
    lines.append(f"[EXACT PORTFOLIO — {len(portfolio)} positions — reference ONLY these]")
    if not portfolio:
        lines.append("[PORTFOLIO IS EMPTY — trader has no open positions — do not invent any]")
    for p in portfolio:
        lines.append(
            f"- {p.get('symbol')}: {p.get('quantity')} shares @ avg {p.get('avg_buy_price')}\n"
            f"  Current: {p.get('current_price')} | P&L: {p.get('pnl_pct'):+.1f}% ({p.get('pnl_value'):+.0f} BDT)\n"
            f"  Signal: {p.get('signal')} — {p.get('reason')}"
        )
        if (p.get("pnl_pct") or 0) <= -8.0:
            alert_count = get_position_alert_count(conn, trader_id, str(p.get("symbol")), target_date)
            pnl_v = float(p.get("pnl_pct") or 0.0)
            sym = str(p.get("symbol") or "UNKNOWN")
            if alert_count >= 5:
                urgency_text = (
                    f"🚨 CRITICAL — {sym} DOWN {pnl_v:.1f}% FOR {alert_count} CONSECUTIVE SESSIONS. "
                    f"IMMEDIATE EXIT REQUIRED. EVERY SESSION YOU HOLD INCREASES YOUR LOSS."
                )
            elif alert_count >= 3:
                urgency_text = (
                    f"⚠️ URGENT — {sym} DOWN {pnl_v:.1f}% FOR {alert_count} SESSIONS. "
                    f"EXIT ON NEXT AVAILABLE PRICE. NO EXCEPTIONS."
                )
            else:
                urgency_text = (
                    f"❗ ALERT — {sym} DOWN {pnl_v:.1f}%. "
                    f"STOP-LOSS THRESHOLD BREACHED. REVIEW POSITION."
                )
            lines.append(f"  {urgency_text}")
    lines.append("[END OF PORTFOLIO — do not reference any other stocks as portfolio positions]")
    lines.append("")
    lines.append(f"Watchlist ({len(watchlist)} stocks):")
    for w in watchlist:
        lines.append(
            f"- {w.get('symbol')} — Current: {w.get('current_price')} | Signal: {w.get('signal')}"
        )

    # Stock-specific intents from trader preferences
    intents = stock_intents or []
    if intents:
        lines.append("")
        lines.append(f"Trader stock intents ({len(intents)}):")
        for s in intents:
            pnl_str = f"{s['pnl_pct']:+.1f}%" if s.get("pnl_pct") is not None else "N/A"
            target = s.get("target_price")
            stop = s.get("stop_price")
            lines.append(
                f"- {s['symbol']}: avg buy {s['avg_buy_price']} | now {s['current_price']} ({pnl_str}) | "
                f"intent: {s['intent']} | target: {target} | stop: {stop} | timeframe: {s.get('timeframe', '')}"
            )
    lines.append("")
    buy_count = int(analysis.get("buy_signal_total") or len(analysis.get("buy_signals") or []))
    watch_count = int(analysis.get("watch_signal_total") or len(analysis.get("watch_signals") or []))
    exit_count = len(analysis.get("exit_signals") or [])
    total_a = analysis.get("total_analysed", 0)
    lines.append("Market class breakdown:")
    lines.append(f"BUY signals: {buy_count} | WATCH: {watch_count} | EXIT: {exit_count}")
    lines.append(f"Stocks analysed: {total_a}")
    lines.append(f"(trader_id={trader_id})")

    # Inject scorecard and accuracy context
    if scorecard:
        sc = scorecard
        lines.append("")
        lines.append("NexTrade performance scorecard:")
        if sc.get("win_rate") is None:
            lines.append(sc.get("message") or "Building accuracy baseline — insufficient data")
        else:
            lines.append(
                f"Win rate: {sc['win_rate']}% "
                f"({sc.get('wins', 0)} wins / {sc.get('losses', 0)} losses, unresolved {sc.get('unresolved', 0)}) "
                f"| Sample size: {sc.get('sample_size', 0)}"
            )
            lines.append(f"Avg P&L on resolved: {sc.get('avg_pnl_on_resolved', 0):+.2f}%")
        if sc.get("top_wins"):
            lines.append("Recent wins:")
            for w in sc["top_wins"][:3]:
                lines.append(
                    f"✅ {w.get('symbol')} {w.get('signal_type', '')} @ {w.get('price_at_signal')} "
                    f"→ {w.get('price_at_eval')} ({(w.get('pnl_pct') or 0):+.1f}%)"
                )
        losses_key = sc.get("recent_losses") or sc.get("top_losses") or []
        if losses_key:
            lines.append("Recent losses:")
            for l in losses_key[:3]:
                lines.append(
                    f"❌ {l.get('symbol')} {l.get('signal_type', '')} @ {l.get('price_at_signal')} "
                    f"→ {l.get('price_at_eval')} ({(l.get('pnl_pct') or 0):+.1f}%)"
                )
    if accuracy_context:
        lines.append("")
        lines.append(accuracy_context)

    dw = (data_warning or "").strip()
    if dw:
        lines.append("")
        lines.append(dw)

    return effective_system, "\n".join(lines)


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
    """Strip HTML-like tags the model may emit; body is then html-escaped for Telegram HTML mode."""
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
    pulse_type: str = "eod",
    scorecard: dict | None = None,
) -> str:
    buy_count = int(analysis.get("buy_signal_total") or len(analysis.get("buy_signals") or []))
    watch_count = int(analysis.get("watch_signal_total") or len(analysis.get("watch_signals") or []))
    exit_count = len(analysis.get("exit_signals") or [])
    total_a = analysis.get("total_analysed", 0)

    body = html.escape(_plain_text_for_telegram_body(deepseek_response or ""), quote=True)
    session_label = get_session_label(session_no)
    header = f"📊 <b>NexTrade {session_label}</b>"
    if session_no == -1:
        subheader = f"📅 {target_date.strftime('%a %d %b')} | Market opens at 10:00am"
    elif 1 <= session_no <= 10:
        subheader = f"📅 {target_date.strftime('%d %b')} | Live market update"
    elif session_no == 0:
        subheader = f"📅 {target_date.strftime('%a %d %b')} | Market closed"
    else:
        subheader = f"📅 {target_date.strftime('%a %d %b')} | Market update"
    header = f"{header}\n{subheader}"

    # NexTrade accuracy scorecard section
    sc = scorecard or {}
    if sc.get("total_evaluated", 0) > 0:
        scorecard_section = (
            "\n━━━━━━━━━━━━━━━━━━\n"
            "📈 <b>NexTrade Scorecard</b> (last 7 days)\n"
            f"Win rate: <b>{sc['win_rate']:.0f}%</b> | "
            f"{sc['wins']}W {sc['losses']}L {sc['neutrals']}N\n"
            f"Avg P&amp;L: <b>{sc['avg_pnl_pct']:+.1f}%</b> per signal\n"
        )
        if sc.get("top_wins"):
            scorecard_section += "\nTop wins:\n"
            for w in sc["top_wins"][:2]:
                scorecard_section += f"- {w['symbol']}: {w['pnl_pct']:+.1f}%\n"
        if sc.get("top_losses"):
            scorecard_section += "\nRecent losses:\n"
            for l in sc["top_losses"][:2]:
                scorecard_section += f"- {l['symbol']}: {l['pnl_pct']:+.1f}%\n"
    else:
        scorecard_section = (
            "\n━━━━━━━━━━━━━━━━━━\n"
            "📈 <b>NexTrade Scorecard</b>\n"
            "Building accuracy baseline — first week of live signals.\n"
        )

    score_title = "Yesterday's signals" if session_no == -1 else "Signals today"
    score = (
        f"{scorecard_section}"
        "━━━━━━━━━━━━━━━━━━\n"
        f"📊 <b>{score_title}</b>\n"
        f"Buy: {buy_count} | Watch: {watch_count} | Exit: {exit_count}\n"
        f"Stocks analysed: {total_a}\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "<i>Powered by NexTrade</i>"
    )
    return f"{header}\n\n{body}{score}"


def _current_session_no_dhaka() -> int:
    tz = pytz.timezone("Asia/Dhaka")
    now_dhaka = datetime.now(tz)
    market_open = time(10, 0)
    market_close = time(14, 30)
    t = now_dhaka.time()
    if t < market_open or t > market_close:
        return 0
    start_dt = now_dhaka.replace(hour=10, minute=0, second=0, microsecond=0)
    session_no = (int((now_dhaka - start_dt).total_seconds() // 60) // 30) + 1
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
        # Always use actual today for market context, regardless of analysis fallback date
        analysis["market_context"] = get_market_context(conn, actual_today)
        # Fetch market trend for prompt injection
        try:
            from analysis.engine import detect_market_trend
            analysis["market_trend"] = detect_market_trend(conn)
        except Exception:
            analysis["market_trend"] = {}
        if (analysis.get("total_analysed") or 0) == 0:
            skip = {"status": "skipped", "reason": "no analysis data for today"}
            try:
                _insert_pulse_log(conn, trader_id, target_date, session_no, skip, json.dumps(skip))
            except Exception:
                pass
            return skip

        portfolio = get_trader_portfolio(conn, trader_id)
        watchlist = get_trader_watchlist(conn, trader_id)

        # Trader personalisation
        preferences: dict = {}
        stock_intents: list = []
        trader_context: str = ""
        try:
            preferences = get_trader_preferences(conn, trader_id)
            stock_intents = get_trader_stock_intents(conn, trader_id)
            trader_context = build_trader_context(preferences, stock_intents)
        except Exception as pref_err:
            logger.warning("Preferences fetch error (non-fatal): %s", pref_err)

        # Run signal evaluation and fetch scorecard / accuracy context
        scorecard: dict = {}
        accuracy_context: str = ""
        try:
            from analysis.evaluator import (
                evaluate_past_signals,
                get_accuracy_context_for_deepseek,
            )
            evaluate_past_signals(days_back=7)
            scorecard = get_scorecard_data(conn, trader_id)
            accuracy_context = get_accuracy_context_for_deepseek()
        except Exception as ev_err:
            logger.warning("Evaluator error (non-fatal): %s", ev_err)

        system_msg, user_msg = build_pulse_prompt(
            analysis, portfolio, watchlist, trader_id, target_date,
            scorecard=scorecard, accuracy_context=accuracy_context,
            trader_context=trader_context,
            preferences=preferences, stock_intents=stock_intents,
            data_warning=data_warning,
            conn=conn,
            session_no=session_no,
        )
        deepseek_input = {"system": system_msg, "user": user_msg}

        try:
            deepseek_output = call_deepseek(system_msg, user_msg)
        except Exception as e:
            deepseek_output = f"DeepSeek API error: {e}\n{traceback.format_exc()}"
            telegram_message = format_telegram_message(
                deepseek_output,
                analysis,
                portfolio,
                target_date,
                session_no=session_no,
                pulse_type=pulse_type,
                scorecard=scorecard,
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
            session_no=session_no,
            pulse_type=pulse_type,
            scorecard=scorecard,
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

        # --- Yesterday's top WATCH signals ---
        cur = conn.cursor()
        cur.execute(
            """
            SELECT DISTINCT ON (symbol)
                symbol, confidence_score, raw_output
            FROM analysis_results
            WHERE analysis_date = %s AND overall_signal = 'WATCH'
            ORDER BY symbol, confidence_score DESC NULLS LAST
            LIMIT 5
            """,
            (yesterday,),
        )
        watch_rows = cur.fetchall()
        cur.close()

        watch_signals: list[dict] = []
        for sym, conf, raw in watch_rows:
            r = raw if isinstance(raw, dict) else {}
            sr = r.get("sr") or {}
            cp = _float_or_none(sr.get("current_price")) or _float_or_none(r.get("current_price"))
            reason = r.get("signal_reason") or ""
            watch_signals.append({
                "symbol": sym,
                "confidence": _float_or_none(conf) or 0.0,
                "current_price": cp,
                "reason": reason,
            })
        watch_signals.sort(key=lambda x: x["confidence"], reverse=True)

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
        lines.append("Market opens at 10:00am. Current time: 9:55am.")
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
        lines.append(f"YESTERDAY'S TOP WATCH SIGNALS ({len(watch_signals)}):")
        for w in watch_signals:
            lines.append(f"- {w['symbol']} @ {w['current_price']} | {w['reason']}")
        lines.append("")
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
        lines.append(f"WATCHLIST ({len(watchlist)} stocks):")
        for w in watchlist:
            lines.append(
                f"- {w['symbol']}: target {w.get('target_price')}, "
                f"signal {w.get('signal')}"
            )

        user_msg = "\n".join(lines)

        preferences: dict = {}
        stock_intents: list = []
        trader_ctx = ""
        try:
            preferences = get_trader_preferences(conn, trader_id)
            stock_intents = get_trader_stock_intents(conn, trader_id)
            trader_ctx = build_trader_context(preferences, stock_intents)
        except Exception as pref_err:
            logger.warning("Premarket preferences fetch error (non-fatal): %s", pref_err)

        base_system = build_system_prompt(preferences or {})
        premarket_addon = (
            "PREMARKET BRIEFING MODE:\n"
            "- Market opens in 5 minutes; be specific and actionable.\n"
            "- Plain text in the body; sentence case throughout; max 400 words.\n"
            "- No HTML tags in the body (no <b> or <i>); symbols in plain text only.\n"
            "- Focus on: what to watch at open, key levels, risk reminders.\n"
        )
        system_msg = base_system
        if trader_ctx.strip():
            system_msg += "\n\n" + trader_ctx.strip() + "\n"
        system_msg += "\n\n" + premarket_addon

        deepseek_input = {"system": system_msg, "user": user_msg}

        try:
            deepseek_output = call_deepseek(system_msg, user_msg, temperature=0.2)
        except Exception as e:
            deepseek_output = f"DeepSeek API error: {e}"

        # --- Format Telegram message (shared header/footer with pulse formatter) ---
        premarket_analysis = {
            "buy_signal_total": int(analysis.get("buy_signal_total") or len(buy_signals)),
            "watch_signal_total": int(analysis.get("watch_signal_total") or len(watch_signals)),
            "exit_signals": analysis.get("exit_signals") or [],
            "total_analysed": int(analysis.get("total_analysed") or 0),
        }
        telegram_message = format_telegram_message(
            deepseek_output,
            premarket_analysis,
            portfolio,
            target_date,
            session_no=session_no,
            pulse_type="premarket",
            scorecard=None,
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

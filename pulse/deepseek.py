from __future__ import annotations

import html
import json
import logging
import os
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


SYSTEM_PROMPT = """You are ARIA, a sharp and precise DSE (Dhaka Stock Exchange) trading intelligence assistant. You analyse technical signals and provide actionable trading guidance.

Your job is to produce a concise market pulse message.
Be direct, specific, and actionable. No fluff.
Use numbers. Reference specific price levels.

Format your response as plain text only.
No markdown, no asterisks, no bullet symbols.
Use plain dashes for lists. Use CAPS for emphasis.

Rules:
- Never recommend gambling-class stocks for new buys
- Always mention stop-loss for BUY recommendations (use -8% from entry)
- Flag urgent exits for portfolio stocks down more than 8% from avg buy
- Be honest — if market is weak, say so
- English only
- Maximum 800 words total

SELF-AWARENESS RULES:
- You have access to your own signal accuracy data
- If win rate < 50%: be more conservative, say "signals have been mixed recently, proceed with caution"
- If win rate > 65%: be more confident
- Always mention recent win rate in pulse scorecard section
- Never hide losses — acknowledge them directly
- If a stock you previously called BUY is now down, address it directly

MARKET REGIME RULES:
- If market trend is DOWNTREND: open with warning, recommend capital preservation, no new BUY entries
- If market trend is WEAK: be cautious, only highest confidence BUY signals worth mentioning
- If market trend is UPTREND: be more confident, highlight momentum opportunities
- If market trend is STRONG: lead with bullish tone, more aggressive targets
- Always state market trend clearly at top of pulse"""


def build_pulse_prompt(
    analysis: dict,
    portfolio: list[dict],
    watchlist: list[dict],
    trader_id: int,
    target_date: date,
    scorecard: dict | None = None,
    accuracy_context: str = "",
) -> tuple[str, str]:
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
        trend_line = (
            f"MARKET TREND:\n"
            f"Direction: {trend_data.get('trend', 'unknown').upper()}\n"
            f"Consecutive down days: {trend_data.get('consecutive_down_days', 0)}\n"
            f"5-day DSEX change: {trend_data.get('dsex_5d_change_pct', 0):+.1f}%"
        )
    else:
        trend_line = ""

    lines: list[str] = []
    lines.append(f"DATE: {target_date.isoformat()}")
    lines.append("")
    lines.append("MARKET OVERVIEW:")
    lines.append(market_line)
    if trend_line:
        lines.append("")
        lines.append(trend_line)
    lines.append("")
    lines.append(f"TOP BUY SIGNALS TODAY ({buy_total}):")
    for b in buys:
        cp = float(b.get("current_price") or 0.0)
        sl = cp * 0.92
        conf_pct = (b.get("confidence") or 0) * 100.0
        lines.append(
            f"• {b.get('symbol')} ({b.get('stock_class')}) @ {cp}\n"
            f"  Signal: {b.get('reason')}\n"
            f"  Support: {b.get('support')} | Resistance: {b.get('resistance')}\n"
            f"  RSI: {b.get('rsi')} | Confidence: {conf_pct:.0f}%\n"
            f"  Stop-loss: {sl:.2f}"
        )
    lines.append("")
    lines.append("WATCH LIST SIGNALS (top 5):")
    for w in watches:
        lines.append(f"• {w.get('symbol')} @ {w.get('current_price')} — {w.get('reason')}")
    lines.append("")
    lines.append("EXIT SIGNALS:")
    for e in exits:
        lines.append(f"• {e.get('symbol')} — {e.get('reason')}")
    lines.append("")
    lines.append(f"TRADER PORTFOLIO ({len(portfolio)} positions):")
    for p in portfolio:
        lines.append(
            f"• {p.get('symbol')}: {p.get('quantity')} shares @ avg {p.get('avg_buy_price')}\n"
            f"  Current: {p.get('current_price')} | P&L: {p.get('pnl_pct'):+.1f}% ({p.get('pnl_value'):+.0f} BDT)\n"
            f"  Signal: {p.get('signal')} — {p.get('reason')}"
        )
        if (p.get("pnl_pct") or 0) <= -8.0:
            lines.append("  ⚠️ URGENT EXIT — exceeds -8% threshold")
    lines.append("")
    lines.append(f"WATCHLIST ({len(watchlist)} stocks):")
    for w in watchlist:
        lines.append(
            f"• {w.get('symbol')} — Current: {w.get('current_price')} | Signal: {w.get('signal')}"
        )
    lines.append("")
    buy_count = int(analysis.get("buy_signal_total") or len(analysis.get("buy_signals") or []))
    watch_count = int(analysis.get("watch_signal_total") or len(analysis.get("watch_signals") or []))
    exit_count = len(analysis.get("exit_signals") or [])
    total_a = analysis.get("total_analysed", 0)
    lines.append("MARKET CLASS BREAKDOWN:")
    lines.append(f"BUY signals: {buy_count} | WATCH: {watch_count} | EXIT: {exit_count}")
    lines.append(f"Stocks analysed: {total_a}")
    lines.append(f"(trader_id={trader_id})")

    # Inject scorecard and accuracy context
    if scorecard and scorecard.get("total_evaluated", 0) > 0:
        sc = scorecard
        lines.append("")
        lines.append("ARIA PERFORMANCE SCORECARD:")
        lines.append(
            f"Win rate: {sc['win_rate']}% "
            f"({sc['wins']} wins / {sc['losses']} losses / {sc['neutrals']} neutral "
            f"out of {sc['total_evaluated']} evaluated)"
        )
        if sc.get("top_wins"):
            lines.append("Recent wins:")
            for w in sc["top_wins"][:3]:
                lines.append(
                    f"✅ {w['symbol']} {w['signal_type']} @ {w['price_at_signal']} "
                    f"→ {w['price_at_eval']} ({w['pnl_pct']:+.1f}%)"
                )
        if sc.get("top_losses"):
            lines.append("Recent losses:")
            for l in sc["top_losses"][:3]:
                lines.append(
                    f"❌ {l['symbol']} {l['signal_type']} @ {l['price_at_signal']} "
                    f"→ {l['price_at_eval']} ({l['pnl_pct']:+.1f}%)"
                )
    if accuracy_context:
        lines.append("")
        lines.append(accuracy_context)

    return SYSTEM_PROMPT, "\n".join(lines)


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


def format_telegram_message(
    deepseek_response: str,
    analysis: dict,
    portfolio: list,
    target_date: date,
    scorecard: dict | None = None,
) -> str:
    buy_count = int(analysis.get("buy_signal_total") or len(analysis.get("buy_signals") or []))
    watch_count = int(analysis.get("watch_signal_total") or len(analysis.get("watch_signals") or []))
    exit_count = len(analysis.get("exit_signals") or [])
    total_a = analysis.get("total_analysed", 0)

    body = html.escape(deepseek_response or "", quote=True)
    header = (
        "🎯 <b>ARIA Market Pulse</b>\n"
        f"📅 {html.escape(target_date.isoformat(), quote=True)} | DSE Trading Session"
    )

    # ARIA accuracy scorecard section
    sc = scorecard or {}
    if sc.get("total_evaluated", 0) > 0:
        scorecard_section = (
            "\n━━━━━━━━━━━━━━━━━━\n"
            "📈 <b>ARIA SCORECARD</b> (last 7 days)\n"
            f"Win rate: <b>{sc['win_rate']:.0f}%</b> | "
            f"{sc['wins']}W {sc['losses']}L {sc['neutrals']}N\n"
            f"Avg P&amp;L: <b>{sc['avg_pnl_pct']:+.1f}%</b> per signal\n"
        )
        if sc.get("top_wins"):
            scorecard_section += "\n✅ Top wins:\n"
            for w in sc["top_wins"][:2]:
                scorecard_section += f"• {w['symbol']}: {w['pnl_pct']:+.1f}%\n"
        if sc.get("top_losses"):
            scorecard_section += "\n❌ Recent losses:\n"
            for l in sc["top_losses"][:2]:
                scorecard_section += f"• {l['symbol']}: {l['pnl_pct']:+.1f}%\n"
    else:
        scorecard_section = (
            "\n━━━━━━━━━━━━━━━━━━\n"
            "📈 <b>ARIA SCORECARD</b>\n"
            "Building accuracy baseline — first week of live signals.\n"
        )

    score = (
        f"{scorecard_section}"
        "━━━━━━━━━━━━━━━━━━\n"
        "📊 <b>SCORECARD</b>\n"
        f"BUY signals: {buy_count}\n"
        f"WATCH signals: {watch_count}\n"
        f"EXIT signals: {exit_count}\n"
        f"Stocks analysed: {total_a}\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "<i>Powered by ARIA</i>"
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

        # Run signal evaluation and fetch scorecard / accuracy context
        scorecard: dict = {}
        accuracy_context: str = ""
        try:
            from analysis.evaluator import (
                evaluate_past_signals,
                get_recent_scorecard,
                get_accuracy_context_for_deepseek,
            )
            evaluate_past_signals(days_back=7)
            scorecard = get_recent_scorecard(days=7)
            accuracy_context = get_accuracy_context_for_deepseek()
        except Exception as ev_err:
            logger.warning("Evaluator error (non-fatal): %s", ev_err)

        system_msg, user_msg = build_pulse_prompt(
            analysis, portfolio, watchlist, trader_id, target_date,
            scorecard=scorecard, accuracy_context=accuracy_context,
        )
        deepseek_input = {"system": system_msg, "user": user_msg}

        try:
            deepseek_output = call_deepseek(system_msg, user_msg)
        except Exception as e:
            deepseek_output = f"DeepSeek API error: {e}\n{traceback.format_exc()}"
            telegram_message = format_telegram_message(
                deepseek_output, analysis, portfolio, target_date, scorecard=scorecard
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
            deepseek_output, analysis, portfolio, target_date, scorecard=scorecard
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


PREMARKET_SYSTEM_PROMPT = """You are ARIA, a DSE trading intelligence assistant.
Generate a sharp pre-market briefing for the trader.
Market opens in 5 minutes.
Be specific, actionable, and concise.
Maximum 400 words.
Plain text only, no markdown, use CAPS for emphasis.
Focus on: what to watch at open, key levels, risk reminders."""


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
        yesterday = target_date - timedelta(days=1)
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
            at_sl = " — AT STOP-LOSS THRESHOLD — REVIEW AT OPEN" if (p.get("pnl_pct") or 0) <= -8 else ""
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
        deepseek_input = {"system": PREMARKET_SYSTEM_PROMPT, "user": user_msg}

        try:
            deepseek_output = call_deepseek(PREMARKET_SYSTEM_PROMPT, user_msg, temperature=0.2)
        except Exception as e:
            deepseek_output = f"DeepSeek API error: {e}"

        # --- Format Telegram message ---
        body = html.escape(deepseek_output, quote=True)
        telegram_message = (
            f"🌅 <b>ARIA Pre-Market Briefing</b>\n"
            f"📅 {html.escape(target_date.isoformat())} | Market opens in 5 minutes\n\n"
            f"{body}\n\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"💼 Positions: {len(portfolio)} | "
            f"👁 Watchlist: {len(watchlist)} | "
            f"📊 Yesterday BUY signals: {len(buy_signals)}\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"<i>Powered by ARIA</i>"
        )

        try:
            _insert_pulse_log(conn, trader_id, target_date, 0, deepseek_input, deepseek_output)
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

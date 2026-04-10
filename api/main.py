from __future__ import annotations

import logging
import os
import traceback
from datetime import date
from pathlib import Path
from typing import Any

import psycopg2
from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from ingestion.sync_stocks_master import sync_stocks_master
from ingestion.append_price_history import append_price_history
from ingestion.ingest_live_ticks import ingest_live_ticks
from ingestion.cleanup_live_ticks import cleanup_live_ticks
from ingestion.fetch_market_summary import fetch_market_summary
from ingestion.fetch_stock_fundamentals import fetch_stock_fundamentals
from analysis.engine import analyse_all_symbols, analyse_symbol, detect_extreme_moves
from analysis.evaluator import evaluate_past_signals, get_recent_scorecard, calculate_accuracy_scores
from pulse.deepseek import generate_pulse, generate_premarket_briefing
from pulse.telegram import deliver_pulse, deliver_premarket_briefing, send_telegram_message, deliver_extreme_move_alerts  # noqa: F401

logger = logging.getLogger(__name__)

app = FastAPI(title="ARIA Ingestion API", version="1.0.0")


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _get_conn():
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
    return psycopg2.connect(os.environ["DATABASE_URL"])


def _get_db_date(conn) -> date:
    cur = conn.cursor()
    try:
        cur.execute("SELECT CURRENT_DATE")
        return cur.fetchone()[0]
    finally:
        cur.close()


def _float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _int(v: Any) -> int | None:
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Existing job runner
# ---------------------------------------------------------------------------

def _run_job(fn):
    try:
        result = fn()
        return JSONResponse(status_code=200, content=result)
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "message": str(e),
                "traceback": traceback.format_exc(),
            },
        )


# ---------------------------------------------------------------------------
# Root & health
# ---------------------------------------------------------------------------

@app.get("/health")
def health():
    return {"status": "ok", "service": "ARIA Ingestion API", "version": "1.0.0"}


@app.get("/")
def root():
    return {
        "endpoints": [
            # Ingestion
            {"path": "/ingest/sync-stocks", "method": "POST", "description": "Sync stocks master from bdshare"},
            {"path": "/ingest/append-price-history", "method": "POST", "description": "Append today's price history for all symbols"},
            {"path": "/ingest/live-ticks", "method": "POST", "description": "Ingest current live ticks session"},
            {"path": "/ingest/cleanup-live-ticks", "method": "POST", "description": "Archive and clear today's live ticks"},
            {"path": "/ingest/market-summary", "method": "POST", "description": "Fetch and upsert market summary"},
            {"path": "/ingest/stock-fundamentals", "method": "POST", "description": "Fetch and upsert stock fundamentals (P/E, EPS)"},
            # Analysis
            {"path": "/analyse/all", "method": "POST", "description": "Run analysis for all active symbols"},
            {"path": "/analyse/symbol/{symbol}", "method": "POST", "description": "Run analysis for a single symbol"},
            {"path": "/signals/today", "method": "GET", "description": "All active signals for today grouped by type"},
            {"path": "/evaluate/signals", "method": "POST", "description": "Evaluate past signal accuracy"},
            {"path": "/evaluate/scorecard", "method": "GET", "description": "Get recent signal scorecard"},
            {"path": "/evaluate/accuracy", "method": "POST", "description": "Calculate accuracy scores"},
            {"path": "/refresh/all", "method": "POST", "description": "Refresh market data and analysis"},
            {"path": "/refresh/prices", "method": "POST", "description": "Refresh prices and analysis"},
            {"path": "/refresh/analysis", "method": "POST", "description": "Refresh analysis only"},
            {"path": "/stock/search/{query}", "method": "GET", "description": "Search DSE stock symbol by name"},
            {"path": "/alerts/extreme-moves", "method": "POST", "description": "Check and send extreme move alerts"},
            # Pulse
            {"path": "/pulse/deliver/{trader_id}", "method": "POST", "description": "Deliver latest pulse to one trader via Telegram"},
            {"path": "/pulse/deliver/all", "method": "POST", "description": "Deliver latest pulse to all active traders via Telegram"},
            {"path": "/pulse/generate/{trader_id}", "method": "POST", "description": "Generate DeepSeek market pulse + Telegram message for a trader"},
            {"path": "/pulse/premarket/{trader_id}", "method": "POST", "description": "Generate and send pre-market briefing to one trader"},
            {"path": "/pulse/premarket/deliver/all", "method": "POST", "description": "Generate and send pre-market briefing to all active traders"},
            # Chatbot
            {"path": "/stock/{symbol}", "method": "GET", "description": "Latest analysis for a single stock"},
            {"path": "/market/summary", "method": "GET", "description": "Today's market overview with signal counts"},
            {"path": "/portfolio/{trader_id}", "method": "GET", "description": "Trader's current portfolio with P&L"},
            {"path": "/portfolio/{trader_id}/buy", "method": "POST", "description": "Record a buy trade"},
            {"path": "/portfolio/{trader_id}/sell", "method": "POST", "description": "Record a sell trade"},
            {"path": "/watchlist/{trader_id}", "method": "GET", "description": "Trader's watchlist with current signals"},
            {"path": "/watchlist/{trader_id}/add", "method": "POST", "description": "Add a stock to watchlist"},
            {"path": "/watchlist/{trader_id}/remove", "method": "POST", "description": "Remove a stock from watchlist"},
        ]
    }


# ---------------------------------------------------------------------------
# Ingestion endpoints
# ---------------------------------------------------------------------------

@app.post("/ingest/sync-stocks")
def ingest_sync_stocks():
    return _run_job(sync_stocks_master)


@app.post("/ingest/append-price-history")
def ingest_append_price_history():
    return _run_job(append_price_history)


@app.post("/ingest/live-ticks")
def ingest_live_ticks_endpoint():
    # Run the ingest job
    try:
        result = ingest_live_ticks()
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"status": "error", "message": str(e), "traceback": traceback.format_exc()},
        )

    if result.get("status") == "error":
        return JSONResponse(status_code=500, content=result)

    # Check for extreme moves after successful ingest
    conn = None
    extreme_result: dict = {}
    try:
        conn = _get_conn()
        cur = conn.cursor()
        today = _get_db_date(conn)
        cur.execute(
            """
            SELECT symbol, session_no, ltp, high, low, close, ycp, change_val, trade, value, volume
            FROM live_ticks
            WHERE date = %s
            ORDER BY symbol, session_no ASC
            """,
            (today,),
        )
        rows = cur.fetchall()
        cur.close()

        latest_ticks: dict[str, list[dict]] = {}
        for row in rows:
            sym = row[0]
            tick = {
                "session_no": row[1],
                "ltp": _float(row[2]),
                "high": _float(row[3]),
                "low": _float(row[4]),
                "close": _float(row[5]),
                "ycp": _float(row[6]),
                "change_val": _float(row[7]),
                "trade": _int(row[8]),
                "value": _float(row[9]),
                "volume": _int(row[10]),
            }
            latest_ticks.setdefault(sym, []).append(tick)

        moves = detect_extreme_moves(latest_ticks, threshold_pct=5.0)
        result["extreme_moves_detected"] = len(moves)

        if moves:
            alert_result = deliver_extreme_move_alerts(moves)
            result["extreme_move_alert"] = alert_result
        else:
            result["extreme_move_alert"] = {"status": "skipped", "reason": "no extreme moves"}

    except Exception as e:
        logger.exception("extreme move detection error")
        result["extreme_move_alert"] = {"status": "error", "message": str(e)}
    finally:
        if conn:
            conn.close()

    return JSONResponse(status_code=200, content=result)


@app.post("/ingest/cleanup-live-ticks")
def ingest_cleanup_live_ticks():
    return _run_job(cleanup_live_ticks)


@app.post("/alerts/extreme-moves")
def extreme_moves_endpoint(threshold_pct: float = 5.0):
    """Manual trigger: scan today's live ticks and deliver extreme move alerts."""
    conn = None
    try:
        conn = _get_conn()
        cur = conn.cursor()
        today = _get_db_date(conn)
        cur.execute(
            """
            SELECT symbol, session_no, ltp, high, low, close, ycp, change_val, trade, value, volume
            FROM live_ticks
            WHERE date = %s
            ORDER BY symbol, session_no ASC
            """,
            (today,),
        )
        rows = cur.fetchall()
        cur.close()

        latest_ticks: dict[str, list[dict]] = {}
        for row in rows:
            sym = row[0]
            tick = {
                "session_no": row[1],
                "ltp": _float(row[2]),
                "high": _float(row[3]),
                "low": _float(row[4]),
                "close": _float(row[5]),
                "ycp": _float(row[6]),
                "change_val": _float(row[7]),
                "trade": _int(row[8]),
                "value": _float(row[9]),
                "volume": _int(row[10]),
            }
            latest_ticks.setdefault(sym, []).append(tick)

        moves = detect_extreme_moves(latest_ticks, threshold_pct=threshold_pct)

        if not moves:
            return JSONResponse(
                status_code=200,
                content={"status": "ok", "date": today.isoformat(), "moves_detected": 0, "moves": []},
            )

        alert_result = deliver_extreme_move_alerts(moves)
        return JSONResponse(
            status_code=200,
            content={
                "status": "ok",
                "date": today.isoformat(),
                "threshold_pct": threshold_pct,
                "moves_detected": len(moves),
                "moves": moves,
                "alert": alert_result,
            },
        )

    except Exception as e:
        logger.exception("extreme-moves endpoint error")
        return JSONResponse(status_code=500, content={"status": "error", "message": str(e), "traceback": traceback.format_exc()})
    finally:
        if conn:
            conn.close()


@app.post("/ingest/market-summary")
def ingest_market_summary():
    return _run_job(fetch_market_summary)


@app.post("/ingest/stock-fundamentals")
def ingest_stock_fundamentals():
    return _run_job(fetch_stock_fundamentals)


# ---------------------------------------------------------------------------
# Analysis endpoints
# ---------------------------------------------------------------------------

@app.post("/analyse/all")
def analyse_all_endpoint():
    return _run_job(analyse_all_symbols)


@app.post("/analyse/symbol/{symbol}")
def analyse_symbol_endpoint(symbol: str):
    return _run_job(lambda: analyse_symbol(symbol))


# ---------------------------------------------------------------------------
# Refresh endpoints
# ---------------------------------------------------------------------------

def _run_step(fn):
    """Run a job function, return (result_dict, error_str)."""
    try:
        return fn(), None
    except Exception as e:
        return None, str(e)


@app.post("/refresh/all")
def refresh_all():
    from datetime import datetime
    results: dict = {"status": "ok", "message": "Data refreshed successfully", "timestamp": datetime.utcnow().isoformat() + "Z"}

    res, err = _run_step(fetch_market_summary)
    results["market_summary"] = res if res is not None else {"status": "error", "message": err}

    res, err = _run_step(fetch_stock_fundamentals)
    results["fundamentals"] = res if res is not None else {"status": "error", "message": err}

    res, err = _run_step(analyse_all_symbols)
    results["analysis"] = res if res is not None else {"status": "error", "message": err}

    return JSONResponse(status_code=200, content=results)


@app.post("/refresh/prices")
def refresh_prices():
    from datetime import datetime
    results: dict = {"status": "ok", "timestamp": datetime.utcnow().isoformat() + "Z"}

    res, err = _run_step(append_price_history)
    results["prices"] = res if res is not None else {"status": "error", "message": err}

    res, err = _run_step(analyse_all_symbols)
    results["analysis"] = res if res is not None else {"status": "error", "message": err}

    return JSONResponse(status_code=200, content=results)


@app.post("/refresh/analysis")
def refresh_analysis():
    from datetime import datetime
    results: dict = {"status": "ok", "timestamp": datetime.utcnow().isoformat() + "Z"}

    res, err = _run_step(analyse_all_symbols)
    results["analysis"] = res if res is not None else {"status": "error", "message": err}

    return JSONResponse(status_code=200, content=results)


# ---------------------------------------------------------------------------
# Evaluate / accuracy endpoints
# ---------------------------------------------------------------------------

@app.post("/evaluate/signals")
def evaluate_signals_endpoint():
    result = evaluate_past_signals(days_back=7)
    status_code = 200 if result.get("status") == "ok" else 500
    return JSONResponse(status_code=status_code, content=result)


@app.get("/evaluate/scorecard")
def get_scorecard_endpoint():
    result = get_recent_scorecard(days=7)
    return JSONResponse(status_code=200, content=result)


@app.post("/evaluate/accuracy")
def calculate_accuracy_endpoint():
    result = calculate_accuracy_scores(period_days=30)
    status_code = 200 if result.get("status") == "ok" else 500
    return JSONResponse(status_code=status_code, content=result)


@app.get("/signals/today")
def signals_today():
    conn = None
    try:
        conn = _get_conn()
        cur = conn.cursor()
        today = _get_db_date(conn).isoformat()
        cur.execute(
            """
            SELECT signal_type, symbol, price_at_signal, reason
            FROM signals
            WHERE signal_date = %s AND is_active = TRUE
            ORDER BY signal_type, symbol
            """,
            (today,),
        )
        rows = cur.fetchall()
        cur.close()
        out: dict = {"date": today, "BUY": [], "WATCH": [], "EXIT": []}
        for st, sym, price, reason in rows:
            if st not in out:
                continue
            out[st].append({"symbol": sym, "price": _float(price), "reason": reason})
        return out
    except Exception as e:
        logger.exception("signals_today error")
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        if conn:
            conn.close()


# ---------------------------------------------------------------------------
# Pulse endpoints
# ---------------------------------------------------------------------------

@app.post("/pulse/deliver/{trader_id}")
def pulse_deliver_trader(trader_id: int):
    try:
        result = deliver_pulse(trader_id)
        if result.get("status") == "error":
            return JSONResponse(status_code=500, content=result)
        return JSONResponse(status_code=200, content=result)
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "error", "message": str(e), "traceback": traceback.format_exc()})


@app.post("/pulse/deliver/all")
def pulse_deliver_all():
    try:
        result = deliver_pulse()
        if result.get("status") == "error":
            return JSONResponse(status_code=500, content=result)
        return JSONResponse(status_code=200, content=result)
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "error", "message": str(e), "traceback": traceback.format_exc()})


@app.post("/pulse/generate/{trader_id}")
def pulse_endpoint(trader_id: int):
    try:
        result = generate_pulse(trader_id)
        if result.get("status") == "error" and result.get("reason") == "trader not found":
            return JSONResponse(status_code=404, content=result)
        if result.get("status") in ("skipped", "ok"):
            return JSONResponse(status_code=200, content=result)
        return JSONResponse(status_code=500, content=result)
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "error", "message": str(e), "traceback": traceback.format_exc()})


@app.post("/pulse/premarket/deliver/all")
def premarket_deliver_all():
    try:
        result = deliver_premarket_briefing()
        return JSONResponse(status_code=200, content=result)
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "error", "message": str(e), "traceback": traceback.format_exc()})


@app.post("/pulse/premarket/{trader_id}")
def premarket_endpoint(trader_id: int):
    try:
        result = generate_premarket_briefing(trader_id)
        if result.get("status") == "error" and result.get("reason") == "trader not found":
            return JSONResponse(status_code=404, content=result)
        return JSONResponse(status_code=200 if result.get("status") == "ok" else 500, content=result)
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "error", "message": str(e), "traceback": traceback.format_exc()})


# ---------------------------------------------------------------------------
# Chatbot endpoints
# ---------------------------------------------------------------------------

@app.get("/stock/search/{query}")
def search_symbol(query: str):
    conn = None
    try:
        query = query.upper().strip()
        pattern1 = f"%{query}%"
        pattern2 = f"{query[:4]}%"
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute(
            "SELECT DISTINCT symbol FROM stocks_master WHERE symbol ILIKE %s OR symbol ILIKE %s ORDER BY symbol LIMIT 5",
            (pattern1, pattern2),
        )
        matches = [row[0] for row in cur.fetchall()]
        cur.close()
        return {"query": query, "matches": matches}
    except Exception as e:
        logger.exception("search_symbol error query=%s", query)
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        if conn:
            conn.close()


@app.get("/stock/{symbol}")
def get_stock(symbol: str):
    conn = None
    try:
        symbol = symbol.upper().strip()
        conn = _get_conn()
        cur = conn.cursor()

        # Check symbol exists
        cur.execute("SELECT 1 FROM stocks_master WHERE symbol = %s", (symbol,))
        if not cur.fetchone():
            return JSONResponse(status_code=404, content={"error": "Symbol not found"})

        # Latest analysis
        cur.execute(
            """
            SELECT DISTINCT ON (symbol)
                symbol, overall_signal, confidence_score,
                stock_class, support_levels, resistance_levels,
                rsi, macd, volume_signal, breakout_signal,
                fib_levels, raw_output, analysis_date
            FROM analysis_results
            WHERE symbol = %s
            ORDER BY symbol, analysis_date DESC, session_no DESC NULLS LAST, id DESC
            """,
            (symbol,),
        )
        ar = cur.fetchone()

        # Latest price
        cur.execute(
            """
            SELECT date, open, high, low, close, ltp, ycp, volume, trade
            FROM price_history
            WHERE symbol = %s
            ORDER BY date DESC LIMIT 1
            """,
            (symbol,),
        )
        ph = cur.fetchone()

        # Latest fundamentals
        cur.execute(
            """
            SELECT pe_ratio, eps, fetched_at
            FROM stock_fundamentals
            WHERE symbol = %s
            ORDER BY fetched_at DESC LIMIT 1
            """,
            (symbol,),
        )
        fund = cur.fetchone()

        # Active signal
        cur.execute(
            """
            SELECT signal_type, reason, signal_date
            FROM signals
            WHERE symbol = %s AND is_active = TRUE
            ORDER BY signal_date DESC LIMIT 1
            """,
            (symbol,),
        )
        sig = cur.fetchone()
        cur.close()

        current_price = None
        ycp = None
        high_today = None
        low_today = None
        volume = None
        if ph:
            current_price = _float(ph[5]) or _float(ph[4])  # ltp or close
            ycp = _float(ph[6])
            high_today = _float(ph[2])
            low_today = _float(ph[3])
            volume = _int(ph[7])

        change_pct = None
        if current_price is not None and ycp and ycp != 0:
            change_pct = round((current_price - ycp) / ycp * 100, 2)

        signal = "UNKNOWN"
        confidence = None
        stock_class = None
        support = []
        resistance = []
        rsi = None
        breakout = False
        analysis_date = None
        class_flags: dict = {}

        if ar:
            signal = ar[1] or "UNKNOWN"
            confidence = _float(ar[2])
            stock_class = ar[3]
            support = ar[4] if ar[4] else []
            resistance = ar[5] if ar[5] else []
            rsi = _float(ar[6])
            raw = ar[11] if isinstance(ar[11], dict) else {}
            bo = raw.get("breakout") or {}
            breakout = bool(bo.get("breakout"))
            cf = raw.get("class_flags")
            class_flags = cf if isinstance(cf, dict) else {}
            analysis_date = ar[12].strftime("%d %b %Y") if hasattr(ar[12], "strftime") else str(ar[12]) if ar[12] else None

        reason = sig[1] if sig else None

        return {
            "symbol": symbol,
            "stock_class": stock_class,
            "signal": signal,
            "confidence": confidence,
            "reason": reason,
            "current_price": current_price,
            "change_pct": change_pct,
            "high_today": high_today,
            "low_today": low_today,
            "volume": volume,
            "pe_ratio": _float(fund[0]) if fund else None,
            "eps": _float(fund[1]) if fund else None,
            "support": support,
            "resistance": resistance,
            "rsi": rsi,
            "breakout": breakout,
            "analysis_date": analysis_date,
            "class_flags": class_flags,
        }
    except Exception as e:
        logger.exception("get_stock error symbol=%s", symbol)
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        if conn:
            conn.close()


@app.get("/market/summary")
def get_market_summary():
    conn = None
    try:
        conn = _get_conn()
        cur = conn.cursor()
        today = _get_db_date(conn)

        # Latest market summary row
        cur.execute(
            """
            SELECT trade_date, dsex_index, dses_index, total_volume, total_value_mn
            FROM market_summary
            ORDER BY trade_date DESC NULLS LAST
            LIMIT 1
            """
        )
        ms = cur.fetchone()
        if not ms:
            # fallback to date column
            cur.execute(
                """
                SELECT date, dsex_index, dses_index, total_volume, total_value_mn
                FROM market_summary
                ORDER BY date DESC NULLS LAST
                LIMIT 1
                """
            )
            ms = cur.fetchone()

        # Signal counts for today
        cur.execute(
            """
            SELECT overall_signal, COUNT(DISTINCT symbol)
            FROM analysis_results
            WHERE analysis_date = %s
            GROUP BY overall_signal
            """,
            (today,),
        )
        signal_counts: dict = {"BUY": 0, "WATCH": 0, "HOLD": 0, "EXIT": 0}
        for sig_type, cnt in cur.fetchall():
            if sig_type in signal_counts:
                signal_counts[sig_type] = int(cnt)

        # Top 5 BUY signals by confidence
        cur.execute(
            """
            SELECT DISTINCT ON (symbol)
                symbol, confidence_score, raw_output
            FROM analysis_results
            WHERE analysis_date = %s AND overall_signal = 'BUY'
            ORDER BY symbol, confidence_score DESC NULLS LAST, id DESC
            """,
            (today,),
        )
        buy_rows = cur.fetchall()
        buy_rows.sort(key=lambda r: _float(r[1]) or 0, reverse=True)
        top_buys = []
        for sym, conf, raw in buy_rows[:5]:
            r = raw if isinstance(raw, dict) else {}
            sr = r.get("sr") or {}
            cp = _float(sr.get("current_price")) or _float(r.get("current_price"))
            reason = r.get("signal_reason") or ""
            top_buys.append({
                "symbol": sym,
                "confidence": _float(conf),
                "price": cp,
                "reason": reason,
            })

        # Top 3 EXIT signals
        cur.execute(
            """
            SELECT DISTINCT ON (symbol)
                symbol, confidence_score, raw_output
            FROM analysis_results
            WHERE analysis_date = %s AND overall_signal = 'EXIT'
            ORDER BY symbol, confidence_score DESC NULLS LAST, id DESC
            """,
            (today,),
        )
        exit_rows = cur.fetchall()
        top_exits = []
        for sym, conf, raw in exit_rows[:3]:
            r = raw if isinstance(raw, dict) else {}
            sr = r.get("sr") or {}
            cp = _float(sr.get("current_price")) or _float(r.get("current_price"))
            reason = r.get("signal_reason") or ""
            top_exits.append({
                "symbol": sym,
                "confidence": _float(conf),
                "price": cp,
                "reason": reason,
            })

        cur.close()

        return {
            "date": today.strftime("%d %b %Y"),
            "dsex": _float(ms[1]) if ms else None,
            "dses": _float(ms[2]) if ms else None,
            "total_volume": _int(ms[3]) if ms else None,
            "total_value_mn": _float(ms[4]) if ms else None,
            "signals": signal_counts,
            "top_buys": top_buys,
            "top_exits": top_exits,
        }
    except Exception as e:
        logger.exception("get_market_summary error")
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        if conn:
            conn.close()


@app.get("/portfolio/{trader_id}")
def get_portfolio(trader_id: int):
    conn = None
    try:
        conn = _get_conn()
        cur = conn.cursor()

        # Check trader exists
        cur.execute("SELECT name FROM traders WHERE id = %s", (trader_id,))
        trader = cur.fetchone()
        if not trader:
            return JSONResponse(status_code=404, content={"error": "Trader not found"})
        trader_name = trader[0] or f"Trader {trader_id}"

        # Open holdings
        cur.execute(
            """
            SELECT symbol, quantity, avg_buy_price, total_invested
            FROM portfolio_holdings
            WHERE trader_id = %s AND is_open = TRUE
            """,
            (trader_id,),
        )
        holdings = cur.fetchall()

        positions = []
        total_invested = 0.0
        total_current_value = 0.0
        urgent_exits = 0

        for sym, qty, avg_px, invested in holdings:
            qty = int(qty or 0)
            avg_px = float(avg_px or 0)
            invested = float(invested or 0)

            # Current price
            cur.execute(
                "SELECT ltp, close FROM price_history WHERE symbol = %s ORDER BY date DESC LIMIT 1",
                (sym,),
            )
            ph = cur.fetchone()
            current_price = (_float(ph[0]) or _float(ph[1])) if ph else avg_px

            # Current signal
            cur.execute(
                """
                SELECT DISTINCT ON (symbol) overall_signal
                FROM analysis_results
                WHERE symbol = %s
                ORDER BY symbol, analysis_date DESC, session_no DESC NULLS LAST, id DESC
                """,
                (sym,),
            )
            ar = cur.fetchone()
            signal = ar[0] if ar else "HOLD"

            current_value = round(qty * current_price, 2)
            pnl_value = round(current_value - invested, 2)
            pnl_pct = round(pnl_value / invested * 100, 2) if invested else 0.0
            urgent_exit = pnl_pct <= -8.0

            if urgent_exit:
                urgent_exits += 1

            total_invested += invested
            total_current_value += current_value

            positions.append({
                "symbol": sym,
                "quantity": qty,
                "avg_buy_price": avg_px,
                "current_price": current_price,
                "total_invested": round(invested, 2),
                "current_value": current_value,
                "pnl_value": pnl_value,
                "pnl_pct": pnl_pct,
                "signal": signal,
                "urgent_exit": urgent_exit,
            })

        cur.close()

        total_pnl = round(total_current_value - total_invested, 2)
        total_pnl_pct = round(total_pnl / total_invested * 100, 2) if total_invested else 0.0

        return {
            "trader_id": trader_id,
            "trader_name": trader_name,
            "positions": positions,
            "summary": {
                "total_positions": len(positions),
                "total_invested": round(total_invested, 2),
                "current_value": round(total_current_value, 2),
                "total_pnl": total_pnl,
                "total_pnl_pct": total_pnl_pct,
                "urgent_exits": urgent_exits,
            },
        }
    except Exception as e:
        logger.exception("get_portfolio error trader_id=%s", trader_id)
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        if conn:
            conn.close()


@app.post("/portfolio/{trader_id}/buy")
async def record_buy(trader_id: int, request: Request):
    conn = None
    try:
        body = await request.json()
        symbol = (body.get("symbol") or "").upper().strip()
        quantity = _int(body.get("quantity"))
        price = _float(body.get("price"))
        notes = body.get("notes") or ""
        trade_date_raw = body.get("date")

        if not symbol:
            return JSONResponse(status_code=400, content={"error": "symbol is required"})
        if not quantity or quantity <= 0:
            return JSONResponse(status_code=400, content={"error": "quantity must be > 0"})
        if not price or price <= 0:
            return JSONResponse(status_code=400, content={"error": "price must be > 0"})

        conn = _get_conn()
        conn.autocommit = True
        cur = conn.cursor()
        today = _get_db_date(conn)
        trade_date = date.fromisoformat(trade_date_raw) if trade_date_raw else today

        # Validate trader
        cur.execute("SELECT 1 FROM traders WHERE id = %s", (trader_id,))
        if not cur.fetchone():
            return JSONResponse(status_code=404, content={"error": "Trader not found"})

        # Validate symbol
        cur.execute("SELECT 1 FROM stocks_master WHERE symbol = %s", (symbol,))
        if not cur.fetchone():
            return JSONResponse(status_code=404, content={"error": "Symbol not found"})

        total_value = round(quantity * price, 2)

        # Record transaction
        cur.execute(
            """
            INSERT INTO trade_transactions
                (trader_id, symbol, transaction_type, quantity, price, total_value, transaction_date, notes)
            VALUES (%s, %s, 'BUY', %s, %s, %s, %s, %s)
            """,
            (trader_id, symbol, quantity, price, total_value, trade_date, notes),
        )

        # Upsert holding
        cur.execute(
            """
            SELECT id, quantity, avg_buy_price, total_invested
            FROM portfolio_holdings
            WHERE trader_id = %s AND symbol = %s AND is_open = TRUE
            """,
            (trader_id, symbol),
        )
        existing = cur.fetchone()

        if existing:
            h_id, old_qty, old_avg, old_total = existing
            old_qty = int(old_qty)
            old_total = float(old_total)
            new_qty = old_qty + quantity
            new_total = round(old_total + total_value, 2)
            new_avg = round(new_total / new_qty, 2)
            cur.execute(
                """
                UPDATE portfolio_holdings
                SET quantity = %s, avg_buy_price = %s, total_invested = %s,
                    last_updated = %s, updated_at = NOW()
                WHERE id = %s
                """,
                (new_qty, new_avg, new_total, today, h_id),
            )
        else:
            new_qty = quantity
            new_avg = round(price, 2)
            new_total = total_value
            cur.execute(
                """
                INSERT INTO portfolio_holdings
                    (trader_id, symbol, quantity, avg_buy_price, total_invested,
                     first_bought_at, last_updated, is_open)
                VALUES (%s, %s, %s, %s, %s, %s, %s, TRUE)
                """,
                (trader_id, symbol, new_qty, new_avg, new_total, trade_date, today),
            )

        cur.close()
        return {
            "status": "ok",
            "message": "Buy recorded",
            "symbol": symbol,
            "quantity": quantity,
            "price": price,
            "total_value": total_value,
            "new_avg_price": new_avg,
            "new_quantity": new_qty,
        }
    except Exception as e:
        logger.exception("record_buy error trader_id=%s", trader_id)
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        if conn:
            conn.close()


@app.post("/portfolio/{trader_id}/sell")
async def record_sell(trader_id: int, request: Request):
    conn = None
    try:
        body = await request.json()
        symbol = (body.get("symbol") or "").upper().strip()
        quantity = _int(body.get("quantity"))
        price = _float(body.get("price"))
        notes = body.get("notes") or ""
        trade_date_raw = body.get("date")

        if not symbol:
            return JSONResponse(status_code=400, content={"error": "symbol is required"})
        if not quantity or quantity <= 0:
            return JSONResponse(status_code=400, content={"error": "quantity must be > 0"})
        if not price or price <= 0:
            return JSONResponse(status_code=400, content={"error": "price must be > 0"})

        conn = _get_conn()
        conn.autocommit = True
        cur = conn.cursor()
        today = _get_db_date(conn)
        trade_date = date.fromisoformat(trade_date_raw) if trade_date_raw else today

        # Validate trader
        cur.execute("SELECT 1 FROM traders WHERE id = %s", (trader_id,))
        if not cur.fetchone():
            return JSONResponse(status_code=404, content={"error": "Trader not found"})

        # Validate symbol
        cur.execute("SELECT 1 FROM stocks_master WHERE symbol = %s", (symbol,))
        if not cur.fetchone():
            return JSONResponse(status_code=404, content={"error": "Symbol not found"})

        # Check holdings
        cur.execute(
            """
            SELECT id, quantity, avg_buy_price, total_invested
            FROM portfolio_holdings
            WHERE trader_id = %s AND symbol = %s AND is_open = TRUE
            """,
            (trader_id, symbol),
        )
        holding = cur.fetchone()
        if not holding:
            return JSONResponse(status_code=400, content={"error": "Insufficient holdings: no open position for this symbol"})

        h_id, old_qty, avg_px, old_total = holding
        old_qty = int(old_qty)
        avg_px = float(avg_px)

        if quantity > old_qty:
            return JSONResponse(status_code=400, content={"error": f"Insufficient holdings: have {old_qty}, selling {quantity}"})

        total_value = round(quantity * price, 2)

        # Record transaction
        cur.execute(
            """
            INSERT INTO trade_transactions
                (trader_id, symbol, transaction_type, quantity, price, total_value, transaction_date, notes)
            VALUES (%s, %s, 'SELL', %s, %s, %s, %s, %s)
            """,
            (trader_id, symbol, quantity, price, total_value, trade_date, notes),
        )

        # Update holding
        new_qty = old_qty - quantity
        if new_qty == 0:
            cur.execute(
                """
                UPDATE portfolio_holdings
                SET quantity = 0, total_invested = 0, is_open = FALSE,
                    last_updated = %s, updated_at = NOW()
                WHERE id = %s
                """,
                (today, h_id),
            )
        else:
            new_total = round(new_qty * avg_px, 2)
            cur.execute(
                """
                UPDATE portfolio_holdings
                SET quantity = %s, total_invested = %s,
                    last_updated = %s, updated_at = NOW()
                WHERE id = %s
                """,
                (new_qty, new_total, today, h_id),
            )

        # Realised P&L
        cost_basis = round(quantity * avg_px, 2)
        proceeds = total_value
        realised_pnl = round(proceeds - cost_basis, 2)
        realised_pnl_pct = round(realised_pnl / cost_basis * 100, 2) if cost_basis else 0.0

        cur.close()
        return {
            "status": "ok",
            "message": "Sell recorded",
            "symbol": symbol,
            "quantity_sold": quantity,
            "sell_price": price,
            "proceeds": proceeds,
            "realised_pnl": realised_pnl,
            "realised_pnl_pct": realised_pnl_pct,
            "remaining_quantity": new_qty,
        }
    except Exception as e:
        logger.exception("record_sell error trader_id=%s", trader_id)
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        if conn:
            conn.close()


@app.get("/watchlist/{trader_id}")
def get_watchlist(trader_id: int):
    conn = None
    try:
        conn = _get_conn()
        cur = conn.cursor()

        cur.execute("SELECT 1 FROM traders WHERE id = %s", (trader_id,))
        if not cur.fetchone():
            return JSONResponse(status_code=404, content={"error": "Trader not found"})

        cur.execute(
            """
            SELECT tw.symbol, tw.added_at, tw.target_price, tw.notes
            FROM trader_watchlist tw
            WHERE tw.trader_id = %s AND tw.is_active = TRUE
            ORDER BY tw.symbol
            """,
            (trader_id,),
        )
        items = cur.fetchall()

        watchlist = []
        for sym, added_at, target_price, notes in items:
            # Current price
            cur.execute(
                "SELECT ltp, close FROM price_history WHERE symbol = %s ORDER BY date DESC LIMIT 1",
                (sym,),
            )
            ph = cur.fetchone()
            current_price = (_float(ph[0]) or _float(ph[1])) if ph else None

            # Current signal and reason
            cur.execute(
                """
                SELECT DISTINCT ON (symbol) overall_signal, raw_output
                FROM analysis_results
                WHERE symbol = %s
                ORDER BY symbol, analysis_date DESC, session_no DESC NULLS LAST, id DESC
                """,
                (sym,),
            )
            ar = cur.fetchone()
            signal = ar[0] if ar else "UNKNOWN"
            raw_ar = ar[1] if ar else {}
            if not isinstance(raw_ar, dict):
                raw_ar = {}
            reason = raw_ar.get("signal_reason") or None

            target = _float(target_price)
            gap_to_target_pct = None
            if target and current_price and current_price != 0:
                gap_to_target_pct = round((target - current_price) / current_price * 100, 2)

            watchlist.append({
                "symbol": sym,
                "added_at": added_at.isoformat() if hasattr(added_at, "isoformat") else str(added_at),
                "target_price": target,
                "current_price": current_price,
                "signal": signal,
                "reason": reason,
                "gap_to_target_pct": gap_to_target_pct,
            })

        cur.close()
        return {"trader_id": trader_id, "watchlist": watchlist}
    except Exception as e:
        logger.exception("get_watchlist error trader_id=%s", trader_id)
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        if conn:
            conn.close()


@app.post("/watchlist/{trader_id}/add")
async def watchlist_add(trader_id: int, request: Request):
    conn = None
    try:
        body = await request.json()
        symbol = (body.get("symbol") or "").upper().strip()
        target_price = _float(body.get("target_price"))
        notes = body.get("notes") or ""

        if not symbol:
            return JSONResponse(status_code=400, content={"error": "symbol is required"})

        conn = _get_conn()
        conn.autocommit = True
        cur = conn.cursor()

        cur.execute("SELECT 1 FROM traders WHERE id = %s", (trader_id,))
        if not cur.fetchone():
            return JSONResponse(status_code=404, content={"error": "Trader not found"})

        cur.execute("SELECT 1 FROM stocks_master WHERE symbol = %s", (symbol,))
        if not cur.fetchone():
            return JSONResponse(status_code=404, content={"error": "Symbol not found"})

        cur.execute(
            """
            INSERT INTO trader_watchlist (trader_id, symbol, target_price, notes, is_active, added_at)
            VALUES (%s, %s, %s, %s, TRUE, CURRENT_DATE)
            ON CONFLICT (trader_id, symbol)
            DO UPDATE SET is_active = TRUE, target_price = EXCLUDED.target_price,
                          notes = EXCLUDED.notes
            """,
            (trader_id, symbol, target_price, notes),
        )
        cur.close()
        return {"status": "ok", "message": f"{symbol} added to watchlist"}
    except Exception as e:
        logger.exception("watchlist_add error trader_id=%s", trader_id)
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        if conn:
            conn.close()


@app.post("/watchlist/{trader_id}/remove")
async def watchlist_remove(trader_id: int, request: Request):
    conn = None
    try:
        body = await request.json()
        symbol = (body.get("symbol") or "").upper().strip()

        if not symbol:
            return JSONResponse(status_code=400, content={"error": "symbol is required"})

        conn = _get_conn()
        conn.autocommit = True
        cur = conn.cursor()

        cur.execute("SELECT 1 FROM traders WHERE id = %s", (trader_id,))
        if not cur.fetchone():
            return JSONResponse(status_code=404, content={"error": "Trader not found"})

        cur.execute(
            """
            UPDATE trader_watchlist SET is_active = FALSE
            WHERE trader_id = %s AND symbol = %s
            """,
            (trader_id, symbol),
        )
        cur.close()
        return {"status": "ok", "message": f"{symbol} removed from watchlist"}
    except Exception as e:
        logger.exception("watchlist_remove error trader_id=%s", trader_id)
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        if conn:
            conn.close()

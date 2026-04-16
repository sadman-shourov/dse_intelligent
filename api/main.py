from __future__ import annotations

import json
import logging
import os
import traceback
from datetime import date, datetime, time as dtime
from pathlib import Path
from typing import Any

import psycopg2
import pytz
from dotenv import load_dotenv
from fastapi import FastAPI, Request, Body, Query
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
from api.health import check_data_freshness
from pulse.telegram import deliver_pulse, deliver_premarket_briefing, send_telegram_message, deliver_extreme_move_alerts, deliver_pulse_if_needed  # noqa: F401

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
    cur.execute("SELECT CURRENT_DATE")
    row = cur.fetchone()
    cur.close()
    if not row or row[0] is None:
        raise RuntimeError("SELECT CURRENT_DATE returned no date")
    return row[0]


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


def _market_status_context() -> dict[str, Any]:
    dhaka = pytz.timezone("Asia/Dhaka")
    now = datetime.now(dhaka)
    # DSE trading days: Sun, Mon, Tue, Wed, Thu
    is_dse_day = now.weekday() in [0, 1, 2, 3, 6]
    market_open_time = dtime(10, 0)
    market_close_time = dtime(14, 30)
    current_time = now.time()
    is_market_open = (
        is_dse_day and
        market_open_time <= current_time <= market_close_time
    )
    return {
        "is_market_open": is_market_open,
        "market_status": "Open" if is_market_open else "Closed",
        "current_time_dhaka": now.strftime("%I:%M %p"),
    }


def _stock_not_found_response(symbol: str) -> JSONResponse:
    sym = (symbol or "").strip()
    return JSONResponse(
        status_code=404,
        content={
            "error": "Symbol not found",
            "message": (
                f"'{sym}' is not a valid DSE trading code. "
                f"Try GET /stock/search?q={sym} to find the right code."
            ),
        },
    )


def _trader_not_found_response(trader_id: int) -> JSONResponse:
    return JSONResponse(
        status_code=404,
        content={
            "error": "Trader not found",
            "message": f"No trader found with ID {trader_id}.",
        },
    )


def _serialize(obj: Any) -> str:
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def _jsonify(data: Any) -> Any:
    """Recursively coerce any date/datetime objects so JSONResponse never raises."""
    return json.loads(json.dumps(data, default=_serialize))


def serialize_response(data: Any, status_code: int = 200) -> JSONResponse:
    """Return JSONResponse with date/datetime-safe payload (ignores request body)."""
    return JSONResponse(status_code=status_code, content=_jsonify(data))


# ---------------------------------------------------------------------------
# Existing job runner
# ---------------------------------------------------------------------------

def _run_job(fn):
    try:
        result = fn()
        return JSONResponse(status_code=200, content=_jsonify(result))
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


@app.get("/health/data")
def data_health_check():
    conn = _get_conn()
    try:
        result = check_data_freshness(conn)
        status_code = 200 if result["healthy"] else 503
        return JSONResponse(status_code=status_code, content=_jsonify(result))
    finally:
        conn.close()


@app.get("/")
def root():
    return {
        "endpoints": [
            # Health
            {"path": "/health/data", "method": "GET", "description": "Data freshness for pulse (503 if unhealthy)"},
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
            {"path": "/stock/search", "method": "GET", "description": "Search DSE symbol (query param q); exact then fuzzy"},
            {"path": "/stock/search/{query}", "method": "GET", "description": "Search DSE stock symbol by name"},
            {"path": "/alerts/extreme-moves", "method": "POST", "description": "Check and send extreme move alerts"},
            {"path": "/alerts/pipeline-failure", "method": "POST", "description": "Send pipeline failure alert to all traders"},
            # Pulse
            {"path": "/pulse/deliver/all", "method": "POST", "description": "Deliver latest pulse to all active traders via Telegram"},
            {"path": "/pulse/deliver/{trader_id}", "method": "POST", "description": "Deliver latest pulse to one trader via Telegram"},
            {"path": "/pulse/generate/{trader_id}", "method": "POST", "description": "Generate DeepSeek market pulse + Telegram message for a trader"},
            {"path": "/pulse/premarket/deliver/all", "method": "POST", "description": "Generate and send pre-market briefing to all active traders"},
            {"path": "/pulse/premarket/{trader_id}", "method": "POST", "description": "Generate and send pre-market briefing to one trader"},
            # Chatbot
            {"path": "/stock/{symbol}", "method": "GET", "description": "Latest analysis for a single stock"},
            {"path": "/market/summary", "method": "GET", "description": "Today's market overview with signal counts"},
            {"path": "/portfolio/{trader_id}", "method": "GET", "description": "Trader's current portfolio with P&L"},
            {"path": "/portfolio/{trader_id}/buy", "method": "POST", "description": "Record a buy trade"},
            {"path": "/portfolio/{trader_id}/sell", "method": "POST", "description": "Record a sell trade"},
            {"path": "/watchlist/{trader_id}", "method": "GET", "description": "Trader's watchlist with current signals"},
            {"path": "/watchlist/{trader_id}/add", "method": "POST", "description": "Add a stock to watchlist"},
            {"path": "/watchlist/{trader_id}/remove", "method": "POST", "description": "Remove a stock from watchlist"},
            # Trader onboarding & preferences
            {"path": "/trader/register", "method": "POST", "description": "Register a new trader from Telegram"},
            {"path": "/trader/{trader_id}/onboarding", "method": "POST", "description": "Save onboarding data and mark complete"},
            {"path": "/trader/{trader_id}/preferences", "method": "GET", "description": "Get trader preferences"},
            {"path": "/trader/{trader_id}/preferences", "method": "POST", "description": "Update trader preferences"},
            {"path": "/trader/{trader_id}/stock-intent", "method": "POST", "description": "Save or update a stock-specific intent"},
            {"path": "/trader/{trader_id}/stock-intents", "method": "GET", "description": "Get all active stock intents"},
            {"path": "/trader/{trader_id}/stock-intent/{symbol}", "method": "DELETE", "description": "Deactivate a stock intent"},
            {"path": "/trader/by-chat-id/{chat_id}", "method": "GET", "description": "Look up active trader by Telegram chat ID (onboarding UX)"},
            {"path": "/trader/by-telegram/{telegram_chat_id}", "method": "GET", "description": "Look up trader by Telegram chat ID"},
        ]
    }


# ---------------------------------------------------------------------------
# Ingestion endpoints
# ---------------------------------------------------------------------------

@app.post("/ingest/sync-stocks")
async def ingest_sync_stocks(request: Request):
    return _run_job(sync_stocks_master)


@app.post("/ingest/append-price-history")
async def ingest_append_price_history(request: Request):
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

    analysis_result: dict | None = None
    delivery_results: dict[str, Any] = {
        "traders_delivered": 0,
        "traders_skipped": 0,
    }

    if result.get("status") == "ok":
        mkt_result = fetch_market_summary()
        result["market_summary_updated"] = mkt_result.get("status")
        try:
            analysis_result = analyse_all_symbols()
        except Exception as e:
            logger.exception("analyse_all_symbols after live ticks failed")
            analysis_result = {"status": "error", "message": str(e)}
        try:
            delivery_results = deliver_pulse_if_needed()
        except Exception as e:
            logger.exception("deliver_pulse_if_needed failed")
            delivery_results = {
                "status": "error",
                "message": str(e),
                "traders_delivered": 0,
                "traders_skipped": 0,
            }
    elif result.get("status") == "skipped":
        try:
            delivery_results = deliver_pulse_if_needed()
        except Exception as e:
            logger.exception("deliver_pulse_if_needed (skipped ingest) failed")
            delivery_results = {
                "status": "error",
                "message": str(e),
                "traders_delivered": 0,
                "traders_skipped": 0,
            }

    if isinstance(analysis_result, dict) and analysis_result.get("status") == "ok":
        result["analysis"] = {
            "buy_signals": analysis_result.get("buy_signals", 0),
            "watch_signals": analysis_result.get("watch_signals", 0),
            "exit_signals": analysis_result.get("exit_signals", 0),
        }
    elif analysis_result is not None:
        result["analysis"] = {"status": analysis_result.get("status"), "message": analysis_result.get("message")}

    result["pulses_sent"] = int(delivery_results.get("traders_delivered") or 0)
    result["pulses_skipped"] = int(delivery_results.get("traders_skipped") or 0)
    if delivery_results.get("status") == "error":
        result["pulse_delivery_error"] = delivery_results.get("message")

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

    return JSONResponse(status_code=200, content=_jsonify(result))


@app.post("/ingest/cleanup-live-ticks")
async def ingest_cleanup_live_ticks(request: Request):
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


@app.post("/alerts/pipeline-failure")
def pipeline_failure_alert():
    """Send a failure alert to all active traders with Telegram chat IDs."""
    from pulse.telegram import send_telegram_message
    from datetime import datetime
    import pytz

    conn = None
    try:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'traders'
            """
        )
        tcols = {r[0] for r in cur.fetchall()}
        q = """
            SELECT telegram_chat_id, COALESCE(name, '')
            FROM traders
            WHERE telegram_chat_id IS NOT NULL
        """
        if "is_active" in tcols:
            q += " AND is_active = TRUE"
        cur.execute(q)
        traders = cur.fetchall()
        cur.close()

        dhaka = pytz.timezone("Asia/Dhaka")
        now = datetime.now(dhaka).strftime("%d %b %H:%M")

        message = (
            f"⚠️ <b>NexTrade System Alert</b>\n\n"
            f"Data pipeline issue detected at {now} Dhaka time.\n"
            f"Market pulse may be delayed.\n"
            f"We are looking into it."
        )

        alerted = 0
        for row in traders:
            chat_id, name = row
            try:
                res = send_telegram_message(
                    chat_id=str(chat_id),
                    message=message,
                    parse_mode="HTML",
                )
                if res.get("status") == "ok":
                    alerted += 1
            except Exception as e:
                logger.warning("Failed to alert %s: %s", name or chat_id, e)

        return JSONResponse(
            status_code=200,
            content=_jsonify({"status": "ok", "alerted": alerted}),
        )
    except Exception as e:
        logger.exception("pipeline_failure_alert error")
        return JSONResponse(
            status_code=500,
            content=_jsonify(
                {"status": "error", "message": str(e), "traceback": traceback.format_exc()}
            ),
        )
    finally:
        if conn:
            conn.close()


@app.post("/ingest/market-summary")
async def ingest_market_summary(request: Request):
    return _run_job(fetch_market_summary)


@app.post("/ingest/stock-fundamentals")
async def ingest_stock_fundamentals(request: Request):
    return _run_job(fetch_stock_fundamentals)


# ---------------------------------------------------------------------------
# Analysis endpoints
# ---------------------------------------------------------------------------

@app.post("/analyse/all")
async def analyse_all_endpoint(request: Request):
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
        return _jsonify(fn()), None
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

    return JSONResponse(status_code=200, content=_jsonify(results))


@app.post("/refresh/prices")
def refresh_prices():
    from datetime import datetime
    results: dict = {"status": "ok", "timestamp": datetime.utcnow().isoformat() + "Z"}

    res, err = _run_step(append_price_history)
    results["prices"] = res if res is not None else {"status": "error", "message": err}

    res, err = _run_step(analyse_all_symbols)
    results["analysis"] = res if res is not None else {"status": "error", "message": err}

    return JSONResponse(status_code=200, content=_jsonify(results))


@app.post("/refresh/analysis")
def refresh_analysis():
    from datetime import datetime
    results: dict = {"status": "ok", "timestamp": datetime.utcnow().isoformat() + "Z"}

    res, err = _run_step(analyse_all_symbols)
    results["analysis"] = res if res is not None else {"status": "error", "message": err}

    return JSONResponse(status_code=200, content=_jsonify(results))


# ---------------------------------------------------------------------------
# Evaluate / accuracy endpoints
# ---------------------------------------------------------------------------

@app.post("/evaluate/signals")
async def evaluate_signals_endpoint(request: Request):
    result = evaluate_past_signals(days_back=7)
    status_code = 200 if result.get("status") == "ok" else 500
    return serialize_response(result, status_code=status_code)


@app.get("/evaluate/scorecard")
def get_scorecard_endpoint():
    result = get_recent_scorecard(days=7)
    return JSONResponse(status_code=200, content=_jsonify(result))


@app.post("/evaluate/accuracy")
async def calculate_accuracy_endpoint(request: Request):
    result = calculate_accuracy_scores(period_days=30)
    status_code = 200 if result.get("status") == "ok" else 500
    return serialize_response(result, status_code=status_code)


@app.get("/signals/today")
def signals_today():
    conn = None
    cur = None
    try:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT DISTINCT ON (symbol)
                symbol,
                overall_signal,
                confidence_score,
                raw_output->>'signal_reason' AS reason,
                (raw_output->>'current_price')::numeric AS price
            FROM analysis_results
            WHERE analysis_date = CURRENT_DATE
            ORDER BY symbol, confidence_score DESC
            """
        )
        rows = cur.fetchall()

        out: dict = {"BUY": [], "WATCH": [], "HOLD": [], "EXIT": []}
        for sym, signal, _confidence, reason, price in rows:
            if signal in out:
                out[signal].append(
                    {
                        "symbol": sym,
                        "price": float(price) if price is not None else 0,
                        "reason": reason or "",
                    }
                )

        out["date"] = str(date.today())
        return out
    except Exception as e:
        logger.exception("signals_today error")
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        if cur is not None:
            try:
                cur.close()
            except Exception:
                pass
        if conn is not None:
            conn.close()


# ---------------------------------------------------------------------------
# Pulse endpoints
# ---------------------------------------------------------------------------

@app.post("/pulse/deliver/all")
async def pulse_deliver_all(request: Request):
    try:
        result = deliver_pulse()
        if result.get("status") == "error":
            return serialize_response(result, status_code=500)
        return serialize_response(result)
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.post("/pulse/deliver/{trader_id}")
def pulse_deliver_trader(trader_id: int):
    try:
        result = deliver_pulse(trader_id)
        if result.get("status") == "error":
            return JSONResponse(status_code=500, content=_jsonify(result))
        return JSONResponse(status_code=200, content=_jsonify(result))
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "error", "message": str(e), "traceback": traceback.format_exc()})


@app.post("/pulse/generate/{trader_id}")
def pulse_endpoint(trader_id: int):
    try:
        result = generate_pulse(trader_id)
        if result.get("status") == "error" and result.get("reason") == "trader not found":
            return JSONResponse(status_code=404, content=_jsonify(result))
        if result.get("status") in ("skipped", "ok"):
            return JSONResponse(status_code=200, content=_jsonify(result))
        return JSONResponse(status_code=500, content=_jsonify(result))
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "error", "message": str(e), "traceback": traceback.format_exc()})


@app.post("/pulse/premarket/deliver/all")
async def premarket_deliver_all(request: Request):
    try:
        result = deliver_premarket_briefing()
        return serialize_response(result)
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


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

@app.get("/stock/search")
def search_stocks(q: str = Query(..., min_length=1, description="DSE trading code fragment or full symbol")):
    """Exact match first, then up to 5 active symbols containing the query."""
    conn = None
    cur = None
    try:
        qn = q.strip().upper()
        if not qn:
            return {
                "matches": [],
                "exact": False,
                "message": "Query q cannot be empty.",
            }
        conn = _get_conn()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT symbol, is_dsex, is_active
            FROM stocks_master
            WHERE symbol = %s
            """,
            (qn,),
        )
        exact = cur.fetchone()
        if exact:
            return {
                "matches": [{"symbol": exact[0], "is_dsex": exact[1]}],
                "exact": True,
            }

        cur.execute(
            """
            SELECT symbol, is_dsex
            FROM stocks_master
            WHERE symbol ILIKE %s AND is_active = TRUE
            ORDER BY
                CASE WHEN symbol ILIKE %s THEN 0 ELSE 1 END,
                symbol ASC
            LIMIT 5
            """,
            (f"%{qn}%", f"{qn}%"),
        )
        rows = cur.fetchall()

        if not rows:
            return {
                "matches": [],
                "exact": False,
                "message": (
                    f"No stocks found matching '{qn}'. "
                    f"Try the full DSE trading code."
                ),
            }

        return {
            "matches": [{"symbol": r[0], "is_dsex": r[1]} for r in rows],
            "exact": False,
            "message": f"Found {len(rows)} matches for '{qn}'",
        }
    except Exception as e:
        logger.exception("search_stocks error q=%s", q)
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        if cur is not None:
            try:
                cur.close()
            except Exception:
                pass
        if conn is not None:
            conn.close()


@app.get("/stock/search/{query}")
def search_symbol(query: str):
    conn = None
    cur = None
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
        return {"query": query, "matches": matches}
    except Exception as e:
        logger.exception("search_symbol error query=%s", query)
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        if cur is not None:
            try:
                cur.close()
            except Exception:
                pass
        if conn is not None:
            conn.close()


@app.get("/stock/{symbol}")
def get_stock(symbol: str):
    conn = None
    cur = None
    try:
        symbol = symbol.upper().strip()
        conn = _get_conn()
        cur = conn.cursor()

        # Check symbol exists
        cur.execute("SELECT 1 FROM stocks_master WHERE symbol = %s", (symbol,))
        if not cur.fetchone():
            return _stock_not_found_response(symbol)

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

        cur.execute(
            "SELECT MAX(date) FROM price_history WHERE symbol = %s",
            (symbol,),
        )
        latest_price_row = cur.fetchone()
        latest_price_date = latest_price_row[0] if latest_price_row else None

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
        raw: dict = {}

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

        data_as_of = (
            latest_price_date.strftime("%d %b %Y")
            if latest_price_date
            else analysis_date
        )

        reason = (raw.get("signal_reason") if ar and isinstance(raw, dict) else None)

        market_ctx = _market_status_context()

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
            "data_as_of": data_as_of,
            "class_flags": class_flags,
            "is_market_open": market_ctx["is_market_open"],
            "market_status": market_ctx["market_status"],
            "current_time_dhaka": market_ctx["current_time_dhaka"],
            "ma20": raw.get("moving_averages", {}).get("ma20"),
            "ma50": raw.get("moving_averages", {}).get("ma50"),
            "ma200": raw.get("moving_averages", {}).get("ma200"),
            "above_ma50": raw.get("moving_averages", {}).get("above_ma50"),
            "above_ma200": raw.get("moving_averages", {}).get("above_ma200"),
            "ma_trend": raw.get("moving_averages", {}).get("trend"),
            "rsi_direction": raw.get("rsi_direction")
            if raw.get("rsi_direction") is not None
            else (raw.get("rsi") or {}).get("rsi_direction"),
            "averaging_zone": raw.get("averaging_zone")
            if raw.get("averaging_zone") is not None
            else (raw.get("rsi") or {}).get("averaging_zone"),
            "volume_price_pattern": raw.get("volume_price_pattern")
            if raw.get("volume_price_pattern") is not None
            else (raw.get("volume_profile") or {}).get("volume_price_pattern"),
            "averaging_signal": raw.get("averaging_signal")
            if raw.get("averaging_signal") is not None
            else (raw.get("volume_profile") or {}).get("averaging_signal"),
        }
    except Exception as e:
        logger.exception("get_stock error symbol=%s", symbol)
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        if cur is not None:
            try:
                cur.close()
            except Exception:
                pass
        if conn is not None:
            conn.close()


@app.get("/market/summary")
def get_market_summary():
    conn = None
    cur = None
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
            WHERE analysis_date = CURRENT_DATE
            GROUP BY overall_signal
            """
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


        market_ctx = _market_status_context()
        is_market_open = market_ctx["is_market_open"]

        return {
            "date": today.strftime("%d %b %Y"),
            "dsex": _float(ms[1]) if ms else None,
            "dses": _float(ms[2]) if ms else None,
            "total_volume": _int(ms[3]) if ms else None,
            "total_value_mn": _float(ms[4]) if ms else None,
            "signals": signal_counts,
            "top_buys": top_buys,
            "top_exits": top_exits,
            "is_market_open": is_market_open,
            "market_status": "Open" if is_market_open else "Closed",
            "current_time_dhaka": market_ctx["current_time_dhaka"],
            "dsex_note": (
                "Live DSEX index unavailable. Showing last close."
                if not is_market_open
                else "DSEX as of yesterday's close. Updates after 4:10pm."
            ),
            "dsex_date": ms[0].isoformat() if ms else None,
        }
    except Exception as e:
        logger.exception("get_market_summary error")
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        if cur is not None:
            try:
                cur.close()
            except Exception:
                pass
        if conn is not None:
            conn.close()


@app.get("/portfolio/{trader_id}")
def get_portfolio(trader_id: int):
    conn = None
    cur = None
    try:
        conn = _get_conn()
        cur = conn.cursor()

        # Check trader exists
        cur.execute("SELECT name FROM traders WHERE id = %s", (trader_id,))
        trader = cur.fetchone()
        if not trader:
            return _trader_not_found_response(trader_id)
        trader_name = trader[0] or f"Trader {trader_id}"

        # Open holdings
        cur.execute(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'portfolio_holdings'
            """
        )
        ph_cols = {r[0] for r in cur.fetchall()}
        extra_cols = ""
        if "target_price" in ph_cols:
            extra_cols += ", target_price"

        cur.execute(
            f"""
            SELECT symbol, quantity, avg_buy_price, total_invested{extra_cols}
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

        for row in holdings:
            if "target_price" in ph_cols:
                sym, qty, avg_px, invested, tgt_px = row[0], row[1], row[2], row[3], row[4]
            else:
                sym, qty, avg_px, invested = row[0], row[1], row[2], row[3]
                tgt_px = None
            qty = int(qty or 0)
            avg_px = float(avg_px or 0)
            invested = float(invested or 0)
            tgt_px = _float(tgt_px) if tgt_px is not None else None

            # Today's analysis price (live tick in raw_output) with price_history fallback
            cur.execute(
                """
                SELECT
                    ar.overall_signal,
                    COALESCE(
                        NULLIF(trim(ar.raw_output->>'current_price'), '')::numeric,
                        ph.ltp,
                        ph.close
                    ) AS current_price
                FROM (SELECT 1) AS _
                LEFT JOIN LATERAL (
                    SELECT overall_signal, raw_output
                    FROM analysis_results
                    WHERE symbol = %s AND analysis_date = CURRENT_DATE
                    ORDER BY session_no DESC NULLS LAST, id DESC
                    LIMIT 1
                ) ar ON TRUE
                LEFT JOIN LATERAL (
                    SELECT ltp, close
                    FROM price_history
                    WHERE symbol = %s
                    ORDER BY date DESC
                    LIMIT 1
                ) ph ON TRUE
                """,
                (sym, sym),
            )
            row_px = cur.fetchone()
            signal = "HOLD"
            current_price = avg_px
            if row_px:
                signal = row_px[0] or "HOLD"
                cp = _float(row_px[1])
                if cp is not None:
                    current_price = cp

            current_value = round(qty * current_price, 2)
            pnl_value = round(current_value - invested, 2)
            pnl_pct = round(pnl_value / invested * 100, 2) if invested else 0.0
            urgent_exit = pnl_pct <= -8.0

            if urgent_exit:
                urgent_exits += 1

            total_invested += invested
            total_current_value += current_value

            pos = {
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
            }
            if "target_price" in ph_cols:
                pos["target_price"] = tgt_px
            positions.append(pos)


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
        if cur is not None:
            try:
                cur.close()
            except Exception:
                pass
        if conn is not None:
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
        target_price = _float(body.get("target_price"))
        trade_date_raw = body.get("date")

        if not symbol or not quantity or quantity <= 0 or not price or price <= 0:
            return JSONResponse(
                status_code=400,
                content={
                    "error": "Invalid input",
                    "message": "Required fields: symbol, quantity (>0), price (>0)",
                },
            )

        conn = _get_conn()
        conn.autocommit = True
        cur = conn.cursor()
        today = _get_db_date(conn)
        trade_date = date.fromisoformat(trade_date_raw) if trade_date_raw else today

        # Validate trader
        cur.execute("SELECT 1 FROM traders WHERE id = %s", (trader_id,))
        if not cur.fetchone():
            return _trader_not_found_response(trader_id)

        # Validate symbol
        cur.execute("SELECT 1 FROM stocks_master WHERE symbol = %s", (symbol,))
        if not cur.fetchone():
            return _stock_not_found_response(symbol)

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

        if target_price is not None and target_price > 0:
            cur.execute(
                """
                UPDATE portfolio_holdings
                SET target_price = %s, updated_at = NOW()
                WHERE trader_id = %s AND symbol = %s AND is_open = TRUE
                """,
                (target_price, trader_id, symbol),
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
            "target_price": target_price,
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

        if not symbol or not quantity or quantity <= 0 or not price or price <= 0:
            return JSONResponse(
                status_code=400,
                content={
                    "error": "Invalid input",
                    "message": "Required fields: symbol, quantity (>0), price (>0)",
                },
            )

        conn = _get_conn()
        conn.autocommit = True
        cur = conn.cursor()
        today = _get_db_date(conn)
        trade_date = date.fromisoformat(trade_date_raw) if trade_date_raw else today

        # Validate trader
        cur.execute("SELECT 1 FROM traders WHERE id = %s", (trader_id,))
        if not cur.fetchone():
            return _trader_not_found_response(trader_id)

        # Validate symbol
        cur.execute("SELECT 1 FROM stocks_master WHERE symbol = %s", (symbol,))
        if not cur.fetchone():
            return _stock_not_found_response(symbol)

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
            return JSONResponse(
                status_code=400,
                content={
                    "error": "Insufficient holdings",
                    "message": (
                        f"You only have 0 shares of {symbol}. "
                        f"Cannot sell {quantity}."
                    ),
                },
            )

        h_id, old_qty, avg_px, old_total = holding
        old_qty = int(old_qty)
        avg_px = float(avg_px)

        if quantity > old_qty:
            return JSONResponse(
                status_code=400,
                content={
                    "error": "Insufficient holdings",
                    "message": (
                        f"You only have {old_qty} shares of {symbol}. "
                        f"Cannot sell {quantity}."
                    ),
                },
            )

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
    cur = None
    try:
        conn = _get_conn()
        cur = conn.cursor()

        cur.execute("SELECT 1 FROM traders WHERE id = %s", (trader_id,))
        if not cur.fetchone():
            return _trader_not_found_response(trader_id)

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

        return {"trader_id": trader_id, "watchlist": watchlist}
    except Exception as e:
        logger.exception("get_watchlist error trader_id=%s", trader_id)
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        if cur is not None:
            try:
                cur.close()
            except Exception:
                pass
        if conn is not None:
            conn.close()


# ---------------------------------------------------------------------------
# Trader onboarding & preferences endpoints
# ---------------------------------------------------------------------------

@app.post("/trader/register")
async def register_trader(request: Request):
    conn = None
    try:
        body = await request.json()
        if not isinstance(body, dict):
            body = {}
        name = (body.get("name") or "").strip()
        chat_raw = body.get("chat_id") or body.get("telegram_id") or body.get("telegram_chat_id") or ""
        telegram_id = str(chat_raw).strip()
        if not telegram_id:
            return JSONResponse(
                status_code=400,
                content={
                    "error": "chat_id is required",
                    "message": "Required field: chat_id (Telegram chat ID).",
                },
            )
        if not name:
            return JSONResponse(
                status_code=400,
                content={
                    "error": "name is required",
                    "message": "Required field: name.",
                },
            )

        conn = _get_conn()
        cur = conn.cursor()

        cur.execute(
            "SELECT id, name, onboarding_complete FROM traders WHERE telegram_chat_id = %s LIMIT 1",
            (telegram_id,),
        )
        row = cur.fetchone()
        if row:
            cur.close()
            return _jsonify({
                "status": "exists",
                "trader_id": int(row[0]),
                "message": "Trader already registered",
                "trader": {
                    "trader_id": int(row[0]),
                    "name": row[1] or "",
                    "onboarding_complete": bool(row[2]),
                },
            })

        cur.execute(
            """
            INSERT INTO traders (name, telegram_chat_id, is_active, onboarding_complete)
            VALUES (%s, %s, TRUE, FALSE)
            RETURNING id
            """,
            (name, telegram_id),
        )
        new_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        return _jsonify({
            "status": "ok",
            "trader_id": int(new_id),
            "name": name,
            "message": f"Welcome to ARIA, {name}! Your trading profile has been created.",
            "trader": {"trader_id": int(new_id), "name": name, "onboarding_complete": False},
        })
    except Exception as e:
        logger.exception("register_trader error")
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        if conn:
            conn.close()


@app.post("/trader/{trader_id}/onboarding")
async def complete_onboarding(trader_id: int, request: Request):
    conn = None
    try:
        body = await request.json()
        trading_style = body.get("trading_style", "")
        holding_period = body.get("holding_period", "")
        risk_tolerance = body.get("risk_tolerance", "")
        strategy_notes = body.get("strategy_notes", "")

        conn = _get_conn()
        cur = conn.cursor()

        cur.execute(
            """
            UPDATE traders
            SET trading_style = %s, holding_period = %s, risk_tolerance = %s,
                strategy_notes = %s, onboarding_complete = TRUE
            WHERE id = %s
            """,
            (trading_style, holding_period, risk_tolerance, strategy_notes, trader_id),
        )
        cur.execute(
            """
            INSERT INTO trader_preferences (trader_id, trading_style, holding_period, risk_tolerance, updated_at)
            VALUES (%s, %s, %s, %s, NOW())
            ON CONFLICT (trader_id) DO UPDATE SET
                trading_style = EXCLUDED.trading_style,
                holding_period = EXCLUDED.holding_period,
                risk_tolerance = EXCLUDED.risk_tolerance,
                updated_at = NOW()
            """,
            (trader_id, trading_style, holding_period, risk_tolerance),
        )
        conn.commit()

        cur.execute(
            "SELECT id, name, trading_style, holding_period, risk_tolerance, strategy_notes, onboarding_complete FROM traders WHERE id = %s",
            (trader_id,),
        )
        row = cur.fetchone()
        cur.close()
        if not row:
            return _trader_not_found_response(trader_id)
        return _jsonify({
            "status": "ok",
            "trader_id": int(row[0]),
            "name": row[1] or "",
            "trading_style": row[2] or "",
            "holding_period": row[3] or "",
            "risk_tolerance": row[4] or "",
            "strategy_notes": row[5] or "",
            "onboarding_complete": bool(row[6]),
        })
    except Exception as e:
        logger.exception("complete_onboarding trader_id=%s", trader_id)
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        if conn:
            conn.close()


@app.post("/trader/{trader_id}/preferences")
async def update_preferences(trader_id: int, request: Request):
    conn = None
    try:
        body = await request.json()
        if not isinstance(body, dict):
            return JSONResponse(status_code=400, content={"error": "JSON object body required"})

        conn = _get_conn()
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM traders WHERE id = %s", (trader_id,))
        if not cur.fetchone():
            cur.close()
            return _trader_not_found_response(trader_id)

        patch = {k: v for k, v in body.items() if v is not None}
        patch_json = json.dumps(patch, default=str)

        cur.execute(
            """
            UPDATE traders
            SET preferences = COALESCE(preferences, '{}'::jsonb) || %s::jsonb,
                updated_at = NOW()
            WHERE id = %s
            RETURNING preferences
            """,
            (patch_json, trader_id),
        )
        row = cur.fetchone()
        conn.commit()
        prefs = row[0] if row and row[0] is not None else {}
        if isinstance(prefs, str):
            prefs = json.loads(prefs)
        cur.close()
        return _jsonify({"status": "ok", "preferences": prefs})
    except Exception as e:
        logger.exception("update_preferences trader_id=%s", trader_id)
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        if conn:
            conn.close()


@app.get("/trader/{trader_id}/preferences")
def get_preferences(trader_id: int):
    conn = None
    try:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT COALESCE(preferences, '{}'::jsonb)
            FROM traders
            WHERE id = %s
            """,
            (trader_id,),
        )
        row = cur.fetchone()
        cur.close()
        if not row:
            return _trader_not_found_response(trader_id)
        prefs = row[0]
        if isinstance(prefs, str):
            prefs = json.loads(prefs)
        return _jsonify({"status": "ok", "preferences": prefs})
    except Exception as e:
        logger.exception("get_preferences trader_id=%s", trader_id)
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        if conn:
            conn.close()


@app.post("/trader/{trader_id}/stock-intent")
async def save_stock_intent(trader_id: int, request: Request):
    conn = None
    try:
        body = await request.json()
        symbol = (body.get("symbol") or "").upper().strip()
        if not symbol:
            return JSONResponse(status_code=400, content={"error": "symbol is required"})
        avg_buy_price = _float(body.get("avg_buy_price"))
        intent = (body.get("intent") or "HOLD").upper()
        target_price = _float(body.get("target_price"))
        stop_price = _float(body.get("stop_price"))
        timeframe = body.get("timeframe", "")
        notes = body.get("notes", "")

        conn = _get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO trader_stock_intents
                (trader_id, symbol, avg_buy_price, intent, target_price,
                 stop_price, timeframe, notes, is_active, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, TRUE, NOW())
            ON CONFLICT (trader_id, symbol) DO UPDATE SET
                avg_buy_price = EXCLUDED.avg_buy_price,
                intent = EXCLUDED.intent,
                target_price = EXCLUDED.target_price,
                stop_price = EXCLUDED.stop_price,
                timeframe = EXCLUDED.timeframe,
                notes = EXCLUDED.notes,
                is_active = TRUE,
                updated_at = NOW()
            """,
            (trader_id, symbol, avg_buy_price, intent, target_price,
             stop_price, timeframe, notes),
        )
        conn.commit()
        cur.close()
        return _jsonify({
            "status": "ok",
            "trader_id": trader_id,
            "symbol": symbol,
            "avg_buy_price": avg_buy_price,
            "intent": intent,
            "target_price": target_price,
            "stop_price": stop_price,
            "timeframe": timeframe,
            "notes": notes,
        })
    except Exception as e:
        logger.exception("save_stock_intent trader_id=%s symbol=%s", trader_id, symbol if 'symbol' in dir() else '?')
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        if conn:
            conn.close()


@app.get("/trader/{trader_id}/stock-intents")
def get_stock_intents(trader_id: int):
    conn = None
    try:
        conn = _get_conn()
        cur = conn.cursor()
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
                tsi.updated_at,
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
        cur.close()

        intents = []
        for r in rows:
            avg = _float(r[1]) or 0.0
            current = _float(r[10]) or 0.0
            pnl = round(((current - avg) / avg * 100), 2) if avg > 0 else 0.0
            intents.append({
                "symbol": r[0],
                "avg_buy_price": avg,
                "intent": r[2],
                "target_price": _float(r[3]),
                "stop_price": _float(r[4]),
                "timeframe": r[5] or "",
                "notes": r[6] or "",
                "updated_at": r[7].isoformat() if hasattr(r[7], "isoformat") else str(r[7]),
                "current_signal": r[8] or "",
                "confidence": _float(r[9]),
                "current_price": current,
                "pnl_pct": pnl,
            })
        return _jsonify({"trader_id": trader_id, "intents": intents})
    except Exception as e:
        logger.exception("get_stock_intents trader_id=%s", trader_id)
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        if conn:
            conn.close()


@app.delete("/trader/{trader_id}/stock-intent/{symbol}")
def delete_stock_intent(trader_id: int, symbol: str):
    conn = None
    try:
        symbol = symbol.upper().strip()
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute(
            "UPDATE trader_stock_intents SET is_active = FALSE WHERE trader_id = %s AND symbol = %s",
            (trader_id, symbol),
        )
        conn.commit()
        cur.close()
        return _jsonify({"status": "ok", "trader_id": trader_id, "symbol": symbol, "is_active": False})
    except Exception as e:
        logger.exception("delete_stock_intent trader_id=%s symbol=%s", trader_id, symbol)
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        if conn:
            conn.close()


@app.get("/trader/by-chat-id/{chat_id}")
def get_trader_by_chat_id(chat_id: str):
    conn = None
    cur = None
    try:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, name, telegram_chat_id, is_active
            FROM traders
            WHERE telegram_chat_id = %s AND is_active = TRUE
            """,
            (str(chat_id).strip(),),
        )
        row = cur.fetchone()
        if not row:
            return JSONResponse(
                status_code=404,
                content={
                    "found": False,
                    "message": "New user - onboarding required",
                },
            )

        cur.execute(
            """
            SELECT COUNT(*), COALESCE(SUM(total_invested), 0)
            FROM portfolio_holdings
            WHERE trader_id = %s AND is_open = TRUE
            """,
            (row[0],),
        )
        portfolio = cur.fetchone()
        open_positions = int(portfolio[0] or 0)
        total_invested = float(portfolio[1] or 0)
        disp_name = (row[1] or "").strip() or f"Trader {int(row[0])}"
        return _jsonify({
            "found": True,
            "trader_id": int(row[0]),
            "name": disp_name,
            "chat_id": row[2],
            "is_active": bool(row[3]),
            "open_positions": open_positions,
            "total_invested": total_invested,
            "message": f"Welcome back {disp_name}!",
        })
    except Exception as e:
        logger.exception("get_trader_by_chat_id chat_id=%s", chat_id)
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        if cur is not None:
            try:
                cur.close()
            except Exception:
                pass
        if conn is not None:
            conn.close()


@app.get("/trader/by-telegram/{telegram_chat_id}")
def get_trader_by_telegram(telegram_chat_id: str):
    conn = None
    cur = None
    try:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'traders'
            """
        )
        tcols = {r[0] for r in cur.fetchall()}
        if "telegram_chat_id" not in tcols:
            return JSONResponse(
                status_code=501,
                content={"error": "traders.telegram_chat_id column is not available"},
            )
        chat = telegram_chat_id.strip()
        extra_cols = {"onboarding_complete", "trading_style", "risk_tolerance"} & tcols
        select_extra = ", ".join(sorted(extra_cols)) if extra_cols else ""
        select_sql = f"SELECT id, name{', ' + select_extra if select_extra else ''} FROM traders WHERE telegram_chat_id = %s LIMIT 1"
        cur.execute(select_sql, (chat,))
        row = cur.fetchone()
        if not row:
            return JSONResponse(
                status_code=404,
                content={
                    "error": "Trader not found",
                    "message": (
                        "No trader registered for this Telegram chat ID. "
                        "Use POST /trader/register with your chat_id."
                    ),
                },
            )
        tid = int(row[0])
        name = (row[1] or "").strip() or f"Trader {tid}"
        result: dict = {"trader_id": tid, "name": name, "telegram_chat_id": chat}
        col_names = ["id", "name"] + sorted(extra_cols)
        for i, col in enumerate(col_names[2:], start=2):
            result[col] = row[i] if i < len(row) else None
        return _jsonify(result)
    except Exception as e:
        logger.exception("get_trader_by_telegram chat_id=%s", telegram_chat_id)
        return JSONResponse(status_code=500, content={"error": str(e)})
    finally:
        if cur is not None:
            try:
                cur.close()
            except Exception:
                pass
        if conn is not None:
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
            return _trader_not_found_response(trader_id)

        cur.execute("SELECT 1 FROM stocks_master WHERE symbol = %s", (symbol,))
        if not cur.fetchone():
            return _stock_not_found_response(symbol)

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
            return _trader_not_found_response(trader_id)

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

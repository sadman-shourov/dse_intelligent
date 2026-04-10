from __future__ import annotations

import json
import logging
import os
import sys
import time
from datetime import date
from pathlib import Path
from typing import Any

import httpx
import psycopg2
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def load_env() -> None:
    load_dotenv(_project_root() / ".env")


def get_db_connection():
    load_env()
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


def _table_columns(cur, table: str) -> set[str]:
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        """,
        (table,),
    )
    return {r[0] for r in cur.fetchall()}


def get_active_traders(conn) -> list[dict[str, Any]]:
    cur = conn.cursor()
    try:
        cols = _table_columns(cur, "traders")
        if "telegram_chat_id" not in cols:
            logger.warning("traders.telegram_chat_id missing; no traders eligible for Telegram")
            return []
        select_parts = ["id"]
        if "name" in cols:
            select_parts.append("name")
        else:
            select_parts.append("NULL::text AS name")
        select_parts.append("telegram_chat_id")
        q = f"""
            SELECT {", ".join(select_parts)}
            FROM traders
            WHERE telegram_chat_id IS NOT NULL
        """
        if "is_active" in cols:
            q += " AND is_active = TRUE"
        q += " ORDER BY id"
        cur.execute(q)
        out: list[dict[str, Any]] = []
        for row in cur.fetchall():
            tid, name, chat_id = row
            out.append(
                {
                    "id": int(tid),
                    "name": (name or "").strip() or f"Trader {tid}",
                    "telegram_chat_id": str(chat_id).strip(),
                }
            )
        return out
    finally:
        cur.close()


def get_latest_pulse(conn, trader_id: int, target_date: date) -> dict[str, Any] | None:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT id, deepseek_output, telegram_sent
            FROM pulse_log
            WHERE trader_id = %s
              AND pulse_date = %s
            ORDER BY id DESC
            LIMIT 1
            """,
            (trader_id, target_date),
        )
        row = cur.fetchone()
        if not row:
            return None
        pid, deepseek_output, telegram_sent = row
        return {
            "id": int(pid),
            "deepseek_output": deepseek_output if deepseek_output is not None else "",
            "telegram_sent": bool(telegram_sent),
        }
    finally:
        cur.close()


def mark_pulse_sent(conn, pulse_id: int, message_id: int) -> None:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE pulse_log
            SET telegram_sent = TRUE,
                sent_at = NOW()
            WHERE id = %s
            """,
            (pulse_id,),
        )
    finally:
        cur.close()


def mark_pulse_telegram_failed(conn, pulse_id: int) -> None:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE pulse_log
            SET telegram_sent = FALSE,
                sent_at = NULL
            WHERE id = %s
            """,
            (pulse_id,),
        )
    finally:
        cur.close()


def send_telegram_message(
    chat_id: str,
    message: str,
    parse_mode: str = "HTML",
    retry_count: int = 3,
    retry_delay: int = 5,
) -> dict[str, Any]:
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    if not token or not str(token).strip():
        raise RuntimeError("TELEGRAM_BOT_TOKEN is missing or empty in .env")

    url = TELEGRAM_API.format(token=token.strip())
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": parse_mode,
        "disable_web_page_preview": True,
    }

    last_error: str | None = None
    for attempt in range(1, retry_count + 1):
        try:
            response = httpx.post(url, json=payload, timeout=15)
            data = response.json()

            if data.get("ok"):
                return {
                    "status": "ok",
                    "message_id": data["result"]["message_id"],
                    "chat_id": chat_id,
                    "attempt": attempt,
                }
            last_error = data.get("description", "Unknown error")
            logger.warning("Telegram error (attempt %s): %s", attempt, last_error)

            le = (last_error or "").lower()
            if "chat not found" in le:
                break
            if "bot was blocked" in le:
                break

        except Exception as e:
            last_error = str(e)
            logger.warning("Send failed (attempt %s): %s", attempt, last_error)

        if attempt < retry_count:
            time.sleep(retry_delay)

    return {
        "status": "error",
        "error": last_error,
        "chat_id": chat_id,
        "attempts": retry_count,
    }


def truncate_telegram_message(message: str, limit: int = 4000) -> str:
    if len(message) <= limit:
        return message
    cut = message[:limit]
    best = -1
    for ender in (".\n", ". ", "!\n", "! ", "?\n", "? ", "\n\n"):
        i = cut.rfind(ender)
        if i > best:
            best = i
    if best >= max(80, limit // 5):
        base = cut[: best + 1].rstrip()
    else:
        base = cut.rstrip()
    suffix = "\n\n<i>Message truncated. Full report available.</i>"
    return base + suffix


def _pulse_output_is_sendable(deepseek_output: str) -> bool:
    s = (deepseek_output or "").strip()
    if not s:
        return False
    if s.startswith("{"):
        try:
            obj = json.loads(s)
            if isinstance(obj, dict):
                st = obj.get("status")
                if st is not None and st != "ok":
                    return False
        except json.JSONDecodeError:
            pass
    head = s[:400].lower()
    if "deepseek api error" in head or s.lower().startswith("pulse error:"):
        return False
    return True


def _build_message_from_pulse_log(conn, trader_id: int, target_date: date, deepseek_output: str) -> str | None:
    if not _pulse_output_is_sendable(deepseek_output):
        return None
    from pulse.deepseek import format_telegram_message, get_analysis_summary, get_trader_portfolio

    analysis = get_analysis_summary(conn, target_date)
    portfolio = get_trader_portfolio(conn, trader_id)
    return format_telegram_message(deepseek_output, analysis, portfolio, target_date)


def deliver_pulse(trader_id: int | None = None) -> dict[str, Any]:
    load_env()
    details: list[dict[str, Any]] = []
    delivered = 0
    failed = 0

    try:
        conn = get_db_connection()
    except Exception as e:
        logger.exception("Database connection failed: %s", e)
        return {
            "status": "error",
            "reason": f"database connection failed: {e}",
            "date": None,
            "traders_total": 0,
            "traders_delivered": 0,
            "traders_failed": 0,
            "details": [],
        }

    conn.autocommit = True
    try:
        target_date = get_db_date(conn)
        traders = get_active_traders(conn)
        if trader_id is not None:
            traders = [t for t in traders if t["id"] == trader_id]

        total = len(traders)

        for t in traders:
            tid = t["id"]
            name = t["name"]
            chat = t["telegram_chat_id"]
            latest = get_latest_pulse(conn, tid, target_date)
            telegram_message: str | None = None
            pulse_row_id: int | None = None

            if latest is None:
                from pulse.deepseek import generate_pulse

                logger.info("No pulse_log for trader_id=%s on %s; generating pulse", tid, target_date)
                result = generate_pulse(tid)
                if result.get("status") != "ok":
                    logger.warning(
                        "generate_pulse failed for trader %s (%s): %s",
                        tid,
                        name,
                        result,
                    )
                    failed += 1
                    details.append(
                        {
                            "trader_id": tid,
                            "name": name,
                            "status": "skipped",
                            "reason": result.get("status", "error"),
                            "detail": result.get("message") or result.get("reason"),
                        }
                    )
                    continue
                telegram_message = result.get("telegram_message")
                latest = get_latest_pulse(conn, tid, target_date)
                if latest:
                    pulse_row_id = latest["id"]
            else:
                pulse_row_id = latest["id"]
                telegram_message = _build_message_from_pulse_log(
                    conn, tid, target_date, latest["deepseek_output"]
                )
                if telegram_message is None:
                    logger.warning(
                        "Pulse row %s for trader %s (%s) is not sendable; skipping Telegram",
                        pulse_row_id,
                        tid,
                        name,
                    )
                    failed += 1
                    details.append(
                        {
                            "trader_id": tid,
                            "name": name,
                            "status": "skipped",
                            "reason": "unsendable_pulse_output",
                            "pulse_id": pulse_row_id,
                        }
                    )
                    continue

            if not telegram_message or not str(telegram_message).strip():
                logger.warning("Empty telegram_message for trader %s (%s); skip", tid, name)
                failed += 1
                details.append(
                    {
                        "trader_id": tid,
                        "name": name,
                        "status": "skipped",
                        "reason": "empty_message",
                        "pulse_id": pulse_row_id,
                    }
                )
                continue

            msg = truncate_telegram_message(str(telegram_message).strip())
            try:
                send_res = send_telegram_message(chat_id=chat, message=msg)
            except Exception as e:
                logger.exception("send_telegram_message raised for trader %s (%s): %s", tid, name, e)
                if pulse_row_id is not None:
                    try:
                        mark_pulse_telegram_failed(conn, pulse_row_id)
                    except Exception:
                        logger.exception("mark_pulse_telegram_failed failed for pulse_id=%s", pulse_row_id)
                failed += 1
                details.append(
                    {
                        "trader_id": tid,
                        "name": name,
                        "status": "error",
                        "error": str(e),
                        "pulse_id": pulse_row_id,
                    }
                )
                continue

            if send_res.get("status") == "ok":
                if pulse_row_id is None:
                    logger.error(
                        "Telegram send ok but no pulse_log id for trader %s (%s); cannot mark sent",
                        tid,
                        name,
                    )
                    failed += 1
                    details.append(
                        {
                            "trader_id": tid,
                            "name": name,
                            "status": "error",
                            "error": "pulse_log row not found after send",
                            "message_id": send_res.get("message_id"),
                            "pulse_id": None,
                        }
                    )
                else:
                    mark_pulse_sent(conn, pulse_row_id, int(send_res["message_id"]))
                    delivered += 1
                    logger.info(
                        "Delivered pulse to trader %s (%s) message_id=%s",
                        tid,
                        name,
                        send_res.get("message_id"),
                    )
                    details.append(
                        {
                            "trader_id": tid,
                            "name": name,
                            "status": "delivered",
                            "message_id": send_res.get("message_id"),
                            "pulse_id": pulse_row_id,
                            "attempt": send_res.get("attempt"),
                        }
                    )
            else:
                if pulse_row_id is not None:
                    try:
                        mark_pulse_telegram_failed(conn, pulse_row_id)
                    except Exception:
                        logger.exception("mark_pulse_telegram_failed failed for pulse_id=%s", pulse_row_id)
                failed += 1
                logger.warning(
                    "Telegram delivery failed for trader %s (%s): %s",
                    tid,
                    name,
                    send_res.get("error"),
                )
                details.append(
                    {
                        "trader_id": tid,
                        "name": name,
                        "status": "failed",
                        "error": send_res.get("error"),
                        "pulse_id": pulse_row_id,
                        "attempts": send_res.get("attempts"),
                    }
                )

        return {
            "status": "ok",
            "date": target_date,
            "traders_total": total,
            "traders_delivered": delivered,
            "traders_failed": failed,
            "details": details,
        }
    finally:
        conn.close()


def send_extreme_move_alert(
    trader_id: int,
    chat_id: str,
    moves: list[dict],
    portfolio_symbols: set[str],
) -> dict:
    """Build and send an extreme move alert to one trader."""
    portfolio_moves = [m for m in moves if m["symbol"] in portfolio_symbols]
    market_moves = [m for m in moves if m["symbol"] not in portfolio_symbols]

    message = "⚡ <b>ARIA Extreme Move Alert</b>\n\n"

    if portfolio_moves:
        message += "🔴 <b>YOUR PORTFOLIO:</b>\n"
        for m in portfolio_moves:
            emoji = "🚀" if m["direction"] == "up" else "💥"
            message += (
                f"{emoji} <b>{m['symbol']}</b>: "
                f"{m['change_pct']:+.1f}% @ {m['current_price']}\n"
            )
            if m["direction"] == "down" and abs(m["change_pct"]) >= 8:
                message += "   ⚠️ STOP-LOSS ZONE — Review immediately\n"
        message += "\n"

    if market_moves[:5]:
        message += "📊 <b>MARKET MOVERS:</b>\n"
        for m in market_moves[:5]:
            emoji = "📈" if m["direction"] == "up" else "📉"
            message += (
                f"{emoji} {m['symbol']}: "
                f"{m['change_pct']:+.1f}% @ {m['current_price']}\n"
            )

    message += f"\n<i>Session {moves[0]['session_no']} | {len(moves)} stocks moved >5%</i>"

    return send_telegram_message(chat_id=chat_id, message=message, parse_mode="HTML")


def deliver_extreme_move_alerts(moves: list[dict]) -> dict:
    """Send extreme move alerts to all affected active traders."""
    if not moves:
        return {"status": "skipped", "reason": "no extreme moves"}

    load_env()
    try:
        conn = get_db_connection()
    except Exception as e:
        logger.exception("DB connection failed in deliver_extreme_move_alerts")
        return {"status": "error", "reason": str(e)}

    conn.autocommit = True
    delivered = 0
    skipped = 0
    details: list[dict] = []

    try:
        traders = get_active_traders(conn)
        significant = [m for m in moves if abs(m["change_pct"]) >= 8.0]

        for t in traders:
            tid = t["id"]
            name = t["name"]
            chat = t["telegram_chat_id"]

            # Fetch this trader's open portfolio symbols
            cur = conn.cursor()
            cur.execute(
                """
                SELECT symbol FROM portfolio_holdings
                WHERE trader_id = %s AND is_open = TRUE
                """,
                (tid,),
            )
            portfolio_symbols: set[str] = {r[0] for r in cur.fetchall()}
            cur.close()

            portfolio_moves = [m for m in moves if m["symbol"] in portfolio_symbols]

            # Only alert if they have a portfolio hit OR there's a >8% market move
            if not portfolio_moves and not significant:
                skipped += 1
                details.append({"trader_id": tid, "name": name, "status": "skipped", "reason": "no relevant moves"})
                continue

            try:
                res = send_extreme_move_alert(tid, chat, moves, portfolio_symbols)
            except Exception as e:
                logger.exception("send_extreme_move_alert failed for trader %s", tid)
                details.append({"trader_id": tid, "name": name, "status": "error", "error": str(e)})
                continue

            if res.get("status") == "ok":
                delivered += 1
                logger.info("Extreme move alert delivered to trader %s (%s)", tid, name)
                details.append({"trader_id": tid, "name": name, "status": "delivered", "message_id": res.get("message_id")})
            else:
                details.append({"trader_id": tid, "name": name, "status": "failed", "error": res.get("error")})

        return {
            "status": "ok",
            "moves_detected": len(moves),
            "traders_alerted": delivered,
            "traders_skipped": skipped,
            "details": details,
        }
    finally:
        conn.close()


def deliver_premarket_briefing(trader_id: int | None = None) -> dict:
    """Generate and send pre-market briefing to one or all active traders."""
    load_env()
    from pulse.deepseek import generate_premarket_briefing

    details: list[dict] = []
    delivered = 0
    failed = 0

    try:
        conn = get_db_connection()
    except Exception as e:
        logger.exception("Database connection failed: %s", e)
        return {
            "status": "error",
            "reason": f"database connection failed: {e}",
            "traders_total": 0,
            "traders_delivered": 0,
            "traders_failed": 0,
            "details": [],
        }

    conn.autocommit = True
    try:
        traders = get_active_traders(conn)
        if trader_id is not None:
            traders = [t for t in traders if t["id"] == trader_id]
        total = len(traders)

        for t in traders:
            tid = t["id"]
            name = t["name"]
            chat = t["telegram_chat_id"]

            try:
                result = generate_premarket_briefing(tid)
            except Exception as e:
                logger.exception("generate_premarket_briefing failed for trader %s", tid)
                failed += 1
                details.append({"trader_id": tid, "name": name, "status": "error", "error": str(e)})
                continue

            if result.get("status") != "ok":
                failed += 1
                details.append({"trader_id": tid, "name": name, "status": "failed", "reason": result.get("reason") or result.get("message")})
                continue

            msg = truncate_telegram_message(result["telegram_message"])
            try:
                send_res = send_telegram_message(chat_id=chat, message=msg)
            except Exception as e:
                logger.exception("send_telegram_message raised for trader %s", tid)
                failed += 1
                details.append({"trader_id": tid, "name": name, "status": "error", "error": str(e)})
                continue

            if send_res.get("status") == "ok":
                # Mark the latest pulse_log row sent
                try:
                    cur = conn.cursor()
                    cur.execute(
                        """
                        UPDATE pulse_log SET telegram_sent = TRUE, sent_at = NOW()
                        WHERE trader_id = %s AND pulse_date = CURRENT_DATE
                          AND session_no = 0
                          AND id = (
                              SELECT id FROM pulse_log
                              WHERE trader_id = %s AND pulse_date = CURRENT_DATE
                                AND session_no = 0
                              ORDER BY id DESC LIMIT 1
                          )
                        """,
                        (tid, tid),
                    )
                    cur.close()
                except Exception:
                    logger.exception("Failed to mark premarket pulse sent for trader %s", tid)
                delivered += 1
                logger.info("Delivered premarket briefing to trader %s (%s) message_id=%s", tid, name, send_res.get("message_id"))
                details.append({
                    "trader_id": tid,
                    "name": name,
                    "status": "delivered",
                    "message_id": send_res.get("message_id"),
                    "attempt": send_res.get("attempt"),
                })
            else:
                failed += 1
                logger.warning("Telegram delivery failed for trader %s (%s): %s", tid, name, send_res.get("error"))
                details.append({"trader_id": tid, "name": name, "status": "failed", "error": send_res.get("error")})

        return {
            "status": "ok",
            "type": "premarket",
            "traders_total": total,
            "traders_delivered": delivered,
            "traders_failed": failed,
            "details": details,
        }
    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        _c = get_db_connection()
        _c.close()
    except Exception as e:
        print(f"DB connection failed: {e}", file=sys.stderr)
        sys.exit(1)

    if len(sys.argv) > 1:
        out = deliver_pulse(int(sys.argv[1]))
    else:
        out = deliver_pulse()

    print(json.dumps(out, indent=2, default=str))

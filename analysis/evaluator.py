from __future__ import annotations

import json
import logging
import os
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import psycopg2
from dotenv import load_dotenv

logger = logging.getLogger(__name__)


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


def _determine_outcome(signal_type: str, pnl_pct: float) -> str:
    if signal_type == "BUY":
        if pnl_pct > 2.0:
            return "WIN"
        elif pnl_pct < -2.0:
            return "LOSS"
        return "NEUTRAL"
    elif signal_type == "EXIT":
        # Good exit = price fell after we called EXIT
        if pnl_pct < -2.0:
            return "WIN"
        elif pnl_pct > 2.0:
            return "LOSS"
        return "NEUTRAL"
    elif signal_type == "WATCH":
        if pnl_pct > 3.0:
            return "WIN"
        elif pnl_pct < -3.0:
            return "LOSS"
        return "NEUTRAL"
    return "NEUTRAL"


def evaluate_past_signals(days_back: int = 5) -> dict:
    """
    Evaluate all BUY/EXIT/WATCH signals from the last N trading days.
    Upserts results into signal_outcomes.
    """
    conn = get_db_connection()
    conn.autocommit = True
    try:
        target_date = get_db_date(conn)
        eval_from = target_date - timedelta(days=days_back)

        cur = conn.cursor()
        cur.execute(
            """
            SELECT s.symbol, s.signal_type, s.signal_date,
                   s.price_at_signal, s.reason
            FROM signals s
            WHERE s.signal_date >= %s
              AND s.signal_date < %s
              AND s.signal_type IN ('BUY', 'EXIT', 'WATCH')
            ORDER BY s.signal_date DESC, s.symbol
            """,
            (eval_from, target_date),
        )
        signals = cur.fetchall()

        if not signals:
            cur.close()
            return {
                "status": "ok",
                "evaluated": 0,
                "wins": 0,
                "losses": 0,
                "neutrals": 0,
                "win_rate": 0,
                "avg_pnl_pct": 0,
                "best_performer": None,
                "worst_performer": None,
                "period": f"last {days_back} days",
                "message": "No signals found for evaluation period",
            }

        wins = 0
        losses = 0
        neutrals = 0
        pnl_list: list[float] = []
        best: tuple[str, float] | None = None
        worst: tuple[str, float] | None = None

        for symbol, signal_type, signal_date, price_at_signal, reason in signals:
            price_at_signal_f = _float(price_at_signal)
            if not price_at_signal_f or price_at_signal_f <= 0:
                continue

            # Get latest price
            cur.execute(
                """
                SELECT ltp, close FROM price_history
                WHERE symbol = %s ORDER BY date DESC LIMIT 1
                """,
                (symbol,),
            )
            pr = cur.fetchone()
            if not pr:
                continue
            current_price = _float(pr[0]) or _float(pr[1])
            if not current_price or current_price <= 0:
                continue

            pnl_pct = ((current_price - price_at_signal_f) / price_at_signal_f) * 100.0
            outcome = _determine_outcome(signal_type, pnl_pct)
            days_held = (target_date - signal_date).days

            # Track stats
            if outcome == "WIN":
                wins += 1
            elif outcome == "LOSS":
                losses += 1
            else:
                neutrals += 1
            pnl_list.append(pnl_pct)

            if best is None or pnl_pct > best[1]:
                best = (f"{symbol} {pnl_pct:+.1f}%", pnl_pct)
            if worst is None or pnl_pct < worst[1]:
                worst = (f"{symbol} {pnl_pct:+.1f}%", pnl_pct)

            # Upsert into signal_outcomes
            cur.execute(
                """
                INSERT INTO signal_outcomes (
                    symbol, signal_type, signal_date,
                    price_at_signal, eval_date, price_at_eval,
                    pnl_pct, outcome, days_held, signal_reason
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol, signal_date, signal_type)
                DO UPDATE SET
                    eval_date = EXCLUDED.eval_date,
                    price_at_eval = EXCLUDED.price_at_eval,
                    pnl_pct = EXCLUDED.pnl_pct,
                    outcome = EXCLUDED.outcome,
                    days_held = EXCLUDED.days_held
                """,
                (
                    symbol, signal_type, signal_date,
                    price_at_signal_f, target_date, current_price,
                    round(pnl_pct, 4), outcome, days_held, reason or "",
                ),
            )

        cur.close()

        total = wins + losses + neutrals
        win_rate = round((wins / total) * 100, 1) if total > 0 else 0.0
        avg_pnl = round(sum(pnl_list) / len(pnl_list), 2) if pnl_list else 0.0

        return {
            "status": "ok",
            "evaluated": total,
            "wins": wins,
            "losses": losses,
            "neutrals": neutrals,
            "win_rate": win_rate,
            "avg_pnl_pct": avg_pnl,
            "best_performer": best[0] if best else None,
            "worst_performer": worst[0] if worst else None,
            "period": f"last {days_back} days",
        }
    except Exception as e:
        logger.exception("evaluate_past_signals error")
        return {"status": "error", "message": str(e)}
    finally:
        conn.close()


def calculate_accuracy_scores(period_days: int = 30) -> dict:
    """
    Calculate accuracy scores for the last N days and upsert into accuracy_scores.
    """
    conn = get_db_connection()
    conn.autocommit = True
    try:
        target_date = get_db_date(conn)
        period_start = target_date - timedelta(days=period_days)

        cur = conn.cursor()
        cur.execute(
            """
            SELECT symbol, signal_type, signal_date,
                   price_at_signal, price_at_eval, pnl_pct, outcome
            FROM signal_outcomes
            WHERE signal_date >= %s AND outcome != 'PENDING'
            ORDER BY signal_date DESC
            """,
            (period_start,),
        )
        rows = cur.fetchall()

        if not rows:
            cur.close()
            return {
                "status": "ok",
                "period": f"last {period_days} days",
                "message": "No evaluated signals yet",
            }

        overall = {"wins": 0, "losses": 0, "neutrals": 0, "pnl_sum": 0.0, "count": 0}
        by_type: dict[str, dict] = {}
        best_sym: tuple[str, float] | None = None
        worst_sym: tuple[str, float] | None = None

        for symbol, signal_type, signal_date, price_at_signal, price_at_eval, pnl_pct, outcome in rows:
            pnl = _float(pnl_pct) or 0.0
            overall["count"] += 1
            overall["pnl_sum"] += pnl
            if outcome == "WIN":
                overall["wins"] += 1
            elif outcome == "LOSS":
                overall["losses"] += 1
            else:
                overall["neutrals"] += 1

            if signal_type not in by_type:
                by_type[signal_type] = {"wins": 0, "losses": 0, "neutrals": 0, "pnl_sum": 0.0, "count": 0}
            by_type[signal_type]["count"] += 1
            by_type[signal_type]["pnl_sum"] += pnl
            if outcome == "WIN":
                by_type[signal_type]["wins"] += 1
            elif outcome == "LOSS":
                by_type[signal_type]["losses"] += 1
            else:
                by_type[signal_type]["neutrals"] += 1

            if best_sym is None or pnl > best_sym[1]:
                best_sym = (symbol, pnl)
            if worst_sym is None or pnl < worst_sym[1]:
                worst_sym = (symbol, pnl)

        total = overall["count"]
        win_rate = round((overall["wins"] / total) * 100, 2) if total > 0 else 0.0
        avg_pnl = round(overall["pnl_sum"] / total, 4) if total > 0 else 0.0

        # Upsert overall row
        cur.execute(
            """
            INSERT INTO accuracy_scores (
                period_start, period_end, signal_type,
                total_signals, wins, losses, neutrals,
                win_rate, avg_pnl_pct, best_signal, worst_signal
            ) VALUES (%s, %s, NULL, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (period_start, period_end, signal_type)
            DO UPDATE SET
                total_signals = EXCLUDED.total_signals,
                wins = EXCLUDED.wins,
                losses = EXCLUDED.losses,
                neutrals = EXCLUDED.neutrals,
                win_rate = EXCLUDED.win_rate,
                avg_pnl_pct = EXCLUDED.avg_pnl_pct,
                best_signal = EXCLUDED.best_signal,
                worst_signal = EXCLUDED.worst_signal
            """,
            (
                period_start, target_date,
                total, overall["wins"], overall["losses"], overall["neutrals"],
                win_rate, avg_pnl,
                best_sym[0] if best_sym else None,
                worst_sym[0] if worst_sym else None,
            ),
        )

        # Upsert per signal_type rows
        type_breakdown: list[dict] = []
        for st, d in by_type.items():
            n = d["count"]
            wr = round((d["wins"] / n) * 100, 2) if n > 0 else 0.0
            ap = round(d["pnl_sum"] / n, 4) if n > 0 else 0.0
            cur.execute(
                """
                INSERT INTO accuracy_scores (
                    period_start, period_end, signal_type,
                    total_signals, wins, losses, neutrals,
                    win_rate, avg_pnl_pct
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (period_start, period_end, signal_type)
                DO UPDATE SET
                    total_signals = EXCLUDED.total_signals,
                    wins = EXCLUDED.wins,
                    losses = EXCLUDED.losses,
                    neutrals = EXCLUDED.neutrals,
                    win_rate = EXCLUDED.win_rate,
                    avg_pnl_pct = EXCLUDED.avg_pnl_pct
                """,
                (period_start, target_date, st, n, d["wins"], d["losses"], d["neutrals"], wr, ap),
            )
            type_breakdown.append({"signal_type": st, "total": n, "win_rate": wr, "avg_pnl_pct": ap})

        cur.close()

        return {
            "status": "ok",
            "period": f"last {period_days} days",
            "period_start": period_start.isoformat(),
            "period_end": target_date.isoformat(),
            "total_signals": total,
            "wins": overall["wins"],
            "losses": overall["losses"],
            "neutrals": overall["neutrals"],
            "win_rate": win_rate,
            "avg_pnl_pct": avg_pnl,
            "best_signal": best_sym[0] if best_sym else None,
            "worst_signal": worst_sym[0] if worst_sym else None,
            "by_signal_type": type_breakdown,
        }
    except Exception as e:
        logger.exception("calculate_accuracy_scores error")
        return {"status": "error", "message": str(e)}
    finally:
        conn.close()


def get_recent_scorecard(days: int = 7) -> dict:
    """
    Human-readable scorecard for the last N days. Called by DeepSeek pulse generator.
    """
    empty = {
        "period": f"last {days} days",
        "total_evaluated": 0,
        "wins": 0,
        "losses": 0,
        "neutrals": 0,
        "win_rate": 0,
        "avg_pnl_pct": 0,
        "top_wins": [],
        "top_losses": [],
        "pending": 0,
        "accuracy_trend": "insufficient data",
    }

    conn = get_db_connection()
    try:
        target_date = get_db_date(conn)
        since = target_date - timedelta(days=days)

        cur = conn.cursor()
        cur.execute(
            """
            SELECT symbol, signal_type, signal_date,
                   price_at_signal, price_at_eval, pnl_pct, outcome
            FROM signal_outcomes
            WHERE signal_date >= %s
            ORDER BY pnl_pct DESC
            """,
            (since,),
        )
        rows = cur.fetchall()

        # Count pending
        cur.execute(
            """
            SELECT COUNT(*) FROM signal_outcomes
            WHERE signal_date >= %s AND outcome = 'PENDING'
            """,
            (since,),
        )
        pending = cur.fetchone()[0] or 0
        cur.close()

        evaluated = [r for r in rows if r[6] != "PENDING"]
        if not evaluated:
            empty["pending"] = pending
            return empty

        wins = [r for r in evaluated if r[6] == "WIN"]
        losses = [r for r in evaluated if r[6] == "LOSS"]
        neutrals = [r for r in evaluated if r[6] == "NEUTRAL"]
        total = len(evaluated)
        win_rate = round((len(wins) / total) * 100, 1) if total > 0 else 0.0
        pnl_vals = [_float(r[5]) or 0.0 for r in evaluated]
        avg_pnl = round(sum(pnl_vals) / len(pnl_vals), 2) if pnl_vals else 0.0

        def _fmt(rows_subset: list, top_n: int = 3) -> list[dict]:
            return [
                {
                    "symbol": r[0],
                    "signal_type": r[1],
                    "signal_date": r[2].isoformat() if hasattr(r[2], "isoformat") else str(r[2]),
                    "price_at_signal": _float(r[3]),
                    "price_at_eval": _float(r[4]),
                    "pnl_pct": round(_float(r[5]) or 0.0, 2),
                }
                for r in rows_subset[:top_n]
            ]

        # Accuracy trend: compare this week win_rate vs prior week
        prior_since = since - timedelta(days=days)
        conn2 = get_db_connection()
        try:
            cur2 = conn2.cursor()
            cur2.execute(
                """
                SELECT outcome FROM signal_outcomes
                WHERE signal_date >= %s AND signal_date < %s AND outcome != 'PENDING'
                """,
                (prior_since, since),
            )
            prior_rows = cur2.fetchall()
            cur2.close()
        finally:
            conn2.close()

        trend = "insufficient data"
        if prior_rows:
            prior_wins = sum(1 for r in prior_rows if r[0] == "WIN")
            prior_rate = (prior_wins / len(prior_rows)) * 100
            if win_rate > prior_rate + 5:
                trend = "improving"
            elif win_rate < prior_rate - 5:
                trend = "declining"
            else:
                trend = "stable"

        # top_wins sorted by pnl DESC, top_losses by pnl ASC
        wins_sorted = sorted(wins, key=lambda r: _float(r[5]) or 0.0, reverse=True)
        losses_sorted = sorted(losses, key=lambda r: _float(r[5]) or 0.0)

        return {
            "period": f"last {days} days",
            "total_evaluated": total,
            "wins": len(wins),
            "losses": len(losses),
            "neutrals": len(neutrals),
            "win_rate": win_rate,
            "avg_pnl_pct": avg_pnl,
            "top_wins": _fmt(wins_sorted),
            "top_losses": _fmt(losses_sorted),
            "pending": pending,
            "accuracy_trend": trend,
        }
    except Exception as e:
        logger.exception("get_recent_scorecard error")
        empty["error"] = str(e)
        return empty
    finally:
        conn.close()


def get_accuracy_context_for_deepseek() -> str:
    """
    Plain-text summary of ARIA's track record for injection into DeepSeek prompt.
    """
    conn = get_db_connection()
    try:
        target_date = get_db_date(conn)
        since = target_date - timedelta(days=30)

        cur = conn.cursor()
        cur.execute(
            """
            SELECT signal_type, outcome, pnl_pct
            FROM signal_outcomes
            WHERE signal_date >= %s AND outcome != 'PENDING'
            """,
            (since,),
        )
        rows = cur.fetchall()
        cur.close()

        if len(rows) < 10:
            # Find earliest signal date
            cur2 = conn.cursor()
            cur2.execute("SELECT MIN(signal_date) FROM signal_outcomes")
            r = cur2.fetchone()
            cur2.close()
            since_date = r[0].isoformat() if r and r[0] else "recently"
            return (
                f"ARIA TRACK RECORD: Insufficient data yet. "
                f"System live since {since_date}. Building accuracy baseline."
            )

        by_type: dict[str, dict] = {}
        for signal_type, outcome, pnl_pct in rows:
            if signal_type not in by_type:
                by_type[signal_type] = {"wins": 0, "total": 0, "pnl_sum": 0.0}
            by_type[signal_type]["total"] += 1
            by_type[signal_type]["pnl_sum"] += _float(pnl_pct) or 0.0
            if outcome == "WIN":
                by_type[signal_type]["wins"] += 1

        total = len(rows)
        wins = sum(1 for r in rows if r[1] == "WIN")
        win_rate = round((wins / total) * 100)
        avg_pnl = round(sum(_float(r[2]) or 0.0 for r in rows) / total, 1)

        lines = [
            f"ARIA TRACK RECORD (last 30 days):",
            f"- Overall win rate: {win_rate}% ({wins}/{total} signals)",
        ]
        for st, d in by_type.items():
            n = d["total"]
            wr = round((d["wins"] / n) * 100)
            ap = round(d["pnl_sum"] / n, 1)
            lines.append(f"- {st} signals: {wr}% accuracy, avg {ap:+.1f}% gain")
        lines.append(f"- Overall avg P&L per signal: {avg_pnl:+.1f}%")
        lines.append("- Use this to calibrate confidence in current signals.")

        return "\n".join(lines)
    except Exception as e:
        logger.exception("get_accuracy_context_for_deepseek error")
        return "ARIA TRACK RECORD: Unable to load accuracy data."
    finally:
        conn.close()


def standalone_run():
    import json as _json
    result = evaluate_past_signals(days_back=7)
    print(_json.dumps(result, indent=2, default=str))
    print()
    scorecard = get_recent_scorecard(days=7)
    print(_json.dumps(scorecard, indent=2, default=str))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    standalone_run()

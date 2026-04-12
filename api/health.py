"""
Data freshness checks before pulse generation and for /health/data.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

import pytz


def _db_date(conn) -> date:
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


def _max_market_summary_date(cur, cols: set[str]) -> date | None:
    dcol = "trade_date" if "trade_date" in cols else "date" if "date" in cols else None
    if not dcol:
        return None
    cur.execute(f"SELECT MAX({dcol}) FROM market_summary")
    row = cur.fetchone()
    return row[0] if row else None


def _to_utc_aware(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def check_data_freshness(conn) -> dict[str, Any]:
    """
    Check if critical data is fresh enough to generate a reliable pulse.
    """
    dhaka = pytz.timezone("Asia/Dhaka")
    now = datetime.now(dhaka)
    target_date = _db_date(conn)

    is_market_hours = (
        now.weekday() < 5
        and (now.hour > 10 or (now.hour == 10 and now.minute >= 0))
        and (now.hour < 14 or (now.hour == 14 and now.minute <= 30))
    )

    issues: list[str] = []
    warnings: list[str] = []

    cur = conn.cursor()
    try:
        ms_cols = _table_columns(cur, "market_summary")

        cur.execute(
            """
            SELECT MAX(id), MAX(analysis_date)
            FROM analysis_results
            WHERE analysis_date = %s
            """,
            (target_date,),
        )
        r = cur.fetchone()
        if not r or r[0] is None:
            issues.append("No analysis run today")

        last_fetch: datetime | None = None
        if is_market_hours:
            cur.execute(
                """
                SELECT COUNT(*), MAX(fetched_at), MAX(session_no)
                FROM live_ticks
                WHERE date = %s AND ltp > 0
                """,
                (target_date,),
            )
            r = cur.fetchone()
            tick_count = int(r[0] or 0) if r else 0
            last_fetch = r[1] if r else None

            if tick_count == 0:
                issues.append("No live ticks today — market data missing")
            elif last_fetch:
                now_utc = datetime.now(timezone.utc)
                lf_utc = _to_utc_aware(last_fetch)
                age_mins = max(0, int((now_utc - lf_utc).total_seconds() // 60))
                if age_mins > 45:
                    issues.append(f"Live ticks stale — last fetch {age_mins} mins ago")

            cur.execute(
                """
                SELECT MAX(created_at)
                FROM analysis_results
                WHERE analysis_date = %s
                """,
                (target_date,),
            )
            last_analysis = cur.fetchone()[0]
            if last_analysis and last_fetch:
                la_utc = _to_utc_aware(last_analysis)
                lf_utc = _to_utc_aware(last_fetch)
                if lf_utc > la_utc:
                    warnings.append(
                        "Live ticks newer than latest analysis — analysis may be slightly stale"
                    )

        cur.execute("SELECT MAX(date) FROM price_history")
        latest_ph = cur.fetchone()[0]
        if not latest_ph:
            issues.append("price_history is empty")
        elif isinstance(latest_ph, date) and isinstance(target_date, date):
            if (target_date - latest_ph).days > 3:
                issues.append(f"price_history stale — latest {latest_ph}")

        latest_ms = _max_market_summary_date(cur, ms_cols)
        if not latest_ms or (isinstance(latest_ms, date) and (target_date - latest_ms).days > 3):
            warnings.append(f"market_summary stale — latest {latest_ms}")

    finally:
        cur.close()

    is_healthy = len(issues) == 0

    return {
        "healthy": is_healthy,
        "is_market_hours": is_market_hours,
        "issues": issues,
        "warnings": warnings,
        "target_date": str(target_date),
        "checks": {
            "analysis_today": not any("analysis" in i.lower() for i in issues),
            "live_ticks_fresh": not any("tick" in i.lower() for i in issues),
            "price_history_ok": not any("price_history" in i.lower() for i in issues),
            "market_summary_ok": not any("market_summary" in w.lower() for w in warnings),
        },
    }

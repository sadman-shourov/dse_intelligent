"""Walk-forward backtest of the signals table against actual forward prices.

For each BUY signal:
  - Entry: signal-day close (or next trading-day close if signal-day price missing)
  - Exit: holding-day close, or earlier if a stop-loss or take-profit triggers
  - Cost: 0.4% round-trip (DSE retail typical)

For each EXIT signal, we measure forward drawdown — a "good" EXIT means
price actually fell after we said EXIT.

Outputs:
  - Overall PnL vs equal-weighted benchmark (entered same day, same horizon)
  - Bucketed by confidence_score (joined from analysis_results)
  - Bucketed by stock_class
  - Bucketed by signal_type
  - Bucketed by reason keyword
  - Calibration check: does higher confidence -> higher return?
"""
from __future__ import annotations

import os
import statistics
from collections import defaultdict
from datetime import date, timedelta

import psycopg2
from dotenv import load_dotenv

ROUND_TRIP_COST = 0.004  # 0.4% all-in (commission + tax + slippage)
HOLD_DAYS = [5, 10, 20]
STOP_LOSS_PCT = -0.08
TAKE_PROFIT_PCT = 0.15


def main():
    load_dotenv(".env")
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    cur = conn.cursor()

    # Pull all signals + matching analysis confidence + stock_class
    cur.execute(
        """
        SELECT s.symbol, s.signal_type, s.signal_date, s.price_at_signal,
               s.reason,
               ar.confidence_score, ar.stock_class, ar.overall_signal
        FROM signals s
        LEFT JOIN LATERAL (
            SELECT confidence_score, stock_class, overall_signal
            FROM analysis_results
            WHERE symbol = s.symbol
              AND analysis_date <= s.signal_date
            ORDER BY analysis_date DESC, session_no DESC NULLS LAST, id DESC
            LIMIT 1
        ) ar ON TRUE
        WHERE s.signal_type IN ('BUY','EXIT','WATCH')
        ORDER BY s.signal_date, s.symbol
        """
    )
    signals = cur.fetchall()
    print(f"Loaded {len(signals)} signals")

    # Forward price lookup: build (symbol, date) -> close
    cur.execute(
        "SELECT symbol, date, close, high, low FROM price_history WHERE close IS NOT NULL"
    )
    px = {}
    sym_dates = defaultdict(list)  # symbol -> sorted list of (date, close, high, low)
    for sym, d, close, high, low in cur.fetchall():
        px[(sym, d)] = (float(close), float(high or close), float(low or close))
        sym_dates[sym].append((d, float(close), float(high or close), float(low or close)))
    for s in sym_dates:
        sym_dates[s].sort(key=lambda x: x[0])
    print(f"Loaded price history for {len(sym_dates)} symbols")

    def next_trading_close(symbol, after_date):
        """Find first trading day >= after_date for this symbol, return (date, close)."""
        for d, c, _h, _l in sym_dates.get(symbol, []):
            if d >= after_date:
                return d, c
        return None, None

    def forward_bars(symbol, entry_date, n):
        """Return list of (date, close, high, low) for n trading bars AFTER entry_date."""
        out = []
        for d, c, h, l in sym_dates.get(symbol, []):
            if d > entry_date:
                out.append((d, c, h, l))
                if len(out) >= n:
                    break
        return out

    # Universe avg return per (entry_date, horizon) for benchmark
    # We'll compute on the fly per trade — average forward return of a random symbol
    all_symbols = list(sym_dates.keys())

    results = []  # one dict per evaluated BUY signal
    exit_results = []  # one dict per evaluated EXIT signal

    for sym, stype, sdate, psig, reason, conf, cls, ov_sig in signals:
        # entry the day AFTER signal-day to avoid look-ahead (you can't trade on close
        # while watching that same close form). Use next trading day's close.
        entry_d, entry_close = next_trading_close(sym, sdate + timedelta(days=1))
        if entry_d is None or entry_close is None or entry_close <= 0:
            continue

        for hold in HOLD_DAYS:
            bars = forward_bars(sym, entry_d, hold)
            if len(bars) < hold:
                continue  # not enough forward data

            # Apply stop-loss / take-profit intrabar
            exit_close = bars[-1][1]
            exit_reason = "TIME"
            for d, c, h, l in bars:
                hi_ret = (h - entry_close) / entry_close
                lo_ret = (l - entry_close) / entry_close
                if lo_ret <= STOP_LOSS_PCT:
                    exit_close = entry_close * (1 + STOP_LOSS_PCT)
                    exit_reason = "STOP"
                    break
                if hi_ret >= TAKE_PROFIT_PCT:
                    exit_close = entry_close * (1 + TAKE_PROFIT_PCT)
                    exit_reason = "TARGET"
                    break

            gross_ret = (exit_close - entry_close) / entry_close
            net_ret = gross_ret - ROUND_TRIP_COST

            # Benchmark: average forward return of all symbols entered same day, same horizon
            # (cheap proxy for an equal-weighted basket)
            row = {
                "symbol": sym,
                "signal_type": stype,
                "signal_date": sdate,
                "entry_date": entry_d,
                "entry_close": entry_close,
                "exit_close": exit_close,
                "exit_reason": exit_reason,
                "hold_days": hold,
                "gross_ret": gross_ret,
                "net_ret": net_ret,
                "confidence": float(conf) if conf is not None else None,
                "stock_class": cls,
                "ov_signal": ov_sig,
                "reason": reason or "",
            }
            if stype == "BUY":
                results.append(row)
            elif stype == "EXIT":
                exit_results.append(row)

    print(f"\nEvaluated BUY trades: {len(results)}")
    print(f"Evaluated EXIT trades: {len(exit_results)}")

    # Benchmark: for each (entry_date, hold), avg forward return across whole universe
    bench_cache = {}

    def benchmark_return(entry_date, hold):
        key = (entry_date, hold)
        if key in bench_cache:
            return bench_cache[key]
        rets = []
        for s in all_symbols:
            bars = forward_bars(s, entry_date, hold)
            if len(bars) >= hold:
                _, entry_d2, _ = next_trading_close(s, entry_date), None, None  # noqa
                # we want entry at entry_date for this symbol
                eclose = None
                for d, c, _h, _l in sym_dates[s]:
                    if d == entry_date:
                        eclose = c
                        break
                if eclose and eclose > 0:
                    rets.append((bars[-1][1] - eclose) / eclose)
        v = statistics.mean(rets) if rets else 0.0
        bench_cache[key] = v
        return v

    def stats(rows):
        if not rows:
            return None
        net = [r["net_ret"] for r in rows]
        wins = sum(1 for v in net if v > 0)
        return {
            "n": len(rows),
            "win_rate": wins / len(rows),
            "avg_net": statistics.mean(net),
            "median_net": statistics.median(net),
            "stdev": statistics.pstdev(net),
            "best": max(net),
            "worst": min(net),
            "sharpe_like": (statistics.mean(net) / statistics.pstdev(net)) if statistics.pstdev(net) > 0 else 0,
        }

    print("\n" + "=" * 70)
    print("OVERALL BUY PERFORMANCE (net of 0.4% cost, by holding period)")
    print("=" * 70)
    for hold in HOLD_DAYS:
        rows = [r for r in results if r["hold_days"] == hold]
        s = stats(rows)
        if not s:
            continue
        # benchmark on same trades' entry dates
        bench = [benchmark_return(r["entry_date"], hold) for r in rows]
        bench_mean = statistics.mean(bench) if bench else 0
        edge = s["avg_net"] - (bench_mean - ROUND_TRIP_COST)
        print(f"\n  {hold}d hold:  n={s['n']}  win={s['win_rate']*100:.1f}%  "
              f"avg_net={s['avg_net']*100:+.2f}%  stdev={s['stdev']*100:.2f}%  "
              f"best={s['best']*100:+.1f}%  worst={s['worst']*100:+.1f}%")
        print(f"            benchmark_net={bench_mean*100 - ROUND_TRIP_COST*100:+.2f}%   "
              f"EDGE vs benchmark={edge*100:+.2f}%   sharpe-like={s['sharpe_like']:.2f}")

    print("\n" + "=" * 70)
    print("EXIT QUALITY (a 'good' EXIT means price fell after; signed by -1)")
    print("=" * 70)
    for hold in HOLD_DAYS:
        rows = [r for r in exit_results if r["hold_days"] == hold]
        if not rows:
            continue
        # invert returns: positive value means EXIT was correct
        signed = [-r["gross_ret"] for r in rows]
        wins = sum(1 for v in signed if v > 0)
        print(f"  {hold}d:  n={len(rows)}  EXIT_correct_rate={wins/len(rows)*100:.1f}%  "
              f"avg_avoided={statistics.mean(signed)*100:+.2f}%")

    print("\n" + "=" * 70)
    print("BUY by CONFIDENCE bucket (hold=10d)")
    print("=" * 70)
    buckets = [(0, 0.3), (0.3, 0.5), (0.5, 0.7), (0.7, 0.85), (0.85, 1.01)]
    h = 10
    for lo, hi in buckets:
        rows = [r for r in results
                if r["hold_days"] == h
                and r["confidence"] is not None
                and lo <= r["confidence"] < hi]
        s = stats(rows)
        if not s:
            print(f"  conf [{lo:.2f},{hi:.2f}): no data")
            continue
        print(f"  conf [{lo:.2f},{hi:.2f}):  n={s['n']:4d}  win={s['win_rate']*100:5.1f}%  "
              f"avg_net={s['avg_net']*100:+.2f}%  median={s['median_net']*100:+.2f}%")

    print("\n" + "=" * 70)
    print("BUY by STOCK_CLASS (hold=10d)")
    print("=" * 70)
    by_class = defaultdict(list)
    for r in results:
        if r["hold_days"] == h:
            by_class[r["stock_class"] or "(none)"].append(r)
    for cls in sorted(by_class.keys()):
        s = stats(by_class[cls])
        if s:
            print(f"  {cls:14s}  n={s['n']:4d}  win={s['win_rate']*100:5.1f}%  "
                  f"avg_net={s['avg_net']*100:+.2f}%")

    print("\n" + "=" * 70)
    print("BUY by REASON keyword (hold=10d, top reasons)")
    print("=" * 70)
    keywords = ["BREAKOUT", "MACD", "RSI", "VOLUME", "SUPPORT", "ACCUMULATION",
                "COILING", "MA50", "OVERSOLD", "MOMENTUM"]
    for kw in keywords:
        rows = [r for r in results if r["hold_days"] == h and kw.lower() in r["reason"].lower()]
        s = stats(rows)
        if s and s["n"] >= 5:
            print(f"  {kw:14s}  n={s['n']:4d}  win={s['win_rate']*100:5.1f}%  "
                  f"avg_net={s['avg_net']*100:+.2f}%")

    print("\n" + "=" * 70)
    print("EXIT-REASON DISTRIBUTION (BUY trades, hold=10d)")
    print("=" * 70)
    exit_reasons = defaultdict(int)
    for r in results:
        if r["hold_days"] == h:
            exit_reasons[r["exit_reason"]] += 1
    tot = sum(exit_reasons.values()) or 1
    for k, v in sorted(exit_reasons.items(), key=lambda x: -x[1]):
        print(f"  {k}: {v} ({v/tot*100:.1f}%)")

    conn.close()


if __name__ == "__main__":
    main()

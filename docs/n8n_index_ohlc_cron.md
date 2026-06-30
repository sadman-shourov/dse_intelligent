# n8n — Index OHLC candles

Index candles in `index_ohlc` come from two places:

- **History (before today)**: one-time backfill from stocknow. Already loaded
  (DSEX back to 2003, DS30/DSES to 2014). Nothing to schedule.
- **Going forward**: self-generated from our own pipeline. No third-party
  dependency. This is the part you wire in n8n.

## How self-generation works

1. **Intraday — already happening.** During market hours your market-hours
   chain calls `POST /ingest/live-ticks` every few minutes. That runs
   `fetch_market_summary`, which now also appends the live DSEX/DS30/DSES value
   to the `index_ticks` table. Every run = one more sample.
   **No new node needed. It rides on the live-ticks call you already make.**

2. **End of day — add one node.** After close, `POST /ingest/index-ohlc-rollup`
   reads today's samples and writes one candle per index:
   `open` = first sample, `high` = max, `low` = min, `close` = last,
   `volume` = the day's total market volume. Written with `source = 'self'`.

## EOD chain — where the new node goes

Add the rollup as one HTTP Request node, **right after `Fetch Market Summary`**
(so the closing sample and the day's volume are in before it runs):

```
EOD Cron
  → Append Price History
  → Fetch Market Summary
  → Index OHLC Rollup   ← NEW
  → Cleanup Live Ticks
  → EOD Analysis
  → EOD Pulse
  → Evaluate Signals
```

Order note: `Cleanup Live Ticks` only clears `live_ticks`. It never touches
`index_ticks`, so the rollup is safe on either side of it. Placing it right
after `Fetch Market Summary` is simplest and guarantees the final sample is in.

### The new HTTP Request node

- **Method**: POST
- **URL**: `https://piloting-intellegnce.p-stageenv.xyz/ingest/index-ohlc-rollup`
- **Timeout**: 30000 ms is plenty (pure DB work, no external fetch).
- No body, no auth (same as your other `/ingest/*` nodes).

### Rollup response (reference)

```json
{
  "status": "ok",
  "date": "2026-07-01",
  "samples_today": 23,
  "indices_written": ["DSEX", "DS30", "DSES"],
  "indices": {
    "DSEX": {"status": "ok", "samples": 23, "open": 5762.83, "high": 5781.4, "low": 5755.1, "close": 5770.2}
  }
}
```

- `status`: `ok` | `skipped` | `error`
- `skipped` means there were not enough samples to build a real candle — fewer
  than 4 samples, or spread over less than 30 minutes. The job writes nothing,
  so a flat/near-flat artifact never overwrites a real candle. This is the
  correct, healthy result when you test the node off-hours (the market is closed,
  so only one sample exists) or on a holiday.
- `samples_today`: how many intraday samples fed the candle. More samples =
  tighter high/low. A normal trading day clears the guard easily. If a trading
  day is skipped, your market-hours chain barely ran — investigate that.

### Testing the node off-hours

Clicking **Execute step** in n8n also runs the upstream Fetch Market Summary,
which records one sample. With a single off-hours sample the rollup returns
`skipped` (guard not met) and writes nothing. That is expected and correct — it
proves the node is wired. The real `ok` candle appears after a full trading day.

## Source of truth

- New days are owned by the EOD rollup (`source='self'`).
- History stays as the one-time stocknow backfill (`source='stocknow'`),
  untouched.
- There is no scheduled stocknow append. If a day's self-generation ever fails
  (cron outage, no samples), recover by re-running the one-time backfill
  `python -m ingestion.backfill_index_ohlc` — it is idempotent and fills gaps.

## Quick reference (expression mode)

```text
{{ $json.status !== "error" }}
```

## Importable rollup node

```json
{
  "parameters": {
    "method": "POST",
    "url": "https://piloting-intellegnce.p-stageenv.xyz/ingest/index-ohlc-rollup",
    "options": { "timeout": 30000 }
  },
  "name": "Index OHLC Rollup",
  "type": "n8n-nodes-base.httpRequest",
  "typeVersion": 4.2,
  "position": [0, 0]
}
```

# n8n — Market Hours Cron chain

Target flow: **Cron → POST `/ingest/live-ticks`**

That single endpoint:

1. Fetches live ticks (or returns `skipped` outside the session window)
2. Updates market summary when ingest succeeds
3. Runs full symbol analysis when ingest status is `ok`
4. For each active trader, runs proactive pulse logic (send only when actionable)
5. Returns a summary including `analysis`, `pulses_sent`, and `pulses_skipped`

## 1. n8n workflow

Use one **HTTP Request** node per session tick:

- **Method**: POST
- **URL**: `https://<your-api>/ingest/live-ticks` (or local `http://localhost:8000/ingest/live-ticks`)

No separate "Run Analysis" or "Deliver Pulse" nodes are required; those steps run inside the endpoint.

## 2. Response fields (reference)

- `status`: `ok` | `skipped` | `error`
- `market_summary_updated`: present when ingest was `ok`
- `analysis`: when analysis ran successfully — `buy_signals`, `watch_signals`, `exit_signals`
- `pulses_sent` / `pulses_skipped`: proactive Telegram delivery counts
- `extreme_moves_detected` / `extreme_move_alert`: unchanged from before

When ingest is `skipped` (e.g. market closed), analysis may be omitted, but **`deliver_pulse_if_needed` still runs** so portfolio-level alerts can fire.

## 3. Optional monitoring

- If `status === "error"`, treat as failure (alert / retry).
- Low `pulses_sent` is normal when nothing actionable; do not treat as an error by itself.

## Quick reference (expression mode)

Single step — no IF chain required for analysis/pulse; optional check on HTTP status only:

```text
{{ $json.status !== "error" }}
```

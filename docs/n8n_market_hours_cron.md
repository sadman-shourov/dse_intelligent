# n8n — Market Hours Cron chain

Target flow: **Cron → Ingest Live Ticks → Run Analysis → Deliver Pulse**

## 1. After **Ingest Live Ticks**

Add an **IF** node:

- **Condition**: combine with **OR**
  - `{{ $json.status }}` **equals** `ok`
  - **OR** `{{ $json.status }}` **equals** `skipped`
- **True** → connect to **Run Analysis**
- **False** → **stop** (do not run analysis; log / notify as needed)

Rationale: only abort when the ingest step reports a hard failure; `skipped` (e.g. market closed) may still be an intentional outcome—adjust if your ingest never returns `skipped` on success paths.

## 2. After **Run Analysis**

Add an **IF** node:

- **Condition**: `{{ $json.status }}` **equals** `ok`
- **True** → connect to **Deliver Pulse**
- **False** → **stop** (no pulse on failed analysis)

### Optional warning (not blocking)

If the analysis response includes a symbol count (e.g. `done`):

- If `done < 300`, append a **warning** (Set node or Code) but **continue** to Deliver Pulse if status is still `ok`.

## 3. After **Deliver Pulse**

- If `traders_delivered === 0` (or missing and you treat as failure), **log as error** (e.g. Telegram, Slack, or n8n Error Workflow).
- Do not treat as success for alerting purposes when no traders received the pulse.

## Quick reference (expression mode)

Ingest gate (OR):

```text
{{ $json.status === "ok" || $json.status === "skipped" }}
```

Analysis gate:

```text
{{ $json.status === "ok" }}
```

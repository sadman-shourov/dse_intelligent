# AGENTS.md

## Cursor Cloud specific instructions

ARIA is a single Python **FastAPI** service (`api.main:app`) for Dhaka Stock Exchange (DSE)
analysis. It reads/writes a **PostgreSQL** database (production uses Neon; locally a Postgres
cluster on `127.0.0.1:5432`). Live market data is pulled from `dsebd.org` via the `bdshare`
package. Optional LLM "pulse" generation uses OpenRouter; optional delivery uses Telegram.

The cloud VM is pre-provisioned by the update script (Python deps) plus a one-time setup
(local Postgres, `.env`, CA trust fix) captured in the VM snapshot. The notes below are the
non-obvious things that are easy to get wrong.

### Running the service (dev)
- Activate the venv first: `source .venv/bin/activate`.
- Start the dev server with hot reload: `./run_api.sh` (= `uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload`). Listens on `:8000`.
- Health check: `curl http://127.0.0.1:8000/health`. Endpoint catalogue: `GET /`.
- There is **no test suite and no linter config** in the repo. For a static sanity check use `python -m py_compile api/*.py analysis/*.py ingestion/*.py pulse/*.py db/*.py` and `python -c "import api.main"`.

### PostgreSQL (must be started manually)
- The local cluster does not always auto-start. Start it with `sudo pg_ctlcluster 16 main start` and verify with `pg_lsclusters`.
- Connection (already in `.env`): `postgresql://aria:aria@127.0.0.1:5432/aria`.
- **Schema load gotcha:** do **not** rely on `python db/run_schema.py` for a fresh DB. `db/schema.sql` contains `DELETE FROM market_summary WHERE trade_date > CURRENT_DATE;`, but that table's column is named `date`, so the statement errors and `run_schema.py` aborts before creating the later tables. Load the schema with psql instead, which continues past that one harmless error: `PGPASSWORD=aria psql -h 127.0.0.1 -U aria -d aria -v ON_ERROR_STOP=0 -f db/schema.sql`. The `.env` DB already has all 20 tables applied.

### DSE / bdshare TLS gotcha (most common ingestion failure)
- `bdshare` fetches from `https://dsebd.org`, which serves an **incomplete TLS chain** (it omits the Sectigo "Public Server Authentication CA DV R36" intermediate). Its fallback domain `dsebd.com.bd` no longer resolves in DNS. So `bdshare` calls (and every `/ingest/*` endpoint) fail by default with `SSLError: CERTIFICATE_VERIFY_FAILED (unable to get local issuer certificate)`, with a misleading "last error" pointing at the dead fallback domain.
- Fix already applied to the VM: the missing intermediate is installed in the system CA store, and `~/.bashrc` exports `REQUESTS_CA_BUNDLE` / `SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt`. New interactive shells inherit this, so `requests`/`httpx`/`ssl` can verify `dsebd.org`. **Start the server from a shell that has these vars set** (a normal login shell does).
- If a future shell/process lacks the vars (ingestion suddenly fails SSL again), either `export REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt`, or re-install the intermediate: `curl -s http://crt.sectigo.com/SectigoPublicServerAuthenticationCADVR36.crt | openssl x509 -inform DER -out /usr/local/share/ca-certificates/sectigo_r36.crt && sudo update-ca-certificates`.
- DSE only returns data during/after Bangladesh trading hours; off-hours some ingestion endpoints may return empty datasets even when connectivity is fine. `/ingest/sync-stocks` (symbol list) works any time.

### Environment variables (`.env` at repo root)
- `DATABASE_URL` — required for everything.
- `OPENROUTER_API_KEY` — only needed for `/pulse/*` LLM generation; core API, ingestion and analysis work without it.
- `TELEGRAM_BOT_TOKEN` — only needed for Telegram delivery (`/pulse/deliver/*`, `/alerts/*` send paths).

#!/bin/bash
# Deploy ARIA via Docker Compose v2 (docker compose, not docker-compose).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if ! command -v docker >/dev/null 2>&1; then
  echo "Error: docker is not installed or not on PATH."
  exit 1
fi

if docker compose version >/dev/null 2>&1; then
  COMPOSE=(docker compose)
elif command -v docker-compose >/dev/null 2>&1; then
  echo "Warning: legacy docker-compose v1 detected."
  echo "It often breaks with newer Python requests/urllib3 (http+docker scheme error)."
  echo "Install Compose v2 instead: sudo apt-get install -y docker-compose-plugin"
  echo "Then run: docker compose up --build -d"
  exit 1
else
  echo "Error: Docker Compose v2 plugin not found."
  echo "Install with: sudo apt-get install -y docker-compose-plugin"
  exit 1
fi

if [[ ! -f .env ]]; then
  echo "Error: .env not found in $ROOT"
  echo "Create it with DATABASE_URL, OPENROUTER_API_KEY, OPENROUTER_MODEL, TELEGRAM_BOT_TOKEN"
  exit 1
fi

"${COMPOSE[@]}" down
"${COMPOSE[@]}" up --build -d
"${COMPOSE[@]}" ps

echo ""
echo "Deploy complete. Health check: curl -f http://localhost:8000/health"

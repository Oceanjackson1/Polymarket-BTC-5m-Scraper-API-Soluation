#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT=/opt/polymarket-agent/current
ENV_FILE="${POLYMARKET_ENV_FILE:-/etc/polymarket-agent.env}"

if [[ -f "$ENV_FILE" ]]; then
  set -a
  source "$ENV_FILE"
  set +a
fi

PUBLISH_DIR="${POLYMARKET_PUBLISH_DIR:-/data/polymarket-agent/publish}"
API_BIND="${POLYMARKET_API_BIND:-0.0.0.0}"
API_PORT="${POLYMARKET_API_PORT:-8080}"
API_DEFAULT_LIMIT="${POLYMARKET_API_DEFAULT_LIMIT:-200}"
API_MAX_LIMIT="${POLYMARKET_API_MAX_LIMIT:-5000}"
API_AUTH_MODE="${POLYMARKET_API_AUTH_MODE:-bearer}"
API_BEARER_TOKEN="${POLYMARKET_API_BEARER_TOKEN:-}"
LOG_LEVEL="${POLYMARKET_LOG_LEVEL:-INFO}"

if [[ "$API_AUTH_MODE" == "bearer" && -z "$API_BEARER_TOKEN" ]]; then
  echo "POLYMARKET_API_BEARER_TOKEN is required" >&2
  exit 1
fi

exec "$REPO_ROOT/.venv/bin/python" "$REPO_ROOT/scripts/run_data_api.py" \
  --publish-dir "$PUBLISH_DIR" \
  --host "$API_BIND" \
  --port "$API_PORT" \
  --default-limit "$API_DEFAULT_LIMIT" \
  --max-limit "$API_MAX_LIMIT" \
  --auth-mode "$API_AUTH_MODE" \
  --log-level "$LOG_LEVEL"

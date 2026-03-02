#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT=/opt/polymarket-agent/current
ENV_FILE="${POLYMARKET_ENV_FILE:-/etc/polymarket-agent.env}"

if [[ -f "$ENV_FILE" ]]; then
  set -a
  source "$ENV_FILE"
  set +a
fi

OUTPUT_DIR="${POLYMARKET_OUTPUT_DIR:-/data/polymarket-agent/output}"
PUBLISH_DIR="${POLYMARKET_PUBLISH_DIR:-/data/polymarket-agent/publish}"
STATE_DIR="${POLYMARKET_STATE_DIR:-/data/polymarket-agent/state}"
INTERVAL_SECONDS="${POLYMARKET_MATERIALIZE_INTERVAL_SECONDS:-60}"
STABLE_SECONDS="${POLYMARKET_MATERIALIZE_STABLE_SECONDS:-300}"
LINK_MODE="${POLYMARKET_MATERIALIZE_LINK_MODE:-hardlink}"
LOG_LEVEL="${POLYMARKET_LOG_LEVEL:-INFO}"

mkdir -p "$PUBLISH_DIR" "$STATE_DIR"

exec "$REPO_ROOT/.venv/bin/python" "$REPO_ROOT/scripts/materialize_orderbook_data.py" \
  --source-dir "$OUTPUT_DIR/orderbook" \
  --publish-dir "$PUBLISH_DIR" \
  --interval-seconds "$INTERVAL_SECONDS" \
  --stable-seconds "$STABLE_SECONDS" \
  --state-file "$STATE_DIR/materialize_state.json" \
  --link-mode "$LINK_MODE" \
  --log-level "$LOG_LEVEL"

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
TIMEFRAMES="${POLYMARKET_TIMEFRAMES:-5m,15m,1h,4h}"
POLL_INTERVAL="${POLYMARKET_POLL_INTERVAL_SECONDS:-30}"
GRACE_PERIOD="${POLYMARKET_GRACE_PERIOD_SECONDS:-120}"
FLUSH_INTERVAL="${POLYMARKET_FLUSH_INTERVAL_SECONDS:-5}"
LOG_LEVEL="${POLYMARKET_LOG_LEVEL:-INFO}"

mkdir -p "$OUTPUT_DIR"

exec "$REPO_ROOT/.venv/bin/python" "$REPO_ROOT/scripts/run_recorder.py" \
  --output-dir "$OUTPUT_DIR" \
  --poll-interval "$POLL_INTERVAL" \
  --grace-period "$GRACE_PERIOD" \
  --flush-interval "$FLUSH_INTERVAL" \
  --log-level "$LOG_LEVEL" \
  --timeframes "$TIMEFRAMES"

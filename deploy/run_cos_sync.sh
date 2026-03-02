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
SYNC_INTERVAL="${TENCENT_COS_SYNC_INTERVAL_SECONDS:-60}"
STABLE_SECONDS="${TENCENT_COS_STABLE_SECONDS:-60}"
LOG_LEVEL="${POLYMARKET_LOG_LEVEL:-INFO}"

mkdir -p "$STATE_DIR"

exec "$REPO_ROOT/.venv/bin/python" "$REPO_ROOT/scripts/sync_to_tencent_cos.py" \
  --source-dir "$PUBLISH_DIR" \
  --bucket "${TENCENT_COS_BUCKET:-}" \
  --region "${TENCENT_COS_REGION:-}" \
  --prefix "${TENCENT_COS_PREFIX:-}" \
  --interval-seconds "$SYNC_INTERVAL" \
  --stable-seconds "$STABLE_SECONDS" \
  --state-file "$STATE_DIR/cos_sync_state.json" \
  --log-level "$LOG_LEVEL"

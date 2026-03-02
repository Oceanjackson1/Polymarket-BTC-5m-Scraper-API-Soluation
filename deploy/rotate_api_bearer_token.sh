#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="${POLYMARKET_ENV_FILE:-/etc/polymarket-agent.env}"
TOKEN_FILE="${POLYMARKET_API_TOKEN_FILE:-/root/polymarket_api_bearer_token.txt}"
API_PORT=8080
PRINT_TOKEN=0
USER_TOKEN=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env-file)
      ENV_FILE="$2"
      shift 2
      ;;
    --token-file)
      TOKEN_FILE="$2"
      shift 2
      ;;
    --token)
      USER_TOKEN="$2"
      shift 2
      ;;
    --print-token)
      PRINT_TOKEN=1
      shift
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

if [[ -f "$ENV_FILE" ]]; then
  set -a
  source "$ENV_FILE"
  set +a
  API_PORT="${POLYMARKET_API_PORT:-$API_PORT}"
fi

TOKEN="$USER_TOKEN"
if [[ -z "$TOKEN" ]]; then
  TOKEN="$(python3 - <<'PY'
import secrets
print(secrets.token_urlsafe(32))
PY
)"
fi

mkdir -p "$(dirname "$TOKEN_FILE")"
printf '%s\n' "$TOKEN" > "$TOKEN_FILE"
chmod 600 "$TOKEN_FILE"

touch "$ENV_FILE"
if grep -q '^POLYMARKET_API_BEARER_TOKEN=' "$ENV_FILE"; then
  sed -i "s#^POLYMARKET_API_BEARER_TOKEN=.*#POLYMARKET_API_BEARER_TOKEN=${TOKEN}#" "$ENV_FILE"
else
  printf '\nPOLYMARKET_API_BEARER_TOKEN=%s\n' "$TOKEN" >> "$ENV_FILE"
fi

systemctl restart polymarket-data-api.service

for _ in $(seq 1 20); do
  if curl -sf "http://127.0.0.1:${API_PORT}/health" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

echo "token rotated"
echo "env file: $ENV_FILE"
echo "token file: $TOKEN_FILE"

if [[ "$PRINT_TOKEN" -eq 1 ]]; then
  printf '%s\n' "$TOKEN"
fi

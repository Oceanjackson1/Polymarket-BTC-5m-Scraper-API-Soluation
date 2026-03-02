#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT=/opt/polymarket-agent/current
ENV_FILE="${POLYMARKET_ENV_FILE:-/etc/polymarket-agent.env}"
NGINX_TEMPLATE="$REPO_ROOT/deploy/nginx/polymarket-data-api.conf"
NGINX_TARGET=/etc/nginx/conf.d/polymarket-data-api.conf
NGINX_DEFAULT_CONF=/etc/nginx/conf.d/default.conf

if [[ -f "$ENV_FILE" ]]; then
  set -a
  source "$ENV_FILE"
  set +a
fi

install_nginx_package() {
  if command -v nginx >/dev/null 2>&1; then
    return
  fi

  if command -v apt-get >/dev/null 2>&1; then
    apt-get update
    DEBIAN_FRONTEND=noninteractive apt-get install -y nginx openssl
    return
  fi

  if command -v dnf >/dev/null 2>&1; then
    dnf install -y nginx openssl
    return
  fi

  if command -v yum >/dev/null 2>&1; then
    yum install -y nginx openssl
    return
  fi

  echo "Unsupported package manager. Install nginx and openssl manually." >&2
  exit 1
}

enable_selinux_proxy_connect() {
  if ! command -v getenforce >/dev/null 2>&1; then
    return
  fi

  if [[ "$(getenforce)" == "Disabled" ]]; then
    return
  fi

  if command -v setsebool >/dev/null 2>&1; then
    setsebool -P httpd_can_network_connect 1 || true
  fi
}

escape_sed() {
  printf '%s' "$1" | sed 's/[\/&]/\\&/g'
}

SERVER_NAME="${POLYMARKET_API_SERVER_NAME:-_}"
TLS_CERT_PATH="${POLYMARKET_TLS_CERT_PATH:-/etc/nginx/ssl/polymarket-data-api.crt}"
TLS_KEY_PATH="${POLYMARKET_TLS_KEY_PATH:-/etc/nginx/ssl/polymarket-data-api.key}"
TLS_CERT_DAYS="${POLYMARKET_TLS_CERT_DAYS:-365}"
TLS_CN="${POLYMARKET_TLS_CN:-}"
TLS_SAN="${POLYMARKET_TLS_SAN:-}"

if [[ -z "$TLS_CN" ]]; then
  TLS_CN="$(hostname -I 2>/dev/null | awk '{print $1}')"
fi
if [[ -z "$TLS_CN" ]]; then
  TLS_CN="$(hostname -f 2>/dev/null || hostname)"
fi

if [[ -z "$TLS_SAN" ]]; then
  if [[ "$TLS_CN" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    TLS_SAN="IP:${TLS_CN},IP:127.0.0.1,DNS:localhost"
  else
    TLS_SAN="DNS:${TLS_CN},DNS:localhost,IP:127.0.0.1"
  fi
fi

install_nginx_package
enable_selinux_proxy_connect

mkdir -p "$(dirname "$TLS_CERT_PATH")"
mkdir -p /etc/nginx/conf.d

if [[ -f "$NGINX_DEFAULT_CONF" ]]; then
  mv -f "$NGINX_DEFAULT_CONF" "${NGINX_DEFAULT_CONF}.disabled"
fi

if [[ ! -s "$TLS_CERT_PATH" || ! -s "$TLS_KEY_PATH" ]]; then
  if openssl req -help 2>&1 | grep -q -- '-addext'; then
    openssl req -x509 -nodes -newkey rsa:2048 \
      -keyout "$TLS_KEY_PATH" \
      -out "$TLS_CERT_PATH" \
      -days "$TLS_CERT_DAYS" \
      -subj "/CN=${TLS_CN}" \
      -addext "subjectAltName=${TLS_SAN}"
  else
    TMP_EXT="$(mktemp)"
    trap 'rm -f "$TMP_EXT"' EXIT
    cat > "$TMP_EXT" <<EOF
[req]
distinguished_name=req_distinguished_name
[req_distinguished_name]
[v3_req]
subjectAltName=${TLS_SAN}
EOF
    openssl req -x509 -nodes -newkey rsa:2048 \
      -keyout "$TLS_KEY_PATH" \
      -out "$TLS_CERT_PATH" \
      -days "$TLS_CERT_DAYS" \
      -subj "/CN=${TLS_CN}" \
      -extensions v3_req \
      -config "$TMP_EXT"
    rm -f "$TMP_EXT"
    trap - EXIT
  fi
  chmod 600 "$TLS_KEY_PATH"
  chmod 644 "$TLS_CERT_PATH"
fi

cp "$NGINX_TEMPLATE" "$NGINX_TARGET"
sed -i \
  -e "s/__SERVER_NAME__/$(escape_sed "$SERVER_NAME")/g" \
  -e "s#__TLS_CERT_PATH__#$(escape_sed "$TLS_CERT_PATH")#g" \
  -e "s#__TLS_KEY_PATH__#$(escape_sed "$TLS_KEY_PATH")#g" \
  "$NGINX_TARGET"

nginx -t
systemctl enable --now nginx
systemctl reload nginx

echo "nginx config installed at $NGINX_TARGET"
echo "tls cert path: $TLS_CERT_PATH"
echo "tls key path: $TLS_KEY_PATH"

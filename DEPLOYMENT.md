# Cloud Deployment

This project can run as two long-lived services on a Linux VM:

1. `polymarket-orderbook-recorder`
2. `polymarket-materialize`
3. `polymarket-cos-sync`
4. `polymarket-data-api`

The intended layout on the server is:

```text
/opt/polymarket-agent/current     # repo checkout
/opt/polymarket-agent/current/.venv
/data/polymarket-agent/output     # recorder output
/data/polymarket-agent/publish    # raw/curated publish tree
/data/polymarket-agent/state      # sync state
/etc/polymarket-agent.env         # runtime config and COS credentials
```

## Notes

- The recorder now supports `5m,15m,1h,4h` through `--timeframes`.
- The recorder performs a forced flush/fsync every few seconds to reduce data loss during restarts.
- Closed markets are materialized into a `raw` and `curated` publish tree before COS upload.
- The materializer also publishes `meta/keysets/index.json` and `meta/keysets/dt=.../timeframe=.../manifest.json` so data can be batch-read without COS `ListBucket`.
- COS region for Tokyo is typically `ap-tokyo`.
- The optional API exposes the local publish tree over HTTP, with configurable `/v1/*` auth: `bearer` or `none`.
- Nginx can terminate HTTPS on `443` and reverse-proxy the API from `127.0.0.1:8080`.
- Recommended production binding is `POLYMARKET_API_BIND=127.0.0.1` so only Nginx is internet-facing.
- For a Feilian-only private deployment, use `POLYMARKET_API_AUTH_MODE=none` and keep the service on private networking only.

## Server Bootstrap

```bash
apt-get update
apt-get install -y python3 python3-venv python3-pip rsync
mkdir -p /opt/polymarket-agent /data/polymarket-agent/output /data/polymarket-agent/publish /data/polymarket-agent/state
```

## Upload Project

Use `rsync` or `scp` to copy this repository to:

```text
/opt/polymarket-agent/current
```

## Python Environment

```bash
cd /opt/polymarket-agent/current
python3 -m venv .venv
.venv/bin/pip install --upgrade pip
.venv/bin/pip install -r requirements-deploy.txt
chmod +x deploy/run_orderbook_recorder.sh deploy/run_materialize.sh deploy/run_cos_sync.sh deploy/run_data_api.sh
chmod +x deploy/install_nginx_proxy.sh deploy/rotate_api_bearer_token.sh
```

## Runtime Config

Create `/etc/polymarket-agent.env` from `deploy/polymarket-agent.env.example`.

Key API auth settings:

- `POLYMARKET_API_AUTH_MODE=bearer`
  Default. All `/v1/*` endpoints require `Authorization: Bearer <token>`.
- `POLYMARKET_API_AUTH_MODE=none`
  Scheme A. Use only for Feilian/VPN/private-network deployments where network access is already restricted.

## Install Services

```bash
cp deploy/systemd/polymarket-orderbook-recorder.service /etc/systemd/system/
cp deploy/systemd/polymarket-materialize.service /etc/systemd/system/
cp deploy/systemd/polymarket-cos-sync.service /etc/systemd/system/
cp deploy/systemd/polymarket-data-api.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now polymarket-orderbook-recorder.service
systemctl enable --now polymarket-materialize.service
systemctl enable --now polymarket-cos-sync.service
systemctl enable --now polymarket-data-api.service
```

The recorder service sets `LimitNOFILE=65535` because each active market uses three CSV file handles.

## HTTPS Proxy

Install Nginx and a self-signed TLS certificate, then proxy `443 -> 127.0.0.1:8080`:

```bash
cd /opt/polymarket-agent/current
./deploy/install_nginx_proxy.sh
```

Defaults:

- TLS cert: `/etc/nginx/ssl/polymarket-data-api.crt`
- TLS key: `/etc/nginx/ssl/polymarket-data-api.key`
- Nginx vhost: `/etc/nginx/conf.d/polymarket-data-api.conf`

If you later have a real domain and managed cert, replace the cert/key paths in `/etc/polymarket-agent.env` and rerun `./deploy/install_nginx_proxy.sh`.

## Token Rotation

Rotate the API bearer token and restart the API service:

```bash
cd /opt/polymarket-agent/current
./deploy/rotate_api_bearer_token.sh
```

The token is stored at:

- `/etc/polymarket-agent.env`
- `/root/polymarket_api_bearer_token.txt`

If `POLYMARKET_API_AUTH_MODE=none`, the API no longer uses the bearer token for `/v1/*`.

## Verification

```bash
systemctl status polymarket-orderbook-recorder.service --no-pager
systemctl status polymarket-materialize.service --no-pager
systemctl status polymarket-cos-sync.service --no-pager
systemctl status polymarket-data-api.service --no-pager
systemctl status nginx --no-pager
journalctl -u polymarket-orderbook-recorder.service -n 100 --no-pager
journalctl -u polymarket-materialize.service -n 100 --no-pager
journalctl -u polymarket-cos-sync.service -n 100 --no-pager
journalctl -u polymarket-data-api.service -n 100 --no-pager
find /data/polymarket-agent/output/orderbook -maxdepth 2 -type f | head
find /data/polymarket-agent/publish -maxdepth 4 -type f | head
curl http://127.0.0.1:${POLYMARKET_API_PORT:-8080}/health
curl -H "Authorization: Bearer ${POLYMARKET_API_BEARER_TOKEN}" "http://127.0.0.1:${POLYMARKET_API_PORT:-8080}/v1/keysets/index"
curl -k https://127.0.0.1/health
curl -k -H "Authorization: Bearer ${POLYMARKET_API_BEARER_TOKEN}" "https://127.0.0.1/v1/keysets/index"
```

For Feilian-only private mode:

```bash
sed -i 's/^POLYMARKET_API_AUTH_MODE=.*/POLYMARKET_API_AUTH_MODE=none/' /etc/polymarket-agent.env
systemctl restart polymarket-data-api.service
curl -k "https://127.0.0.1/v1/keysets/index"
```

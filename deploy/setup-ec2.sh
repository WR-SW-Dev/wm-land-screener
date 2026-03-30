#!/usr/bin/env bash
# =============================================================================
# WM Land Screener — EC2 Setup Script
# Target: Ubuntu 22.04, t3.medium, 20GB EBS, Elastic IP
#
# Usage:
#   Self-signed cert (no domain):  sudo bash setup-ec2.sh
#   Let's Encrypt (with domain):   sudo bash setup-ec2.sh YOUR_DOMAIN YOUR_EMAIL
# =============================================================================
set -euo pipefail

DOMAIN="${1:-}"
EMAIL="${2:-}"
APP_DIR="/home/ubuntu/wm-land-screener"
REPO="https://github.com/WR-SW-Dev/wm-land-screener.git"
BRANCH="Austin-changes"

echo "=== 1/7  System packages ==="
apt-get update
apt-get install -y python3-pip python3-venv libgdal-dev nginx git curl openssl
if [ -n "$DOMAIN" ]; then
    apt-get install -y certbot python3-certbot-nginx
fi

echo "=== 2/7  Clone repo ==="
if [ ! -d "$APP_DIR" ]; then
    sudo -u ubuntu git clone -b "$BRANCH" "$REPO" "$APP_DIR"
else
    echo "Repo already exists — pulling latest..."
    sudo -u ubuntu git -C "$APP_DIR" pull origin "$BRANCH"
fi

echo "=== 3/7  Python venv + deps ==="
sudo -u ubuntu python3 -m venv "$APP_DIR/.venv"
sudo -u ubuntu "$APP_DIR/.venv/bin/pip" install --upgrade pip
sudo -u ubuntu "$APP_DIR/.venv/bin/pip" install -r "$APP_DIR/requirements.txt"

echo "=== 4/7  Systemd service ==="
cp "$APP_DIR/deploy/wm-land-screener.service" /etc/systemd/system/
systemctl daemon-reload
systemctl enable wm-land-screener
systemctl restart wm-land-screener

echo "=== 5/7  SSL certificate ==="
if [ -n "$DOMAIN" ]; then
    echo "Will use Let's Encrypt for $DOMAIN (after nginx starts)..."
    SERVER_NAME="$DOMAIN"
    SSL_CERT="/etc/letsencrypt/live/$DOMAIN/fullchain.pem"
    SSL_KEY="/etc/letsencrypt/live/$DOMAIN/privkey.pem"
    USE_LE=true
else
    echo "No domain provided — generating self-signed certificate..."
    mkdir -p /etc/nginx/ssl
    openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
        -keyout /etc/nginx/ssl/selfsigned.key \
        -out /etc/nginx/ssl/selfsigned.crt \
        -subj "/C=US/ST=Michigan/L=GrandHaven/O=WakeroRobin/CN=wm-land-screener"
    SERVER_NAME="_"
    SSL_CERT="/etc/nginx/ssl/selfsigned.crt"
    SSL_KEY="/etc/nginx/ssl/selfsigned.key"
    USE_LE=false
fi

echo "=== 6/7  Nginx config ==="
if [ "$USE_LE" = true ]; then
    # HTTP-only first so certbot can do its challenge
    cat > /etc/nginx/sites-available/wm-land-screener <<NGINX
server {
    listen 80;
    server_name $SERVER_NAME;

    location / {
        proxy_pass http://127.0.0.1:8501;
        proxy_http_version 1.1;
        proxy_set_header Upgrade \$http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
        proxy_read_timeout 86400;
    }

    location /_stcore/stream {
        proxy_pass http://127.0.0.1:8501/_stcore/stream;
        proxy_http_version 1.1;
        proxy_set_header Upgrade \$http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host \$host;
        proxy_read_timeout 86400;
    }
}
NGINX
    ln -sf /etc/nginx/sites-available/wm-land-screener /etc/nginx/sites-enabled/
    rm -f /etc/nginx/sites-enabled/default
    nginx -t && systemctl restart nginx

    echo "Running certbot..."
    certbot --nginx -d "$DOMAIN" --non-interactive --agree-tos -m "$EMAIL" --redirect
else
    # Self-signed: full HTTPS config immediately
    cat > /etc/nginx/sites-available/wm-land-screener <<NGINX
server {
    listen 80;
    server_name _;
    return 301 https://\$host\$request_uri;
}

server {
    listen 443 ssl http2;
    server_name _;

    ssl_certificate     $SSL_CERT;
    ssl_certificate_key $SSL_KEY;
    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_ciphers HIGH:!aNULL:!MD5;

    location / {
        proxy_pass http://127.0.0.1:8501;
        proxy_http_version 1.1;
        proxy_set_header Upgrade \$http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
        proxy_read_timeout 86400;
    }

    location /_stcore/stream {
        proxy_pass http://127.0.0.1:8501/_stcore/stream;
        proxy_http_version 1.1;
        proxy_set_header Upgrade \$http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host \$host;
        proxy_read_timeout 86400;
    }
}
NGINX
    ln -sf /etc/nginx/sites-available/wm-land-screener /etc/nginx/sites-enabled/
    rm -f /etc/nginx/sites-enabled/default
    nginx -t && systemctl restart nginx
fi

echo "=== 7/7  Verify ==="
systemctl status wm-land-screener --no-pager
PUBLIC_IP=$(curl -s http://169.254.169.254/latest/meta-data/public-ipv4 || echo "UNKNOWN")
echo ""
echo "============================================"
if [ -n "$DOMAIN" ]; then
    echo "  App: https://$DOMAIN"
else
    echo "  App: https://$PUBLIC_IP"
    echo "  (Browser will show a certificate warning — click Advanced > Proceed)"
fi
echo "  Logs: journalctl -u wm-land-screener -f"
echo "============================================"

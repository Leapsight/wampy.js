#!/usr/bin/env bash
set -euo pipefail

PORT="${PORT:-8080}"
CERT_DIR="/tmp/wampy-demo-certs"
DOMAINS="oidc-demo.wellos localhost 127.0.0.1 ::1"

mkdir -p "$CERT_DIR"

KEY="$CERT_DIR/key.pem"
CERT="$CERT_DIR/keycert.pem"

if [ ! -f "$KEY" ] || [ ! -f "$CERT" ]; then
    if ! command -v mkcert &>/dev/null; then
        echo "mkcert is required: brew install mkcert"
        exit 1
    fi
    echo "Generating certs with mkcert for: $DOMAINS"
    mkcert -install 2>/dev/null || true
    mkcert -key-file "$KEY" -cert-file "$CERT" $DOMAINS
fi

echo "Serving at https://oidc-demo.wellos:${PORT}/test/oidc-demo.html"

cd "$(dirname "$0")/.."

python3 -c "
import http.server, ssl, os
port = int(os.environ.get('PORT', 8080))
ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
ctx.load_cert_chain('${CERT}', '${KEY}')
server = http.server.HTTPServer(('0.0.0.0', port), http.server.SimpleHTTPRequestHandler)
server.socket = ctx.wrap_socket(server.socket, server_side=True)
server.serve_forever()
"

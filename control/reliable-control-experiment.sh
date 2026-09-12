#!/usr/bin/env bash
set -euo pipefail

for command in openssl python3; do
  command -v "$command" >/dev/null || {
    echo "missing required command: $command" >&2
    exit 2
  }
done

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
SERVER_BIN="$ROOT_DIR/target/debug/game-server"
CLIENT_BIN="$ROOT_DIR/target/debug/control-client"
for binary in "$SERVER_BIN" "$CLIENT_BIN"; do
  [[ -x "$binary" ]] || {
    echo "missing binary: $binary" >&2
    exit 2
  }
done

TMP_DIR=$(mktemp -d)
CERT_PEM="$TMP_DIR/cert.pem"
KEY_PEM="$TMP_DIR/key.pem"
SERVER_LOG="$TMP_DIR/server.log"
PORT=4466
SERVER_PID=""

cleanup() {
  status=$?
  set +e
  if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill -TERM "$SERVER_PID" 2>/dev/null
    wait "$SERVER_PID" 2>/dev/null
  fi
  if [[ $status -ne 0 ]] && [[ -f "$SERVER_LOG" ]]; then
    echo "--- game-server reliable-control experiment log ---" >&2
    cat "$SERVER_LOG" >&2
  fi
  rm -rf "$TMP_DIR"
  exit "$status"
}
trap cleanup EXIT

openssl ecparam -name prime256v1 -genkey -noout -out "$KEY_PEM"
openssl req -new -x509 -sha256 -key "$KEY_PEM" -out "$CERT_PEM" -days 1 \
  -subj "/CN=game-server-control" \
  -addext "subjectAltName=IP:127.0.0.1,DNS:localhost" >/dev/null 2>&1
CERT_HASH=$(openssl x509 -in "$CERT_PEM" -noout -fingerprint -sha256 | cut -d= -f2 | tr 'A-F' 'a-f')

env \
  GAME_SERVER_PORT="$PORT" \
  GAME_SERVER_CERT_PEM="$CERT_PEM" \
  GAME_SERVER_KEY_PEM="$KEY_PEM" \
  GAME_SERVER_SESSION_PATH=/game \
  GAME_SERVER_DRAIN_GRACE_MS=50 \
  "$SERVER_BIN" >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!
sleep 0.45
if ! kill -0 "$SERVER_PID" 2>/dev/null; then
  wait "$SERVER_PID"
fi

URL="https://127.0.0.1:$PORT/game"
receipt=$("$CLIENT_BIN" "$URL" "$CERT_HASH")

python3 - "$receipt" <<'PY'
import json
import sys
receipt = json.loads(sys.argv[1])
assert receipt["expectationsHold"], receipt
assert receipt["acceptedRoundTrip"], receipt
assert receipt["serviceRejection"], receipt
assert receipt["malformedRejected"], receipt
assert receipt["oversizedRejected"], receipt
assert receipt["connectionRemainedUsable"], receipt
assert receipt["datagramProgressWhileControlStalled"], receipt
assert receipt["stalledStreamTimedOut"], receipt
assert receipt["concurrencyBoundObserved"], receipt
PY

grep -q "reliable control stream timed out" "$SERVER_LOG"
grep -q "reliable control stream rejected: concurrency limit reached" "$SERVER_LOG"

kill -TERM "$SERVER_PID"
wait "$SERVER_PID"
SERVER_PID=""

python3 - "$receipt" <<'PY'
import json
import sys
print(json.dumps({
    "mode": "game-server-reliable-control",
    "client": json.loads(sys.argv[1]),
    "timeoutObserved": True,
    "concurrencyLimitObserved": True,
    "expectationsHold": True,
}, separators=(",", ":")))
PY

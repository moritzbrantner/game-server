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
CLIENT_BIN="$ROOT_DIR/target/debug/netem-client"
for binary in "$SERVER_BIN" "$CLIENT_BIN"; do
  [[ -x "$binary" ]] || {
    echo "missing binary: $binary" >&2
    exit 2
  }
done

TMP_DIR=$(mktemp -d)
CERT_PEM="$TMP_DIR/cert.pem"
KEY_PEM="$TMP_DIR/key.pem"
RECOVERY_PATH="$TMP_DIR/match.recovery"
SERVER_LOG="$TMP_DIR/server.log"
PORT=4455
SERVER_PID=""

cleanup() {
  status=$?
  set +e
  if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill -TERM "$SERVER_PID" 2>/dev/null
    wait "$SERVER_PID" 2>/dev/null
  fi
  if [[ $status -ne 0 ]] && [[ -f "$SERVER_LOG" ]]; then
    echo "--- game-server restart experiment log ---" >&2
    cat "$SERVER_LOG" >&2
  fi
  rm -rf "$TMP_DIR"
  exit "$status"
}
trap cleanup EXIT

openssl ecparam -name prime256v1 -genkey -noout -out "$KEY_PEM"
openssl req -new -x509 -sha256 -key "$KEY_PEM" -out "$CERT_PEM" -days 1 \
  -subj "/CN=game-server-recovery" \
  -addext "subjectAltName=IP:127.0.0.1,DNS:localhost" >/dev/null 2>&1
CERT_HASH=$(openssl x509 -in "$CERT_PEM" -noout -fingerprint -sha256 | cut -d= -f2 | tr 'A-F' 'a-f')

start_server() {
  : >"$SERVER_LOG"
  env \
    GAME_SERVER_PORT="$PORT" \
    GAME_SERVER_CERT_PEM="$CERT_PEM" \
    GAME_SERVER_KEY_PEM="$KEY_PEM" \
    GAME_SERVER_SESSION_PATH=/game \
    GAME_SERVER_RECOVERY_PATH="$RECOVERY_PATH" \
    GAME_SERVER_DRAIN_GRACE_MS=50 \
    "$SERVER_BIN" >>"$SERVER_LOG" 2>&1 &
  SERVER_PID=$!
  sleep 0.45
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    wait "$SERVER_PID"
  fi
}

stop_server() {
  kill -TERM "$SERVER_PID"
  wait "$SERVER_PID"
  SERVER_PID=""
}

URL="https://127.0.0.1:$PORT/game"
start_server
first=$("$CLIENT_BIN" "$URL" "$CERT_HASH" 20 10 2000 10 1)

readarray -t first_values < <(python3 - "$first" <<'PY'
import json
import sys
receipt = json.loads(sys.argv[1])
assert receipt["expectationsHold"], receipt
assert receipt["connectionEpoch"] == 1, receipt
assert receipt["firstSentSequence"] == 1, receipt
assert receipt["finalSentSequence"] == 20, receipt
assert receipt["finalAppliedSequence"] == 20, receipt
print(receipt["playerId"])
print(receipt["connectionEpoch"])
print(receipt["reconnectToken"])
PY
)
PLAYER_ID=${first_values[0]}
FIRST_EPOCH=${first_values[1]}
RECONNECT_TOKEN=${first_values[2]}

stop_server
[[ -f "$RECOVERY_PATH" ]] || {
  echo "graceful shutdown did not create recovery image" >&2
  exit 1
}

start_server
[[ ! -e "$RECOVERY_PATH" ]] || {
  echo "startup did not consume the recovery image" >&2
  exit 1
}

second=$("$CLIENT_BIN" "$URL/reconnect/$RECONNECT_TOKEN" "$CERT_HASH" 20 10 2000 10 21)

python3 - "$first" "$second" "$PLAYER_ID" "$FIRST_EPOCH" <<'PY'
import json
import sys
first = json.loads(sys.argv[1])
second = json.loads(sys.argv[2])
player_id = int(sys.argv[3])
first_epoch = int(sys.argv[4])
assert second["expectationsHold"], second
assert second["playerId"] == player_id, (first, second)
assert second["connectionEpoch"] == first_epoch + 1, (first, second)
assert second["firstSentSequence"] == 21, second
assert second["finalSentSequence"] == 40, second
assert second["finalAppliedSequence"] == 40, second
assert second["welcomeTick"] >= first["lastAcceptedTick"], (first, second)
PY

stop_server
[[ -f "$RECOVERY_PATH" ]] || {
  echo "second graceful shutdown did not recreate recovery image" >&2
  exit 1
}

python3 - "$first" "$second" <<'PY'
import json
import sys
print(json.dumps({
    "mode": "game-server-graceful-restart-recovery",
    "beforeRestart": json.loads(sys.argv[1]),
    "afterRestart": json.loads(sys.argv[2]),
    "recoveryImageRecreated": True,
    "expectationsHold": True,
}, separators=(",", ":")))
PY

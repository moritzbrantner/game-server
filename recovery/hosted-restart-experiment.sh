#!/usr/bin/env bash
set -Eeuo pipefail

PHASE=setup
report_error() {
  status=$?
  echo "hosted-restart failure: phase=$PHASE line=$LINENO status=$status command=$BASH_COMMAND" >&2
}
trap report_error ERR

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
RECOVERY_DIR="$TMP_DIR/host.recovery"
SERVER_LOG="$TMP_DIR/server.log"
CORRUPT_LOG="$TMP_DIR/corrupt.log"
PORT=4485
STATUS_PORT=4486
SERVER_PID=""

cleanup() {
  status=$?
  set +e
  if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill -TERM "$SERVER_PID" 2>/dev/null
    wait "$SERVER_PID" 2>/dev/null
  fi
  if [[ $status -ne 0 ]]; then
    [[ -f "$SERVER_LOG" ]] && {
      echo "--- hosted recovery server log ---" >&2
      cat "$SERVER_LOG" >&2
    }
    [[ -s "$CORRUPT_LOG" ]] && {
      echo "--- corrupt hosted recovery startup log ---" >&2
      cat "$CORRUPT_LOG" >&2
    }
  fi
  rm -rf "$TMP_DIR"
  exit "$status"
}
trap cleanup EXIT

openssl ecparam -name prime256v1 -genkey -noout -out "$KEY_PEM"
openssl req -new -x509 -sha256 -key "$KEY_PEM" -out "$CERT_PEM" -days 1 \
  -subj "/CN=game-server-host-recovery" \
  -addext "subjectAltName=IP:127.0.0.1,DNS:localhost" >/dev/null 2>&1
CERT_HASH=$(openssl x509 -in "$CERT_PEM" -noout -fingerprint -sha256 | cut -d= -f2 | tr 'A-F' 'a-f')

server_env() {
  env \
    GAME_SERVER_PORT="$PORT" \
    GAME_SERVER_STATUS_PORT="$STATUS_PORT" \
    GAME_SERVER_CERT_PEM="$CERT_PEM" \
    GAME_SERVER_KEY_PEM="$KEY_PEM" \
    GAME_SERVER_SESSION_PATH=/game \
    GAME_SERVER_MATCH_IDS=alpha,beta \
    GAME_SERVER_RECOVERY_DIR="$RECOVERY_DIR" \
    GAME_SERVER_DRAIN_GRACE_MS=50 \
    "$@"
}

start_server() {
  : >"$SERVER_LOG"
  server_env "$SERVER_BIN" >>"$SERVER_LOG" 2>&1 &
  SERVER_PID=$!
  python3 - "$STATUS_URL" "$SERVER_PID" <<'PY'
import os
import sys
import time
import urllib.error
import urllib.request

base = sys.argv[1]
pid = int(sys.argv[2])
deadline = time.monotonic() + 5.0
last_error = None
while time.monotonic() < deadline:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        raise SystemExit("game-server exited before becoming ready")
    try:
        with urllib.request.urlopen(base + "/readyz", timeout=0.2) as response:
            if response.status == 200:
                sys.exit(0)
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as error:
        last_error = error
    time.sleep(0.05)
raise SystemExit(f"game-server did not become ready: {last_error}")
PY
}

stop_server() {
  kill -TERM "$SERVER_PID"
  wait "$SERVER_PID"
  SERVER_PID=""
}

assert_bundle() {
  [[ -d "$RECOVERY_DIR" ]] || {
    echo "hosted graceful shutdown did not create recovery bundle" >&2
    exit 1
  }
  for file in manifest alpha.recovery beta.recovery; do
    [[ -f "$RECOVERY_DIR/$file" ]] || {
      echo "hosted recovery bundle is missing $file" >&2
      exit 1
    }
  done
  mapfile -t manifest <"$RECOVERY_DIR/manifest"
  [[ "${manifest[*]}" == "GSHR 1 alpha beta" ]] || {
    echo "unexpected hosted recovery manifest: ${manifest[*]}" >&2
    exit 1
  }
}

ALPHA_URL="https://127.0.0.1:$PORT/game/matches/alpha"
BETA_URL="https://127.0.0.1:$PORT/game/matches/beta"
STATUS_URL="http://127.0.0.1:$STATUS_PORT"

PHASE=fresh-start
start_server
PHASE=fresh-alpha
alpha_first=$("$CLIENT_BIN" "$ALPHA_URL" "$CERT_HASH" 20 10 2000 10 1)
PHASE=fresh-beta
beta_first=$("$CLIENT_BIN" "$BETA_URL" "$CERT_HASH" 12 10 2000 10 1)

PHASE=parse-fresh-alpha
readarray -t alpha_values < <(python3 - "$alpha_first" <<'PY'
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
PHASE=parse-fresh-beta
readarray -t beta_values < <(python3 - "$beta_first" <<'PY'
import json
import sys
receipt = json.loads(sys.argv[1])
assert receipt["expectationsHold"], receipt
assert receipt["connectionEpoch"] == 1, receipt
assert receipt["firstSentSequence"] == 1, receipt
assert receipt["finalSentSequence"] == 12, receipt
assert receipt["finalAppliedSequence"] == 12, receipt
print(receipt["playerId"])
print(receipt["connectionEpoch"])
print(receipt["reconnectToken"])
PY
)

PHASE=first-shutdown
stop_server
PHASE=first-bundle
assert_bundle

PHASE=recovered-start
start_server
PHASE=consume-check
[[ ! -e "$RECOVERY_DIR" ]] || {
  echo "startup did not consume the hosted recovery bundle" >&2
  exit 1
}

PHASE=reconnect-alpha
alpha_second=$(
  "$CLIENT_BIN" "$ALPHA_URL/reconnect/${alpha_values[2]}" "$CERT_HASH" 20 10 2000 10 21
)
PHASE=reconnect-beta
beta_second=$(
  "$CLIENT_BIN" "$BETA_URL/reconnect/${beta_values[2]}" "$CERT_HASH" 12 10 2000 10 13
)

PHASE=verify-reconnect
python3 - "$alpha_first" "$alpha_second" "${alpha_values[0]}" "${alpha_values[1]}" \
  "$beta_first" "$beta_second" "${beta_values[0]}" "${beta_values[1]}" <<'PY'
import json
import sys
alpha_first = json.loads(sys.argv[1])
alpha_second = json.loads(sys.argv[2])
alpha_player = int(sys.argv[3])
alpha_epoch = int(sys.argv[4])
beta_first = json.loads(sys.argv[5])
beta_second = json.loads(sys.argv[6])
beta_player = int(sys.argv[7])
beta_epoch = int(sys.argv[8])

assert alpha_second["expectationsHold"], alpha_second
assert alpha_second["playerId"] == alpha_player, (alpha_first, alpha_second)
assert alpha_second["connectionEpoch"] == alpha_epoch + 1, (alpha_first, alpha_second)
assert alpha_second["firstSentSequence"] == 21, alpha_second
assert alpha_second["finalSentSequence"] == 40, alpha_second
assert alpha_second["finalAppliedSequence"] == 40, alpha_second
assert alpha_second["welcomeTick"] >= alpha_first["lastAcceptedTick"], (alpha_first, alpha_second)

assert beta_second["expectationsHold"], beta_second
assert beta_second["playerId"] == beta_player, (beta_first, beta_second)
assert beta_second["connectionEpoch"] == beta_epoch + 1, (beta_first, beta_second)
assert beta_second["firstSentSequence"] == 13, beta_second
assert beta_second["finalSentSequence"] == 24, beta_second
assert beta_second["finalAppliedSequence"] == 24, beta_second
assert beta_second["welcomeTick"] >= beta_first["lastAcceptedTick"], (beta_first, beta_second)
PY

PHASE=second-shutdown
stop_server
PHASE=second-bundle
assert_bundle

PHASE=corrupt-beta
printf 'bad' >"$RECOVERY_DIR/beta.recovery"
trap - ERR
set +e
server_env "$SERVER_BIN" >"$CORRUPT_LOG" 2>&1
corrupt_status=$?
set -e
trap report_error ERR
[[ $corrupt_status -ne 0 ]] || {
  echo "server accepted a corrupted per-match hosted recovery image" >&2
  exit 1
}
PHASE=corrupt-bundle
assert_bundle

PHASE=complete
python3 - "$alpha_first" "$alpha_second" "$beta_first" "$beta_second" <<'PY'
import json
import sys
print(json.dumps({
    "mode": "game-server-hosted-graceful-restart-recovery",
    "alphaBeforeRestart": json.loads(sys.argv[1]),
    "alphaAfterRestart": json.loads(sys.argv[2]),
    "betaBeforeRestart": json.loads(sys.argv[3]),
    "betaAfterRestart": json.loads(sys.argv[4]),
    "recoveryBundleRecreated": True,
    "corruptMatchFailsClosed": True,
    "expectationsHold": True,
}, separators=(",", ":")))
PY

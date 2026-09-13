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
NETEM_CLIENT_BIN="$ROOT_DIR/target/debug/netem-client"
CONTROL_CLIENT_BIN="$ROOT_DIR/target/debug/control-client"
for binary in "$SERVER_BIN" "$NETEM_CLIENT_BIN" "$CONTROL_CLIENT_BIN"; do
  [[ -x "$binary" ]] || {
    echo "missing binary: $binary" >&2
    exit 2
  }
done

TMP_DIR=$(mktemp -d)
CERT_PEM="$TMP_DIR/cert.pem"
KEY_PEM="$TMP_DIR/key.pem"
SERVER_LOG="$TMP_DIR/server.log"
PORT=4477
STATUS_PORT=4478
SERVER_PID=""

cleanup() {
  status=$?
  set +e
  if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill -TERM "$SERVER_PID" 2>/dev/null
    wait "$SERVER_PID" 2>/dev/null
  fi
  if [[ $status -ne 0 ]] && [[ -f "$SERVER_LOG" ]]; then
    echo "--- game-server match-host dispatch experiment log ---" >&2
    cat "$SERVER_LOG" >&2
  fi
  rm -rf "$TMP_DIR"
  exit "$status"
}
trap cleanup EXIT

openssl ecparam -name prime256v1 -genkey -noout -out "$KEY_PEM"
openssl req -new -x509 -sha256 -key "$KEY_PEM" -out "$CERT_PEM" -days 1 \
  -subj "/CN=game-server-host" \
  -addext "subjectAltName=IP:127.0.0.1,DNS:localhost" >/dev/null 2>&1
CERT_HASH=$(openssl x509 -in "$CERT_PEM" -noout -fingerprint -sha256 | cut -d= -f2 | tr 'A-F' 'a-f')

env \
  GAME_SERVER_PORT="$PORT" \
  GAME_SERVER_STATUS_PORT="$STATUS_PORT" \
  GAME_SERVER_CERT_PEM="$CERT_PEM" \
  GAME_SERVER_KEY_PEM="$KEY_PEM" \
  GAME_SERVER_SESSION_PATH=/game \
  GAME_SERVER_MATCH_IDS=alpha,beta \
  GAME_SERVER_DRAIN_GRACE_MS=1000 \
  "$SERVER_BIN" >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!
sleep 0.45
if ! kill -0 "$SERVER_PID" 2>/dev/null; then
  wait "$SERVER_PID"
fi

ALPHA_URL="https://127.0.0.1:$PORT/game/matches/alpha"
BETA_URL="https://127.0.0.1:$PORT/game/matches/beta"
MISSING_URL="https://127.0.0.1:$PORT/game/matches/missing"
STATUS_URL="http://127.0.0.1:$STATUS_PORT"

python3 - "$STATUS_URL" <<'PY'
import json
import sys
import urllib.error
import urllib.request

base = sys.argv[1]

def get(path):
    with urllib.request.urlopen(base + path, timeout=2) as response:
        return response.status, json.load(response)

status_code, health = get("/healthz")
assert status_code == 200, (status_code, health)
assert health == {"healthy": True}, health

status_code, ready = get("/readyz")
assert status_code == 200, (status_code, ready)
assert ready == {"ready": True, "draining": False}, ready

status_code, status = get("/status")
assert status_code == 200, (status_code, status)
assert status["version"] == 1, status
assert status["healthy"], status
assert status["ready"], status
assert not status["draining"], status
assert status["capacity"] == {
    "hostedMatches": 2,
    "maxMatches": 2,
    "remainingMatches": 0,
    "playerCapacity": 8,
}, status
assert [match["id"] for match in status["matches"]] == ["alpha", "beta"], status
assert all(match["ready"] for match in status["matches"]), status

status_code, alpha = get("/matches/alpha/status")
assert status_code == 200, (status_code, alpha)
assert alpha["id"] == "alpha", alpha
assert alpha["healthy"] and alpha["ready"], alpha
assert not alpha["draining"] and not alpha["frozen"], alpha

try:
    get("/matches/missing/status")
except urllib.error.HTTPError as error:
    assert error.code == 404, error.code
else:
    raise AssertionError("unknown match status unexpectedly succeeded")
PY

alpha_receipt=$("$NETEM_CLIENT_BIN" "$ALPHA_URL" "$CERT_HASH" 5 5 2000 4 10)
beta_receipt=$("$NETEM_CLIENT_BIN" "$BETA_URL" "$CERT_HASH" 3 5 2000 4 1)
control_receipt=$("$CONTROL_CLIENT_BIN" "$ALPHA_URL" "$CERT_HASH" alpha)

python3 - "$alpha_receipt" "$beta_receipt" "$control_receipt" <<'PY'
import json
import sys
alpha = json.loads(sys.argv[1])
beta = json.loads(sys.argv[2])
control = json.loads(sys.argv[3])
assert alpha["expectationsHold"], alpha
assert beta["expectationsHold"], beta
assert alpha["playerId"] == 1, alpha
assert beta["playerId"] == 1, beta
assert alpha["finalAppliedSequence"] == 14, alpha
assert beta["finalAppliedSequence"] == 3, beta
assert control["expectationsHold"], control
assert control["matchScopeRoundTrip"], control
PY

if timeout 10 "$NETEM_CLIENT_BIN" "$MISSING_URL" "$CERT_HASH" 1 0 250 0 1 \
  >"$TMP_DIR/missing.out" 2>"$TMP_DIR/missing.err"; then
  echo "unknown hosted match unexpectedly accepted a WebTransport session" >&2
  exit 1
fi

kill -TERM "$SERVER_PID"

python3 - "$STATUS_URL" <<'PY'
import json
import sys
import time
import urllib.error
import urllib.request

base = sys.argv[1]
deadline = time.monotonic() + 0.75
ready = None
while time.monotonic() < deadline:
    try:
        urllib.request.urlopen(base + "/readyz", timeout=0.2)
    except urllib.error.HTTPError as error:
        if error.code == 503:
            ready = json.load(error)
            break
    except urllib.error.URLError:
        pass
    time.sleep(0.02)
assert ready == {"ready": False, "draining": True}, ready

with urllib.request.urlopen(base + "/healthz", timeout=0.2) as response:
    assert response.status == 200, response.status
    assert json.load(response) == {"healthy": True}

try:
    urllib.request.urlopen(base + "/matches/alpha/readyz", timeout=0.2)
except urllib.error.HTTPError as error:
    assert error.code == 503, error.code
    alpha = json.load(error)
    assert alpha["id"] == "alpha", alpha
    assert not alpha["ready"], alpha
    assert alpha["draining"], alpha
else:
    raise AssertionError("draining match unexpectedly remained ready")
PY

wait "$SERVER_PID"
SERVER_PID=""

python3 - "$alpha_receipt" "$beta_receipt" "$control_receipt" <<'PY'
import json
import sys
print(json.dumps({
    "mode": "game-server-match-host-dispatch",
    "alpha": json.loads(sys.argv[1]),
    "beta": json.loads(sys.argv[2]),
    "control": json.loads(sys.argv[3]),
    "statusContractVersion": 1,
    "statusReadyBeforeDrain": True,
    "statusNotReadyDuringDrain": True,
    "unknownMatchRejected": True,
    "expectationsHold": True,
}, separators=(",", ":")))
PY

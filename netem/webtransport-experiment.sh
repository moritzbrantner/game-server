#!/usr/bin/env bash
set -euo pipefail

if [[ ${EUID} -ne 0 ]]; then
  echo "WebTransport netem experiment must run as root" >&2
  exit 2
fi

for command in ip tc openssl python3; do
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
    echo "missing binary: $binary (build game-server and netem-client first)" >&2
    exit 2
  }
done

RUN_SUFFIX="$$"
CLIENT_NS="gs-netem-client-$RUN_SUFFIX"
SERVER_NS="gs-netem-server-$RUN_SUFFIX"
CLIENT_VETH="gsc${RUN_SUFFIX: -5}"
SERVER_VETH="gss${RUN_SUFFIX: -5}"
TMP_DIR=$(mktemp -d)
SERVER_LOG="$TMP_DIR/server.log"
OUTAGE_RECEIPT="$TMP_DIR/outage.json"
CERT_PEM="$TMP_DIR/cert.pem"
KEY_PEM="$TMP_DIR/key.pem"
SERVER_PID=""
OUTAGE_PID=""

cleanup() {
  set +e
  [[ -n "$OUTAGE_PID" ]] && kill "$OUTAGE_PID" 2>/dev/null
  [[ -n "$SERVER_PID" ]] && kill "$SERVER_PID" 2>/dev/null
  for namespace in "$CLIENT_NS" "$SERVER_NS"; do
    if ip netns list | grep -q "^${namespace}\\b"; then
      ip netns pids "$namespace" | xargs -r kill
      ip netns del "$namespace"
    fi
  done
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

openssl ecparam -name prime256v1 -genkey -noout -out "$KEY_PEM"
openssl req -new -x509 -sha256 -key "$KEY_PEM" -out "$CERT_PEM" -days 1 \
  -subj "/CN=game-server-netem" \
  -addext "subjectAltName=IP:10.204.0.2,DNS:localhost" >/dev/null 2>&1
CERT_HASH=$(openssl x509 -in "$CERT_PEM" -noout -fingerprint -sha256 | cut -d= -f2 | tr 'A-F' 'a-f')

ip netns add "$CLIENT_NS"
ip netns add "$SERVER_NS"
ip link add "$CLIENT_VETH" type veth peer name "$SERVER_VETH"
ip link set "$CLIENT_VETH" netns "$CLIENT_NS"
ip link set "$SERVER_VETH" netns "$SERVER_NS"
ip -n "$CLIENT_NS" link set lo up
ip -n "$SERVER_NS" link set lo up
ip -n "$CLIENT_NS" link set "$CLIENT_VETH" name eth0
ip -n "$SERVER_NS" link set "$SERVER_VETH" name eth0
ip -n "$CLIENT_NS" addr add 10.204.0.1/24 dev eth0
ip -n "$SERVER_NS" addr add 10.204.0.2/24 dev eth0
ip -n "$CLIENT_NS" link set eth0 up
ip -n "$SERVER_NS" link set eth0 up

ip netns exec "$SERVER_NS" env \
  GAME_SERVER_PORT=4433 \
  GAME_SERVER_CERT_PEM="$CERT_PEM" \
  GAME_SERVER_KEY_PEM="$KEY_PEM" \
  GAME_SERVER_SESSION_PATH=/game \
  "$SERVER_BIN" >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!
sleep 0.4

URL="https://10.204.0.2:4433/game"
baseline=$(ip netns exec "$CLIENT_NS" "$CLIENT_BIN" "$URL" "$CERT_HASH" 60 10 1500 10)

ip netns exec "$CLIENT_NS" tc qdisc replace dev eth0 root netem \
  delay 25ms 5ms 20% loss random 5% reorder 20% 25% rate 10mbit
ip netns exec "$SERVER_NS" tc qdisc replace dev eth0 root netem \
  delay 25ms 5ms 20% loss random 5% reorder 20% 25% rate 10mbit
impaired=$(ip netns exec "$CLIENT_NS" "$CLIENT_BIN" "$URL" "$CERT_HASH" 120 12 2500 30)

ip netns exec "$CLIENT_NS" tc qdisc replace dev eth0 root netem delay 20ms loss random 2%
ip netns exec "$SERVER_NS" tc qdisc replace dev eth0 root netem delay 20ms loss random 2%
ip netns exec "$CLIENT_NS" "$CLIENT_BIN" "$URL" "$CERT_HASH" 200 10 3000 40 >"$OUTAGE_RECEIPT" &
OUTAGE_PID=$!
sleep 0.55
ip netns exec "$CLIENT_NS" tc qdisc replace dev eth0 root netem loss 100%
ip netns exec "$SERVER_NS" tc qdisc replace dev eth0 root netem loss 100%
sleep 0.45
ip netns exec "$CLIENT_NS" tc qdisc replace dev eth0 root netem delay 20ms loss random 2%
ip netns exec "$SERVER_NS" tc qdisc replace dev eth0 root netem delay 20ms loss random 2%
wait "$OUTAGE_PID"
OUTAGE_PID=""
outage=$(cat "$OUTAGE_RECEIPT")

client_qdisc=$(ip netns exec "$CLIENT_NS" tc -s qdisc show dev eth0 | tr '\n' ' ')
server_qdisc=$(ip netns exec "$SERVER_NS" tc -s qdisc show dev eth0 | tr '\n' ' ')

python3 - "$baseline" "$impaired" "$outage" "$client_qdisc" "$server_qdisc" <<'PY'
import json
import sys
baseline = json.loads(sys.argv[1])
impaired = json.loads(sys.argv[2])
outage = json.loads(sys.argv[3])
assert baseline["expectationsHold"], baseline
assert impaired["expectationsHold"], impaired
assert outage["expectationsHold"], outage
assert impaired["finalAppliedSequence"] == impaired["finalSentSequence"], impaired
assert outage["finalAppliedSequence"] == outage["finalSentSequence"], outage
print(json.dumps({
    "mode": "game-server-webtransport-netem",
    "baseline": baseline,
    "steadyImpairment": impaired,
    "transientOutage": outage,
    "clientQdiscEvidence": sys.argv[4],
    "serverQdiscEvidence": sys.argv[5],
    "expectationsHold": True,
}, separators=(",", ":")))
PY

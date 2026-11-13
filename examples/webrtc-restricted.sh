#!/usr/bin/env bash
set -euo pipefail

# Disposable peer only: this never changes the host or the VPS firewall.
# Usage: ./examples/webrtc-restricted.sh <public-lobby-id> <turn-ip> [relay] [loss-percent]
root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
lobby=${1:?public lobby id required}
turn_ip=${2:?deployed TURN IPv4 address required}
mode=${3:-}
loss=${4:-0}
if [[ ! "$turn_ip" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  printf '%s\n' 'TURN address must be IPv4' >&2
  exit 2
fi
if [[ ! "$loss" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
  printf '%s\n' 'Loss must be a numeric percentage' >&2
  exit 2
fi
docker build -q -t game-server-webrtc-network-proof - < "$root/examples/webrtc-restricted.Dockerfile"
docker run --rm --cap-add NET_ADMIN \
  -v "$root/target/debug/examples/webrtc-proof:/proof:ro" \
  game-server-webrtc-network-proof sh -eu -c '
    printf "%s\n" "table inet proof {" "chain output {" \
      "type filter hook output priority 0; policy drop;" \
      "oifname lo accept" "udp dport 53 accept" "tcp dport {53,443} accept" \
      "ip daddr $2 udp dport 3478 counter accept" "counter drop" "}" "}" | nft -f -
    if [ "$4" != 0 ]; then
      tc qdisc add dev eth0 root handle 1: prio
      tc qdisc add dev eth0 parent 1:3 handle 30: netem loss "$4%"
      tc filter add dev eth0 protocol ip parent 1: prio 1 u32 \
        match ip dst "$2/32" match ip protocol 17 0xff \
        match ip dport 3478 0xffff flowid 1:3
    fi
    /proof client "$(hostname -i)" "$1" "$3"
    nft list table inet proof
    tc -s qdisc show dev eth0
  ' proof "$lobby" "$turn_ip" "$mode" "$loss"

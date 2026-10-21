#!/usr/bin/env bash
# Task 10: manual/scripted election check.
#
# Builds the node binary, launches a real 3-process cluster on localhost,
# lets it run, and verifies from each node's own log output that:
#   1. exactly one node reports state=Leader and the other two state=Follower,
#   2. all three agree on the same term, and
#   3. that outcome is STABLE: the leader/term seen a few seconds later is
#      identical to the first sample (no constant re-elections).
# Exits 0 on success, 1 on failure; node logs are kept for inspection.
#
# Usage (from the repo root):   bash test/election_check.sh

set -u

SETTLE_SECONDS=3     # time allowed for the first election to finish
OBSERVE_SECONDS=4    # extra time over which stability is checked
BASE_PORT=9101       # different from the manual-run ports (9001-9003)

WORKDIR="$(mktemp -d)"
BIN="$WORKDIR/raft-kv-node"
PIDS=()

cleanup() {
  for pid in "${PIDS[@]:-}"; do kill "$pid" 2>/dev/null; done
  wait 2>/dev/null
}
trap cleanup EXIT

echo "Building node binary..."
go build -o "$BIN" ./cmd || { echo "FAIL: build failed"; exit 1; }

# Cluster definition: ids 1..3 on consecutive ports.
addr() { echo "127.0.0.1:$((BASE_PORT + $1))"; }
peers_for() {
  local self=$1 out=""
  for id in 1 2 3; do
    [ "$id" -eq "$self" ] && continue
    out="${out:+$out,}$id=$(addr "$id")"
  done
  echo "$out"
}

for id in 1 2 3; do
  "$BIN" -id="$id" -addr="$(addr "$id")" -peers="$(peers_for "$id")" \
    > "$WORKDIR/node$id.log" 2>&1 &
  PIDS+=($!)
done
echo "Launched 3 nodes (logs in $WORKDIR)"

# last_state <id> -> prints "<state> <term>" from that node's latest status line.
last_state() {
  grep -o 'state=[A-Za-z]* term=[0-9]*' "$WORKDIR/node$1.log" | tail -n 1 \
    | sed -E 's/state=([A-Za-z]*) term=([0-9]*)/\1 \2/'
}

# snapshot -> prints "<leaders> <followers> <terms-distinct> <leader-id> <term>"
snapshot() {
  local leaders=0 followers=0 leader_id=0 terms="" term=0 s st t
  for id in 1 2 3; do
    s="$(last_state "$id")"
    st="${s% *}"; t="${s#* }"
    case "$st" in
      Leader)   leaders=$((leaders + 1)); leader_id=$id; term=$t ;;
      Follower) followers=$((followers + 1)) ;;
    esac
    terms="$terms $t"
  done
  local distinct
  distinct="$(echo "$terms" | tr ' ' '\n' | grep -v '^$' | sort -u | wc -l | tr -d ' ')"
  echo "$leaders $followers $distinct $leader_id $term"
}

sleep "$SETTLE_SECONDS"
sleep 1   # make sure each node has printed at least one status line
FIRST="$(snapshot)"
sleep "$OBSERVE_SECONDS"
SECOND="$(snapshot)"

echo "First sample  (leaders followers distinct-terms leader-id term): $FIRST"
echo "Second sample (leaders followers distinct-terms leader-id term): $SECOND"

read -r L F D LID TERM <<< "$SECOND"
FAILED=0
[ "$L" -eq 1 ] || { echo "FAIL: expected exactly 1 leader, saw $L"; FAILED=1; }
[ "$F" -eq 2 ] || { echo "FAIL: expected exactly 2 followers, saw $F"; FAILED=1; }
[ "$D" -eq 1 ] || { echo "FAIL: nodes disagree on the term"; FAILED=1; }
[ "$FIRST" = "$SECOND" ] || { echo "FAIL: cluster state changed between samples (not stable)"; FAILED=1; }

if [ "$FAILED" -eq 0 ]; then
  echo "PASS: node $LID is the stable leader for term $TERM; the other two are followers."
  exit 0
fi
echo "Logs kept in $WORKDIR for inspection."
trap - EXIT; cleanup
exit 1
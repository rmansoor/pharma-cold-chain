#!/usr/bin/env bash
# Restart check for SID-14: persisted evidence and checkpoints survive a full stack restart.
# Run after the demo (simulator + verify) has committed at least one snapshot.
# Usage: scripts/restart-check.sh [compose-project-name]
set -euo pipefail
cd "$(dirname "$0")/.."
project="${1:-pharma-cold-chain}"
dc() { docker compose -p "$project" "$@"; }
state() { dc --profile demo run --rm -q verify io.coldchain.Inspect 2>/dev/null | grep '^STATE'; }
field() { sed -E "s/.* $2=([0-9]+).*/\1/" <<<"$1"; }

before=$(state); echo "before:  $before"
[[ $before == "STATE empty" ]] && { echo "FAIL: no committed snapshot yet; run the demo first"; exit 1; }

dc down            # containers removed, named volumes kept
dc up -d pipeline cases
sleep 30                      # well past several 5 s triggers
idle=$(state); echo "restart: $idle"
[[ $idle == "$before" ]] || { echo "FAIL: restart without new input changed persisted state"; exit 1; }

now=$(date -u +%Y-%m-%dT%H:%M:%SZ)
echo "{\"eventId\":\"restart-check-$now\",\"revision\":1,\"kind\":\"INVENTORY\",\"eventTime\":\"$now\",\"productId\":\"UNIT-A\",\"location\":\"WAREHOUSE-1\",\"status\":\"IN_STOCK\"}" \
 | dc exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic coldchain.inventory
for _ in $(seq 1 120); do
 after=$(state); [[ $(field "$after" lastSnapshot) -gt $(field "$before" lastSnapshot) ]] && break; sleep 5
done
echo "after:   $after"

b=$(field "$before" bronze); a=$(field "$after" bronze)
[[ $a -eq $((b+1)) ]] || { echo "FAIL: expected bronze $((b+1)), got $a (replayed or lost offsets)"; exit 1; }
[[ $(field "$after" bronzeKeys) -eq $a ]] || { echo "FAIL: duplicate Bronze source coordinates"; exit 1; }
[[ $(field "$after" lastSnapshot) -eq $(( $(field "$before" lastSnapshot)+1 )) ]] || { echo "FAIL: snapshot IDs did not continue the checkpoint lineage"; exit 1; }
[[ $(field "$after" history) -gt $(field "$before" history) ]] || { echo "FAIL: prior assessment history not retained"; exit 1; }
echo "PASS: state survived restart; checkpoint resumed at the next offset and snapshot; no Bronze duplicates."

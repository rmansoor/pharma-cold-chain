#!/usr/bin/env bash
# Crash check for SID-15: a pipeline killed after the Bronze commit, before Spark marks the batch done,
# replays that batch on restart without duplicating Bronze rows or losing offsets.
# Uses its own disposable Compose project (volumes are deleted); stop other stacks on ports 29092/4041 first.
# Usage: scripts/crash-check.sh [compose-project-name]
set -euo pipefail
cd "$(dirname "$0")/.."
project="${1:-pharma-cold-chain-crash}"
dc() { docker compose -p "$project" "$@"; }
state() { dc --profile demo run --rm -q verify io.coldchain.Inspect 2>/dev/null | grep '^STATE'; }
field() { sed -E "s/.* $2=(-?[0-9]+).*/\1/" <<<"$1"; }
fail() { echo "FAIL: $*"; exit 1; }

dc --profile demo down -v --remove-orphans >/dev/null 2>&1
dc up -d topics >/dev/null
# Publish before the pipeline starts so batch 0 holds every published contract example.
examples=$(ls contracts/examples/*.json | wc -l | tr -d ' ')
for f in contracts/examples/*.json; do
 line=$(tr -d '\n' <"$f")
 case $(sed -E 's/.*"kind": *"([A-Z]+)".*/\1/' <<<"$line") in
  READING) topic=coldchain.sensors;; SPEC) topic=coldchain.specifications;; INVENTORY) topic=coldchain.inventory;; *) topic=coldchain.traceability;;
 esac
 echo "$line" | dc exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic "$topic"
done

# Build so a stale image from an earlier run cannot hide contract changes.
FAIL_AFTER_BRONZE_BATCH=0 dc up -d --build pipeline >/dev/null 2>&1
for _ in $(seq 1 60); do
 status=$(docker inspect -f '{{.State.Status}} {{.State.ExitCode}}' "$(dc ps -aq pipeline)")
 [[ $status == exited* ]] && break; sleep 5
done
[[ $status == exited* ]] || fail "pipeline did not stop at the injected failure"
code=${status#exited }
dc logs pipeline | grep -q "INJECTED FAILURE after Bronze batch=0" || fail "pipeline exited ($code) without the injected failure"
crashed=$(state); echo "crashed: $crashed (exit $code)"
[[ $(field "$crashed" bronze) -eq $examples && $(field "$crashed" commits) -eq 0 ]] || fail "expected $examples Bronze rows and no Gold commit after the crash"

FAIL_AFTER_BRONZE_BATCH= dc up -d pipeline >/dev/null
for _ in $(seq 1 40); do
 resumed=$(state); [[ $(field "$resumed" lastSnapshot) -ge 0 ]] && break; sleep 5
done
echo "resumed: $resumed"
[[ $(field "$resumed" lastSnapshot) -eq 0 ]] || fail "batch 0 was not replayed and committed"
[[ $(field "$resumed" bronze) -eq $examples && $(field "$resumed" bronzeKeys) -eq $examples ]] || fail "Bronze rows lost or duplicated by the replay"
[[ $(field "$resumed" quarantine) -eq 0 ]] || fail "a published contract example was quarantined"
dc --profile demo down -v >/dev/null 2>&1
echo "PASS: crash after Bronze commit replayed batch 0; $examples offsets ingested once; snapshot 0 committed."

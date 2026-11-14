#!/usr/bin/env bash
# Crash check. SID-15: a pipeline killed after the Bronze commit, before Spark marks the batch done, replays that batch on
# restart without duplicating Bronze rows or losing offsets. SID-22: a pipeline killed after writing Gold history but before
# the manifest leaves a snapshot readers cannot see; the restart publishes it exactly once. SID-25: a pipeline killed after
# Kafka accepted a snapshot's review items, before the relay recorded them, resends them on restart; the case store, itself
# restarted meanwhile, records each item once.
# Waits up to 10 minutes for each batch, since batches on a shared CI runner can take minutes.
# Uses its own disposable Compose project (volumes are deleted); stop other stacks on ports 29092/4041 first.
# Usage: scripts/crash-check.sh [compose-project-name]
set -euo pipefail
cd "$(dirname "$0")/.."
project="${1:-pharma-cold-chain-crash}"
dc() { docker compose -p "$project" "$@"; }
state() { dc --profile demo run --rm -q verify io.coldchain.Inspect 2>/dev/null | grep '^STATE'; }
field() { [[ $1 =~ (^| )$2=(-?[0-9]+) ]] || fail "no $2 in: $1"; echo "${BASH_REMATCH[2]}"; }
fail() { echo "FAIL: $*" >&2; exit 1; }

dc --profile demo down -v --remove-orphans >/dev/null 2>&1
dc up -d topics >/dev/null
# Publish before the pipeline starts so batch 0 holds every published contract example.
examples=$(ls contracts/examples/*.json | wc -l | tr -d ' ')
publish() {
 local line topic; line=$(tr -d '\n' <"$1")
 case $(sed -E 's/.*"kind": *"([A-Z]+)".*/\1/' <<<"$line") in
  READING|CALIBRATION) topic=coldchain.sensors;; SPEC) topic=coldchain.specifications;; INVENTORY) topic=coldchain.inventory;; *) topic=coldchain.traceability;;
 esac
 echo "$line" | dc exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic "$topic"
}
for f in contracts/examples/*.json; do publish "$f"; done
stopped() {
 for _ in $(seq 1 120); do
  status=$(docker inspect -f '{{.State.Status}} {{.State.ExitCode}}' "$(dc ps -aq pipeline)")
  [[ $status == exited* ]] && return 0; sleep 5
 done
 return 1
}

# Build every image so a stale image from an earlier run cannot hide contract or state changes.
dc --profile demo build pipeline cases verify >/dev/null 2>&1
FAIL_AT=bronze:0 dc up -d pipeline >/dev/null 2>&1
stopped || fail "pipeline did not stop at the injected failure"
code=${status#exited }
dc logs pipeline | grep -q "INJECTED FAILURE after bronze batch=0" || fail "pipeline exited ($code) without the injected failure"
crashed=$(state); echo "crashed: $crashed (exit $code)"
[[ $(field "$crashed" bronze) -eq $examples && $(field "$crashed" commits) -eq 0 ]] || fail "expected $examples Bronze rows and no Gold commit after the crash"

FAIL_AT= dc up -d pipeline >/dev/null
for _ in $(seq 1 120); do
 resumed=$(state); [[ $(field "$resumed" lastSnapshot) -ge 0 ]] && break; sleep 5
done
echo "resumed: $resumed"
[[ $(field "$resumed" lastSnapshot) -eq 0 ]] || fail "batch 0 was not replayed and committed"
[[ $(field "$resumed" bronze) -eq $examples && $(field "$resumed" bronzeKeys) -eq $examples ]] || fail "Bronze rows lost or duplicated by the replay"
[[ $(field "$resumed" quarantine) -eq 0 ]] || fail "a published contract example was quarantined"
echo "PASS: crash after Bronze commit replayed batch 0; $examples offsets ingested once; snapshot 0 committed."

# Phase 2: crash batch 1 after its Gold history is written, before its manifest.
dc stop pipeline >/dev/null 2>&1
FAIL_AT=history:1 dc up -d pipeline >/dev/null 2>&1
# A correction (revision 2 of the example reading), so the incremental snapshot recomputes the units that sensor measured.
correction=$(mktemp)
sed -E 's/"revision": 1/"revision": 2/; s/"temperatureC": 5.4/"temperatureC": 9.4/' contracts/examples/reading.json >"$correction"
publish "$correction"; rm -f "$correction"
stopped || fail "pipeline did not stop at the injected history failure"
dc logs pipeline | grep -q "INJECTED FAILURE after history batch=1" || fail "pipeline exited without the injected history failure"
partial=$(state); echo "partial: $partial"
[[ $(field "$partial" lastSnapshot) -eq 0 && $(field "$partial" commits) -eq 1 ]] || fail "snapshot 1 was published without its manifest"
[[ $(field "$partial" uncommitted) -gt 0 ]] || fail "expected Gold history rows of the unpublished snapshot 1"
[[ $(field "$partial" visible) -eq $(field "$resumed" visible) ]] || fail "readers saw rows of the unpublished snapshot"
FAIL_AT= dc up -d pipeline >/dev/null
for _ in $(seq 1 120); do
 published=$(state); [[ $(field "$published" lastSnapshot) -ge 1 ]] && break; sleep 5
done
echo "published: $published"
[[ $(field "$published" lastSnapshot) -eq 1 && $(field "$published" commits) -eq 2 ]] || fail "snapshot 1 was not published after the restart"
[[ $(field "$published" uncommitted) -eq 0 ]] || fail "rows without a manifest remain"
# The restart deletes the unpublished rows and writes the same ones again: no duplicate, nothing lost.
[[ $(field "$published" history) -eq $(field "$partial" history) ]] || fail "the retry duplicated or lost Gold history"
[[ $(field "$published" bronze) -eq $((examples+1)) ]] || fail "Bronze rows lost or duplicated"
echo "PASS: crash before the manifest left snapshot 1 invisible; the restart published it once with no duplicate rows."

# Phase 3: the case store consumes everything relayed so far; then batch 2 stops after relaying, before recording it.
delivered() {
 for _ in $(seq 1 120); do
  # Inspect counts relayed items and pending snapshots one after the other while the relay runs, so wait for all three.
  now=$(state); [[ $(field "$now" pending) -eq 0 && $(field "$now" relayed) -eq $(field "$now" outbox) && $(field "$now" deliveries) -eq $(field "$now" outbox) ]] && return 0; sleep 5
 done
 return 1
}
dc up -d cases >/dev/null 2>&1
delivered || fail "review items were not delivered to the case store: $now"
before=$now; echo "delivered: $before"
[[ $(field "$before" outbox) -gt 0 ]] || fail "expected review items from the correction"
dc stop pipeline >/dev/null 2>&1
FAIL_AT=relay:2 dc up -d pipeline >/dev/null 2>&1
correction=$(mktemp)
sed -E 's/"revision": 1/"revision": 3/; s/"temperatureC": 5.4/"temperatureC": 15.4/' contracts/examples/reading.json >"$correction"
publish "$correction"; rm -f "$correction"
stopped || fail "pipeline did not stop at the injected relay failure"
dc logs pipeline | grep -q "INJECTED FAILURE after relay batch=2" || fail "pipeline exited without the injected relay failure"
sent=$(state); echo "sent: $sent"
[[ $(field "$sent" lastSnapshot) -eq 2 && $(field "$sent" pending) -eq 1 ]] || fail "expected snapshot 2 committed and its items not recorded as relayed"
[[ $(field "$sent" outbox) -gt $(field "$before" outbox) ]] || fail "snapshot 2 produced no review items"
# The consumer is killed too, and both restart; the relay sends snapshot 2's items a second time.
dc kill cases >/dev/null 2>&1
dc up -d cases >/dev/null 2>&1
FAIL_AT= dc up -d pipeline >/dev/null 2>&1
delivered || fail "review items were not delivered after the restart: $now"
echo "redelivered: $now"
[[ $(field "$now" notifications) -eq $(field "$now" outbox) && $(field "$now" deliveries) -eq $(field "$now" outbox) ]] || fail "a review item was lost or notified twice"
[[ $(field "$now" relayed) -eq $(field "$now" outbox) ]] || fail "the relay did not record every committed item"
dc --profile demo down -v >/dev/null 2>&1
echo "PASS: crash after relaying snapshot 2 resent its items on restart; the restarted case store recorded each review item once."

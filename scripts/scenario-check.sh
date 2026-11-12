#!/usr/bin/env bash
# Run-scoped Kafka-to-Gold scenario for SID-28, in its own disposable Compose project. Every step waits for a committed
# snapshot acknowledging what was sent (io.coldchain.Scenario), never for a fixed time:
#  1. phase 1 of run A (duplicates, a conflict, a malformed record, a pallet split, a reading at the limit, a gap);
#  2. a full stack restart, after which phase 1 still holds;
#  3. phase 2 (late corrections) with the pipeline halted after its Gold history write, before the manifest: readers keep
#     phase 1 and phase 2 is not acknowledged; the restart publishes it once and it is acknowledged;
#  4. run B in the same lake passes on its own records, and run A's result is unchanged;
#  5. a run that sent nothing is not acknowledged by the retained results (old results cannot satisfy a new run).
# Usage: scripts/scenario-check.sh [compose-project-name]. Stop other stacks on ports 29092 and 4041 first.
set -euo pipefail
cd "$(dirname "$0")/.."
project="${1:-pharma-cold-chain-scenario}"
export ALLOW_SYNTHETIC_SPECS=true
dc() { docker compose -p "$project" "$@"; }
scenario() { dc --profile demo run --rm -q scenario "$@" 2>/dev/null | grep -E "^(SENT|ACKNOWLEDGED|NOT ACKNOWLEDGED|PASS|FAIL)"; }
state() { dc --profile demo run --rm -q verify io.coldchain.Inspect 2>/dev/null | grep '^STATE'; }
field() { [[ $1 =~ (^| )$2=(-?[0-9]+) ]] || fail "no $2 in: $1"; echo "${BASH_REMATCH[2]}"; }
fail() { echo "FAIL: $*" >&2; exit 1; }
# Waits for the pipeline container to exit (an injected halt), at most 10 minutes.
halted() { for _ in $(seq 1 120); do [[ $(docker inspect -f '{{.State.Status}}' "$(dc ps -aq pipeline)") == exited ]] && return 0; sleep 5; done; fail "pipeline did not halt"; }
a="A$(date +%s)" b="B$(date +%s)" c="C$(date +%s)"

dc --profile demo down -v --remove-orphans >/dev/null 2>&1 || true
dc --profile demo build >/dev/null
dc up -d pipeline cases >/dev/null 2>&1

echo "== 1. run $a phase 1"
scenario send 1 "$a"; scenario await 1 "$a" | tee /dev/stderr | grep -q "^PASS" || fail "phase 1"

echo "== 2. full restart"
dc down >/dev/null 2>&1; dc up -d pipeline cases >/dev/null 2>&1
scenario await 1 "$a" 60 | grep -q "^PASS" || fail "phase 1 lost across the restart"; echo "phase 1 still holds after the restart"

echo "== 3. phase 2 with a crash before its manifest"
next=$(( $(field "$(state)" lastSnapshot) + 1 ))
FAIL_AT="history:$next" dc up -d pipeline >/dev/null 2>&1
scenario send 2 "$a"; halted
partial=$(state); echo "halted: $partial"
(( $(field "$partial" uncommitted) > 0 )) || fail "the crash left no unpublished rows to discard"
scenario await 1 "$a" 30 | grep -q "^PASS" || fail "readers lost phase 1 during the partial publication"
out=$(scenario await 2 "$a" 30 || true); grep -q "^NOT ACKNOWLEDGED" <<<"$out" || fail "phase 2 acknowledged before its manifest: $out"
echo "readers keep phase 1; phase 2 not acknowledged before its manifest"
dc up -d pipeline >/dev/null 2>&1
scenario await 2 "$a" | tee /dev/stderr | grep -q "^PASS" || fail "phase 2"
after=$(state)
[[ $(field "$after" uncommitted) -eq 0 && $(field "$after" bronzeKeys) -eq $(field "$after" bronze) ]] || fail "unpublished rows or duplicates remain: $after"

echo "== 4. run $b in the same lake"
scenario send 1 "$b"; scenario await 1 "$b" | tee /dev/stderr | grep -q "^PASS" || fail "run $b phase 1"
scenario await 2 "$a" 60 | grep -q "^PASS" || fail "run $b changed run $a's result"; echo "run $a unchanged"

echo "== 5. a run that sent nothing"
out=$(scenario await 1 "$c" 30 || true); grep -q "^NOT ACKNOWLEDGED" <<<"$out" || fail "retained results satisfied run $c: $out"
echo "run $c not acknowledged: $(sed 's/.*: //' <<<"$out" | cut -c1-120)"
dc --profile demo down -v >/dev/null 2>&1
echo "PASS: both phases acknowledged by committed snapshots across a restart and a partial publication; runs isolated; retained results satisfy no new run."

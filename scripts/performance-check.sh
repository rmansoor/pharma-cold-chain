#!/usr/bin/env bash
# Kafka-to-Gold measurements for SID-28 in a disposable Compose project: sustained throughput and publication-lag
# percentiles under a paced load, the time to publish one correction, recovery after a crash before a manifest, and the same
# load with every record under one key (one Kafka partition). Every wait is for a committed snapshot covering what was sent.
# Usage: scripts/performance-check.sh [project] [containers] [units per container] [readings per second] [seconds]
set -euo pipefail
cd "$(dirname "$0")/.."
project="${1:-pharma-cold-chain-perf}" containers="${2:-40}" units="${3:-5}" rate="${4:-50}" seconds="${5:-240}"
export ALLOW_SYNTHETIC_SPECS=true
dc() { docker compose -p "$project" "$@"; }
scenario() { dc --profile demo run --rm -q scenario "$@" 2>/dev/null | grep -E "^(SENT|ACKNOWLEDGED|NOT ACKNOWLEDGED|PASS|FAIL|REPORT)"; }
state() { dc --profile demo run --rm -q verify io.coldchain.Inspect 2>/dev/null | grep '^STATE'; }
field() { [[ $1 =~ (^| )$2=(-?[0-9]+) ]] || fail "no $2 in: $1"; echo "${BASH_REMATCH[2]}"; }
num() { sed -E "s/.* $1 ([0-9]+).*/\1/" <<<"$2"; }
fail() { echo "FAIL: $*" >&2; exit 1; }
ms() { python3 -c 'import time;print(int(time.time()*1000))'; }
halted() { for _ in $(seq 1 120); do [[ $(docker inspect -f '{{.State.Status}}' "$(dc ps -aq pipeline)") == exited ]] && return 0; sleep 5; done; fail "pipeline did not halt"; }
loadrun() { # run skewed? -> prints the load's report
 local run=$1 mode=${2:-} t0 sent n
 t0=$(ms); sent=$(scenario load "$run" "$containers" "$units" "$rate" "$seconds" $mode); echo "$sent" >&2
 n=$(sed -E 's/.*: ([0-9]+) records.*/\1/' <<<"$sent")
 # The wait ends early, as a failure, if the pipeline exits while the load is outstanding.
 scenario await-load "$run" "$n" >"$tmp/ack" & local w=$!
 while kill -0 $w 2>/dev/null; do
  [[ $(docker inspect -f '{{.State.Status}}' "$(dc ps -aq pipeline)") == exited ]] && { kill $w; fail "pipeline exited during load $run: $(docker inspect -f 'code={{.State.ExitCode}} oomKilled={{.State.OOMKilled}}' "$(dc ps -aq pipeline)")"; }
  sleep 10
 done
 wait $w || true; cat "$tmp/ack" >&2; grep -q "^ACKNOWLEDGED" "$tmp/ack" || fail "load $run not published"
 echo "$(scenario report "$t0") published=$(( ($(ms)-t0)/1000 ))s"
}
tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT
dc --profile demo down -v --remove-orphans >/dev/null 2>&1 || true
dc --profile demo build >/dev/null
dc up -d pipeline cases >/dev/null 2>&1
u="U$(date +%s)"

echo "== uniform load: $containers containers x $units units, $rate readings/s for $seconds s"
uniform=$(loadrun "$u"); echo "$uniform"

echo "== correction"
t=$(ms); scenario correct "$u" >/dev/null
ack=$(scenario await-correction "$u"); echo "$ack"
correction=$(( $(num at "$ack") - t ))

echo "== recovery after a crash before the manifest"
next=$(( $(field "$(state)" lastSnapshot) + 1 )) r="R$(date +%s)"
FAIL_AT="history:$next" dc up -d pipeline >/dev/null 2>&1
scenario send 1 "$r" >/dev/null; halted
t=$(ms); dc up -d pipeline >/dev/null 2>&1
scenario await 1 "$r" | grep -q "^PASS" || fail "recovery"
recovery=$(( $(ms) - t ))

echo "== skewed load: every record under one key"
skewed=$(loadrun "S$(date +%s)" skewed); echo "$skewed"
dc --profile demo down -v >/dev/null 2>&1
echo "RESULT uniform: $uniform"
echo "RESULT skewed:  $skewed"
echo "RESULT correctionMs=$correction recoveryMs=$recovery (restart to the acknowledged snapshot)"

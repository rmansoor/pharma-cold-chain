#!/usr/bin/env bash
# Operations drill for SID-26, in its own disposable Compose project with the ops profile (monitor, Prometheus, Grafana):
#  1. baseline: the demo publishes normally; record the measured figures; no alert is active;
#  2. stalled publication: the pipeline hangs inside a batch (STALL_AT) while its process keeps running; the
#     ColdChainPublicationStalled alert must fire, and triage must locate the hang from outside the process;
#  3. recovery: restarting the pipeline without the stall publishes the batch once and the alert resolves;
#  4. checkpoint migration: a new namespace (its own checkpoint, Bronze, Silver and Gold) rebuilt from Kafka must publish the
#     same state digests as the old one, with its review items held on a separate topic, and pass Replay;
#  5. storage: per-area sizes and growth from the monitor.
# Waits up to 10 minutes per batch and 12 minutes for the alert. Stop other stacks on ports 29092, 4041, 9400, 9490 and 3400 first.
# Usage: scripts/ops-drill.sh [compose-project-name]; KEEP=1 leaves the stack running afterwards.
set -euo pipefail
cd "$(dirname "$0")/.."
project="${1:-pharma-cold-chain-ops}"
export ALLOW_SYNTHETIC_SPECS=true
dc() { docker compose -p "$project" --profile ops "$@"; }
inspect() { dc --profile demo run --rm -q -e DATA_DIR="${2:-/data}" verify io.coldchain.Inspect 2>/dev/null | grep "^$1"; }
field() { [[ $1 =~ (^| )$2=(-?[0-9]+) ]] || fail "no $2 in: $1"; echo "${BASH_REMATCH[2]}"; }
fail() { echo "FAIL: $*" >&2; exit 1; }
metric() { curl -sf localhost:9400/metrics | awk -v n="$1" '$1==n{print $2}'; }
# State of one alert in Prometheus: inactive, pending or firing.
alert() { curl -sf localhost:9490/api/v1/alerts | python3 -c "import sys,json;s=[a['state'] for a in json.load(sys.stdin)['data']['alerts'] if a['labels']['alertname']=='$1'];print(s[0] if s else 'inactive')"; }
committed() { # waits until the lastSnapshot of /data is at least $1
 local s
 for _ in $(seq 1 120); do s=$(inspect STATE); [[ $s != "STATE empty" ]] && (( $(field "$s" lastSnapshot) >= $1 )) && return 0; sleep 5; done
 fail "snapshot $1 not committed"
}
figures() { for m in coldchain_publication_lag_seconds coldchain_batch_seconds coldchain_backlog_records_total coldchain_oldest_unpublished_age_seconds coldchain_commit_age_seconds coldchain_data_bytes coldchain_quarantine_open coldchain_conflict_cases_open coldchain_missing_coverage_products coldchain_evidence_revisions; do printf '%s=%s ' "${m#coldchain_}" "$(metric $m)"; done; echo; }

dc --profile demo down -v --remove-orphans >/dev/null 2>&1 || true
dc --profile demo build >/dev/null
dc up -d pipeline cases monitor prometheus grafana >/dev/null 2>&1

echo "== 1. baseline"
dc --profile demo run --rm simulator >/dev/null 2>&1
dc --profile demo run --rm verify 2>&1 | grep -E "^(PASS|FAIL)" | grep -q ^PASS \
 || fail "demo verifier (pipeline $(docker inspect -f '{{.State.Status}} exit={{.State.ExitCode}} oomKilled={{.State.OOMKilled}}' "$(dc ps -aq pipeline)"))"
sleep 30   # two scrapes after the last commit
echo "baseline: $(figures)"
dc logs --no-color pipeline | grep -o 'COMMITTED snapshot=.*' | sed -E 's/(snapshot=[0-9]+).*(canonicalMs=.*)/\1 \2/'
[[ $(alert ColdChainPublicationStalled) == inactive ]] || fail "stall alert active at baseline"
[[ $(metric coldchain_oldest_unpublished_age_seconds) == 0 ]] || fail "records unpublished at baseline"

echo "== 2. stalled publication"
before=$(inspect STATE); next=$(( $(field "$before" lastSnapshot) + 1 ))
STALL_AT="silver:$next" dc up -d pipeline >/dev/null 2>&1
now=$(date -u +%Y-%m-%dT%H:%M:%SZ)
echo "{\"eventId\":\"ops-drill-$now\",\"revision\":1,\"kind\":\"INVENTORY\",\"eventTime\":\"$now\",\"productId\":\"UNIT-A\",\"location\":\"WAREHOUSE-1\",\"status\":\"IN_STOCK\"}" \
 | dc exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --producer.config /etc/coldchain/kafka/client.properties --topic coldchain.inventory
sent=$(date +%s)
for _ in $(seq 1 120); do dc logs --no-color pipeline | grep -q "INJECTED STALL after silver batch=$next" && break; sleep 5; done
dc logs --no-color pipeline | grep -q "INJECTED STALL after silver batch=$next" || fail "pipeline did not reach the stall"
for _ in $(seq 1 144); do state=$(alert ColdChainPublicationStalled); [[ $state == firing ]] && break; sleep 5; done
[[ $state == firing ]] || fail "ColdChainPublicationStalled did not fire"
echo "alert firing $(( $(date +%s) - sent )) s after the record was sent: $(figures)"
[[ $(docker inspect -f '{{.State.Status}}' "$(dc ps -q pipeline)") == running ]] || fail "the pipeline process should still be running"
curl -sf localhost:4041/api/v1/applications >/dev/null || fail "the Spark UI should still answer"
echo "pipeline container running, Spark UI answering, lastSnapshot=$(field "$(inspect STATE)" lastSnapshot)"
# Triage from outside the process: the driver's thread dump in the Spark UI shows where the batch waits.
app=$(curl -sf localhost:4041/api/v1/applications | python3 -c 'import sys,json;print(json.load(sys.stdin)[0]["id"])')
curl -sf "localhost:4041/api/v1/applications/$app/executors/driver/threads" | python3 -c "
import sys,json
waiting=[x['threadName'] for x in json.load(sys.stdin) if 'Pipeline.inject' in json.dumps(x.get('stackTrace'))]
print('thread dump: '+(', '.join(waiting)+' waiting in Pipeline.inject' if waiting else 'no thread in Pipeline.inject'))"

echo "== 3. recovery"
dc up -d pipeline >/dev/null 2>&1   # STALL_AT unset: the container is recreated and the batch retried
committed "$next"
for _ in $(seq 1 24); do [[ $(alert ColdChainPublicationStalled) == inactive ]] && break; sleep 5; done
[[ $(alert ColdChainPublicationStalled) == inactive ]] || fail "alert did not resolve"
after=$(inspect STATE)
[[ $(field "$after" bronze) -eq $(( $(field "$before" bronze) + 1 )) && $(field "$after" bronzeKeys) -eq $(field "$after" bronze) ]] || fail "Bronze lost or duplicated: $before / $after"
echo "recovered $(( $(date +%s) - sent )) s after the record was sent; alert resolved: $(figures)"

echo "== 4. checkpoint migration"
dc exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --command-config /etc/coldchain/kafka/client.properties --create --if-not-exists --topic coldchain.review-items.migration --partitions 4 --replication-factor 1 >/dev/null
old=$(inspect DIGESTS)
dc stop pipeline >/dev/null 2>&1
PIPELINE_DATA_DIR=/data/migrated REVIEW_TOPIC=coldchain.review-items.migration dc up -d pipeline monitor >/dev/null 2>&1
migrated=""
for _ in $(seq 1 120); do migrated=$(inspect DIGESTS /data/migrated || true); [[ $migrated == "$old" ]] && break; sleep 5; done
echo "old:      $old"; echo "migrated: $migrated"
[[ $migrated == "$old" ]] || fail "the migrated namespace does not publish the same state"
dc --profile demo run --rm verify io.coldchain.Replay /data/migrated /tmp/replay 2>&1 | grep -E "^(MATCH|MISMATCH)" || fail "Replay of the migrated namespace"
[[ $(inspect STATE) == "$after" ]] || fail "the old namespace changed during the migration"
echo "migrated namespace: $(inspect STATE /data/migrated)"

echo "== 5. storage"
sleep 30
curl -sf localhost:9400/metrics | grep -E '^coldchain_(storage_bytes|data_bytes|volume_free_bytes)' | sort -t' ' -k2 -nr | head -8
[[ ${KEEP:-} == 1 ]] || dc --profile demo down -v >/dev/null 2>&1
echo "PASS: stall alerted while the process ran, recovered once and resolved; migrated namespace equals the old one and replays to MATCH."

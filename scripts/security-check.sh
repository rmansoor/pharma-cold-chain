#!/usr/bin/env bash
# Security check for SID-27 on a running stack (after the demo): every unauthorized access below must be refused, and each
# positive control must succeed, so a refusal is not just a broken client.
#  Kafka: no TLS/SASL, a wrong password, and each principal outside its ACLs; TLS certificate of the broker.
#  Containers: non-root users, read-only lake for readers, only the service's own credentials, no secrets in the environment.
#  Application: a scope, a replay, a retention plan and an approval outside the caller's roles, all audited.
# Usage: scripts/security-check.sh [compose-project-name]
set -euo pipefail
cd "$(dirname "$0")/.."
project="${1:-pharma-cold-chain}"
dc() { docker compose -p "$project" "$@"; }
fail() { echo "FAIL: $*" >&2; exit 1; }
ok() { echo "ok: $*"; }
# A Kafka CLI as one principal, with only that principal's credentials mounted (as the services get them).
as() { local p=$1; shift
 docker run --rm --network "${project}_default" --user "$([[ $p == admin ]] && echo 1000 || echo 10001)" \
  --mount "type=volume,src=${project}_kafka-secrets,dst=/etc/coldchain/kafka,readonly,volume-subpath=$p" \
  -e KAFKA_LOG4J_OPTS=-Dlog4j.configuration=file:/opt/kafka/config/tools-log4j.properties apache/kafka:3.9.1 "$@" 2>&1; }
K=/opt/kafka/bin C=/etc/coldchain/kafka/client.properties B=kafka:9092
expect_denied() { local what=$1 out; shift; out=$("$@" || true)
 grep -qiE "not authorized|AuthorizationException|Authentication failed|SaslAuthenticationException|SSL handshake|disconnected|timed out|TimeoutException" <<<"$out" \
  || fail "$what was not refused: $(tail -3 <<<"$out")"
 ok "$what refused ($(grep -oiE "[A-Za-z]*(AuthorizationException|AuthenticationException)|not authorized[^.]*|Authentication failed[^.]*|SSL handshake[^.]*|TimeoutException|disconnected|timed out" <<<"$out" | head -1))"; }


echo "== Kafka"
cert=$(dc exec -T kafka bash -c "openssl s_client -connect kafka:9092 -servername kafka </dev/null 2>/dev/null | openssl x509 -noout -subject -ext subjectAltName 2>/dev/null" | tr '\n' ' ')
[[ $cert == *"CN=kafka"* && $cert == *"DNS:localhost"* ]] || fail "broker certificate: $cert"; ok "TLS on kafka:9092: $cert"
expect_denied "a client without TLS or credentials" as pipeline $K/kafka-topics.sh --bootstrap-server $B --list --command-config /dev/null
dc exec -T kafka bash -c "sed 's/password=\"[0-9a-f]*\"/password=\"wrong\"/' $C > /tmp/wrong.properties"
expect_denied "a wrong password" dc exec -T kafka $K/kafka-topics.sh --bootstrap-server $B --command-config /tmp/wrong.properties --list
# Positive control and least privilege at once: each principal sees exactly the topics it was granted.
inputs="coldchain.inventory coldchain.sensors coldchain.specifications coldchain.traceability"
for grant in "pipeline:$inputs coldchain.review-items" "simulator:$inputs" "monitor:$inputs" "cases:coldchain.review-items"; do
 p=${grant%%:*} want=$(tr ' ' '\n' <<<"${grant#*:}" | sort | tr '\n' ' ')
 got=$(as "$p" $K/kafka-topics.sh --bootstrap-server $B --command-config $C --list | grep '^coldchain' | sort | tr '\n' ' ')
 [[ $got == "$want" ]] || fail "$p sees [$got], expected [$want]"
done
ok "each principal sees only its granted topics (pipeline 5, simulator 4, monitor 4, cases 1)"
expect_denied "the simulator reading review items" as simulator $K/kafka-console-consumer.sh --bootstrap-server $B --consumer.config $C --topic coldchain.review-items --partition 0 --offset earliest --max-messages 1 --timeout-ms 15000
expect_denied "the case store writing sensor readings" bash -c "echo '{}' | docker run --rm -i --network ${project}_default --user 10001 --mount type=volume,src=${project}_kafka-secrets,dst=/etc/coldchain/kafka,readonly,volume-subpath=cases apache/kafka:3.9.1 $K/kafka-console-producer.sh --bootstrap-server $B --producer.config $C --topic coldchain.sensors --request-required-acks all --timeout 5000 2>&1"
expect_denied "the monitor writing sensor readings" bash -c "echo '{}' | docker run --rm -i --network ${project}_default --user 10001 --mount type=volume,src=${project}_kafka-secrets,dst=/etc/coldchain/kafka,readonly,volume-subpath=monitor apache/kafka:3.9.1 $K/kafka-console-producer.sh --bootstrap-server $B --producer.config $C --topic coldchain.sensors --request-required-acks all --timeout 5000 2>&1"
expect_denied "the pipeline creating a topic" as pipeline $K/kafka-topics.sh --bootstrap-server $B --command-config $C --create --topic coldchain.rogue --partitions 1 --replication-factor 1
expect_denied "the pipeline changing ACLs" as pipeline $K/kafka-acls.sh --bootstrap-server $B --command-config $C --add --allow-principal User:pipeline --operation All --topic '*'

echo "== Containers"
[[ $(dc exec -T pipeline id -u) == 10001 ]] || fail "pipeline runs as $(dc exec -T pipeline id -u)"; ok "pipeline runs as uid 10001"
[[ $(dc exec -T pipeline ls /etc/coldchain/kafka | tr '\n' ' ') == "client.properties truststore.p12 " ]] || fail "pipeline sees other credentials"
grep -q 'username="pipeline"' <(dc exec -T pipeline cat /etc/coldchain/kafka/client.properties) || fail "pipeline mounts another principal"
ok "pipeline mounts only its own credentials"
for s in pipeline cases; do dc exec -T "$s" env | grep -qiE "password|secret" && fail "$s has a secret in its environment"; done; ok "no secrets in service environments"
out=$(dc --profile demo run --rm -q --entrypoint sh verify -c 'touch /data/x' 2>&1 || true)
grep -qi "read-only" <<<"$out" || fail "the verifier could write the lake: $out"; ok "readers mount the lake read-only"

echo "== Application roles"
deny() { local what=$1 out; shift; out=$("$@" 2>&1 || true); grep -q "^DENIED" <<<"$out" || fail "$what was not denied: $(tail -2 <<<"$out")"; ok "$what denied"; }
deny "a recipient querying as investigator" dc --profile demo run --rm -q -e COLDCHAIN_PRINCIPAL=hospital-demo verify io.coldchain.Query --scope investigator
deny "a recipient replaying" dc --profile demo run --rm -q -e COLDCHAIN_PRINCIPAL=hospital-demo verify io.coldchain.Replay /data /tmp/replay
deny "quality planning a retention purge" dc --profile ops run --rm -q -e COLDCHAIN_PRINCIPAL=demo-quality retention plan 365
deny "an operator approving a rule change" dc --profile demo run --rm -q -e COLDCHAIN_PRINCIPAL=demo-operator verify io.coldchain.Governance approve RULE_CHANGE x reason
dc --profile demo run --rm -q -e COLDCHAIN_PRINCIPAL=hospital-demo verify io.coldchain.Query --scope recipient:HOSPITAL-DEMO | grep -q "^SNAPSHOT" || fail "the hospital cannot read its own units"
ok "positive control: the hospital reads its own units"
audit=$(dc --profile demo run --rm -q -e COLDCHAIN_PRINCIPAL=demo-quality verify io.coldchain.Governance audit 200)
for e in '"principal":"hospital-demo","action":"READ","target":"investigator","outcome":"DENIED"' '"principal":"hospital-demo","role":"OPERATOR","action":"REPLAY"' \
 '"principal":"demo-quality","role":"OPERATOR","action":"RETENTION_PLAN"' '"principal":"demo-operator","role":"QUALITY","action":"APPROVE"' '"principal":"hospital-demo","action":"READ","target":"Filter'; do
 grep -qF "$e" <<<"$audit" || fail "audit lacks $e"
done
ok "every denial and the allowed read are in the audit log ($(grep -c '"at"' <<<"$audit") events)"
echo "PASS: unauthorized Kafka, storage and application access refused; roles separated; access audited."

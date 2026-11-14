#!/bin/bash
# Creates the topics and grants each principal only what it uses, as admin (compose service topics). Kafka denies anything
# not granted (allow.everyone.if.no.acl.found=false). Safe to run again.
set -euo pipefail
B=kafka:9092 C=/etc/coldchain/kafka/client.properties K=/opt/kafka/bin
inputs="coldchain.sensors coldchain.traceability coldchain.inventory coldchain.specifications"
for topic in $inputs coldchain.review-items; do
 $K/kafka-topics.sh --bootstrap-server $B --command-config $C --create --if-not-exists --topic "$topic" --partitions 4 --replication-factor 1 --config retention.ms=604800000
done
acl() { $K/kafka-acls.sh --bootstrap-server $B --command-config $C --add "$@" >/dev/null; }
for topic in $inputs; do
 acl --allow-principal User:simulator --operation Write --operation Describe --topic "$topic"
 acl --allow-principal User:pipeline --operation Read --operation Describe --topic "$topic"
 acl --allow-principal User:monitor --operation Read --operation Describe --topic "$topic"
done
# The pipeline relays review items (to coldchain.review-items, or a holding topic during a migration); the case store reads them.
acl --allow-principal User:pipeline --operation Write --operation Describe --topic coldchain.review-items --resource-pattern-type prefixed
acl --allow-principal User:cases --operation Read --operation Describe --topic coldchain.review-items
# Spark's Kafka source names its consumer groups spark-kafka-source-<query>.
acl --allow-principal User:pipeline --allow-principal User:cases --operation Read --group spark-kafka-source --resource-pattern-type prefixed
echo "topics and ACLs ready"
$K/kafka-acls.sh --bootstrap-server $B --command-config $C --list | grep -c "principal=User" | xargs echo "ACL entries:"

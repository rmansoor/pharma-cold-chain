#!/bin/sh
# Generates this deployment's Kafka security material into /secrets (the kafka-secrets volume), once: a private CA, the
# broker's TLS keystore (SAN kafka, localhost), one random SASL password per principal, each principal's client configuration
# and the broker configuration with its listeners, JAAS users and ACL authorizer. Nothing is committed or passed in the
# environment; every service mounts only its own directory, read-only (compose.yaml). To rotate everything, stop the stack,
# remove the kafka-secrets volume and start it again.
set -eu
S=/secrets
if [ -f "$S/.complete" ]; then echo "keys: present"; exit 0; fi
umask 077
rm -rf "$S"/ca "$S"/broker "$S"/admin "$S"/pipeline "$S"/cases "$S"/simulator "$S"/monitor
mkdir -p "$S/ca" "$S/broker" "$S/admin" "$S/pipeline" "$S/cases" "$S/simulator" "$S/monitor"
secret() { openssl rand -hex 24; }
store=$(secret)
openssl req -x509 -newkey rsa:3072 -nodes -days 825 -subj "/CN=coldchain-local-ca" -keyout "$S/ca/ca.key" -out "$S/ca/ca.crt" 2>/dev/null
openssl req -newkey rsa:3072 -nodes -subj "/CN=kafka" -keyout "$S/ca/broker.key" -out "$S/ca/broker.csr" 2>/dev/null
printf 'subjectAltName=DNS:kafka,DNS:localhost\nextendedKeyUsage=serverAuth,clientAuth\n' >"$S/ca/broker.ext"
openssl x509 -req -in "$S/ca/broker.csr" -CA "$S/ca/ca.crt" -CAkey "$S/ca/ca.key" -CAcreateserial -days 825 -extfile "$S/ca/broker.ext" -out "$S/ca/broker.crt" 2>/dev/null
openssl pkcs12 -export -in "$S/ca/broker.crt" -inkey "$S/ca/broker.key" -name kafka -passout "pass:$store" -out "$S/broker/broker.p12"
for d in broker admin pipeline cases simulator monitor; do
 keytool -importcert -noprompt -alias coldchain-ca -file "$S/ca/ca.crt" -keystore "$S/$d/truststore.p12" -storetype PKCS12 -storepass "$store" >/dev/null 2>&1
done
# The CA key signs nothing after this; only the certificate is kept, for clients outside the stack.
rm -f "$S/ca/ca.key" "$S/ca/broker.key" "$S/ca/broker.csr" "$S/ca/ca.srl"
users=""
for p in admin pipeline cases simulator monitor; do
 pw=$(secret); eval "pw_$p=$pw"; users="$users user_$p=\"$pw\""
 cat >"$S/$p/client.properties" <<EOF
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="$p" password="$pw";
ssl.truststore.location=/etc/coldchain/kafka/truststore.p12
ssl.truststore.type=PKCS12
ssl.truststore.password=$store
EOF
done
jaas="org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"$pw_admin\"$users;"
cat >"$S/broker/server.properties" <<EOF
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@kafka:9093
listeners=CLIENT://:9092,CONTROLLER://:9093,EXTERNAL://:29092
advertised.listeners=CLIENT://kafka:9092,EXTERNAL://localhost:29092
listener.security.protocol.map=CLIENT:SASL_SSL,CONTROLLER:SASL_SSL,EXTERNAL:SASL_SSL
inter.broker.listener.name=CLIENT
controller.listener.names=CONTROLLER
sasl.enabled.mechanisms=PLAIN
sasl.mechanism.inter.broker.protocol=PLAIN
sasl.mechanism.controller.protocol=PLAIN
listener.name.client.plain.sasl.jaas.config=$jaas
listener.name.external.plain.sasl.jaas.config=$jaas
listener.name.controller.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="$pw_admin" user_admin="$pw_admin";
ssl.keystore.location=/etc/coldchain/broker/broker.p12
ssl.keystore.type=PKCS12
ssl.keystore.password=$store
ssl.truststore.location=/etc/coldchain/broker/truststore.p12
ssl.truststore.type=PKCS12
ssl.truststore.password=$store
authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer
super.users=User:admin
allow.everyone.if.no.acl.found=false
log.dirs=/var/lib/kafka/data
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
auto.create.topics.enable=false
group.initial.rebalance.delay.ms=0
EOF
/opt/kafka/bin/kafka-storage.sh random-uuid >"$S/broker/cluster.id"
cp "$S/admin/client.properties" "$S/broker/admin.properties"
# Kafka runs as appuser (1000); the application image as coldchain (10001). Each reads only its own directory.
chown -R 1000:1000 "$S/broker" "$S/admin"
chown -R 10001:10001 "$S/pipeline" "$S/cases" "$S/simulator" "$S/monitor"
chmod 0500 "$S"/broker "$S"/admin "$S"/pipeline "$S"/cases "$S"/simulator "$S"/monitor
chmod 0400 "$S"/*/*
chmod 0755 "$S/ca"; chmod 0444 "$S/ca/ca.crt" "$S/ca/broker.crt"
touch "$S/.complete"
echo "keys: generated CA, broker keystore and credentials for admin, pipeline, cases, simulator, monitor"

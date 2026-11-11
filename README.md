# Pharmaceutical cold-chain medallion pipeline

Java 17 + Kafka 3.9.1 + Spark 3.5.7 + Delta Lake 3.3.3. A local reference implementation for revised product exposure assessments with explicit missing coverage, retained evidence and committed Gold snapshots.

## Design documents

- [Architecture and workflow](docs/architecture.md): implemented topology, target production design and scenario.
- [Detailed design](docs/detailed-design.md): contracts, schemas, interval algorithm, revisions and publication.
- [Event contracts v1](docs/event-contracts.md): fields, units, topics, owners and identity rules; [JSON Schema and examples](contracts/).
- [Decisions and operations](docs/decisions-and-operations.md): tradeoffs, runbook, failure modes, security and tests.
- [Epic and 16 implementation stories](docs/delivery-backlog.md): dependencies and acceptance criteria.
- [Structured Jira backlog](docs/jira-backlog.json) and [planning CSV](docs/jira-backlog.csv).

## Run

```sh
mvn -s maven-settings.xml -B test
export ALLOW_SYNTHETIC_SPECS=true   # the demo's thresholds are synthetic; without this the pipeline ignores them
docker compose up -d --build pipeline cases
docker compose --profile demo run --rm simulator
docker compose --profile demo run --rm verify
scripts/restart-check.sh
docker compose --profile ops up -d monitor prometheus grafana   # metrics :9400, alerts :9490, dashboard :3400 (localhost)
scripts/ops-drill.sh   # disposable project: stalled-publication alert, recovery, checkpoint migration
```

Needs about 4 GB of Docker memory. Versions, measured resources and the restart check are in [Decisions and operations](docs/decisions-and-operations.md#local-operating-runbook). CI runs the tests, a secret scan, and the Compose demo and restart check.

Host Kafka: localhost:29092. Spark UI: localhost:4041. New project volumes are separate from the earlier fraud project. Each batch updates Silver and recomputes Gold only for what its changes can affect; `io.coldchain.Replay` recomputes everything into an isolated namespace as the reference. Each snapshot's new, revised and withdrawn review items are relayed to Kafka and kept, once each, in the `cases` service's case store. The ops profile adds a monitor that reads Kafka, the manifests and the lake from outside the pipeline, Prometheus alert rules (a stalled publication pages even while the pipeline process is up) and a Grafana dashboard; runbooks and proposed service objectives are in [Decisions and operations](docs/decisions-and-operations.md#observability-and-service-objectives). Synthetic thresholds are not product-handling guidance. No clinical decisions, automatic recalls or compliance certification are implemented.

49 local Spark transformation and contract tests pass (`mvn -s maven-settings.xml -B test`). CI also runs the alert-rule tests, the Compose demo, restart and crash checks, and the operations drill. Jira: epic SID-12, stories SID-13..SID-29 (see docs/jira-backlog.json).

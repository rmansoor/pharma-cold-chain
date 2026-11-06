# Pharmaceutical cold-chain medallion pipeline

Java 17 + Kafka 3.9.1 + Spark 3.5.7 + Delta Lake 3.3.3. A local reference implementation for revised product exposure assessments with explicit missing coverage, retained evidence and committed Gold snapshots.

## Design documents

- [Architecture and workflow](docs/architecture.md): implemented topology, target production design and scenario.
- [Detailed design](docs/detailed-design.md): contracts, schemas, interval algorithm, revisions and publication.
- [Decisions and operations](docs/decisions-and-operations.md): tradeoffs, runbook, failure modes, security and tests.
- [Epic and 16 implementation stories](docs/delivery-backlog.md): dependencies and acceptance criteria.
- [Structured Jira backlog](docs/jira-backlog.json) and [planning CSV](docs/jira-backlog.csv).

## Run

```sh
mvn -s maven-settings.xml -B test
docker compose up -d --build pipeline
docker compose --profile demo run --rm simulator
docker compose --profile demo run --rm verify
```

Host Kafka: localhost:29092. Spark UI: localhost:4041. New project volumes are separate from the earlier fraud project. The prototype reconstructs all retained Bronze evidence per nonempty batch; incremental processing is planned. Synthetic thresholds are not product-handling guidance. No clinical decisions, automatic recalls or compliance certification are implemented.

Three local Spark transformation tests passed during initial implementation. Real-broker integration and fault-injection validation must be recorded separately. Jira exports contain planning IDs, not created issue keys, until publication is verified.

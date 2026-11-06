# Decisions, tradeoffs, operations and verification

## Architecture decision records

| ADR | Decision | Rationale | Tradeoff / revisit trigger |
|---|---|---|---|
| 001 | Java 17 and Spark SQL | Consistent with the prior Java/Spark project and strong temporal batch APIs | More verbose than Python; SQL view names currently assume one evaluation per session |
| 002 | Kafka transport plus Delta storage | Separate event ingestion from durable, revisable evidence | Additional persisted copies and operational surfaces |
| 003 | Delta 3.3.3 with Spark 3.5.7 / Scala 2.12 | Keep a compatible Spark 3.5 dependency line | Versions are pinned, not claimed to be newest; upgrades need compatibility tests |
| 004 | Preserve Bronze before interpretation | Late corrections and investigations need original evidence | Storage retention and restricted access must be planned |
| 005 | Full retained-history recomputation first | Simple correction semantics and a testable baseline | Becomes too costly at scale; replace with impacted-scope recomputation |
| 006 | Union intervals before summing | Avoid inflated duration from overlapping evidence | Sorting/window operations and temporal joins are expensive |
| 007 | Manifest-last Gold publication | Consumers see only complete product snapshots | Requires reader discipline; no general multi-table transaction |
| 008 | Missing coverage is insufficient data | Avoid treating a sensor outage as normal conditions | More quality-review workload and need for explicit resolution |
| 009 | Single local writer and volumes | Repeatable demonstration with minimal infrastructure | No HA, shared cloud storage, multiple writers or disaster recovery |
| 010 | Human quality authority | Pipeline explains evidence and prioritizes review | Approval and disposition workflow must be integrated separately |

## Local operating runbook

Requirements: Docker Compose v2, network for image/dependency downloads and at least about 6 GB available Docker memory. Run from project root:

```sh
docker compose up -d --build pipeline
docker compose --profile demo run --rm simulator
docker compose --profile demo run --rm verify
```

Host broker localhost:29092; Spark UI localhost:4041. Internal Kafka kafka:9092. DATA_DIR defaults to /data; pipeline-data persists Bronze, Silver, Gold and checkpoints. The simulator pauses 40 seconds between initial and corrected assignments. This is a demo timing assumption, not a reliable synchronization contract; the hardened verifier/simulator story replaces it with committed-snapshot acknowledgement and run-scoped IDs.

```sh
mvn -s maven-settings.xml -B test
docker compose logs -f pipeline
docker compose restart pipeline
docker compose down
```

`docker compose down` preserves named volumes. `docker compose down -v` deliberately destroys this project's events, lake data and checkpoints; use only for a deliberate disposable-demo reset. macOS Docker may need /Applications/Docker.app/Contents/Resources/bin on PATH. JDK 17 is used in containers and for the local build. The application has no automated retention purge.

## Failure and recovery matrix

| Failure | Expected recovery | Verification required |
|---|---|---|
| Repeated Kafka payload at new offset | Bronze retains both; canonical ID/revision collapses retries | Assert exposure and coverage remain 600 seconds |
| Invalid JSON | Bronze retains input; quarantine records contract failure | Assert source coordinates and raw evidence |
| Higher revision arrives late | Supersede prior canonical event and rebuild assessments | Assert old product loses exposure, new product gains it |
| Conflicting same revision | Block ID; expose conflicting_ids; retain raw evidence | Assert no arbitrary winner |
| Crash after Bronze | Resume batch using checkpoint; source-coordinate MERGE idempotent | Fault-injection integration test |
| Crash after history, before manifest | Ignore unpublished snapshot; retry then commit | Fault-injection reader test |
| Expired Kafka offsets | failOnDataLoss=true fails visibly | Backfill plan and broker retention test |
| Lost local volumes | Demo has no replicated recovery | Backups/shared storage are production work |
| Checkpoint reset with retained Gold | Unsupported: batch-ID collision risk | New lineage namespace required |

## Security and governance

Demo inputs are synthetic and contain no patient records. Production should minimize personal data: inventory location and lot traceability often suffice without patient identity. Apply TLS/SASL and least-privilege Kafka ACLs, scoped storage permissions, encrypted storage, secret management and access audit. Separate operator replay permissions from source-data deletion and quality approval. Define domain-approved retention and deletion policies rather than assuming indefinite storage. Controls and validated regulatory alignment require a separate review; this prototype does not establish compliance.

## Observability and service objectives

Track Kafka backlog, micro-batch runtime, publication age, quarantine/conflict counts, coverage gaps, changed-assessment counts, temporal fanout, Delta file counts, disk consumption and checkpoint health. Alert on stalled commit manifests even if Spark remains running. Record source offsets and code/rule versions with each published snapshot in the target design.

Candidate engineering objectives to negotiate: publication within 60 seconds at the agreed load, correction within two minutes of receipt, no duplicate product/snapshot keys, complete lineage for every review item and documented recovery point/time. These are proposed acceptance targets, not measured performance or commitments.

## Test strategy and current evidence

Three local Spark tests passed: revision changes exposure A->B while duplicates remain bounded; overlapping readings use interval union; malformed/conflicting contracts are excluded. No real-Kafka container run or fault-injection result has yet been recorded for this project.

Required tests: half-open boundaries; multiple disjoint placements; nested splits; zero coverage; partial coverage; safe full coverage; exact threshold; non-finite temperature; incompatible topics; missing/overlapping specifications; simultaneous assignments; sensor calibration; equal-time inventory changes; product removal; snapshot retry; reader during partial write; new lineage replay; load/skew/backlog recovery; access-denial paths.

A story is done only with reviewable code, meaningful passing tests, updated design/runbook, documented limitations and evidence satisfying its acceptance criteria. Existing prototype coverage does not automatically close a Jira story.

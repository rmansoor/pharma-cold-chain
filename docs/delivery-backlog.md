# Delivery backlog

One epic and 16 stories in Jira project SIDEBUSINESS (key `SID`), created 2026-11-05: epic **SID-12**, stories **SID-13..SID-28** (CC-*n* = SID-(12+*n*); `jira-backlog.json` holds the full mapping). Every dependency below exists in Jira as a *Blocks* link. No assignee, sprint, due date or story points are assumed.

## Epic

Cold-chain intelligence: traceable medallion pipeline and revised exposure assessments

Build a Java/Kafka/Spark/Delta Lake pipeline that traces serialized pharmaceutical products through time-varying containers and sensor assignments, evaluates synthetic exposure rules, locates affected inventory, and publishes reproducible revised assessments for authorized quality review.

Scope: governed contracts; Bronze evidence; Silver temporal lineage and quality; Gold exposure/uncertainty; committed publication; late-correction impact processing; inventory queries; idempotent alerts; operational/security controls; verification.

Non-goals: clinical suitability determination, automatic recall/product release, patient record ingestion, production compliance certification. A local prototype already has three passing Spark tests; that is not production acceptance.

Epic acceptance: a run-scoped scenario with duplicate and late evidence plus a corrected sensor assignment moves the review from the formerly linked product to the correctly linked product, retains prior published evidence, locates affected inventory, and handles missing coverage explicitly. Publication/recovery/permission tests and measured operating limits must pass.

Design package: architecture.md, detailed-design.md, decisions-and-operations.md, delivery-backlog.md. Publication/attachment of these docs to Jira is recorded separately; local paths are not shared URLs. All children remain backlog until acceptance is verified.

## Story index

| Planning ID | Story | Phase | Dependencies |
|---|---|---|---|
| CC-01 | [Cold Chain] Define governed event contracts and identity rules | Foundation | None |
| CC-02 | [Cold Chain] Provision reproducible Kafka and Delta development runtime | Foundation | CC-01 |
| CC-03 | [Cold Chain] Implement idempotent Bronze ingestion with source provenance | Foundation | CC-01, CC-02 |
| CC-04 | [Cold Chain] Implement Silver validation and conflict quarantine | Foundation | CC-03 |
| CC-05 | [Cold Chain] Build versioned product and container lineage | Domain and exposure | CC-04 |
| CC-06 | [Cold Chain] Reconstruct sensor assignments and custody timelines | Domain and exposure | CC-04, CC-05 |
| CC-07 | [Cold Chain] Govern temporal product specifications and measurement quality | Domain and exposure | CC-01, CC-04 |
| CC-08 | [Cold Chain] Compute traceable exposure intervals without double counting | Domain and exposure | CC-05, CC-06, CC-07 |
| CC-09 | [Cold Chain] Implement explicit uncertainty and product assessment policy | Domain and exposure | CC-08 |
| CC-10 | [Cold Chain] Publish atomic consumer-visible Gold snapshots and provenance | Delivery and hardening | CC-09 |
| CC-11 | [Cold Chain] Implement affected-product incremental recomputation and replay | Delivery and hardening | CC-08, CC-10 |
| CC-12 | [Cold Chain] Serve affected inventory and recipient impact queries | Delivery and hardening | CC-10 |
| CC-13 | [Cold Chain] Deliver idempotent investigation alerts and revisions | Delivery and hardening | CC-10, CC-12 |
| CC-14 | [Cold Chain] Add operational metrics, dashboards and incident runbooks | Delivery and hardening | CC-03, CC-10 |
| CC-15 | [Cold Chain] Implement access controls, retention and audit safeguards | Delivery and hardening | CC-02, CC-03 |
| CC-16 | [Cold Chain] Build run-scoped integration, recovery and performance verification | Delivery and hardening | CC-10, CC-11, CC-12, CC-14, CC-15 |

## CC-01 - [Cold Chain] Define governed event contracts and identity rules

As a source integrator, I need explicit versioned contracts so incompatible evidence is caught before promotion.

Scope: Data foundation

Acceptance criteria:
1. Publish schemas and examples for all five event kinds and the four topic mappings, including units, half-open UTC intervals and source ownership.
2. Validate nonblank identifiers, monotonic revision semantics, interval bounds and finite values; add positive and negative contract tests.
3. Canonical semantic payload hashes distinguish real conflicts from JSON formatting differences.

Dependencies (planning IDs): None

Definition of done: code reviewed; meaningful tests pass; design/runbook updated; evidence recorded; limitations explicit. Prototype coverage does not by itself close this story.

## CC-02 - [Cold Chain] Provision reproducible Kafka and Delta development runtime

As an engineer, I need an isolated reproducible runtime to develop and verify the pipeline.

Scope: Platform

Acceptance criteria:
1. A fresh Compose deployment starts the broker, four topics and a Java 17 Spark/Delta application with nonconflicting host ports.
2. Persist evidence and checkpoints across restart; pin compatible versions and document resource requirements.
3. Build and transformation tests run in CI; secrets are absent from images and source.

Dependencies (planning IDs): CC-01

Definition of done: code reviewed; meaningful tests pass; design/runbook updated; evidence recorded; limitations explicit. Prototype coverage does not by itself close this story.

## CC-03 - [Cold Chain] Implement idempotent Bronze ingestion with source provenance

As an investigator, I need original source evidence retained for correction and replay.

Scope: Bronze

Acceptance criteria:
1. Preserve raw payload, topic, partition, offset, broker timestamp, ingestion timestamp and source-system identity.
2. Reprocessing the same source offset creates no duplicate Bronze key; repeated payload at a new offset remains evidence.
3. Restart and fail-after-write tests demonstrate resumable ingestion without silent offset loss.

Dependencies (planning IDs): CC-01, CC-02

Definition of done: code reviewed; meaningful tests pass; design/runbook updated; evidence recorded; limitations explicit. Prototype coverage does not by itself close this story.

## CC-04 - [Cold Chain] Implement Silver validation and conflict quarantine

As a data steward, I need actionable quarantine and conflict evidence.

Scope: Silver

Acceptance criteria:
1. Invalid records carry reason codes, source coordinates and raw references; no invalid contract enters canonical Silver.
2. Competing same-ID/revision contracts produce a conflict case and never select an arbitrary winner.
3. Add corrected-event reprocessing and resolution rules with auditable outcomes.

Dependencies (planning IDs): CC-03

Definition of done: code reviewed; meaningful tests pass; design/runbook updated; evidence recorded; limitations explicit. Prototype coverage does not by itself close this story.

## CC-05 - [Cold Chain] Build versioned product and container lineage

As an investigator, I need product location within changing packaging over time.

Scope: Traceability

Acceptance criteria:
1. Materialize product, carton and pallet membership intervals with source ID/revision and effective time.
2. Handle split, merge and repack events; detect cycles, contradictory parents and overlapping impossible placements.
3. A boundary test identifies the exact affected serialized units before and after a pallet split.

Dependencies (planning IDs): CC-04

Definition of done: code reviewed; meaningful tests pass; design/runbook updated; evidence recorded; limitations explicit. Prototype coverage does not by itself close this story.

## CC-06 - [Cold Chain] Reconstruct sensor assignments and custody timelines

As a quality analyst, I need reliable sensor-to-container and custody associations.

Scope: Traceability

Acceptance criteria:
1. Persist revisioned assignment/custody intervals and link provenance to each temporal edge.
2. A correction invalidates both the former and revised product mappings; ambiguity is visible rather than silently resolved.
3. Cover late events, simultaneous assignments, missing custody and equal-time transitions in tests.

Dependencies (planning IDs): CC-04, CC-05

Definition of done: code reviewed; meaningful tests pass; design/runbook updated; evidence recorded; limitations explicit. Prototype coverage does not by itself close this story.

## CC-07 - [Cold Chain] Govern temporal product specifications and measurement quality

As a quality owner, I need approved product rules and trustworthy measurements.

Scope: Risk data

Acceptance criteria:
1. Version specifications by effective interval and product; reject or explicitly resolve overlaps with documented precedence.
2. Normalize supported temperature units and expose calibration validity, sampling gaps and measurement uncertainty.
3. Never extend a point observation into an unsupported interval; use synthetic thresholds only in demos.

Dependencies (planning IDs): CC-01, CC-04

Definition of done: code reviewed; meaningful tests pass; design/runbook updated; evidence recorded; limitations explicit. Prototype coverage does not by itself close this story.

## CC-08 - [Cold Chain] Compute traceable exposure intervals without double counting

As an analyst, I need correct exposure duration for each product and assessment scope.

Scope: Exposure engine

Acceptance criteria:
1. Intersect placement, assignment, reading and specification validity using half-open interval boundaries.
2. Union overlapping coverage and excursion intervals before summing; duplicates and sensor overlap cannot inflate seconds.
3. Persist interval evidence with exact source revisions; verify boundary, overlap, gap and split cases.

Dependencies (planning IDs): CC-05, CC-06, CC-07

Definition of done: code reviewed; meaningful tests pass; design/runbook updated; evidence recorded; limitations explicit. Prototype coverage does not by itself close this story.

## CC-09 - [Cold Chain] Implement explicit uncertainty and product assessment policy

As a quality reviewer, I need evidence-based outcomes that preserve uncertainty.

Scope: Exposure engine

Acceptance criteria:
1. Define review, insufficient-data and no-observed-excursion states without implying product release approval.
2. Expose missing coverage, ambiguous lineage/specification and stale inventory independently of the primary outcome.
3. Version rules and assessment scope; test exact threshold, partial coverage and mixed specification periods.

Dependencies (planning IDs): CC-08

Definition of done: code reviewed; meaningful tests pass; design/runbook updated; evidence recorded; limitations explicit. Prototype coverage does not by itself close this story.

## CC-10 - [Cold Chain] Publish atomic consumer-visible Gold snapshots and provenance

As a consumer, I need a consistent complete assessment snapshot.

Scope: Gold

Acceptance criteria:
1. Publish assessment history before a manifest and require readers to pin one committed snapshot.
2. Include lineage/run identity, source high-watermarks, input table versions, rule version and evidence revisions.
3. Fault-injection tests prove retries are idempotent and partially written snapshots remain invisible.

Dependencies (planning IDs): CC-09

Definition of done: code reviewed; meaningful tests pass; design/runbook updated; evidence recorded; limitations explicit. Prototype coverage does not by itself close this story.

## CC-11 - [Cold Chain] Implement affected-product incremental recomputation and replay

As an operator, I need late corrections processed without full-history scans.

Scope: Corrections

Acceptance criteria:
1. Compute impact from old and new assignment/placement/specification values and reevaluate all affected scopes.
2. Incremental output matches the full-recomputation reference over the same source history.
3. Isolate replay namespaces and retain revision history; measure correction latency at agreed scale.

Dependencies (planning IDs): CC-08, CC-10

Definition of done: code reviewed; meaningful tests pass; design/runbook updated; evidence recorded; limitations explicit. Prototype coverage does not by itself close this story.

## CC-12 - [Cold Chain] Serve affected inventory and recipient impact queries

As an investigator, I need to find affected stock and downstream recipients.

Scope: Serving

Acceptance criteria:
1. Expose committed-snapshot queries by product, lot, shipment, location and recipient with full assessment evidence.
2. Carry latest-known location, inventory timestamp and staleness; enforce scoped read access.
3. Pagination and filters produce consistent results under concurrent publication; no patient identity is required.

Dependencies (planning IDs): CC-10

Definition of done: code reviewed; meaningful tests pass; design/runbook updated; evidence recorded; limitations explicit. Prototype coverage does not by itself close this story.

## CC-13 - [Cold Chain] Deliver idempotent investigation alerts and revisions

As a reviewer, I need new and revised investigation items without duplicate notifications.

Scope: Integration

Acceptance criteria:
1. Use a transactional publication/outbox or equivalent durable handoff keyed by assessment revision.
2. Emit new, revised and withdrawn review items with reason and evidence references.
3. Retry and consumer-restart tests prove no lost publication and idempotent case-store updates.

Dependencies (planning IDs): CC-10, CC-12

Definition of done: code reviewed; meaningful tests pass; design/runbook updated; evidence recorded; limitations explicit. Prototype coverage does not by itself close this story.

## CC-14 - [Cold Chain] Add operational metrics, dashboards and incident runbooks

As an operator, I need to detect backlog, stale publication and data-quality failures.

Scope: Operations

Acceptance criteria:
1. Expose backlog, batch runtime, commit age, conflict/quarantine rates, missing coverage, state/storage growth and revision counts.
2. Alert on a stalled manifest even when the process remains running; define proposed SLOs with measured baselines.
3. Publish triage, replay, checkpoint migration, storage and recovery runbooks with evidence from a drill.

Dependencies (planning IDs): CC-03, CC-10

Definition of done: code reviewed; meaningful tests pass; design/runbook updated; evidence recorded; limitations explicit. Prototype coverage does not by itself close this story.

## CC-15 - [Cold Chain] Implement access controls, retention and audit safeguards

As a platform owner, I need controlled evidence access and governed retention.

Scope: Security

Acceptance criteria:
1. Implement TLS/SASL, least-privilege topic/storage permissions and managed secret handling for the chosen deployment.
2. Audit reads, replays, rule changes and retention actions; separate operational and quality approval roles.
3. Document domain-approved retention and deletion policy and verify unauthorized access is denied.

Dependencies (planning IDs): CC-02, CC-03

Definition of done: code reviewed; meaningful tests pass; design/runbook updated; evidence recorded; limitations explicit. Prototype coverage does not by itself close this story.

## CC-16 - [Cold Chain] Build run-scoped integration, recovery and performance verification

As a delivery owner, I need trustworthy evidence of correctness and operating limits.

Scope: Verification

Acceptance criteria:
1. Run the full Kafka-to-Gold correction scenario with unique run IDs and committed-snapshot acknowledgement, replacing timing sleeps.
2. Cover duplicate/conflict/late data, nested split, threshold/gap, restart and partial-publication failures; old retained results cannot satisfy a new run.
3. Publish throughput, tail latency, correction time, skew and recovery measurements against agreed targets with known limitations.

Dependencies (planning IDs): CC-10, CC-11, CC-12, CC-14, CC-15

Definition of done: code reviewed; meaningful tests pass; design/runbook updated; evidence recorded; limitations explicit. Prototype coverage does not by itself close this story.

## Delivery sequence

Foundation: CC-01 through CC-04. Domain/exposure: CC-05 through CC-09. Consistent publication: CC-10. Incremental correction and serving: CC-11/12. Integration, operations and security: CC-13/14/15. Release evidence: CC-16. Security design starts with the foundation; delivery phases are not a reason to delay threat modeling.

## Jira publication state

Prepared locally. Actual Jira issue keys and URLs must be recorded after verified creation. CSV is a planning export; Jira parent mapping requires the actual epic key and project-specific import field mapping.

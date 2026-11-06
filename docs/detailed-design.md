# Detailed technical design

## 1. Identity and time semantics

Every source event has eventId, positive revision, kind and eventTime ([event-contracts.md](event-contracts.md) is the full contract). eventId is globally unique across all kinds and keeps one kind across revisions; the highest revision wins regardless of arrival order. A retry must carry the same semantic payload for its ID and revision: payloadHash is SHA-256 over the parsed contract fields, so key order, whitespace and timestamp/number spelling do not count as conflicts. Event time and reading end times must not exceed the Kafka timestamp by more than five minutes.

Intervals are half-open [startTime,endTime). UTC is used by Spark SQL. Measurements explicitly cover a supplied interval at temperatureC; the prototype assumes that value represents the interval uniformly. It never extends a point reading across an unobserved gap. Durations are measured at whole-second precision, and interval bounds with a nonzero fractional second are rejected as invalid contracts.

Event-time lateness is not bounded by a watermark in this implementation. Historical source records are reevaluated from Bronze on each nonempty batch. This intentionally supports late corrections, at the cost of unbounded scan work as retained history grows. Five-second triggers schedule work; they are not a five-second assessment SLA.

## 2. Source contracts

| Kind | Kafka topic | Required business fields |
|---|---|---|
| READING | coldchain.sensors | sensorId, startTime, endTime, temperatureC (-100 to 100) |
| PLACEMENT | coldchain.traceability | productId, productCode, lotId, containerId, startTime, endTime |
| ASSIGNMENT | coldchain.traceability | sensorId, containerId, startTime, endTime |
| INVENTORY | coldchain.inventory | productId, location, status: IN_STOCK or DISPATCHED |
| SPEC | coldchain.specifications | productCode, startTime, endTime, minC <= maxC, maxExcursionSeconds >= 0 |

All kinds also require the common identity/time fields. Source JSON is parsed with one explicit nullable schema; event-specific predicates decide validity. Invalid contracts are not silently promoted. Every identifier must contain a non-whitespace character; temperatures and specification bounds must be finite and within -100 to 100 °C. The published JSON Schema is contracts/coldchain-event-v1.schema.json, with one example per kind in contracts/examples.

Product specifications are operational input, not static Java constants. Overlapping specification validity or simultaneous competing assignments currently participate in joins without an ambiguity resolver. The target must either reject conflicts or apply an approved precedence rule and expose uncertainty. The current minimum permitted excursion among matching specifications is conservative but cannot replace approved product policy.

## 3. Persisted tables

| Path under DATA_DIR | Key / write mode | Content |
|---|---|---|
| bronze/events | topic, partition, offset / insert-only MERGE | Raw JSON and broker timestamp; duplicates with distinct offsets remain evidence |
| silver/quarantine | rebuilt snapshot | Invalid raw records, source coordinates, INVALID_CONTRACT |
| silver/conflicting_ids | rebuilt snapshot | IDs with competing semantic payloads for one revision, or used for more than one kind |
| silver/events_current | latest eventId revision / rebuilt snapshot | Valid canonical fields plus source metadata |
| gold/assessment_history | productId, snapshotId / insert-only MERGE | Coverage, excursion, missing data, rule, inventory and evidence IDs |
| gold/commits | snapshotId / insert-only MERGE | Publication timestamp and product count |
| checkpoints/medallion | Spark-managed | Kafka progress and micro-batch identity |

No table is automatically partitioned in the demo. Production partitioning/clustering must be chosen using product cardinality, date access patterns and measured file sizes. Over-partitioning by product ID should be avoided. Retention and compaction are explicit operational policies; no automatic VACUUM or evidence deletion is configured.

## 4. Temporal reconstruction algorithm

For a candidate placement p, matching assignment a, reading r and specification s:

segmentStart = max(p.start, a.start, r.start, s.start)
segmentEnd = min(p.end, a.end, r.end, s.end)

Retain a segment only when segmentStart < segmentEnd. Match p.containerId = a.containerId, a.sensorId = r.sensorId and p.productCode = s.productCode. Exposure is outside [minC,maxC]; the endpoints are inclusive acceptable bounds in the illustrative rule.

Compute three interval sets per product: EXPECTED from placements; COVERAGE from valid measured segments; UNSAFE from segments whose temperature is outside the matching specification. For each set, sort intervals, compute maximum prior end, start a new island only when the next start exceeds the prior end, and sum each merged island's duration. Touching intervals form one island. This prevents duplicate or overlapping evidence from multiplying duration.

coveredSeconds can never exceed expectedSeconds under valid intersection logic. missingSeconds = expectedSeconds - coveredSeconds. Assessments operate per product across the entire retained placement history, not per shipment or excursion episode. Shipment-level identity and episode boundaries are planned design work.

## 5. Assessment semantics

- QUALITY_REVIEW: excursionSeconds exceeds the allowed threshold derived from matching segments. This takes precedence even if some coverage is missing.
- INSUFFICIENT_DATA: coverage is incomplete or no matching specification/evidence supports evaluation.
- NO_OBSERVED_EXCURSION: complete modeled coverage and no threshold breach. This is not a certificate of product quality or a release authorization.

Inventory is selected by newest eventTime, then revision, then eventId. It is latest known, not necessarily current physical location. UNKNOWN is used when inventory is absent. A stale-inventory indicator is planned. Gold contains the rule version exposure-v1 and sorted reading, assignment and specification event IDs. It currently lacks source revision numbers, placement IDs and lot/product-code fields in the assessment row; exact versioned evidence and lot-level serving are explicit backlog items.

assessmentHash covers principal metrics, assessment, inventory, evidence IDs and ruleVersion. It is a content convenience, not the unique publication key or a complete provenance digest. snapshotId is the Spark batch ID within one retained checkpoint lineage.

## 6. Publication and restart

One writer is assumed. Bronze MERGE is idempotent on source coordinates. Gold MERGE is idempotent on product/snapshot. Silver overwrite is atomic per Delta table, not across all tables. The Gold manifest is written last. An in-flight Silver version must not be treated as an approved Gold snapshot.

A reader first pins a committed snapshotId, then reads history at exactly that ID; it must never independently select the newest row for each product. A crash after Gold history but before the manifest leaves an unpublished snapshot. A retry inserts no duplicate assessment key and then publishes the manifest. Consumers ignore any unpublished rows.

Do not delete checkpoints while retaining Gold under the same namespace: batch IDs restart and collide. A replay must have an isolated data/checkpoint namespace. Production requires run/lineage identity, source high-watermarks, exact Delta input versions and code/rule version in the manifest. Multiple concurrent writers are unsupported.

## 7. Scalability design

The reference implementation rereads all Bronze, canonicalizes all events and recomputes all products per nonempty batch. This is a correctness-oriented baseline, not an incremental production engine. Complexity grows with retained source history and join fanout. Production needs a changed-entity impact index: reading -> sensor assignment interval -> container lineage -> products -> assessment scope. Corrections must include both previous and revised assignments so old exposure is removed as well as new exposure added.

Persist versioned domain tables for placements, container ancestry, assignments, readings, specifications and custody. Materialize interval evidence and recompute affected scopes; use controlled backfills for older corrections. Measure skew by hot sensors, container fanout, overlapping intervals and product history length. Proposed latency, throughput and recovery targets must be negotiated and load-tested; none are asserted as achieved.

## 8. Code responsibilities

Model.java: contract validation, semantic payload hashes, conflict exclusion, canonical revisions, interval joins/unions and assessment SQL. Pipeline.java: Kafka source, foreachBatch persistence and committed publication. Simulator.java: synthetic initial state, retries, malformed event and assignment correction. VerifyDemo.java: read committed assessments and check the expected correction. Inspect.java: print persisted-state counts used by scripts/restart-check.sh. ModelTest.java: six real Spark tests for correction/deduplication, interval union, invalid/conflicting contracts, published examples, per-rule contract violations, and formatting-insensitive conflict detection.

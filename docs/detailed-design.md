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
| PACKING | coldchain.traceability | childId, childLevel, parentId, parentLevel (above childLevel), startTime, endTime; productCode and lotId for a UNIT child |
| ASSIGNMENT | coldchain.traceability | sensorId, containerId, startTime, endTime |
| CUSTODY | coldchain.traceability | containerId, custodianId, startTime, endTime |
| INVENTORY | coldchain.inventory | productId, location, status: IN_STOCK or DISPATCHED |
| SPEC | coldchain.specifications | productCode, startTime, endTime, minC <= maxC, maxExcursionSeconds >= 0 |

All kinds also require the common identity/time fields. Source JSON is parsed with one explicit nullable schema; event-specific predicates decide validity. Invalid contracts are not silently promoted. Every identifier must contain a non-whitespace character; temperatures and specification bounds must be finite and within -100 to 100 °C. The published JSON Schema is contracts/coldchain-event-v1.schema.json, with one example per kind in contracts/examples.

Product specifications are operational input, not static Java constants. Overlapping specification validity or simultaneous competing assignments currently participate in joins without an ambiguity resolver. The target must either reject conflicts or apply an approved precedence rule and expose uncertainty. The current minimum permitted excursion among matching specifications is conservative but cannot replace approved product policy.

## 3. Persisted tables

| Path under DATA_DIR | Key / write mode | Content |
|---|---|---|
| bronze/events | topic, partition, offset / insert-only MERGE | Raw JSON, Kafka key, broker timestamp (kafkaTime), sourceSystem and sourceSystemDeclared, ingestedAt and ingestBatchId. A replayed offset keeps its first ingestedAt and ingestBatchId; a repeated payload at a new offset is a new row of evidence. Rows written before these columns existed have nulls in them |
| silver/quarantine | caseId (source coordinates) / rebuilt snapshot | Invalid raw records, source coordinates, parsed eventId/revision, reason codes, resolution status ([event-contracts.md](event-contracts.md#validation-outcome)) |
| silver/conflict_cases | caseId / rebuilt snapshot | PAYLOAD_MISMATCH and KIND_MISMATCH cases with every competing variant and its coordinates, and resolution status |
| silver/case_log | caseId, status / insert-only MERGE | Audit trail: the snapshot and time each quarantine or conflict case was first OPEN and first RESOLVED |
| silver/events_current | latest eventId revision / rebuilt snapshot | Valid canonical fields plus source metadata |
| silver/memberships | eventId / rebuilt snapshot | Every direct membership (PLACEMENT as UNIT → CONTAINER, and PACKING) with child/parent IDs and levels, interval, eventId, revision and the anomalies it takes part in |
| silver/lineage_anomalies | rebuilt snapshot | CONTRADICTORY_PARENT, LEVEL_MISMATCH and CYCLE with subject ID, event IDs and window |
| silver/timelines | eventId, revision / rebuilt snapshot | Every valid revision of each ASSIGNMENT and CUSTODY event with source coordinates, sourceSystem, a current flag and, on current rows, anomalies |
| silver/timeline_anomalies | rebuilt snapshot | SENSOR_IN_TWO_CONTAINERS and CUSTODY_OVERLAP with subject, event IDs and window |
| silver/mapping_invalidations | rebuilt snapshot | Per assignment correction (fromRevision → toRevision), the units mapped by the FORMER and the REVISED revision, with sensor, container and window |
| silver/unit_containers | rebuilt snapshot | Resolved container interval per serialized unit, with productCode, lotId, path (unit → … → container) and lineageIds (the membership events used) |
| silver/specifications | rebuilt snapshot | The specification that applies to each product code over time after precedence (latest approval wins), with thresholds, allowance and the synthetic flag |
| silver/specification_overlaps | rebuilt snapshot | Every window where specifications overlap: all candidates, the one that applies (or none) and the resolution |
| silver/readings | eventId / rebuilt snapshot | Each current reading: normalized °C, reported value and unit, point flag, covering calibrations, largest uncertainty, calibrated seconds and calibration status |
| silver/sampling_gaps | rebuilt snapshot | Windows inside each current assignment with no interval reading from its sensor |
| gold/assessment_history | productId, snapshotId / insert-only MERGE | Coverage, excursion, missing data, rule, inventory and evidence IDs |
| gold/commits | snapshotId / insert-only MERGE | Publication timestamp and product count |
| checkpoints/medallion | Spark-managed | Kafka progress and micro-batch identity |

No table is automatically partitioned in the demo. Production partitioning/clustering must be chosen using product cardinality, date access patterns and measured file sizes. Over-partitioning by product ID should be avoided. Retention and compaction are explicit operational policies; no automatic VACUUM or evidence deletion is configured.

## 4. Temporal reconstruction algorithm

Lineage (Lineage.java) first resolves where each unit is. Starting from every direct membership, it repeatedly joins the parent to that parent's own memberships where the intervals overlap, intersecting the intervals and recording the path, for at most 8 hops. Because each hop must go up a level, a valid chain has at most 3 hops; a longer one implies a level mismatch, which is reported. Memberships involved in an anomaly are removed before resolution. A resolved interval is a path from a UNIT to a CONTAINER, and it is what the algorithm below calls a placement.

For a candidate resolved placement p, matching assignment a, reading r and specification s:

segmentStart = max(p.start, a.start, r.start, s.start)
segmentEnd = min(p.end, a.end, r.end, s.end)

Retain a segment only when segmentStart < segmentEnd. Match p.containerId = a.containerId, a.sensorId = r.sensorId and p.productCode = s.productCode, where s is the specification that applies after precedence (Specifications.java). Exposure is outside [minC,maxC]; the endpoints are inclusive acceptable bounds. A point reading (start = end) never forms a segment; it is matched separately to the placement, assignment and specification that contain its instant, and contributes no seconds.

Further interval sets measure quality: CALIBRATED (segments intersected with a calibration of the reading's sensor), POSSIBLE (UNSAFE, plus calibrated time within the calibration's uncertainty of minC or maxC), LOCATED (resolved placements), SPECIFIED (placements intersected with an applicable specification) and CUSTODY. uncalibratedSeconds = COVERAGE − CALIBRATED, unspecifiedSeconds = LOCATED − SPECIFIED and missingCustodySeconds = LOCATED − CUSTODY.

Compute three interval sets per product: EXPECTED from the unit's direct memberships (including ones excluded by an anomaly, and time when its carton is not yet on a pallet), so unresolved time counts as missing coverage; COVERAGE from valid measured segments; UNSAFE from segments whose temperature is outside the matching specification. For each set, sort intervals, compute maximum prior end, start a new island only when the next start exceeds the prior end, and sum each merged island's duration. Touching intervals form one island. This prevents duplicate or overlapping evidence from multiplying duration.

coveredSeconds can never exceed expectedSeconds under valid intersection logic. missingSeconds = expectedSeconds - coveredSeconds. Assessments operate per product across the entire retained placement history, not per shipment or excursion episode. Shipment-level identity and episode boundaries are planned design work.

## 5. Assessment semantics

`assessmentReasons` lists, in this order, every reason that keeps a unit from NO_OBSERVED_EXCURSION:

| Reason | When |
|---|---|
| EXCURSION_ABOVE_ALLOWANCE | excursionSeconds exceeds the allowance (the smallest maxExcursionSeconds among matched specifications) |
| OUT_OF_RANGE_POINT | A point reading at an instant the unit was measured is outside the specification: an observed excursion of unknown duration |
| MISSING_COVERAGE | coveredSeconds < expectedSeconds |
| MISSING_SPECIFICATION | Part of the unit's located time has no applicable specification (none, unresolved tie, or synthetic where not allowed) |
| UNCALIBRATED_MEASUREMENT | Some covered time is measured only by sensors without a valid calibration |
| WITHIN_MEASUREMENT_UNCERTAINTY | Excursion is within the allowance, but possibleExcursionSeconds (out of range or within uncertainty of a limit) exceeds it |

- QUALITY_REVIEW: EXCURSION_ABOVE_ALLOWANCE or OUT_OF_RANGE_POINT. This takes precedence even if some coverage is missing. Uncalibrated out-of-range readings count as exposure: missing calibration never hides a risk.
- INSUFFICIENT_DATA: any other reason, or no matched specification at all. The measurements cannot show that the unit stayed in range.
- NO_OBSERVED_EXCURSION: no reason: complete, specified and calibrated coverage, with no excursion beyond the allowance even allowing for uncertainty. This is not a certificate of product quality or a release authorization.

Inventory is selected by newest eventTime, then revision, then eventId. It is latest known, not necessarily current physical location. UNKNOWN is used when inventory is absent. A stale-inventory indicator is planned. Gold contains the rule version exposure-v4 (exposure through resolved packaging lineage, excluding contradictory sensor assignments, with specification precedence, calibration, uncertainty and point observations), missingCustodySeconds and custodianIds, possibleExcursionSeconds, uncalibratedSeconds, unspecifiedSeconds, outOfRangePoints, calibrationIds, maxUncertaintyC, syntheticThresholds, assessmentReasons, and sorted reading, assignment, specification and lineage (membership) event IDs. exposure-v4 gives the same excursion and coverage numbers as exposure-v3 when specifications do not overlap and there are no point readings; the assessment can be stricter, because NO_OBSERVED_EXCURSION now also needs calibrated measurement and a margin for uncertainty. It currently lacks source revision numbers and lot/product-code fields in the assessment row; exact versioned evidence and lot-level serving are explicit backlog items.

assessmentHash covers principal metrics, assessment, inventory, evidence IDs and ruleVersion. It is a content convenience, not the unique publication key or a complete provenance digest. snapshotId is the Spark batch ID within one retained checkpoint lineage.

## 6. Publication and restart

One writer is assumed. Bronze MERGE is idempotent on source coordinates; scripts/crash-check.sh kills the pipeline right after the Bronze commit (FAIL_AFTER_BRONZE_BATCH) and checks that the restarted query replays that batch without duplicating or losing offsets. Gold MERGE is idempotent on product/snapshot. Silver overwrite is atomic per Delta table, not across all tables. The Gold manifest is written last. An in-flight Silver version must not be treated as an approved Gold snapshot.

A reader first pins a committed snapshotId, then reads history at exactly that ID; it must never independently select the newest row for each product. A crash after Gold history but before the manifest leaves an unpublished snapshot. A retry inserts no duplicate assessment key and then publishes the manifest. Consumers ignore any unpublished rows.

Do not delete checkpoints while retaining Gold under the same namespace: batch IDs restart and collide. A replay must have an isolated data/checkpoint namespace. Production requires run/lineage identity, source high-watermarks, exact Delta input versions and code/rule version in the manifest. Multiple concurrent writers are unsupported.

## 7. Scalability design

The reference implementation rereads all Bronze, canonicalizes all events and recomputes all products per nonempty batch. This is a correctness-oriented baseline, not an incremental production engine. Complexity grows with retained source history and join fanout. Production needs a changed-entity impact index: reading -> sensor assignment interval -> container lineage -> products -> assessment scope. Corrections must include both previous and revised assignments so old exposure is removed as well as new exposure added.

Persist versioned domain tables for placements, container ancestry, assignments, readings, specifications and custody. Materialize interval evidence and recompute affected scopes; use controlled backfills for older corrections. Measure skew by hot sensors, container fanout, overlapping intervals and product history length. Proposed latency, throughput and recovery targets must be negotiated and load-tested; none are asserted as achieved.

## 8. Code responsibilities

Timelines.java: assignment and custody revision history, timeline anomalies and mapping invalidations. Lineage.java: direct memberships, lineage anomalies and resolved unit-to-container intervals. Model.java: contract validation with per-rule reason codes, quarantine and conflict cases with resolution rules, semantic payload hashes, canonical revisions, interval joins/unions and assessment SQL. Pipeline.java: Kafka source, foreachBatch persistence and committed publication. Simulator.java: synthetic initial state, retries, malformed event and assignment correction. VerifyDemo.java: read committed assessments and check the expected correction. Inspect.java: print persisted-state counts used by scripts/restart-check.sh and scripts/crash-check.sh. BronzeTest.java: provenance fields, source-system fallback, replay without duplicates, repeated payloads as evidence, and appending to a Bronze table from before the provenance columns. ModelTest.java: six real Spark tests for correction/deduplication, interval union, invalid/conflicting contracts, published examples, the reason code for each contract violation, and formatting-insensitive conflict detection. SilverTest.java: quarantine evidence and resolution by correction, conflict cases with all variants and resolution by a higher revision, and the case log recording each transition once. LineageTest.java: the exact units affected before and after a pallet split, merge and repack over time, and contradictions, level mismatches and cycles that are reported and kept out of lineage. TimelineTest.java: a correction that arrives before the revision it replaces invalidates the former and revised units, one sensor in two containers versus two sensors in one, and custody handover, gap and overlap.

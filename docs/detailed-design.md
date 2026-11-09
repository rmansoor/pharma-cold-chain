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
| silver/exposure_segments | rebuilt snapshot | Every elementary measured segment of the current snapshot: product, interval, seconds, lineage, assignment, sensor, reading and specification, temperature, limits, outOfRange, and the exact source revisions |
| gold/exposure_intervals | productId, snapshotId, metric, startTime / insert-only MERGE | The disjoint intervals behind every Gold duration (section 4) and each out-of-range point, with seconds, ruleVersion and the exact revision of every source event: kind, eventId, revision, payloadHash, topic, partition, offset |
| gold/assessment_history | productId, snapshotId / insert-only MERGE | Coverage, excursion, missing data, rule, inventory, evidence IDs and sourceRevisions |
| gold/assessment_policies | policyVersion / insert-only MERGE | Each policy version: states with meaning and impliesRelease (always false), reasons with condition and state, flags, inventory age limit, ruleVersion and assessment scope |
| gold/commits | snapshotId / insert-only MERGE | The manifest: one row per published snapshot with lineage and run identity, code, rule and policy versions, source high-watermarks, input and output table versions, and evidence and assessment digests (section 6) |
| checkpoints/medallion | Spark-managed | Kafka progress and micro-batch identity |

No table is automatically partitioned in the demo. Production partitioning/clustering must be chosen using product cardinality, date access patterns and measured file sizes. Over-partitioning by product ID should be avoided. Retention and compaction are explicit operational policies; no automatic VACUUM or evidence deletion is configured.

## 4. Temporal reconstruction algorithm

Lineage (Lineage.java) first resolves where each unit is. Starting from every direct membership, it repeatedly joins the parent to that parent's own memberships where the intervals overlap, intersecting the intervals and recording the path, for at most 8 hops. Because each hop must go up a level, a valid chain has at most 3 hops; a longer one implies a level mismatch, which is reported. Memberships involved in an anomaly are removed before resolution. A resolved interval is a path from a UNIT to a CONTAINER, and it is what the algorithm below calls a placement.

For a candidate resolved placement p, matching assignment a, reading r and specification s:

segmentStart = max(p.start, a.start, r.start, s.start)
segmentEnd = min(p.end, a.end, r.end, s.end)

Retain a segment only when segmentStart < segmentEnd. Match p.containerId = a.containerId, a.sensorId = r.sensorId and p.productCode = s.productCode, where s is the specification that applies after precedence (Specifications.java). Exposure is outside [minC,maxC]; the endpoints are inclusive acceptable bounds. A point reading (start = end) never forms a segment; it is matched separately to the placement, assignment and specification that contain its instant, and contributes no seconds.

Exposure.java turns these into one interval set per product and metric. Every raw interval carries the IDs of the events it depends on (membership chain, assignment, reading, specification, calibration or custody).

| Metric | Raw intervals | Gold column |
|---|---|---|
| EXPECTED | The unit's direct memberships, including ones excluded by an anomaly and time when its carton is not yet on a pallet, so unresolved time counts as missing coverage | expectedSeconds |
| COVERED | Measured segments | coveredSeconds |
| EXCURSION | Segments whose temperature is outside the specification | excursionSeconds |
| POSSIBLE_EXCURSION | EXCURSION, plus calibrated time within the calibration's uncertainty of minC or maxC | possibleExcursionSeconds |
| CALIBRATED | Segments intersected with a calibration of the reading's sensor | (used for UNCALIBRATED) |
| LOCATED, SPECIFIED, CUSTODY | Resolved placements; placements with an applicable specification; placements with a custodian | (used for the gaps below) |
| MISSING, UNCALIBRATED, UNSPECIFIED, MISSING_CUSTODY | EXPECTED − COVERED, COVERED − CALIBRATED, LOCATED − SPECIFIED, LOCATED − CUSTODY | missingSeconds, uncalibratedSeconds, unspecifiedSeconds, missingCustodySeconds |
| OUT_OF_RANGE_POINT | Out-of-range point readings, as zero-length intervals at their instant | (outOfRangePoints counts the readings) |

Each set is merged before anything is summed: sort its intervals, compute the maximum prior end, and start a new island only when the next start exceeds it. Touching intervals form one island, so retries under another ID, overlapping readings and two sensors in one container never count a second twice. A difference is cut from the merged sets: the boundaries of both sets divide time into elementary windows, a window inside an island of the first set and inside no island of the second belongs to the difference, and adjacent windows are merged again. Each resulting interval lists every raw interval of its source set that overlaps it (for a difference, the first set), and those event IDs are replaced by the exact current revision: kind, eventId, revision, payloadHash and the Bronze topic, partition and offset. The intervals are persisted per snapshot in gold/exposure_intervals; the elementary segments, with the same source revisions, are in silver/exposure_segments.

Every Gold duration is the sum of that metric's persisted intervals, which are disjoint and do not touch (ExposureTest checks this for every product in every case). coveredSeconds can never exceed expectedSeconds, because a measured segment needs a resolved placement, which lies inside a direct membership. Assessments operate per product across the entire retained placement history, not per shipment or excursion episode. Shipment-level identity and episode boundaries are planned design work.

## 5. Assessment semantics

The policy is defined once in Policy.java (`assessment-policy-v1`). Gold's assessment SQL is generated from it, and the same definition is persisted in gold/assessment_policies, one row per version: each state's meaning, each reason with its condition and the state it leads to, each flag, the inventory age limit, the rule version and the assessment scope. Every Gold row names its `policyVersion`, `ruleVersion` and `assessmentScope`.

| Outcome | Meaning | impliesRelease |
|---|---|---|
| QUALITY_REVIEW | The evidence shows an excursion the policy does not allow. A quality reviewer must assess the product. | false |
| INSUFFICIENT_DATA | The evidence cannot show that the product stayed within its specification. | false |
| NO_OBSERVED_EXCURSION | Complete, specified and calibrated measurement shows no excursion beyond the allowance, allowing for uncertainty. | false |

No outcome releases, rejects or dispositions product. QUALITY_REVIEW prioritizes a human review; NO_OBSERVED_EXCURSION says only what the measurements show. Disposition stays with the quality unit (ADR 010).

`assessmentReasons` lists, in this order, every reason that keeps a unit from NO_OBSERVED_EXCURSION:

| Reason | When |
|---|---|
| EXCURSION_ABOVE_ALLOWANCE | excursionSeconds exceeds the allowance: the smallest maxExcursionSeconds among the specifications that applied. Equal to the allowance is within it |
| OUT_OF_RANGE_POINT | A point reading at an instant the unit was measured is outside the specification: an observed excursion of unknown duration |
| MISSING_COVERAGE | coveredSeconds < expectedSeconds |
| MISSING_SPECIFICATION | Part of the unit's located time has no applicable specification (none, unresolved tie, or synthetic where not allowed) |
| UNCALIBRATED_MEASUREMENT | Some covered time is measured only by sensors without a valid calibration |
| WITHIN_MEASUREMENT_UNCERTAINTY | Excursion is within the allowance, but possibleExcursionSeconds (out of range or within uncertainty of a limit) exceeds it |

- QUALITY_REVIEW: EXCURSION_ABOVE_ALLOWANCE or OUT_OF_RANGE_POINT. This takes precedence even if some coverage is missing. Uncalibrated out-of-range readings count as exposure: missing calibration never hides a risk.
- INSUFFICIENT_DATA: any other reason, or no matched specification at all. The measurements cannot show that the unit stayed in range.
- NO_OBSERVED_EXCURSION: no reason: complete, specified and calibrated coverage, with no excursion beyond the allowance even allowing for uncertainty. This is not a certificate of product quality or a release authorization.

Thresholds: minC and maxC are acceptable values (8.0 °C against a maximum of 8.0 °C is in range). A calibrated reading is a possible excursion only when it is strictly within its uncertainty of a limit (7.5 °C with ±0.5 °C against 8.0 °C is not). An excursion equal to the allowance is within it.

Mixed specification periods: when the specification changes during a unit's history, the cumulative excursion is compared with the smallest allowance among the specifications that applied. This is conservative: 300 s under a later specification that alone allows 600 s still exceeds an earlier 60 s allowance. `excursionBySpecification` lists, for each specification, the excursion seconds under it and its own allowance, so a reviewer can see which period caused the review.

`dataQualityFlags` describes the evidence and never changes the outcome. It is reported in this order on every outcome, so a reviewed unit also shows what was missing or ambiguous:

| Flag | When |
|---|---|
| MISSING_COVERAGE | missingSeconds > 0 |
| AMBIGUOUS_LINEAGE | Any packaging chain from the unit uses a membership with a lineage anomaly (contradictory parent, level mismatch or cycle), at any level |
| AMBIGUOUS_ASSIGNMENT | A container that held the unit had a sensor assigned to two containers at once, during that time |
| AMBIGUOUS_CUSTODY | A container that held the unit had two custodians at once, during that time |
| AMBIGUOUS_SPECIFICATION | Specifications approved at the same instant overlap the unit's located time (UNRESOLVED_SAME_APPROVAL_TIME) |
| MIXED_SPECIFICATIONS | More than one specification applied |
| MISSING_CUSTODY | missingCustodySeconds > 0 |
| UNKNOWN_INVENTORY | No inventory record exists for the unit |
| STALE_INVENTORY | inventoryAgeSeconds > 86,400, or the unit's latest membership starts after the inventory was recorded (movedSinceInventory) |

The ambiguity flags need a resolved placement, except AMBIGUOUS_LINEAGE: a unit that is never located because its chain is ambiguous gets AMBIGUOUS_LINEAGE and MISSING_COVERAGE, not the container's custody or specification flags. inventoryAgeSeconds is measured back from the newest source eventTime in the snapshot, not from the wall clock, so it is reproducible from the evidence.

Assessment scope `PRODUCT_RETAINED_HISTORY`: one serialized unit over its whole retained history, from scopeStart to scopeEnd (the first and last expected instants). Shipment, lot and episode scopes are not modeled. ruleVersion (`exposure-v4`) versions the interval and duration calculation; policyVersion versions the reasons, states and flags built on it. Either changes independently.

Inventory is selected by newest eventTime, then revision, then eventId. It is latest known, not necessarily current physical location. UNKNOWN is used when inventory is absent; inventoryRecordedAt, inventoryAgeSeconds, movedSinceInventory and the inventory flags above say how current it is. Gold contains the rule version exposure-v4 (exposure through resolved packaging lineage, excluding contradictory sensor assignments, with specification precedence, calibration, uncertainty and point observations), missingCustodySeconds and custodianIds, possibleExcursionSeconds, uncalibratedSeconds, unspecifiedSeconds, outOfRangePoints, calibrationIds, maxUncertaintyC, syntheticThresholds, assessmentReasons, sorted reading, assignment, specification and lineage (membership) event IDs, and sourceRevisions: the kind, eventId, revision and payloadHash of every event the assessment depends on, including in-range point readings that change no number. exposure-v4 gives the same excursion and coverage numbers as exposure-v3 when specifications do not overlap and there are no point readings; the assessment can be stricter, because NO_OBSERVED_EXCURSION now also needs calibrated measurement and a margin for uncertainty. SID-20 changes no number or rule, only the evidence, so exposure-v4 is kept. SID-21 adds the policy, flags, breakdown, inventory age and scope without changing any outcome. The assessment row still lacks lot and product-code fields; lot-level serving is a backlog item.

assessmentHash covers principal metrics, assessment, inventory, evidence IDs, sourceRevisions, dataQualityFlags, excursionBySpecification, inventory age, scope, policyVersion and ruleVersion, so a new source revision changes it even when no number changes. It is a content convenience, not the unique publication key. snapshotId is the Spark batch ID within one retained checkpoint lineage.

## 6. Publication and restart

One writer is assumed. Each batch writes, in order:

1. **Bronze.** An insert-only MERGE on source coordinates.
2. **Silver.** Each table is overwritten whole: atomic per table, not across tables.
3. **Gold.** `gold/exposure_intervals`, then `gold/assessment_policies`, then `gold/assessment_history`.
4. **The manifest row,** in `gold/commits`, last of all.

A snapshot without a manifest row does not exist for readers.

**Readers** (Snapshots.java):
- `Snapshots.latest` pins the newest committed snapshot.
- `Snapshots.assessments` and `Snapshots.intervals` read each Gold table filtered to that snapshotId, at the Delta version the manifest recorded. Rows written later can't appear in that read, even by mistake.
- `Snapshots.committedHistory` lists every committed snapshot's assessments.
- A reader must never select the newest row per product on its own.

**Retries.** Batch IDs come from the Spark checkpoint and become snapshot IDs.
- **Before Gold:** a batch that stopped before its manifest left rows that carry its snapshotId but no commit. The retry deletes those rows and writes the snapshot again. Only the retry's rows remain, even if the code or input changed between attempts.
- **After the manifest:** a crash between the manifest and Spark recording the batch as done replays the batch. The replay finds the manifest and writes nothing to Gold, not even a rewrite that would briefly hide published rows.
- **Checkpoint replaced:** batch IDs restart. If gold/commits holds a manifest from another checkpoint lineage, publication is refused, naming both lineages. A replay needs an isolated data/checkpoint namespace.
- Multiple concurrent writers remain unsupported.

**Manifest row** (one per snapshot):

| Field | Meaning |
|---|---|
| snapshotId, committedAt, products | Batch ID, publication time, number of assessments |
| lineageId | Streaming query ID, kept by the checkpoint across restarts (`unattached` when process runs outside a stream) |
| runId | One per pipeline process start |
| codeVersion | `CODE_VERSION` from the deployment (CI sets the commit SHA; `dev` otherwise) |
| ruleVersion, policyVersion | Exposure calculation and outcome policy versions |
| dataAsOf | Newest source eventTime in the canonical input |
| sourceHighWatermarks | Per topic partition, the highest Kafka offset and broker time in the Bronze version read |
| tableVersions | Delta version of every table read (bronze/events, silver/events_current) and written (each Silver table, the case log, each Gold table) |
| evidenceRevisions, evidenceDigest | Count and SHA-256 of the distinct `kind:eventId:revision:payloadHash` source revisions across all assessments |
| assessmentsDigest | SHA-256 of the sorted `productId:assessmentHash` pairs |

The digests cover content only, not times or identities. So a retried snapshot can be compared with a clean run of the same input, and a reader can recompute evidenceDigest from the pinned assessments.

**Fault injection:** `FAIL_AT=<stage>:<batch>`, where stage is bronze, silver, intervals or history. The pipeline stops right after that stage of that batch: in the pipeline the JVM halts, and in tests it throws.
- **PublicationTest:** runs the real `Pipeline.process` against temporary lakes. It stops after the interval write and after the history write, plants a stale uncommitted row, then retries. It checks that readers saw snapshot 0 throughout, that the retried snapshot's digests equal a clean run's, that a replay after commit writes nothing, and that a new lineage is refused.
- **scripts/crash-check.sh:** does the same in Compose, with a real halt after Bronze and after Gold history.

## 7. Scalability design

The reference implementation rereads all Bronze, canonicalizes all events and recomputes all products per nonempty batch. This is a correctness-oriented baseline, not an incremental production engine. Complexity grows with retained source history and join fanout. Production needs a changed-entity impact index: reading -> sensor assignment interval -> container lineage -> products -> assessment scope. Corrections must include both previous and revised assignments so old exposure is removed as well as new exposure added.

Persist versioned domain tables for placements, container ancestry, assignments, readings, specifications and custody. Materialize interval evidence and recompute affected scopes; use controlled backfills for older corrections. Measure skew by hot sensors, container fanout, overlapping intervals and product history length. Proposed latency, throughput and recovery targets must be negotiated and load-tested; none are asserted as achieved.

## 8. Code responsibilities

Timelines.java: assignment and custody revision history, timeline anomalies and mapping invalidations. Lineage.java: direct memberships, lineage anomalies and resolved unit-to-container intervals. Model.java: contract validation with per-rule reason codes, quarantine and conflict cases with resolution rules, semantic payload hashes, canonical revisions, interval joins and assessment SQL. Exposure.java: interval merging, differences and the exact source revisions behind each interval. Policy.java: outcome states, reasons, flags and their versioned definition. Snapshots.java: the pinned-snapshot reader, the manifest, the lineage guard and removal of uncommitted rows. Pipeline.java: Kafka source, foreachBatch persistence and committed publication. Simulator.java: synthetic initial state, retries, malformed event and assignment correction. VerifyDemo.java: read committed assessments and check the expected correction. Inspect.java: print persisted-state counts used by scripts/restart-check.sh and scripts/crash-check.sh. BronzeTest.java: provenance fields, source-system fallback, replay without duplicates, repeated payloads as evidence, and appending to a Bronze table from before the provenance columns. ModelTest.java: six real Spark tests for correction/deduplication, interval union, invalid/conflicting contracts, published examples, the reason code for each contract violation, and formatting-insensitive conflict detection. SilverTest.java: quarantine evidence and resolution by correction, conflict cases with all variants and resolution by a higher revision, and the case log recording each transition once. LineageTest.java: the exact units affected before and after a pallet split, merge and repack over time, and contradictions, level mismatches and cycles that are reported and kept out of lineage. TimelineTest.java: a correction that arrives before the revision it replaces invalidates the former and revised units, one sensor in two containers versus two sensors in one, and custody handover, gap and overlap. ExposureTest.java: half-open boundaries, duplicates and overlapping sensors counted once, gaps persisted as intervals, a split assignment, and evidence naming the exact revisions used; every case checks that each Gold duration is the sum of its persisted, disjoint intervals. PolicyTest.java: exact allowance, limit, uncertainty and inventory-age thresholds, partial coverage, mixed specification periods, each ambiguity flag beside the outcome, stale and unknown inventory, and outcomes that match the persisted policy. PublicationTest.java: failures injected between the Gold writes stay invisible, retries reproduce a clean run's digests, a replay after the manifest writes nothing, a new checkpoint lineage is refused, and the manifest records identity, high-watermarks, table versions and a reproducible evidence digest.

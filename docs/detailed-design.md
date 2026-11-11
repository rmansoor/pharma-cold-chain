# Detailed technical design

## 1. Identity and time semantics

Every source event has eventId, positive revision, kind and eventTime ([event-contracts.md](event-contracts.md) is the full contract). eventId is globally unique across all kinds and keeps one kind across revisions; the highest revision wins regardless of arrival order, and among identical copies of it the first arrival is kept, so a retry changes nothing. A retry must carry the same semantic payload for its ID and revision: payloadHash is SHA-256 over the parsed contract fields, so key order, whitespace and timestamp/number spelling do not count as conflicts. Event time and reading end times must not exceed the Kafka timestamp by more than five minutes.

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
| silver/events_parsed | topic, partition, offset / full build, or append of the records added since the previous snapshot | Every Bronze record parsed: contract fields, payloadHash, reason codes and the valid flag. The incremental build reads an eventId's records here instead of parsing Bronze again |
| silver/quarantine | caseId (source coordinates) / replaced per eventId | Invalid raw records, source coordinates, parsed eventId/revision, reason codes, resolution status ([event-contracts.md](event-contracts.md#validation-outcome)) |
| silver/conflict_cases | caseId / replaced per eventId | PAYLOAD_MISMATCH and KIND_MISMATCH cases with every competing variant and its coordinates, and resolution status |
| silver/case_log | caseId, status / insert-only MERGE | Audit trail: the snapshot and time each quarantine or conflict case was first OPEN and first RESOLVED |
| silver/events_current | latest eventId revision / replaced per eventId | Valid canonical fields plus source metadata |
| silver/memberships | eventId / replaced per child ID in the lineage scope | Every direct membership (PLACEMENT as UNIT → CONTAINER, and PACKING) with child/parent IDs and levels, interval, eventId, revision and the anomalies it takes part in |
| silver/lineage_anomalies | subjectId / replaced per ID in the lineage scope | CONTRADICTORY_PARENT, LEVEL_MISMATCH and CYCLE with subject ID, event IDs and window |
| silver/timelines | eventId, revision / replaced per eventId | Every valid revision of each ASSIGNMENT and CUSTODY event with source coordinates, sourceSystem, a current flag and, on current rows, anomalies |
| silver/timeline_anomalies | anomaly, subjectId / replaced per sensor or container | SENSOR_IN_TWO_CONTAINERS and CUSTODY_OVERLAP with subject, event IDs and window |
| silver/mapping_invalidations | assignmentId / replaced per assignment | Per assignment correction (fromRevision → toRevision), the units mapped by the FORMER and the REVISED revision, with sensor, container and window |
| silver/unit_containers | productId / replaced per unit in the lineage scope | Resolved container interval per serialized unit, with productCode, lotId, path (unit → … → container) and lineageIds (the membership events used) |
| silver/specifications | productCode / replaced per product code | The specification that applies to each product code over time after precedence (latest approval wins), with thresholds, allowance and the synthetic flag |
| silver/specification_overlaps | productCode / replaced per product code | Every window where specifications overlap: all candidates, the one that applies (or none) and the resolution |
| silver/readings | eventId / replaced per reading | Each current reading: normalized °C, reported value and unit, point flag, covering calibrations, largest uncertainty, calibrated seconds and calibration status |
| silver/sampling_gaps | assignmentId / replaced per assignment | Windows inside each current assignment with no interval reading from its sensor |
| gold/exposure_segments | productId, snapshotId / append | Every elementary measured segment of each recomputed unit, per snapshot: product, interval, seconds, lineage, assignment, sensor, reading and specification, temperature, limits, outOfRange, and the exact source revisions |
| gold/exposure_intervals | productId, snapshotId, metric, startTime / append | The disjoint intervals behind every Gold duration (section 4) and each out-of-range point, with seconds, ruleVersion and the exact revision of every source event: kind, eventId, revision, payloadHash, topic, partition, offset |
| gold/assessment_history | productId, snapshotId / append after removing the snapshot's uncommitted rows | Change log: rows for the units each snapshot recomputed, and a tombstone (`removed`) for a unit that no longer exists. Coverage, excursion, missing data, rule, inventory, evidence IDs and sourceRevisions; inventory age and STALE_INVENTORY are derived when read (section 5) |
| gold/snapshot_impacts | snapshotId, eventId / append | Each canonical event that changed since the previous snapshot: ADDED, REVISED or WITHHELD, with old and new revision |
| gold/assessment_policies | policyVersion / insert-only MERGE | Each policy version: states with meaning and impliesRelease (always false), reasons with condition and state, flags, inventory age limit, ruleVersion and assessment scope |
| silver/shipments | eventId / replaced per shipment event | One row per shipped unit: shipmentId, the SHIPMENT event and revision, recipientId, productId, the container and lineage it travelled in (for container lines), dispatchedAt and deliveredAt |
| gold/commits | snapshotId / insert-only MERGE | The manifest: one row per published snapshot with lineage and run identity, code, rule and policy versions, source high-watermarks, input and output table versions, and evidence and assessment digests (section 6) |
| gold/review_outbox | deliveryId / append after removing the snapshot's uncommitted rows | Review items of each snapshot: NEW, REVISED and WITHDRAWN, with reason, both assessments and the evidence revisions (section 9) |
| outbox/relayed | snapshotId / insert-only MERGE | Each committed snapshot whose review items the relay sent and the sink accepted, with item count, time and run |
| checkpoints/medallion | Spark-managed | Kafka progress and micro-batch identity |

The reviewers' case store has its own namespace (CASES_DIR, its own Compose volume):

| Path under CASES_DIR | Key / write mode | Content |
|---|---|---|
| cases/review_notifications | deliveryId / insert-only MERGE | Every review item received, once, with when it first arrived |
| cases/review_items | reviewItemId / MERGE, updated only by a newer snapshotId | One case per unit: the newest item, with status OPEN or WITHDRAWN |
| checkpoints/cases | Spark-managed | Progress on coldchain.review-items |

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

Each set is merged before anything is summed: sort its intervals, compute the maximum prior end, and start a new island only when the next start exceeds it. Touching intervals form one island, so retries under another ID, overlapping readings and two sensors in one container never count a second twice. A difference is cut from the merged sets: the boundaries of both sets divide time into elementary windows, a window inside an island of the first set and inside no island of the second belongs to the difference, and adjacent windows are merged again. Each resulting interval lists every raw interval of its source set that overlaps it (for a difference, the first set), and those event IDs are replaced by the exact current revision: kind, eventId, revision, payloadHash and the Bronze topic, partition and offset. The intervals are persisted per snapshot in gold/exposure_intervals; the elementary segments, with the same source revisions, are in gold/exposure_segments.

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

inventoryAgeSeconds and STALE_INVENTORY depend on the snapshot's dataAsOf (the newest source event anywhere), so they are not stored: Policy.asOf derives them when a snapshot is read, from the manifest's dataAsOf, and Model.assessments derives them for a direct computation. Stored rows, and assessmentHash, carry inventoryRecordedAt and movedSinceInventory instead. Otherwise every new event would change every unit's row.

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

Inventory is selected by newest eventTime, then revision, then eventId. It is latest known, not necessarily current physical location. UNKNOWN is used when inventory is absent; inventoryRecordedAt, inventoryAgeSeconds, movedSinceInventory and the inventory flags above say how current it is. Gold contains the rule version exposure-v4 (exposure through resolved packaging lineage, excluding contradictory sensor assignments, with specification precedence, calibration, uncertainty and point observations), missingCustodySeconds and custodianIds, possibleExcursionSeconds, uncalibratedSeconds, unspecifiedSeconds, outOfRangePoints, calibrationIds, maxUncertaintyC, syntheticThresholds, assessmentReasons, sorted reading, assignment, specification and lineage (membership) event IDs, lotIds and productCodes from the unit's own memberships (more than one of either means the sources disagree), and sourceRevisions: the kind, eventId, revision and payloadHash of every event the assessment depends on, including in-range point readings that change no number. exposure-v4 gives the same excursion and coverage numbers as exposure-v3 when specifications do not overlap and there are no point readings; the assessment can be stricter, because NO_OBSERVED_EXCURSION now also needs calibrated measurement and a margin for uncertainty. SID-20 changes no number or rule, only the evidence, so exposure-v4 is kept. SID-21 adds the policy, flags, breakdown, inventory age and scope without changing any outcome. SID-24 adds lotIds and productCodes to the stored row (rowVersion `assessment-row-v2`) for lot queries.

assessmentHash covers principal metrics, assessment, inventory, evidence IDs, sourceRevisions, the stored dataQualityFlags, excursionBySpecification, inventoryRecordedAt, movedSinceInventory, scope, policyVersion and ruleVersion, so a new source revision changes it even when no number changes. It is a content convenience, not the unique publication key. snapshotId is the Spark batch ID within one retained checkpoint lineage.

## 6. Publication and restart

One writer is assumed. Each batch writes, in order:

1. **Bronze.** An insert-only MERGE on source coordinates.
2. **Silver.** Each table is overwritten whole: atomic per table, not across tables.
3. **Gold.** `gold/exposure_segments` and `gold/exposure_intervals`, then `gold/assessment_policies`, `gold/snapshot_impacts`, `gold/assessment_history` and `gold/review_outbox`.
4. **The manifest row,** in `gold/commits`, last of all.
5. **The relay** sends the committed snapshot's review items and records it in `outbox/relayed` (section 9). It runs after publication, so its failure never undoes or delays a snapshot.

A snapshot without a manifest row does not exist for readers.

**Readers** (Snapshots.java):
- `Snapshots.latest` pins the newest committed snapshot. Its state is, for each unit, the newest history row among committed snapshots up to it, unless that row is a tombstone (section 7).
- `Snapshots.assessments`, `Snapshots.intervals` and `Snapshots.segments` read the state at the Delta versions the manifest recorded, so rows written later can't appear even by mistake. Each unit's intervals and segments come from the snapshot its row came from. `assessments` adds inventory age and STALE_INVENTORY as of the manifest's dataAsOf.
- `Snapshots.committedHistory` lists every committed snapshot's assessments.
- A reader must never select the newest row per product on its own.
- `Snapshots.exists` decides whether a table exists by listing its Delta log for a commit. `DeltaTable.isDeltaTable` is not safe for a reader in another process: polled while the writer creates a table's first commit, it caches "no table" and keeps answering false.

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
| rowVersion | The stored assessment row's layout (`assessment-row-v2` adds lotIds and productCodes); a change forces FULL |
| recomputeMode, recomputeReason, changedEvents, recomputedProducts | FULL or INCREMENTAL and why, how many canonical events changed and how many units were recomputed (section 7) |
| allowSyntheticSpecifications | The configuration the snapshot was computed with; a change forces FULL |
| evidenceRevisions, evidenceDigest | Count and SHA-256 of the distinct `kind:eventId:revision:payloadHash` source revisions across the snapshot's state |
| assessmentsDigest, intervalsDigest | SHA-256 of the sorted `productId:assessmentHash` pairs, and of every interval in the state with its sources |

The digests cover content only, not times or identities. So a retried snapshot can be compared with a clean run of the same input, and a reader can recompute evidenceDigest from the pinned assessments.

**Fault injection:** `FAIL_AT=<stage>:<batch>`, where stage is bronze, silver, intervals, history, outbox or relay (the case store takes `cases`). The pipeline stops right after that stage of that batch: in the pipeline the JVM halts, and in tests it throws.
- **PublicationTest:** runs the real `Pipeline.process` against temporary lakes. It stops after the interval write and after the history write, plants a stale uncommitted row, then retries. It checks that readers saw snapshot 0 throughout, that the retried snapshot's digests equal a clean run's, that a replay after commit writes nothing, and that a new lineage is refused.
- **scripts/crash-check.sh:** does the same in Compose, with a real halt after Bronze and after Gold history.

## 7. Incremental recomputation and replay

Silver (Silver.java) and Gold are each built in full or incrementally. Gold is recomputed only for the units the batch can affect; Silver only for the subjects the new Bronze records can change.

**Silver mode.** Silver is built in full whenever Gold is (below), and also when the previous snapshot did not record its Bronze version and every Silver table (snapshots before SID-29: PREVIOUS_SILVER_FORMAT). The manifest records silverMode and silverReason. An incremental build:

1. Restores every Silver table to the version the previous snapshot recorded, if it moved since. An attempt that stopped before its manifest leaves nothing behind, so each build starts from exactly the published Silver.
2. Parses only the Bronze records that are not in the previous snapshot's Bronze version, and appends them to silver/events_parsed.
3. Recomputes quarantine, conflict cases and canonical selection for every eventId that gained a record, from all of that eventId's parsed records. These depend on one eventId's records only, so the first-arrival rule and conflict withholding are unchanged. A record without an eventId is quarantined on its own.
4. Compares the canonical rows of those eventIds with the previous snapshot's (the changes Gold uses), and replaces, in one MERGE per table, the rows of every subject a change can alter:

| Table | Subject | Recomputed for |
|---|---|---|
| memberships, lineage_anomalies, unit_containers | packaging ID | The lineage scope: every ID below an ID a changed PLACEMENT or PACKING names (old or new value), and every ID above those, in the current graph (both IDs of a changed membership's old value are starting points, so the old value adds nothing). Recomputed from every membership naming a scope ID; rows about other IDs computed on the way are dropped |
| timeline_anomalies | sensor, container | Sensors of changed assignments and containers of changed custody, from all their assignments or custody events |
| timelines | eventId | Events that gained a record, and events in an old or new contradiction of those subjects |
| mapping_invalidations | assignment | Assignments that gained a record, and assignments that ever named a container whose resolved units changed |
| shipments | shipment event | Changed shipments, and shipments of containers whose resolved units changed |
| specifications, specification_overlaps | product code | Product codes of changed specifications |
| readings | reading | Changed readings, and readings of sensors whose calibrations changed |
| sampling_gaps | assignment | Changed assignments, and assignments of sensors whose readings changed |

Why the lineage scope is enough: an anomaly changes only if a membership it names changed, so it is about an ID that membership names or a cycle through it, all of whose IDs lie below and above it. A unit's container path changes only if a membership on it, or that membership's anomalies, changed, so the unit is below a changed ID. Every membership naming a scope ID is read, so each scope ID's anomalies and each scope unit's paths are computed from complete input.


**Mode.** A snapshot is computed FULL when there is no previous snapshot, the previous one predates SID-23, or its ruleVersion, policyVersion, rowVersion, codeVersion or allowSyntheticSpecifications differs from the current run; the manifest records the reason. Otherwise it is INCREMENTAL: the previous manifest names the canonical version it used, and Impact.java compares that version with the new one.

**Changes.** An event changed if its current payloadHash differs between the two canonical versions: ADDED, REVISED, or WITHHELD (no longer current, for example after a conflicting payload). A retry of an unchanged event changes nothing, because the canonical row keeps the first arrival among identical copies. gold/snapshot_impacts records each change with its old and new revision.

**Affected units.** Each change is judged on both its old and its new value, so a corrected assignment affects the units of the container it left as well as the one it names now:

| Changed kind | Units affected |
|---|---|
| PLACEMENT, PACKING | Every unit below the child or the parent, in the graph of current memberships plus the old version of each changed one |
| ASSIGNMENT | Every unit below the old and new container, and below every container the sensor is assigned to (a new assignment can make another one contradictory) |
| READING, CALIBRATION | Every unit below any container the sensor is assigned to |
| CUSTODY | Every unit below the old and new container |
| SPEC | Every unit of the old and new product code |
| INVENTORY | The old and new unit |

This is an over-approximation by entity, not by time: a change outside a unit's intervals still recomputes it. That costs work, never correctness.

**Relevant events.** An affected unit is recomputed from exactly the events its assessment depends on:
- every membership on its chain and every membership that uses an ID on it, so lineage anomalies are found as in a full run;
- every assignment of the containers on the chain, and every assignment of those sensors;
- those sensors' readings and calibrations, the containers' custody, the specifications of its product code and its own inventory.

Only the affected units' rows are kept, because units that merely share part of the chain may have incomplete inputs.

**Change log.** Gold history, intervals and segments are written only for recomputed units, plus a tombstone (`removed`) for a unit that no longer exists. A snapshot's state is each unit's newest row up to it (section 6). Writes are appends after the snapshot's own uncommitted rows are deleted, so they never scan the tables. Manifest digests cover the whole state, not only the rows written.

**Equivalence.** After every batch, IncrementalTest also builds Silver in full from the snapshot's Bronze version and compares every Silver table with the incremental one, row for row; a second sequence aims a correction at each Silver table (calibration, shipment, re-parented carton, LEVEL_MISMATCH and CONTRADICTORY_PARENT, a cycle and its correction, custody overlap, a quarantined record resolved by a valid revision, records without an eventId, a payload conflict and its resolution, a moved assignment). `Replay` compares Silver the same way, table by table, as well as the Gold digests. IncrementalTest runs ten batches covering every kind of correction, including a withheld membership that removes a unit and a configuration change that forces FULL. After each batch it checks two things: that the snapshot recomputed exactly the expected units, and that all five state digests equal a full recomputation of the same canonical events. In deployment, `Replay` recomputes a namespace's Bronze history in full into a separate, empty namespace with its own lineage, and compares the digests with the source snapshot computed from the same Bronze version (CI runs it after the demo). It applies the configuration that snapshot's manifest records (allowSyntheticSpecifications), not its own environment's; the first CI-style run without that reported a MISMATCH that was only a configuration difference.

**Measured latency.** ScaleBenchmark: synthetic units, half in a carton on a pallet; one calibrated sensor per container reading every 10 minutes; one assignment moved. Single runs on a developer laptop under other load (load average 5–14), so the numbers show proportions, not targets. No scale has been agreed.

| Scale | Initial (full) | Correction (incremental) | Full replay of the same history |
|---|---|---|---|
| 1,000 units, 100 containers, 24 h (16,901 events) | 30.2 s | 27.1 s, 20 units recomputed (before the append change below) | 29.3 s |
| 4,000 units, 400 containers, 48 h (125,201 events) | 107.2 s | 52.8 s, 20 units recomputed | 106.5 s |

At 4,000 units the correction's Gold work was 20.5 s against 75–80 s in full: impact 1.3 s, exposure 5.1 s against 35–39 s, publication 14.1 s against 40–42 s. Publication had taken 33 s while segments were rewritten and Gold MERGEs scanned whole tables; appends removed that. The remaining 31 s is the canonical (10–18 s) and Silver (10–17 s) rebuild, which still reads all Bronze. The digests matched the full replay in every run.

**Incremental Silver (SID-29).** The same benchmark, canonical and Silver stage times from the COMMITTED line (load average 7–12):

| Scale | Full build: canonical / rest of Silver | Incremental: canonical / rest of Silver | Correction end to end |
|---|---|---|---|
| 1,000 units, 24 h (16,901 events) | 4.1–6.1 s / 7.7–9.6 s | 5.0 s / 9.3 s | 25.9 s |
| 4,000 units, 48 h (125,201 events) | 9.4–13.4 s / 9.2–13.4 s | 7.8–9.1 s / 12.3–22.5 s (four runs) | 41.0–60.2 s (52.8 s before) |

The incremental build now writes only the affected rows, and Silver matched the full replay table by table in every run. It is not yet faster in a way that matters: at these sizes both builds are dominated by a fixed cost per Spark job and Delta commit, and the incremental build runs more of them (a restore check and a MERGE per table, scope searches). What still grows with history is each MERGE scanning its whole target table for matches and the read of silver/events_parsed for the changed eventIds; partitioning or clustering those tables by key, or deletion vectors, would bound that (one run with deletion vectors was not measurably faster). Above 48 h at 4,000 units the benchmark's initial full build fails in Gold (Model.exposure: Kryo cannot serialize java.nio.HeapByteBuffer under Java 17 module rules), on main as well; it is not caused by SID-29.

Skew is not yet measured: hot sensors, container fanout, overlapping intervals and long product histories. Latency, throughput and recovery targets must be agreed and load-tested; none is claimed.

## 8. Investigation queries

ImpactQueries.java serves "which stock and which recipients are affected" from one committed snapshot. A query reads the snapshot's state (section 6) and `silver/shipments` at the Delta versions its manifest recorded, so Gold and shipments agree with each other and with the snapshot.

**Filters**, combined with AND: productId; lotId, which matches any of a unit's `lotIds`; shipmentId and recipientId, which match any of its shipments; location, the latest known one; and assessment, the outcome. Each result row is one unit with its full assessment evidence: metrics, reasons, flags, `sourceRevisions`, lineage and other evidence IDs. It also carries the latest known location, `inventoryRecordedAt`, `inventoryAgeSeconds` and `inventoryStale` as of the snapshot's `dataAsOf`, and its shipments (shipmentId, recipientId, dispatch, delivery, and the source event and revision). `evidence(scope, snapshot, productId)` returns the intervals behind one unit's assessment.

**Shipments.** A SHIPMENT line ships a single unit, or a container: every unit resolved into it at the dispatch instant, through cartons and pallets. A unit that left the container before dispatch is not shipped with it.

**Scope.** Every call takes one, and there is no unscoped entry point:

| Scope | Reads | Out-of-scope request |
|---|---|---|
| investigator | Every unit and every shipment | – |
| recipient (one or more recipient IDs) | Units shipped to its recipients, with only those shipments | A recipientId filter for another recipient is refused (AccessDenied), as is evidence for a unit it cannot see |
| site (one or more locations) | Units whose latest known location is one of its sites, without shipments | A location filter for another site is refused |

An explicit out-of-scope filter is refused, not answered with an empty page, so a caller can tell "nothing there" from "not yours". The prototype has no authentication: the deployment must establish who the caller is and which scope it gets (CC-15).

**Pagination.** Results are ordered by productId, and pages use keyset pagination: each page returns at most `limit` rows and a cursor. The cursor names the snapshot, the last unit and a fingerprint of the filters and scope. A later page reads that same snapshot whatever has been published since, so a reader paging through results never misses or repeats a unit and never mixes snapshots. A cursor from another query or scope, a malformed cursor, and a cursor naming a snapshot that is not committed are all refused. The cursor carries no authority: the scope is checked on every call. A new query, without a cursor, pins the newest committed snapshot.

**No patient identity.** Recipients are organizations or sites. No stored or returned field identifies a patient, and no query needs one.

`io.coldchain.Query` runs a query from the command line (runbook).

## 9. Review items and the case store

Reviewers are told when an investigation item appears, changes or goes away, without being told twice. The handoff is a transactional outbox: the items are Gold rows of the snapshot, published by its manifest like everything else, and delivered afterwards.

**Items** (Outbox.java). For every unit a snapshot recomputed or removed, the new assessment is compared with the unit's assessment in the previous snapshot's state. QUALITY_REVIEW and INSUFFICIENT_DATA need review; NO_OBSERVED_EXCURSION does not.

| change | changeReason | When |
|---|---|---|
| NEW | ENTERED_REVIEW | The unit needs review and did not before (or was not assessed before) |
| REVISED | ASSESSMENT_CHANGED | It needed review and still does, with a different assessmentHash |
| WITHDRAWN | LEFT_REVIEW | It needed review and now has NO_OBSERVED_EXCURSION |
| WITHDRAWN | UNIT_REMOVED | It needed review and no longer has an assessment (its only membership withheld, say) |

A unit recomputed to the same assessment, or one that never needed review, produces no item: a retried event or a recomputation that changes nothing notifies nobody. Each item carries both assessments and snapshots, the reasons and flags, lots and product codes, policy and rule versions, and `sourceRevisions`, the exact event revisions the assessment rests on (the last assessment's, for a removed unit); intervals and segments are read by productId and snapshotId (section 8). The contract is `contracts/review-item-v1.schema.json`, with examples in `contracts/review-items/`.

**Keys.** `deliveryId` is the SHA-256 of productId, snapshotId and assessmentHash: one per assessment revision. `reviewItemId` is the case, one per unit, since an assessment covers the unit's whole retained history. snapshotId orders an item's revisions.

**Publication.** The items are written before the manifest and removed with the snapshot's other uncommitted rows on a retry, so an item exists exactly when its assessment is published. Replay writes items into its own namespace and never relays them.

**Relay.** After each publication, and every 10 s on a timer, the pipeline relays every committed snapshot not yet in `outbox/relayed`, oldest first. It sends the snapshot's items to `coldchain.review-items` (an idempotent producer with acks=all, keyed by productId so a unit's items keep their order on one partition), waits until Kafka has accepted all of them, and only then records the snapshot. A crash between the two resends the snapshot: delivery is at least once. A failing broker defers the relay (RELAY DEFERRED in the log); it never fails the batch, and the timer retries when no new input arrives.

**Case store** (CaseStore.java, the `cases` service). A Spark stream over `coldchain.review-items`, with its own checkpoint, so a restart reprocesses the batch it had not finished. Both writes are idempotent:
- `cases/review_notifications` inserts an item only if its deliveryId is new: a reviewer is notified once per assessment revision.
- `cases/review_items` holds the newest item per case and takes an item only when its snapshotId is greater than the one held, so a duplicate or an older revision arriving late changes nothing. NEW and REVISED leave the case OPEN; WITHDRAWN marks it WITHDRAWN, and a later NEW reopens it.

**Tests.** AlertTest runs the real pipeline through NEW, REVISED, an unchanged retry, LEFT_REVIEW, re-entry and UNIT_REMOVED. It stops the relay after Kafka accepted a snapshot (resent later), stops a batch after writing its items but before the manifest (never relayed), stops the case store between its two writes, and redelivers everything, including an older revision after a newer one. The case store ends equal to one clean delivery of each committed item, with one notification per item. `scripts/crash-check.sh` does the same in Compose with a real halt after the relay and a killed case store.

**Limitations.** Reviewer actions (acknowledging, closing or assigning a case) are not modeled; a case's status reflects the pipeline only. Items carry stored flags, not STALE_INVENTORY, which depends on when a snapshot is read. A consumer that falls behind by more than the topic's 7-day retention must be rebuilt from `gold/review_outbox` (runbook). A change of rule, policy or code version recomputes every unit in full, so every changed assessment is reported as REVISED at once. One relay per data namespace is assumed, like one pipeline writer.

## 10. Code responsibilities

Timelines.java: assignment and custody revision history, timeline anomalies and mapping invalidations. Lineage.java: direct memberships, lineage anomalies and resolved unit-to-container intervals. Model.java: contract validation with per-rule reason codes, quarantine and conflict cases with resolution rules, semantic payload hashes, canonical revisions, interval joins and assessment SQL. Exposure.java: interval merging, differences and the exact source revisions behind each interval. Policy.java: outcome states, reasons, flags and their versioned definition. Snapshots.java: the pinned-snapshot reader and state resolution, the manifest and digests, the lineage guard and removal of uncommitted rows. Impact.java: changed events, affected units and the events they depend on. Shipments.java: the units each shipment line carried. ImpactQueries.java and Query.java: scoped, paginated investigation queries over one committed snapshot. Outbox.java: review items of each snapshot, their contract and the relay. CaseStore.java: the idempotent reviewers' case store. Replay.java: full recomputation of a namespace's Bronze into an isolated namespace, compared with the source snapshot. Pipeline.java: Kafka source, foreachBatch persistence and committed publication. Simulator.java: synthetic initial state, retries, malformed event and assignment correction. VerifyDemo.java: read committed assessments and check the expected correction. Inspect.java: print persisted-state counts used by scripts/restart-check.sh and scripts/crash-check.sh. BronzeTest.java: provenance fields, source-system fallback, replay without duplicates, repeated payloads as evidence, and appending to a Bronze table from before the provenance columns. ModelTest.java: six real Spark tests for correction/deduplication, interval union, invalid/conflicting contracts, published examples, the reason code for each contract violation, and formatting-insensitive conflict detection. SilverTest.java: quarantine evidence and resolution by correction, conflict cases with all variants and resolution by a higher revision, and the case log recording each transition once. LineageTest.java: the exact units affected before and after a pallet split, merge and repack over time, and contradictions, level mismatches and cycles that are reported and kept out of lineage. TimelineTest.java: a correction that arrives before the revision it replaces invalidates the former and revised units, one sensor in two containers versus two sensors in one, and custody handover, gap and overlap. ExposureTest.java: half-open boundaries, duplicates and overlapping sensors counted once, gaps persisted as intervals, a split assignment, and evidence naming the exact revisions used; every case checks that each Gold duration is the sum of its persisted, disjoint intervals. PolicyTest.java: exact allowance, limit, uncertainty and inventory-age thresholds, partial coverage, mixed specification periods, each ambiguity flag beside the outcome, stale and unknown inventory, and outcomes that match the persisted policy. PublicationTest.java: failures injected between the Gold writes stay invisible, retries reproduce a clean run's digests, a replay after the manifest writes nothing, a new checkpoint lineage is refused, and the manifest records identity, high-watermarks, table versions and a reproducible evidence digest. IncrementalTest.java: ten batches of corrections of every kind, each recomputing exactly the affected units with a state equal to a full recomputation, and Replay into an isolated namespace. ScaleBenchmark.java (run on demand): correction latency at a synthetic scale. QueryTest.java: every filter with full evidence and latest-known location, each scope and its refusals, and paging that stays on its snapshot while a correction is published between pages. AlertTest.java: review items for every kind of assessment change, the published item examples, and relay, publication and case-store crashes with redelivery, without loss or duplicate notification.

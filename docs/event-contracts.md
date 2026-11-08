# Source event contracts (v1)

Contract for every record value on the four cold-chain topics. Machine-readable schema: [`contracts/coldchain-event-v1.schema.json`](../contracts/coldchain-event-v1.schema.json) (`$id` `urn:coldchain:event:v1`). One example per kind: [`contracts/examples/`](../contracts/examples/). `Model.parse` is the enforcing implementation. `ModelTest.publishedExamplesSatisfyTheContract` fails if any published example stops passing it.

## Topics and source ownership

| Topic | Kinds | Owning source (role) | Kafka key |
|---|---|---|---|
| coldchain.sensors | READING, CALIBRATION | Sensor telemetry gateway (readings, and calibration certificates relayed from the metrology system) | eventId |
| coldchain.traceability | PLACEMENT, PACKING, ASSIGNMENT, CUSTODY | Serialization / packing system (placements and packing), logistics operations (sensor-to-container assignments and custody) | eventId |
| coldchain.specifications | SPEC | Quality assurance (approved storage conditions) | eventId |
| coldchain.inventory | INVENTORY | Warehouse management system | eventId |

The owners are roles until named systems and contacts are agreed with each organization.

**Source-system identity.** Producers should set the Kafka header `source-system` to a stable name for the producing system (for example `gateway-eu-3`). Bronze stores it as `sourceSystem` with `sourceSystemDeclared = true`. If the header is missing or blank, Bronze records the topic's owner role instead (`sensor-gateway`, `traceability`, `quality-assurance` or `warehouse-management`) with `sourceSystemDeclared = false`. The header is recorded as provenance and not checked against the topic owner. It is not part of the semantic payload hash. Only the owner publishes, corrects or revises events on its topic. A kind arriving on any other topic is invalid. Processing does not depend on Kafka key ordering, because revisions are resolved by number and not by arrival order.

## Common fields (all kinds)

| Field | Type | Rule |
|---|---|---|
| eventId | string | Required and must contain a non-whitespace character. Globally unique across all kinds, and keeps the same kind across revisions. |
| revision | integer | Required, ≥ 1. |
| kind | enum | READING, CALIBRATION, PLACEMENT, PACKING, ASSIGNMENT, CUSTODY, SPEC or INVENTORY. |
| eventTime | RFC 3339 timestamp | Required. When the source recorded the fact; no more than 5 minutes after the Kafka record timestamp. |

## Kinds

| Kind | Required fields | Units and meaning |
|---|---|---|
| READING | sensorId, startTime, endTime, and either temperatureC or temperature with temperatureUnit | temperatureC is °C. temperature is in temperatureUnit, which is C, F or K, and is normalized to °C (rounded to 10⁻⁶). Either way the value must be finite and −100 to 100 °C. The value applies uniformly across [startTime, endTime). startTime = endTime is a **point observation** at that instant. endTime is no more than 5 minutes after the Kafka timestamp, so a reading cannot cover the future. |
| CALIBRATION | sensorId, startTime, endTime, uncertaintyC | The sensor has a valid calibration during the interval. uncertaintyC is its expanded measurement uncertainty (±), 0 to 10 °C. |
| PLACEMENT | productId, productCode, lotId, containerId, startTime, endTime | The serialized unit productId is in containerId during the interval. |
| ASSIGNMENT | sensorId, containerId, startTime, endTime | The sensor measures that container during the interval. |
| CUSTODY | containerId, custodianId, startTime, endTime | The custodian (an organization or site) is responsible for the container during the interval. |
| SPEC | productCode, startTime, endTime, minC, maxC, maxExcursionSeconds; optional synthetic | Acceptable range [minC, maxC] in °C, inclusive, finite, −100 to 100, with minC ≤ maxC. maxExcursionSeconds (integer ≥ 0) is the cumulative time out of range allowed before quality review. The interval is when the specification is effective; eventTime is when it was approved. synthetic (boolean) marks illustrative thresholds. |
| INVENTORY | productId, location, status | status is IN_STOCK or DISPATCHED. |
| PACKING | childId, childLevel, parentId, parentLevel, startTime, endTime; also productCode and lotId when childLevel is UNIT | childId is inside parentId during the interval. childLevel is UNIT, CARTON or PALLET; parentLevel is CARTON, PALLET or CONTAINER and must be above childLevel (UNIT < CARTON < PALLET < CONTAINER). A UNIT childId is the serialized unit's productId. childId and parentId differ. |

Every identifier field (productId, productCode, lotId, containerId, sensorId, location, childId, parentId, custodianId) must contain a non-whitespace character.

### Specifications and measurement quality

**Precedence.** Specifications for one productCode may overlap. Inside an overlap the specification with the **latest approval (eventTime)** applies; the others do not apply there. If the latest approvals tie, the window cannot be ordered: no specification applies, and the unit shows missing specification and missing coverage. A specification's revision replaces it, as for every kind. `silver/specifications` holds the specification that applies to each product code over time. `silver/specification_overlaps` lists every overlapping window with all candidates and its resolution: LATEST_APPROVAL_APPLIES (with the winner) or UNRESOLVED_SAME_APPROVAL_TIME. Specifications are always in °C.

**Synthetic thresholds.** A SPEC with `synthetic: true` is used only where the deployment sets `ALLOW_SYNTHETIC_SPECS=true` (the Compose demo and CI demo job do). Anywhere else it is ignored entirely: it neither applies nor competes with approved specifications. Gold reports `syntheticThresholds` on every assessment that used one. The demo's DRUG-DEMO thresholds are synthetic.

**Calibration and uncertainty.** A reading counts as calibrated for the part of its interval (or, for a point, the instant) that a CALIBRATION of its sensor covers. Overlapping calibrations are allowed; the largest uncertainty applies. `silver/readings` lists each current reading with its normalized °C value, the value and unit as reported, whether it is a point, the calibrations that cover it, the largest uncertainty, the calibrated seconds and a status: CALIBRATED, PARTIALLY_CALIBRATED or UNCALIBRATED.

**Point observations and sampling gaps.** A point reading is never extended into an interval and never fills a gap: it gives no coverage. A point outside its specification is still an observed excursion. `silver/sampling_gaps` lists every window inside a current assignment where its sensor has no interval reading. The pipeline never interpolates or holds a value between readings.

### Assignments and custody

Each assignment and custody event keeps all of its valid revisions in `silver/timelines`, once per revision with the topic, partition, offset, Kafka time and source system it first arrived with. The highest revision is `current` and is the one used. A correction can arrive before the revision it replaces; revision numbers decide, not arrival order.

| Case | Treatment |
|---|---|
| One sensor assigned to two containers at the same instant | SENSOR_IN_TWO_CONTAINERS in `silver/timeline_anomalies`. It is impossible to know which container was measured, so both assignments are left out for their whole interval and the units show missing coverage. |
| Two sensors assigned to one container | Allowed. Both sensors' readings count; coverage is the union and any out-of-range reading counts as exposure. |
| Two different custodians for one container at the same instant | CUSTODY_OVERLAP. Both custody events are left out for their whole interval. The same custodian recorded twice is not a contradiction. |
| A container with no custodian for part of the time a unit is in it | Gold reports it as `missingCustodySeconds`. Custody does not change the exposure assessment. |
| Handover at one instant (one custody ends when the next starts) | Neither a gap nor an overlap, because intervals are half-open. |

When an assignment revision changes its sensor, container or interval, `silver/mapping_invalidations` lists every unit the **former** revision mapped (side FORMER) and every unit the **revised** one maps (side REVISED), with the window. A revision that changes neither (for example only eventTime) invalidates nothing. Both sides are recomputed in the next Gold snapshot, and earlier assessments remain in history.

### Packaging lineage

PLACEMENT is a membership of a unit directly in a container (UNIT → CONTAINER); PACKING covers cartons and pallets. A unit is in a container at an instant when a chain of memberships (unit → carton → pallet → container, or any shorter chain that ends at a container) holds at that instant. Splits, merges and repacks need no special event: the old membership ends and the new one starts at the same instant, and because intervals are half-open the unit is in exactly one place at the boundary. A correction to a membership is a higher revision of its event, as for every kind.

Silver reports three lineage anomalies in `silver/lineage_anomalies`:

| Anomaly | Meaning |
|---|---|
| CONTRADICTORY_PARENT | One child is in two different parents at the same instant (including a unit packed in a carton and also placed directly in a container). The window is the overlap. |
| LEVEL_MISMATCH | One ID appears at two packaging levels, for example as a carton in one event and a pallet in another. |
| CYCLE | An ID is its own ancestor at some instant. This needs a level mismatch, so both are reported. |

Every membership involved in an anomaly is left out of resolved lineage, for its whole interval. The affected units still count as expected in Gold, so they show up as missing coverage (INSUFFICIENT_DATA) instead of being placed by guesswork. The anomalies are detected on valid contracts, so they are not quarantine reason codes.

## Time

- Timestamps are RFC 3339 with `Z` or a numeric offset. They are normalized to UTC.
- Every interval is half-open, [startTime, endTime), with startTime < endTime. Intervals that touch (one ends exactly when the next starts) do not overlap.
- Interval bounds are whole seconds; `.000` is accepted but `.500` is rejected. Durations are counted in seconds, so the pipeline rejects fractional bounds instead of rounding them.

## Revisions and identity

- A correction reuses the eventId with a higher revision. The highest valid revision wins, whatever order the revisions arrive in, and gaps in the numbering are allowed.
- A retry repeats the same eventId and revision and must have the same **semantic payload**. Differences only in key order, whitespace, timestamp spelling (`Z` or `+00:00`, `.000`) or number spelling (`8` or `8.0`) are not conflicts.
- Two different semantic payloads for the same eventId and revision, or one eventId used for two kinds, open a **conflict case** in `silver/conflict_cases`. While any case for an eventId is open, all of its revisions are withheld from Silver; no variant is chosen. See [Conflict cases](#conflict-cases).

The semantic payload hash (`payloadHash`) is SHA-256 over the parsed contract fields, in schema order, with timestamps rendered in UTC to microsecond precision. Fields outside the contract are ignored, so they neither cause conflicts nor reach Silver.

## Validation outcome

Records that are not valid JSON, or that break any rule above, stay in Bronze as evidence and are never promoted. They are copied to `silver/quarantine` with:

- `caseId` (a hash of the source coordinates) and the coordinates themselves: `topic`, `partition` and `offset`. Bronze is insert-only on those coordinates, so they are a stable reference to the original record.
- `raw`, `kafkaTime`, `sourceSystem` and `ingestedAt` from Bronze.
- `eventId` and `revision` when they could be read, so stewards can see which event the record was meant to be.
- `reasons`: every rule the record breaks, in the order of the table below.
- `status`, `resolution` and `resolvedByRevision` (see [Resolution rules](#resolution-rules)).

### Reason codes

| Code | Applies to | Meaning |
|---|---|---|
| MALFORMED_JSON | all | Not a strict JSON object (RFC 8259). Single quotes, `NaN` and `Infinity` are rejected even though Spark's reader accepts them. When this code is present, no other code is reported. |
| INVALID_ID:eventId | all | Missing, or no non-whitespace character. |
| INVALID_REVISION | all | Missing, not an integer, or below 1. |
| INVALID_KIND | all | Missing or not one of the eight kinds. |
| WRONG_TOPIC | known kinds | The kind arrived on a topic that does not own it. |
| MISSING_EVENT_TIME | all | eventTime is missing or not an RFC 3339 timestamp. |
| EVENT_TIME_AHEAD_OF_KAFKA | all | eventTime is more than 5 minutes after the Kafka record timestamp. |
| INVALID_ID:&lt;field&gt; | per kind | A required identifier (sensorId, productId, productCode, lotId, containerId, location) is missing or blank. |
| INVALID_INTERVAL | READING, CALIBRATION, PLACEMENT, PACKING, ASSIGNMENT, CUSTODY, SPEC | A bound is missing, or startTime is not before endTime. A READING may have startTime = endTime (a point). |
| FRACTIONAL_SECONDS | same kinds | A bound is not a whole second. |
| READING_ENDS_AHEAD_OF_KAFKA | READING | endTime is more than 5 minutes after the Kafka record timestamp. |
| AMBIGUOUS_TEMPERATURE | READING | temperatureC is given together with temperature or temperatureUnit. |
| INVALID_UNIT:temperatureUnit | READING | temperature is given without temperatureUnit, or the unit is not C, F or K. |
| OUT_OF_RANGE:temperatureC, :minC, :maxC | READING, SPEC | Missing, or outside −100 to 100 °C. For a reading in another unit, checked after normalization. |
| OUT_OF_RANGE:uncertaintyC | CALIBRATION | Missing, or outside 0 to 10 °C. |
| SPEC_MIN_ABOVE_MAX | SPEC | minC is greater than maxC. |
| INVALID_EXCURSION_ALLOWANCE | SPEC | maxExcursionSeconds is missing, not an integer, or negative. |
| INVALID_FLAG:synthetic | SPEC | synthetic is present but not a boolean. A mistyped flag must not turn demo thresholds into real ones. |
| INVALID_STATUS | INVENTORY | status is missing or not IN_STOCK or DISPATCHED. |
| INVALID_ID:custodianId | CUSTODY | Missing or blank (containerId is checked as for ASSIGNMENT). |
| INVALID_ID:childId, :parentId | PACKING | Missing or blank. productCode and lotId are also required (INVALID_ID) when childLevel is UNIT. |
| INVALID_LEVEL:childLevel, :parentLevel | PACKING | Missing or not an allowed level for that end. |
| PARENT_NOT_ABOVE_CHILD | PACKING | parentLevel is not above childLevel, for example a pallet in a carton. |
| SELF_CONTAINED | PACKING | childId equals parentId. |

Codes are reported independently, so a record with a zero revision and a blank sensorId gets both. Rules for a kind are checked only when the kind is known.

### Conflict cases

`silver/conflict_cases` has one row per case:

- `PAYLOAD_MISMATCH`: one eventId and revision with more than one semantic payload. `revision` is the disputed revision.
- `KIND_MISMATCH`: one eventId used for more than one kind. `revision` is null.

Each case lists every competing record in `variants` (payloadHash, kind, revision, topic, partition, offset, sourceSystem, kafkaTime), including identical retries, so stewards can see which source sent what. `caseId` is a hash of the case type, eventId and revision, and it stays the same across rebuilds.

### Resolution rules

Bronze is never edited, so a source fixes a problem by publishing a corrected event. Each rebuild reprocesses all evidence and applies these rules:

| Case | Resolved when | resolution |
|---|---|---|
| Invalid record | The same eventId has a valid revision **higher** than the invalid record's revision (a missing or unreadable revision counts as 0). A valid retry at the same revision does not resolve it. | SUPERSEDED_BY_VALID_REVISION |
| PAYLOAD_MISMATCH | The same eventId has a valid revision higher than the disputed one. The eventId is promoted at its highest revision, unless that revision is itself disputed. | SUPERSEDED_BY_HIGHER_REVISION |
| KIND_MISMATCH | Never automatically. The source must publish under a new eventId; the old eventId stays withheld. | — |
| Invalid record with no readable eventId | Never automatically. | — |

Because revisions only grow, a resolved case cannot reopen. A new dispute at a higher revision opens its own case.

`silver/case_log` is the audit trail. It is insert-only on (caseId, status), so it keeps the snapshot and time at which each case was first seen OPEN and first seen RESOLVED, with the resolution rule and the revision that resolved it. Replaying a batch adds nothing.

## Where each rule is enforced

The JSON Schema covers per-field rules: presence, type, non-whitespace identifiers, enums, ranges and whole-second bounds. These rules are enforced only in code because JSON Schema cannot express them: startTime < endTime, minC ≤ maxC, parent level above child level, childId ≠ parentId, kind-to-topic mapping, the 5-minute skew against the Kafka timestamp, and the revision/conflict rules. The schema is not yet run against live traffic; it documents the contract, and the examples are checked against both the schema and the code.

## Changing the contract

Adding an optional field or a new enum value is a minor change: update the schema, examples, parser and tests together. Removing a field, changing a meaning or unit, or tightening a rule is a v2. Run v2 on new topics (or with an explicit version field) and keep v1 evidence readable. Records do not yet carry a version field: v1 is implied by the topic.

## Known limitations

- There is no manual resolution: a steward cannot yet acknowledge or close a KIND_MISMATCH or an unattributable invalid record. Those stay OPEN.
- A field with the wrong JSON type is reported under that field's code (for example INVALID_REVISION for `"revision": "two"`), not as a separate type error.
- Specification precedence uses approval time only. There is no approval workflow, approver identity or electronic signature, and a mistaken later approval wins until it is revised.
- Calibration certificates are not checked against a metrology system, and the uncertainty is a single symmetric value per calibration.
- There is no maximum reading interval: a reading's interval is the gateway's assertion, so a gateway that holds one sample for a long interval is not detected.
- Specifications are only in °C; only readings accept other units.
- A membership needs an end time. An open-ended membership ("still on the pallet") has to be sent with a provisional end and revised later.
- A lineage anomaly excludes every membership involved for its whole interval, not only the overlapping window. There is no steward resolution for anomalies yet; the source has to send corrected revisions.
- An unknown field is ignored without a warning, so a misspelled optional field is silently lost.
- The 5-minute skew is a fixed constant and not configurable per source.

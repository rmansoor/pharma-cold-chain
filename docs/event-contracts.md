# Source event contracts (v1)

Contract for every record value on the four cold-chain topics. Machine-readable schema: [`contracts/coldchain-event-v1.schema.json`](../contracts/coldchain-event-v1.schema.json) (`$id` `urn:coldchain:event:v1`). One example per kind: [`contracts/examples/`](../contracts/examples/). `Model.parse` is the enforcing implementation. `ModelTest.publishedExamplesSatisfyTheContract` fails if any published example stops passing it.

## Topics and source ownership

| Topic | Kinds | Owning source (role) | Kafka key |
|---|---|---|---|
| coldchain.sensors | READING | Sensor telemetry gateway | eventId |
| coldchain.traceability | PLACEMENT, ASSIGNMENT | Serialization / packing system (placements), logistics operations (sensor-to-container assignments) | eventId |
| coldchain.specifications | SPEC | Quality assurance (approved storage conditions) | eventId |
| coldchain.inventory | INVENTORY | Warehouse management system | eventId |

The owners are roles until named systems and contacts are agreed with each organization.

**Source-system identity.** Producers should set the Kafka header `source-system` to a stable name for the producing system (for example `gateway-eu-3`). Bronze stores it as `sourceSystem` with `sourceSystemDeclared = true`. If the header is missing or blank, Bronze records the topic's owner role instead (`sensor-gateway`, `traceability`, `quality-assurance` or `warehouse-management`) with `sourceSystemDeclared = false`. The header is recorded as provenance and not checked against the topic owner. It is not part of the semantic payload hash. Only the owner publishes, corrects or revises events on its topic. A kind arriving on any other topic is invalid. Processing does not depend on Kafka key ordering, because revisions are resolved by number and not by arrival order.

## Common fields (all kinds)

| Field | Type | Rule |
|---|---|---|
| eventId | string | Required and must contain a non-whitespace character. Globally unique across all kinds, and keeps the same kind across revisions. |
| revision | integer | Required, ≥ 1. |
| kind | enum | READING, PLACEMENT, ASSIGNMENT, SPEC or INVENTORY. |
| eventTime | RFC 3339 timestamp | Required. When the source recorded the fact; no more than 5 minutes after the Kafka record timestamp. |

## Kinds

| Kind | Required fields | Units and meaning |
|---|---|---|
| READING | sensorId, startTime, endTime, temperatureC | °C, finite, −100 to 100. The value applies uniformly across [startTime, endTime). endTime is no more than 5 minutes after the Kafka timestamp, so a reading cannot cover the future. |
| PLACEMENT | productId, productCode, lotId, containerId, startTime, endTime | The serialized unit productId is in containerId during the interval. |
| ASSIGNMENT | sensorId, containerId, startTime, endTime | The sensor measures that container during the interval. |
| SPEC | productCode, startTime, endTime, minC, maxC, maxExcursionSeconds | Acceptable range [minC, maxC] in °C, inclusive, finite, −100 to 100, with minC ≤ maxC. maxExcursionSeconds (integer ≥ 0) is the cumulative time out of range allowed before quality review. |
| INVENTORY | productId, location, status | status is IN_STOCK or DISPATCHED. |

Every identifier field (productId, productCode, lotId, containerId, sensorId, location) must contain a non-whitespace character.

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
| INVALID_KIND | all | Missing or not one of the five kinds. |
| WRONG_TOPIC | known kinds | The kind arrived on a topic that does not own it. |
| MISSING_EVENT_TIME | all | eventTime is missing or not an RFC 3339 timestamp. |
| EVENT_TIME_AHEAD_OF_KAFKA | all | eventTime is more than 5 minutes after the Kafka record timestamp. |
| INVALID_ID:&lt;field&gt; | per kind | A required identifier (sensorId, productId, productCode, lotId, containerId, location) is missing or blank. |
| INVALID_INTERVAL | READING, PLACEMENT, ASSIGNMENT, SPEC | A bound is missing, or startTime is not before endTime. |
| FRACTIONAL_SECONDS | READING, PLACEMENT, ASSIGNMENT, SPEC | A bound is not a whole second. |
| READING_ENDS_AHEAD_OF_KAFKA | READING | endTime is more than 5 minutes after the Kafka record timestamp. |
| OUT_OF_RANGE:temperatureC, :minC, :maxC | READING, SPEC | Missing, or outside −100 to 100 °C. |
| SPEC_MIN_ABOVE_MAX | SPEC | minC is greater than maxC. |
| INVALID_EXCURSION_ALLOWANCE | SPEC | maxExcursionSeconds is missing, not an integer, or negative. |
| INVALID_STATUS | INVENTORY | status is missing or not IN_STOCK or DISPATCHED. |

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

The JSON Schema covers per-field rules: presence, type, non-whitespace identifiers, enums, ranges and whole-second bounds. These rules are enforced only in code because JSON Schema cannot express them: startTime < endTime, minC ≤ maxC, kind-to-topic mapping, the 5-minute skew against the Kafka timestamp, and the revision/conflict rules. The schema is not yet run against live traffic; it documents the contract, and the examples are checked against both the schema and the code.

## Changing the contract

Adding an optional field or a new enum value is a minor change: update the schema, examples, parser and tests together. Removing a field, changing a meaning or unit, or tightening a rule is a v2. Run v2 on new topics (or with an explicit version field) and keep v1 evidence readable. Records do not yet carry a version field: v1 is implied by the topic.

## Known limitations

- There is no manual resolution: a steward cannot yet acknowledge or close a KIND_MISMATCH or an unattributable invalid record. Those stay OPEN.
- A field with the wrong JSON type is reported under that field's code (for example INVALID_REVISION for `"revision": "two"`), not as a separate type error.
- Overlapping specifications and simultaneous assignments are valid contracts; resolving them belongs to CC-06 and CC-07.
- An unknown field is ignored without a warning, so a misspelled optional field is silently lost.
- The 5-minute skew is a fixed constant and not configurable per source.

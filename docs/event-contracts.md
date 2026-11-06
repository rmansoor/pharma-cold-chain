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
- Two different semantic payloads for the same eventId and revision, or one eventId used for two kinds, make the eventId **conflicting**. All of its revisions are withheld from Silver and listed in `silver/conflicting_ids` until the source fixes it.

The semantic payload hash (`payloadHash`) is SHA-256 over the parsed contract fields, in schema order, with timestamps rendered in UTC to microsecond precision. Fields outside the contract are ignored, so they neither cause conflicts nor reach Silver.

## Validation outcome

Records that are not valid JSON, or that break any rule above, stay in Bronze as evidence. They are copied to `silver/quarantine` as `INVALID_CONTRACT` with their topic, partition and offset, and they are never promoted.

## Where each rule is enforced

The JSON Schema covers per-field rules: presence, type, non-whitespace identifiers, enums, ranges and whole-second bounds. These rules are enforced only in code because JSON Schema cannot express them: startTime < endTime, minC ≤ maxC, kind-to-topic mapping, the 5-minute skew against the Kafka timestamp, and the revision/conflict rules. The schema is not yet run against live traffic; it documents the contract, and the examples are checked against both the schema and the code.

## Changing the contract

Adding an optional field or a new enum value is a minor change: update the schema, examples, parser and tests together. Removing a field, changing a meaning or unit, or tightening a rule is a v2. Run v2 on new topics (or with an explicit version field) and keep v1 evidence readable. Records do not yet carry a version field: v1 is implied by the topic.

## Known limitations

- Validity rejects a whole event and does not report which rule failed; quarantine rows only say `INVALID_CONTRACT`. Per-rule reason codes belong to CC-04 (SID-16).
- Overlapping specifications and simultaneous assignments are valid contracts; resolving them belongs to CC-06 and CC-07.
- An unknown field is ignored without a warning, so a misspelled optional field is silently lost.
- The 5-minute skew is a fixed constant and not configurable per source.

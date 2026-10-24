# 0004 - Partition topics by source-system/tenant ID; enforce schemas via Schema Registry

## Status
Accepted

## Context
Two related design questions:

1. **Topic layout & partitioning**: should all content systems share one topic, or get
   separate topics? And how should partitions be assigned so that ordering guarantees
   are meaningful?
2. **Contract enforcement**: without a schema registry, nothing stops a producer from
   shipping a malformed or incompatible payload that breaks consumers at runtime —
   this is exactly the kind of integration fragility the project is meant to avoid.

Kafka only guarantees ordering *within a partition*. If events from the same source
system / tenant land on different partitions, a consumer can observe
`PermissionsChanged` before `ItemCreated` for the same item, which is a correctness
bug for most downstream consumers (search index, audit log).

## Decision
- **Topic layout**: one logical topic per domain event category, e.g.
  `content.items.v1`, `content.permissions.v1`, `content.workflow.v1`, rather than one
  topic per source system. This keeps the number of topics bounded regardless of how
  many content systems are added, and lets consumers subscribe by concern (e.g. "all
  permission changes") rather than by source.
- **Partitioning key**: the Kafka message key is the `source` + tenant/item ID
  (`{source-system}:{item-id}`), extracted from the CloudEvents envelope's
  `partitionkey` extension attribute (ADR 0003). This guarantees all events for a given
  item from a given source land on the same partition and are processed in order
  relative to each other.
- **Schema Registry**: Redpanda's built-in Schema Registry (Confluent-API-compatible)
  enforces a **JSON Schema** per event `type` on the `data` field. Producers register/
  validate against the registry before publish; consumers validate on deserialize.
  Compatibility mode is set to `BACKWARD` so new optional fields can be added without
  breaking existing consumers.
- **DLQ topics** (ADR 0005) are exempt from strict schema enforcement, since their job
  is to capture whatever failed, including malformed payloads.

## Consequences
- Consumers that only care about one event category subscribe to one topic instead of
  filtering a firehose — demonstrates deliberate topic design, not just "one topic for
  everything."
- Partitioning by source+item ID bounds parallelism by the cardinality of that key;
  acceptable for this project's scale, and documented as a scaling consideration (a
  true multi-tenant production system might shard differently).
- Producers must fetch/validate against the Schema Registry before every publish,
  adding one round-trip (mitigated by the client's local schema cache).
- Schema evolution policy (`BACKWARD` compatibility) must be documented so any future
  event-type change is checked against it deliberately rather than by accident.

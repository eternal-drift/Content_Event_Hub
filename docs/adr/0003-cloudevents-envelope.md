# 0003 - Standardize on CloudEvents envelope format

## Status
Accepted

## Context
Multiple independent "content systems" will produce events. Without a shared envelope,
each producer would invent its own metadata shape (timestamps, IDs, source
identification, content-type), forcing every consumer to special-case each source —
the exact point-to-point coupling this project is meant to argue against.

The CNCF **CloudEvents** spec (v1.0) defines a minimal, standardized envelope
(`id`, `source`, `type`, `specversion`, `time`, `datacontenttype`, `data`, plus
extension attributes) that is broker-agnostic and already has conventions for Kafka
(binary mode: attributes as headers; structured mode: whole envelope as the value).

Options considered:
- **Raw/ad-hoc JSON per producer** — fastest to write, but reinforces tight coupling
  and gives consumers nothing uniform to depend on; rejected as it undercuts the
  project's own thesis.
- **CloudEvents** — small spec surface, industry-recognized, demonstrates
  interoperable-contract thinking without inventing a bespoke envelope.
- **Custom internal envelope** — same benefits as CloudEvents but reinvents a wheel
  with no recognizability outside this repo.

## Decision
Every domain event (`ItemCreated`, `PermissionsChanged`, `WorkflowTransitioned`, etc.)
is wrapped in a **CloudEvents v1.0 structured-mode JSON envelope**:
- `id`: UUID, producer-assigned, used for idempotency/dedup downstream.
- `source`: URI identifying the originating content system, e.g.
  `urn:content-system:alpha`.
- `type`: reverse-DNS-style event type, e.g. `com.contenthub.item.created.v1`.
- `specversion`: `1.0`.
- `time`: RFC3339 timestamp.
- `datacontenttype`: `application/json`.
- `dataschema`: URI pointing at the Schema Registry entry (ties into ADR 0004).
- `data`: the domain-specific payload, itself schema-registry-enforced.
- Extension attribute `partitionkey`: tenant/source ID used for Kafka partitioning.

The envelope itself is captured as a reusable JSON Schema (`CloudEventEnvelope.json`)
in `ContentEventHub.Contracts`, with per-event-type schemas for `data`.

## Consequences
- All consumers can deserialize the envelope uniformly and dispatch on `type` before
  touching the domain-specific payload — clean separation of transport metadata from
  business payload.
- Event-type versioning is explicit via the `.v1` suffix in `type`, giving a clear
  evolution path (`.v2`) without breaking existing consumers.
- Slight verbosity overhead per message compared to a bare payload; acceptable given
  the interoperability payoff.

# 0002 - Use .NET 8 + Confluent.Kafka as the implementation stack

## Status
Accepted

## Context
The stack choice was left open between C#/.NET (Confluent.Kafka) and Java. Key
considerations:
- **Language signal diversification**: if other portfolio projects are already
  Java/Python-heavy, .NET adds a distinct signal. (If the opposite is true, this
  decision should be revisited — see note below.)
- **Confluent.Kafka** (the official .NET client, wrapping librdkafka) has first-class
  support for manual offset commits, consumer group rebalancing callbacks, Avro/JSON
  Schema serializers integrated with Schema Registry, and transactional/idempotent
  producers — everything needed for the DLQ, replay, and backpressure requirements.
- .NET's `BackgroundService` / hosted-service model and `System.Threading.Channels`
  give a clean, idiomatic way to implement the bounded-queue consumer pattern (ADR
  0007) without hand-rolling thread pools.
- ASP.NET Core minimal APIs give a lightweight way to expose the DLQ replay operator
  endpoint and a metrics endpoint without extra framework weight.

## Decision
Build the system in **.NET 10** (the SDK available in the dev environment at
implementation time; the design has no dependency on a specific .NET version and
would work unchanged on .NET 8+), using:
- `Confluent.Kafka` for producer/consumer clients.
- `Confluent.SchemaRegistry` + `Confluent.SchemaRegistry.Serdes.Json` (or `.Avro`) for
  schema-enforced (de)serialization.
- `System.Threading.Channels` for the bounded in-memory queue backing the
  concurrency-limited consumer (ADR 0007).
- ASP.NET Core minimal API for the DLQ inspection/replay endpoint and
  Prometheus metrics endpoint (`prometheus-net`).
- Solution laid out as multiple projects under `src/`: `ContentEventHub.Contracts`
  (CloudEvents + domain event schemas), `ContentEventHub.Producers`,
  `ContentEventHub.Consumers`, `ContentEventHub.DlqOperator`, `ContentEventHub.Shared`.

## Consequences
- Single-language repo (C#) keeps the weekend-to-week scope realistic; no context-switch
  overhead between Java and .NET tooling.
- Confluent.Kafka depends on librdkafka native binaries; Docker base images must use a
  distro with a compatible glibc (standard `mcr.microsoft.com/dotnet/sdk` images are
  fine) — noted as a build consideration, not a design one.
- JSON Schema is chosen over Avro for the registry (see ADR 0004) partly because the
  JSON Schema serdes story in Confluent.Kafka for .NET is simpler and more readable in
  a portfolio repo than generated Avro classes, at a small cost in wire efficiency.

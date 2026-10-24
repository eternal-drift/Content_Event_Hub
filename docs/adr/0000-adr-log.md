# Architecture Decision Record Log

| ADR | Title | Status |
|-----|-------|--------|
| [0001](0001-use-redpanda-for-local-broker.md) | Use Redpanda (Kafka-API-compatible) for local broker | Accepted |
| [0002](0002-dotnet-confluent-kafka-stack.md) | Use .NET 8 + Confluent.Kafka as the implementation stack | Accepted |
| [0003](0003-cloudevents-envelope.md) | Standardize on CloudEvents envelope format | Accepted |
| [0004](0004-topic-partitioning-schema-registry.md) | Partition topics by source-system/tenant ID; enforce schemas via Schema Registry | Accepted |
| [0005](0005-dead-letter-queue-and-replay.md) | Dead-letter queue with operator-driven replay | Accepted |
| [0006](0006-consumer-group-semantics.md) | Differentiated consumer-group semantics (real-time vs. batch/replay-tolerant) | Accepted |
| [0007](0007-backpressure-bounded-concurrency.md) | Bounded-queue consumer with configurable concurrency for backpressure | Accepted |
| [0008](0008-observability-lag-dlq-metrics.md) | Expose consumer-lag and DLQ-depth metrics via Prometheus/Grafana | Accepted |

ADRs follow Michael Nygard's format: Context, Decision, Consequences.

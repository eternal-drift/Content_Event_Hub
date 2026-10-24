# 0001 - Use Redpanda (Kafka-API-compatible) for local broker

## Status
Accepted

## Context
The project needs a Kafka-protocol broker to demonstrate topic partitioning, consumer
groups, replay, and schema enforcement. Running a full Apache Kafka + ZooKeeper (or
even KRaft-mode Kafka) stack locally for a portfolio project adds JVM footprint, more
moving containers, and slower startup, without adding any decision-making substance to
the project. The goal here is to demonstrate event-driven integration patterns, not to
demonstrate standing up Kafka infrastructure itself.

Options considered:
- **Apache Kafka (KRaft mode)** — the "real" thing recruiters expect to see named, but
  heavier to run locally (JVM, more memory, slower cold start) and no material
  difference in the patterns being demonstrated.
- **Redpanda** — single-binary, C++ implementation, wire-compatible with the Kafka
  client protocol (so Confluent.Kafka / librdkafka clients work unmodified), includes
  a built-in schema registry and HTTP proxy, starts in seconds in a single container.
- **Cloud-managed Kafka (Confluent Cloud, MSK)** — adds cost and external dependency,
  overkill for a local/portfolio project and makes the repo non-self-contained.

## Decision
Use **Redpanda** (single-node, via Docker Compose) as the local broker for development,
demos, and CI. Document that the system is Kafka-protocol-compatible and would run
against Apache Kafka or a managed Kafka service unchanged, since the client code talks
the standard Kafka wire protocol via Confluent.Kafka.

## Consequences
- Faster local startup (one container, no ZooKeeper/JVM), lower resource usage —
  friendlier for a laptop demo and for CI.
- Redpanda's built-in schema registry (API-compatible with Confluent Schema Registry)
  removes one extra container dependency.
- Minor risk: a handful of Kafka broker-admin edge cases (e.g. some advanced ACL or
  transaction edge cases) differ between Redpanda and Apache Kafka; not relevant to
  this project's scope.
- The repo's README must state clearly that Redpanda is used as a lightweight,
  wire-compatible stand-in for Apache Kafka, so reviewers don't mistake this for
  avoidance of "real" Kafka.

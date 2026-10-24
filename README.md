# Content Event Hub

A Kafka-based multi-source event notification system that simulates a common enterprise integration problem: several independent content/document-management systems each emit domain events (item created, permissions changed, workflow transitioned), and multiple downstream consumers (search indexing, audit logging, notification forwarding) need those events reliably, in order, and independently of one another - without point-to-point polling integrations or requiring every system to be online at the same time.

> Any number of source systems publish once; any number of consumers subscribe independently, with guaranteed delivery and replay.

## Why this exists

Point-to-point integrations between content systems and their downstream consumers don't scale: every new consumer means every producer has to know about it, and every outage on either side risks lost updates. This project demonstrates the alternative - an event-driven hub where producers and consumers are fully decoupled, failures are handled explicitly (not silently dropped or retried forever), and different consumers can have different delivery semantics without affecting each other.

## Architecture

```
 Producers (simulated content systems)         Kafka / Redpanda                  Consumers  
 ───────────────────────────────────           ──────────────────                ─────────  
 content-system-alpha   ─┐                     content.items.v1      ┌─▶ notification-forwarder  
 content-system-beta    ─┼─▶ CloudEvents ──▶    content.permissions.v1┼─▶ search-indexer (replay-tolerant)  
 content-system-gamma   ─┘   envelope           content.workflow.v1   │  
                                                        │              │  
                                                        ▼              │  
                                              \*.dlq topics ◀───────────┘  (on failure, after retries)  
                                                        │  
                                                        ▼  
                                           DLQ operator tool (inspect / replay / discard)  
  
 Schema Registry (contract enforcement) ── Prometheus (lag, DLQ depth, throughput) ── Grafana dashboard
```

- **Producers** emit standardized [CloudEvents](https://cloudevents.io/) v1.0 envelopes, not raw JSON blobs.

- **Topics** are organized by event category (not by source system) and partitioned by `source + item ID`, so events for the same item stay ordered.

- **Schema Registry** enforces producer/consumer contracts (JSON Schema, `BACKWARD`-compatible evolution).

- **Dead-letter queue + replay**: failed messages land in a per-topic DLQ with failure metadata; an operator tool lets you inspect, replay, or discard them.

- **Two consumer groups** with different semantics: a near-real-time notification forwarder, and a replay-tolerant batch search-indexer.

- **Backpressure**: consumers use a bounded in-memory queue with a configurable worker-pool size, so throughput/memory trade-offs are explicit rather than left to Kafka client defaults.

- **Observability**: consumer lag, DLQ depth, and throughput/failure counters exposed via Prometheus, visualized on a committed Grafana dashboard.

Full rationale for each decision lives in [docs/adr/](file:///Users/ayanmitra/Source/Content_Event_Hub/docs/adr/) - start with the [ADR log](file:///Users/ayanmitra/Source/Content_Event_Hub/docs/adr/0000-adr-log.md).

## Stack

- **.NET 8** / C\#, using `Confluent.Kafka` (librdkafka-based client)

- **Redpanda** as a lightweight, Kafka-wire-protocol-compatible local broker (includes a built-in, Confluent-API-compatible Schema Registry) - see [ADR 0001](file:///Users/ayanmitra/Source/Content_Event_Hub/docs/adr/0001-use-redpanda-for-local-broker.md) for why Redpanda instead of Apache Kafka directly; the client code is standard Kafka protocol and would run against real Kafka unchanged.

- **Prometheus + Grafana** for metrics and dashboards

- **Docker Compose** to bring up the whole stack (broker, schema registry, producers, consumers, DLQ operator, metrics, dashboard) with one command

## Project layout

```
docs/adr/                      Architecture Decision Records  
src/  
  ContentEventHub.Contracts/   CloudEvents envelope + domain event JSON Schemas  
  ContentEventHub.Producers/   Simulated content-system producers  
  ContentEventHub.Consumers/   Notification-forwarder + search-indexer consumer groups  
  ContentEventHub.DlqOperator/ DLQ inspection/replay HTTP API  
  ContentEventHub.Shared/      Shared Kafka/config/metrics plumbing  
docker-compose.yml             Redpanda, Schema Registry, Prometheus, Grafana, services
```

*(Scaffolding in progress - see ADRs for the design this layout implements.)*

## Getting started

```
docker compose up -d        \# Redpanda, Schema Registry, Prometheus, Grafana  
dotnet build  
dotnet run --project src/ContentEventHub.Producers  
dotnet run --project src/ContentEventHub.Consumers
```

- DLQ operator UI/API: `http://localhost:\<port\>/dlq`

- Grafana dashboard: `http://localhost:3000`

- Prometheus: `http://localhost:9090`

(Ports and exact run instructions will be finalized as the Compose file and projects are added.)

## Design decisions

See [docs/adr/](file:///Users/ayanmitra/Source/Content_Event_Hub/docs/adr/) for the full set of ADRs, covering broker choice, stack choice, event envelope format, topic/partitioning strategy, DLQ and replay design, consumer-group semantics, backpressure handling, and observability.


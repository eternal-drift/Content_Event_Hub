# 0008 - Expose consumer-lag and DLQ-depth metrics via Prometheus/Grafana

## Status
Accepted

## Context
A system with producers, consumers, a DLQ, and backpressure logic (ADRs 0005–0007) is
only demonstrably working if its internal state is observable. Without metrics, "does
this system handle failure and backpressure correctly" can only be answered by reading
logs during a live demo, which is both unconvincing in a repo and not how real
operators would run this in production.

The two signals most directly tied to this project's own architectural claims:
- **Consumer lag** (per consumer group, per partition) — proves the bounded-queue
  backpressure (ADR 0007) is doing something observable under load, and proves the
  batch/replay-tolerant consumer (ADR 0006) behaves differently from the real-time one.
- **DLQ depth** (per DLQ topic) — proves the failure-handling path (ADR 0005) is wired
  up and gives an operator a reason to open the replay tool.

## Decision
- Instrument consumers and the DLQ operator tool with **`prometheus-net`**, exposing
  a `/metrics` endpoint on each service.
- Core metrics:
  - `contenthub_consumer_lag` (gauge, labels: `group`, `topic`, `partition`) — computed
    from `(high-watermark - committed-offset)`, polled periodically via the admin
    client.
  - `contenthub_dlq_depth` (gauge, labels: `topic`) — DLQ topic's current message
    count not yet replayed/discarded.
  - `contenthub_messages_processed_total` / `contenthub_messages_failed_total`
    (counters, labels: `group`, `topic`) — throughput and failure rate.
  - `contenthub_consumer_queue_depth` (gauge, labels: `group`) — current fill level of
    the bounded channel (ADR 0007), the most direct view into backpressure state.
- A small **Grafana** dashboard (provisioned via a committed JSON dashboard definition,
  not clicked together manually) visualizes these four metrics side by side, plus a
  panel correlating DLQ depth with failure-rate counters.
- Prometheus + Grafana run via the same Docker Compose stack as Redpanda, so the whole
  system (broker, schema registry, producers, consumers, DLQ operator, metrics,
  dashboard) comes up with one `docker compose up`.

## Consequences
- Gives the repo a "finished system" feel per the project brief — a dashboard to point
  at live during a demo rather than tailing logs.
- Requires periodic polling of broker metadata (via `AdminClient`) for high-watermark
  and committed-offset to compute lag, rather than a push-based mechanism — the
  simplest correct approach without introducing a dedicated lag-exporter tool.
- The committed Grafana dashboard JSON must be kept in sync with metric name changes;
  treated as part of the codebase, reviewed like code.

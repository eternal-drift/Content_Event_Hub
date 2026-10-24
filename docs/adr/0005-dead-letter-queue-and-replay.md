# 0005 - Dead-letter queue with operator-driven replay

## Status
Accepted

## Context
Consumers will fail to process some messages: malformed payloads that pass schema
validation but fail business-rule checks, transient downstream outages (e.g. a
webhook target is down), or bugs. Without a defined failure path, a naive consumer
either blocks the partition indefinitely (retrying forever) or silently drops the
message (losing data) — both unacceptable for a system whose core pitch is reliable
delivery.

Options considered:
- **Fail-fast with no DLQ** — simplest, but loses messages or halts the consumer group;
  rejected.
- **Infinite in-place retry** — blocks the partition for all other keys behind a poison
  message; rejected as it defeats throughput for unrelated tenants.
- **Per-topic DLQ with retry metadata + operator replay tool** — isolates poison
  messages without blocking the main flow, and gives an explicit, inspectable recovery
  path.

## Decision
- Each consumer applies a bounded **retry policy** (e.g. 3 attempts with exponential
  backoff) for transient failures before giving up on a message.
- On exhausted retries, or on a non-retriable error (e.g. schema says valid but
  business validation fails), the consumer publishes the original CloudEvents envelope
  plus failure metadata (`failure-reason`, `failure-count`, `original-topic`,
  `original-partition`, `original-offset`, `first-failed-at`) to a per-topic DLQ topic,
  e.g. `content.items.v1.dlq`, then commits the original offset so the main flow is
  unblocked.
- A small **operator tool** (ASP.NET Core minimal API, `ContentEventHub.DlqOperator`)
  exposes:
  - `GET /dlq/{topic}` — paginated listing of DLQ messages with failure metadata.
  - `POST /dlq/{topic}/{id}/replay` — re-publishes the original envelope to its
    original topic (optionally after an operator edits the payload) and removes it
    from the active DLQ listing.
  - `POST /dlq/{topic}/{id}/discard` — marks a DLQ entry as permanently discarded
    (audit trail, not a hard delete).
- Replays preserve the original CloudEvents `id`/`time` so downstream dedup logic
  recognizes them as retries of the same logical event.

## Consequences
- Poison messages no longer block partition processing for unrelated keys.
- Operators get a concrete, inspectable recovery workflow instead of needing direct
  Kafka tooling access — this is the single most "production-minded" piece of the
  system and the main thing to walk through in a demo/interview.
- Requires consumers to be idempotent on replay (same event ID reprocessed), which is
  already a natural consequence of at-least-once delivery semantics in Kafka generally.
- DLQ topics need their own (relaxed) retention policy, separate from main topics,
  since failed messages may need to sit for human triage longer than normal retention.

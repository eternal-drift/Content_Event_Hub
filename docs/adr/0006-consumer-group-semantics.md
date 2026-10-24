# 0006 - Differentiated consumer-group semantics (real-time vs. batch/replay-tolerant)

## Status
Accepted

## Context
Not all downstream consumers have the same latency and replay requirements:
- A **notification/webhook forwarder** needs near-real-time delivery (a user expects a
  notification seconds after a permission change), but can tolerate occasional
  duplicate sends (webhook receivers are expected to dedup on event ID).
- A **search-index builder** (or audit log) is replay-tolerant and even replay-reliant:
  it must be able to rebuild its index from scratch by re-reading a topic from the
  beginning, and it operates fine in larger batches with higher per-message latency.

Treating both with the same consumer configuration (same `fetch.min.bytes`,
`max.poll.records`, commit strategy) would either slow down the real-time path or
waste resources over-polling for the batch path.

## Decision
Two distinct consumer groups subscribe independently to the same topics, each tuned
for its semantics:
- **`notification-forwarder` group**: `enable.auto.commit=false` with commit-after-
  process-per-message, low `fetch.min.bytes`/`fetch.wait.max.ms` for low latency,
  pushes to an external webhook sink (simulated HTTP endpoint in this repo). Tolerates
  at-least-once duplicate delivery.
- **`search-indexer` group**: manual commit **after a batch** is fully processed
  (commit every N messages or T seconds, whichever first), higher
  `max.poll.records`/`fetch.min.bytes` for throughput, and supports a documented
  **replay mode** (reset the group's offsets to earliest, or to a specific timestamp,
  and rebuild the index from scratch) — this is the group that most directly
  demonstrates the "replay" part of the pitch.
- Both groups read the same topics independently; neither is aware of the other,
  demonstrating consumer/producer decoupling.

## Consequences
- Configuration differences are explicit and documented per consumer, rather than one
  generic "consumer settings" block — this is the ADR to point at when asked "why two
  consumer groups and not one."
- The search-indexer's replay mode requires the index store to support idempotent
  upserts keyed by item ID (not append-only), since replay will re-process already-
  indexed items.
- Running two consumer groups against the same topics roughly doubles total consumer
  fetch load on the broker relative to one group; acceptable at this project's scale.

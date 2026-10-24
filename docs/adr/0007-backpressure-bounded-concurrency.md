# 0007 - Bounded-queue consumer with configurable concurrency for backpressure

## Status
Accepted

## Context
Kafka's default consumer loop (`poll` → process → commit) is single-threaded per
partition by default in most client patterns. Processing can involve slow I/O (calling
a webhook, writing to a search index), so naive sequential processing under-utilizes
available concurrency, while unbounded fan-out (spawning a task per message with no
limit) risks unbounded memory growth and overwhelming downstream dependencies —
exactly the kind of "relying on Kafka defaults" the project brief calls out as
insufficient.

Options considered:
- **Sequential processing per partition** — simplest, safest, but throughput-limited;
  fine as a baseline but not a demonstration of a deliberate trade-off.
- **Unbounded `Task.Run` per message** — maximizes throughput until it doesn't; no
  backpressure, memory grows with consumer lag, downstream dependencies get hammered.
- **Bounded in-memory queue + fixed worker pool (configurable concurrency)** —
  `poll()` fills a bounded `System.Threading.Channels.Channel<T>`; a fixed-size pool of
  worker tasks drains it; when the channel is full, the poll loop blocks (naturally
  pausing consumption), which signals backpressure upstream through Kafka's own
  consumer-lag mechanism rather than inventing a new one.

## Decision
Implement consumers using a **bounded channel + fixed worker pool**:
- `Channel<ConsumeResult<...>>` with a configurable `Capacity` (e.g. 500).
- A configurable number of worker tasks (e.g. `ConcurrencyLimit = 8`) read from the
  channel and process messages, independently of partition count.
- The poll loop writes to the channel with `BoundedChannelFullMode.Wait` — when full,
  `poll()` pauses, Kafka consumer lag grows (visible in the lag metric, ADR 0008), and
  no unbounded memory growth occurs.
- Offset commits happen only after a message's processing completes successfully (or
  is routed to the DLQ), tracked via an in-memory "highest safely committable offset"
  structure per partition to preserve correctness under out-of-order completion from
  the worker pool.
- `Capacity` and `ConcurrencyLimit` are externalized to configuration per consumer
  group, so the notification-forwarder and search-indexer (ADR 0006) can be tuned
  independently.

## Consequences
- Makes the throughput/memory trade-off an explicit, tunable parameter instead of an
  implicit default — the thing to point at when asked "how does this handle a burst of
  10,000 events."
- Out-of-order completion (worker B finishes offset 105 before worker A finishes
  offset 104) requires careful offset-tracking logic to avoid committing past an
  unprocessed offset; this is the trickiest piece of consumer code in the repo and
  should be unit-tested directly.
- Backpressure is visible externally as consumer lag rather than an internal-only
  signal, which conveniently reuses the same metric already needed for ADR 0008.

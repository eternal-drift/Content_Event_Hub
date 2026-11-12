using ContentEventHub.Shared;

namespace ContentEventHub.Consumers;

public sealed class SearchIndexerOptions
{
    public const string SectionName = "SearchIndexer";

    /// <summary>Replay-tolerant, throughput-oriented group: larger buffer, infrequent commits (ADR 0006, ADR 0007).</summary>
    public BackpressureOptions Backpressure { get; init; } = new()
    {
        Capacity = 1000,
        ConcurrencyLimit = 8,
        CommitIntervalMs = 5000,
    };

    /// <summary>
    /// When true, the indexer seeks every assigned partition to the beginning on
    /// startup and rebuilds the in-memory index from scratch — the "replay mode"
    /// called out in ADR 0006. Requires index upserts to be idempotent, which they
    /// are here since the index is keyed by item ID.
    /// </summary>
    public bool ReplayFromBeginning { get; init; }
}

namespace ContentEventHub.Consumers;

public sealed class SearchIndexerOptions
{
    public const string SectionName = "SearchIndexer";

    public int BatchSize { get; init; } = 50;
    public int BatchWindowSeconds { get; init; } = 5;

    /// <summary>
    /// When true, the indexer seeks every assigned partition to the beginning on
    /// startup and rebuilds the in-memory index from scratch — the "replay mode"
    /// called out in ADR 0006. Requires index upserts to be idempotent, which they
    /// are here since the index is keyed by item ID.
    /// </summary>
    public bool ReplayFromBeginning { get; init; }
}

using System.Collections.Concurrent;

namespace ContentEventHub.Consumers;

/// <summary>
/// Stand-in for a real search index. Upserts are keyed by item ID, so replaying a
/// topic from the beginning re-applies the same updates idempotently instead of
/// duplicating entries (required for ADR 0006's replay mode).
/// </summary>
public sealed class SearchIndexStore
{
    private readonly ConcurrentDictionary<string, IndexedItem> _items = new();

    public void Upsert(string itemId, Action<IndexedItem> mutate)
    {
        var item = _items.GetOrAdd(itemId, static id => new IndexedItem { ItemId = id });
        mutate(item);
    }

    public int Count => _items.Count;

    public IReadOnlyCollection<IndexedItem> Snapshot() => _items.Values.ToList();
}

public sealed class IndexedItem
{
    public required string ItemId { get; init; }
    public string? Title { get; set; }
    public string? ContentType { get; set; }
    public IReadOnlyList<string> GrantedTo { get; set; } = [];
    public string? WorkflowState { get; set; }
    public DateTimeOffset LastUpdatedAt { get; set; }
}

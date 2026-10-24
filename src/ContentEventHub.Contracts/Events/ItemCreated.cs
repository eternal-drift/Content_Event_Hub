namespace ContentEventHub.Contracts.Events;

public sealed class ItemCreated
{
    public required string ItemId { get; init; }
    public required string SourceSystem { get; init; }
    public required string Title { get; init; }
    public required string ContentType { get; init; }
    public required string CreatedBy { get; init; }
    public required DateTimeOffset CreatedAt { get; init; }
}

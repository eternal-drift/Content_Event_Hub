namespace ContentEventHub.Contracts.Events;

public sealed class PermissionsChanged
{
    public required string ItemId { get; init; }
    public required string SourceSystem { get; init; }
    public required string ChangedBy { get; init; }
    public required IReadOnlyList<string> GrantedTo { get; init; }
    public required IReadOnlyList<string> RevokedFrom { get; init; }
    public required DateTimeOffset ChangedAt { get; init; }
}

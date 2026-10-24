namespace ContentEventHub.Contracts.Events;

public sealed class WorkflowTransitioned
{
    public required string ItemId { get; init; }
    public required string SourceSystem { get; init; }
    public required string FromState { get; init; }
    public required string ToState { get; init; }
    public required string TransitionedBy { get; init; }
    public required DateTimeOffset TransitionedAt { get; init; }
}

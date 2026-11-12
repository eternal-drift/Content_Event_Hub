namespace ContentEventHub.DlqOperator;

public enum DlqStatus
{
    Active,
    Replayed,
    Discarded,
}

/// <summary>Non-generic DTO for the operator API — the typed envelope stays inside the handler.</summary>
public sealed record DlqEntry(
    string Id,
    string DlqTopic,
    string OriginalTopic,
    string ConsumerGroup,
    string EventId,
    string EventType,
    string PartitionKey,
    int Partition,
    long Offset,
    string FailureReason,
    int FailureCount,
    DateTimeOffset FirstFailedAt,
    DlqStatus Status);

namespace ContentEventHub.Contracts;

/// <summary>
/// CloudEvents v1.0 structured-mode envelope (see ADR 0003). Transport metadata only —
/// <see cref="Data"/> carries the domain-specific payload, itself schema-registry-enforced.
/// </summary>
public sealed class CloudEventEnvelope<TData>
{
    public required string Id { get; init; }
    public required string Source { get; init; }
    public required string Type { get; init; }
    public string SpecVersion { get; init; } = "1.0";
    public required DateTimeOffset Time { get; init; }
    public string DataContentType { get; init; } = "application/json";
    public string? DataSchema { get; init; }

    /// <summary>
    /// Extension attribute used as the Kafka message key (source-system + item ID),
    /// so events for the same item stay ordered on the same partition (ADR 0004).
    /// </summary>
    public required string PartitionKey { get; init; }

    public required TData Data { get; init; }
}

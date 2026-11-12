namespace ContentEventHub.Shared;

/// <summary>Bounded-channel + worker-pool tuning for one consumer group (ADR 0007).</summary>
public sealed class BackpressureOptions
{
    /// <summary>Max in-flight messages buffered between the poll loop and the worker pool.</summary>
    public int Capacity { get; init; } = 500;

    /// <summary>Fixed number of concurrent worker tasks draining the channel.</summary>
    public int ConcurrencyLimit { get; init; } = 8;

    /// <summary>How often safely-committable offsets are flushed to Kafka.</summary>
    public int CommitIntervalMs { get; init; } = 1000;
}

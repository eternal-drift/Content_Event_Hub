using ContentEventHub.Shared;

namespace ContentEventHub.Consumers;

public sealed class NotificationForwarderOptions
{
    public const string SectionName = "NotificationForwarder";

    /// <summary>Near-real-time group: small buffer, frequent commits (ADR 0006, ADR 0007).</summary>
    public BackpressureOptions Backpressure { get; init; } = new()
    {
        Capacity = 200,
        ConcurrencyLimit = 8,
        CommitIntervalMs = 250,
    };
}

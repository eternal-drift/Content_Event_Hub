namespace ContentEventHub.Consumers;

/// <summary>
/// Deliberate failure injection so the retry/DLQ path (ADR 0005) is actually
/// exercisable in a demo instead of only existing as dead code. Disabled by default.
/// </summary>
public sealed class ChaosOptions
{
    public const string SectionName = "Chaos";

    /// <summary>Probability (0-1) that processing a message raises a transient failure.</summary>
    public double TransientFailureProbability { get; init; }

    /// <summary>Probability (0-1) that processing a message raises a non-retriable failure.</summary>
    public double PermanentFailureProbability { get; init; }
}

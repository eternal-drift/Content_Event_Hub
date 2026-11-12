using ContentEventHub.Shared;

namespace ContentEventHub.Consumers;

public sealed class FailureInjector(ChaosOptions options)
{
    private readonly Random _random = new();

    public void MaybeThrow(string context)
    {
        var roll = _random.NextDouble();

        if (roll < options.PermanentFailureProbability)
        {
            throw new PermanentProcessingException(
                $"[chaos] simulated non-retriable business-rule failure while processing {context}");
        }

        if (roll < options.PermanentFailureProbability + options.TransientFailureProbability)
        {
            throw new TransientProcessingException(
                $"[chaos] simulated transient failure (e.g. downstream timeout) while processing {context}");
        }
    }
}

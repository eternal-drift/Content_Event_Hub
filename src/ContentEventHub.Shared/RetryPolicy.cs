namespace ContentEventHub.Shared;

public enum ProcessingOutcome
{
    Succeeded,
    ExhaustedRetries,
    PermanentFailure,
}

public sealed record ProcessingResult(ProcessingOutcome Outcome, Exception? LastException, int Attempts);

/// <summary>
/// Bounded retry with exponential backoff for transient failures; a
/// <see cref="PermanentProcessingException"/> skips retries entirely (ADR 0005).
/// Callers route anything other than <see cref="ProcessingOutcome.Succeeded"/> to the DLQ.
/// </summary>
public static class RetryPolicy
{
    public static async Task<ProcessingResult> ExecuteAsync(
        Func<Task> action,
        int maxAttempts = 3,
        TimeSpan? baseDelay = null)
    {
        var delay = baseDelay ?? TimeSpan.FromMilliseconds(200);
        Exception? lastException = null;

        for (var attempt = 1; attempt <= maxAttempts; attempt++)
        {
            try
            {
                await action();
                return new ProcessingResult(ProcessingOutcome.Succeeded, null, attempt);
            }
            catch (PermanentProcessingException ex)
            {
                return new ProcessingResult(ProcessingOutcome.PermanentFailure, ex, attempt);
            }
            catch (TransientProcessingException ex)
            {
                lastException = ex;
                if (attempt < maxAttempts)
                {
                    await Task.Delay(delay * attempt);
                }
            }
        }

        return new ProcessingResult(ProcessingOutcome.ExhaustedRetries, lastException, maxAttempts);
    }
}

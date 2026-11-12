namespace ContentEventHub.Shared;

/// <summary>
/// A processing failure that is expected to succeed on retry (e.g. a downstream
/// dependency timeout). The retry policy retries these up to its attempt limit
/// before giving up (ADR 0005).
/// </summary>
public sealed class TransientProcessingException(string message, Exception? inner = null)
    : Exception(message, inner);

/// <summary>
/// A processing failure that will not succeed no matter how many times it's retried
/// (e.g. a business-rule violation). Routed straight to the DLQ without burning
/// retry attempts (ADR 0005).
/// </summary>
public sealed class PermanentProcessingException(string message, Exception? inner = null)
    : Exception(message, inner);

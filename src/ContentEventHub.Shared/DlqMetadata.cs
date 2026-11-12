using System.Text;
using Confluent.Kafka;

namespace ContentEventHub.Shared;

/// <summary>
/// Failure metadata attached as Kafka headers to a message republished onto a DLQ
/// topic (ADR 0005). Kept out of the envelope itself so the DLQ message's value
/// stays schema-compatible with the original topic (needed for replay).
/// </summary>
public sealed record DlqFailureMetadata(
    string ConsumerGroup,
    string FailureReason,
    int FailureCount,
    string OriginalTopic,
    int OriginalPartition,
    long OriginalOffset,
    DateTimeOffset FirstFailedAt)
{
    private const string ConsumerGroupKey = "consumer-group";
    private const string FailureReasonKey = "failure-reason";
    private const string FailureCountKey = "failure-count";
    private const string OriginalTopicKey = "original-topic";
    private const string OriginalPartitionKey = "original-partition";
    private const string OriginalOffsetKey = "original-offset";
    private const string FirstFailedAtKey = "first-failed-at";

    public Headers ToHeaders()
    {
        var headers = new Headers
        {
            { ConsumerGroupKey, Encoding.UTF8.GetBytes(ConsumerGroup) },
            { FailureReasonKey, Encoding.UTF8.GetBytes(FailureReason) },
            { FailureCountKey, Encoding.UTF8.GetBytes(FailureCount.ToString()) },
            { OriginalTopicKey, Encoding.UTF8.GetBytes(OriginalTopic) },
            { OriginalPartitionKey, Encoding.UTF8.GetBytes(OriginalPartition.ToString()) },
            { OriginalOffsetKey, Encoding.UTF8.GetBytes(OriginalOffset.ToString()) },
            { FirstFailedAtKey, Encoding.UTF8.GetBytes(FirstFailedAt.ToString("O")) },
        };
        return headers;
    }

    public static DlqFailureMetadata FromHeaders(Headers headers) => new(
        ConsumerGroup: ReadString(headers, ConsumerGroupKey) ?? "unknown",
        FailureReason: ReadString(headers, FailureReasonKey) ?? "unknown",
        FailureCount: int.TryParse(ReadString(headers, FailureCountKey), out var count) ? count : 0,
        OriginalTopic: ReadString(headers, OriginalTopicKey) ?? "unknown",
        OriginalPartition: int.TryParse(ReadString(headers, OriginalPartitionKey), out var p) ? p : -1,
        OriginalOffset: long.TryParse(ReadString(headers, OriginalOffsetKey), out var o) ? o : -1,
        FirstFailedAt: DateTimeOffset.TryParse(ReadString(headers, FirstFailedAtKey), out var t)
            ? t
            : DateTimeOffset.UtcNow);

    private static string? ReadString(Headers headers, string key) =>
        headers.TryGetLastBytes(key, out var bytes) ? Encoding.UTF8.GetString(bytes) : null;
}

using System.Collections.Concurrent;
using ContentEventHub.Contracts;
using ContentEventHub.Shared;

namespace ContentEventHub.DlqOperator;

/// <summary>
/// Tails one DLQ topic into an in-memory cache and allows replaying entries back to
/// their original topic, or discarding them (ADR 0005). The cache is rebuilt from the
/// DLQ topic on every startup (a fresh consumer-group ID per boot, so it always reads
/// the full DLQ history) — replay/discard status is operator-tool state, not committed
/// anywhere durable, so it resets on restart. Acceptable for this project's scope; a
/// real deployment would back this with a compacted state topic or a database.
/// </summary>
public sealed class DlqTopicHandler<TData> : IDlqTopicHandler, IDisposable
{
    private readonly KafkaOptions _kafkaOptions;
    private readonly ILogger<DlqTopicHandler<TData>> _logger;
    private readonly ConcurrentDictionary<string, CacheEntry> _entries = new();
    private EventConsumer<TData>? _consumer;
    private EventProducer<TData>? _replayProducer;

    public DlqTopicHandler(string originalTopic, KafkaOptions kafkaOptions, ILogger<DlqTopicHandler<TData>> logger)
    {
        OriginalTopic = originalTopic;
        DlqTopic = Topics.DlqFor(originalTopic);
        _kafkaOptions = kafkaOptions;
        _logger = logger;
    }

    public string OriginalTopic { get; }
    public string DlqTopic { get; }

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        var groupOptions = new ConsumerGroupOptions { GroupId = $"dlq-operator-{DlqTopic}-{Guid.NewGuid():n}" };
        _consumer = new EventConsumer<TData>(_kafkaOptions, groupOptions, DlqTopic);
        _replayProducer = new EventProducer<TData>(_kafkaOptions);

        await Task.Run(() =>
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var result = _consumer.Consume(TimeSpan.FromMilliseconds(500));
                if (result is null)
                {
                    continue;
                }

                var metadata = DlqFailureMetadata.FromHeaders(result.Message.Headers);
                var id = $"{result.Partition.Value}:{result.Offset.Value}";

                _entries[id] = new CacheEntry(
                    Id: id,
                    Envelope: result.Message.Value,
                    Metadata: metadata,
                    Partition: result.Partition.Value,
                    Offset: result.Offset.Value,
                    Status: DlqStatus.Active);

                _logger.LogInformation("[dlq-operator] cached {DlqTopic} entry {Id}", DlqTopic, id);
            }
        }, cancellationToken);
    }

    public IReadOnlyList<DlqEntry> List() =>
        _entries.Values
            .OrderByDescending(e => e.Metadata.FirstFailedAt)
            .Select(ToDto)
            .ToList();

    public async Task<DlqOperationOutcome> ReplayAsync(string id, CancellationToken cancellationToken)
    {
        if (!_entries.TryGetValue(id, out var entry))
        {
            return DlqOperationOutcome.NotFound;
        }

        if (entry.Status != DlqStatus.Active)
        {
            return DlqOperationOutcome.AlreadyResolved;
        }

        // Preserve the original CloudEvents Id/Time so downstream idempotent consumers
        // treat this as a retry of the same logical event, not a new one (ADR 0005).
        await _replayProducer!.PublishAsync(OriginalTopic, entry.Envelope, headers: null, cancellationToken);

        _entries[id] = entry with { Status = DlqStatus.Replayed };
        _logger.LogInformation("[dlq-operator] replayed {DlqTopic} entry {Id} back to {OriginalTopic}", DlqTopic, id, OriginalTopic);
        return DlqOperationOutcome.Success;
    }

    public DlqOperationOutcome Discard(string id)
    {
        if (!_entries.TryGetValue(id, out var entry))
        {
            return DlqOperationOutcome.NotFound;
        }

        if (entry.Status != DlqStatus.Active)
        {
            return DlqOperationOutcome.AlreadyResolved;
        }

        _entries[id] = entry with { Status = DlqStatus.Discarded };
        return DlqOperationOutcome.Success;
    }

    public void Dispose()
    {
        _consumer?.Dispose();
        (_replayProducer as IAsyncDisposable)?.DisposeAsync().AsTask().GetAwaiter().GetResult();
    }

    private DlqEntry ToDto(CacheEntry entry) => new(
        Id: entry.Id,
        DlqTopic: DlqTopic,
        OriginalTopic: OriginalTopic,
        ConsumerGroup: entry.Metadata.ConsumerGroup,
        EventId: entry.Envelope.Id,
        EventType: entry.Envelope.Type,
        PartitionKey: entry.Envelope.PartitionKey,
        Partition: entry.Partition,
        Offset: entry.Offset,
        FailureReason: entry.Metadata.FailureReason,
        FailureCount: entry.Metadata.FailureCount,
        FirstFailedAt: entry.Metadata.FirstFailedAt,
        Status: entry.Status);

    private sealed record CacheEntry(
        string Id,
        CloudEventEnvelope<TData> Envelope,
        DlqFailureMetadata Metadata,
        int Partition,
        long Offset,
        DlqStatus Status);
}

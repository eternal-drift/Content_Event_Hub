using Confluent.Kafka;
using ContentEventHub.Contracts;
using ContentEventHub.Contracts.Events;
using ContentEventHub.Shared;
using Microsoft.Extensions.Options;

namespace ContentEventHub.Consumers;

/// <summary>
/// Replay-tolerant, batch-oriented consumer group: buffers messages and commits offsets
/// only after a full batch (by count or time window) has been applied to the index,
/// trading latency for throughput. Supports rebuilding the index from scratch by
/// replaying topics from the beginning (ADR 0006). Messages that fail processing after
/// retry are routed to the DLQ rather than blocking the batch indefinitely (ADR 0005).
/// </summary>
public sealed class SearchIndexerService(
    KafkaOptions kafkaOptions,
    IOptions<SearchIndexerOptions> options,
    IOptions<ChaosOptions> chaosOptions,
    SearchIndexStore indexStore,
    ILogger<SearchIndexerService> logger) : BackgroundService
{
    private const string GroupId = "search-indexer";

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var groupOptions = new ConsumerGroupOptions
        {
            GroupId = GroupId,
            ReplayFromBeginning = options.Value.ReplayFromBeginning,
        };

        if (groupOptions.ReplayFromBeginning)
        {
            logger.LogWarning("[{Group}] replay mode enabled — rebuilding index from the beginning of all topics", GroupId);
        }

        var failureInjector = new FailureInjector(chaosOptions.Value);

        var tasks = new[]
        {
            RunTopicLoopAsync<ItemCreated>(Topics.Items, groupOptions, failureInjector, ApplyItemCreated, stoppingToken),
            RunTopicLoopAsync<PermissionsChanged>(Topics.Permissions, groupOptions, failureInjector, ApplyPermissionsChanged, stoppingToken),
            RunTopicLoopAsync<WorkflowTransitioned>(Topics.Workflow, groupOptions, failureInjector, ApplyWorkflowTransitioned, stoppingToken),
        };

        return Task.WhenAll(tasks);
    }

    private Task RunTopicLoopAsync<TData>(
        string topic,
        ConsumerGroupOptions groupOptions,
        FailureInjector failureInjector,
        Action<CloudEventEnvelope<TData>> apply,
        CancellationToken stoppingToken) =>
        Task.Run(async () =>
        {
            using var consumer = new EventConsumer<TData>(kafkaOptions, groupOptions, topic);
            await using var dlqProducer = new EventProducer<TData>(kafkaOptions);
            var dlqTopic = Topics.DlqFor(topic);

            var batchSize = 0;
            var batchStartedAt = DateTimeOffset.UtcNow;
            // Highest offset seen per partition in the current batch, committed as (offset + 1).
            var pendingOffsets = new Dictionary<TopicPartition, Offset>();

            while (!stoppingToken.IsCancellationRequested)
            {
                var result = consumer.Consume(TimeSpan.FromMilliseconds(500));
                if (result is not null)
                {
                    var envelope = result.Message.Value;
                    var outcome = await RetryPolicy.ExecuteAsync(() =>
                    {
                        failureInjector.MaybeThrow($"{envelope.Type} id={envelope.Id}");
                        apply(envelope);
                        return Task.CompletedTask;
                    });

                    if (outcome.Outcome != ProcessingOutcome.Succeeded)
                    {
                        logger.LogWarning(
                            "[{Group}] {Topic} offset {Offset} failed ({Outcome} after {Attempts} attempts) — routing to {DlqTopic}",
                            GroupId, topic, result.Offset, outcome.Outcome, outcome.Attempts, dlqTopic);

                        var metadata = new DlqFailureMetadata(
                            ConsumerGroup: GroupId,
                            FailureReason: outcome.LastException?.Message ?? "unknown",
                            FailureCount: outcome.Attempts,
                            OriginalTopic: topic,
                            OriginalPartition: result.Partition.Value,
                            OriginalOffset: result.Offset.Value,
                            FirstFailedAt: DateTimeOffset.UtcNow);

                        await dlqProducer.PublishAsync(dlqTopic, envelope, metadata.ToHeaders(), stoppingToken);
                    }

                    // Advance the batch's committable offset whether the message was indexed
                    // or routed to the DLQ — either way it's been handled, not blocking the batch.
                    pendingOffsets[result.TopicPartition] = result.Offset + 1;
                    batchSize++;
                }

                var batchWindowElapsed =
                    DateTimeOffset.UtcNow - batchStartedAt >= TimeSpan.FromSeconds(options.Value.BatchWindowSeconds);

                if (pendingOffsets.Count > 0 && (batchSize >= options.Value.BatchSize || batchWindowElapsed))
                {
                    consumer.Commit(pendingOffsets.Select(kvp => new TopicPartitionOffset(kvp.Key, kvp.Value)));
                    logger.LogInformation(
                        "[{Group}] committed batch of {Count} from {Topic}; index size={IndexSize}",
                        GroupId, batchSize, topic, indexStore.Count);

                    pendingOffsets.Clear();
                    batchSize = 0;
                    batchStartedAt = DateTimeOffset.UtcNow;
                }
            }
        }, stoppingToken);

    private void ApplyItemCreated(CloudEventEnvelope<ItemCreated> envelope) =>
        indexStore.Upsert(envelope.Data.ItemId, item =>
        {
            item.Title = envelope.Data.Title;
            item.ContentType = envelope.Data.ContentType;
            item.LastUpdatedAt = envelope.Time;
        });

    private void ApplyPermissionsChanged(CloudEventEnvelope<PermissionsChanged> envelope) =>
        indexStore.Upsert(envelope.Data.ItemId, item =>
        {
            item.GrantedTo = envelope.Data.GrantedTo;
            item.LastUpdatedAt = envelope.Time;
        });

    private void ApplyWorkflowTransitioned(CloudEventEnvelope<WorkflowTransitioned> envelope) =>
        indexStore.Upsert(envelope.Data.ItemId, item =>
        {
            item.WorkflowState = envelope.Data.ToState;
            item.LastUpdatedAt = envelope.Time;
        });
}

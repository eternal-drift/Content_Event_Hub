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
/// replaying topics from the beginning (ADR 0006).
/// </summary>
public sealed class SearchIndexerService(
    KafkaOptions kafkaOptions,
    IOptions<SearchIndexerOptions> options,
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

        var tasks = new[]
        {
            RunTopicLoopAsync<ItemCreated>(Topics.Items, groupOptions, ApplyItemCreated, stoppingToken),
            RunTopicLoopAsync<PermissionsChanged>(Topics.Permissions, groupOptions, ApplyPermissionsChanged, stoppingToken),
            RunTopicLoopAsync<WorkflowTransitioned>(Topics.Workflow, groupOptions, ApplyWorkflowTransitioned, stoppingToken),
        };

        return Task.WhenAll(tasks);
    }

    private Task RunTopicLoopAsync<TData>(
        string topic,
        ConsumerGroupOptions groupOptions,
        Action<CloudEventEnvelope<TData>> apply,
        CancellationToken stoppingToken) =>
        Task.Run(() =>
        {
            using var consumer = new EventConsumer<TData>(kafkaOptions, groupOptions, topic);

            var batchSize = 0;
            var batchStartedAt = DateTimeOffset.UtcNow;
            // Highest offset seen per partition in the current batch, committed as (offset + 1).
            var pendingOffsets = new Dictionary<TopicPartition, Offset>();

            while (!stoppingToken.IsCancellationRequested)
            {
                var result = consumer.Consume(TimeSpan.FromMilliseconds(500));
                if (result is not null)
                {
                    try
                    {
                        apply(result.Message.Value);
                        pendingOffsets[result.TopicPartition] = result.Offset + 1;
                        batchSize++;
                    }
                    catch (Exception ex)
                    {
                        logger.LogError(ex, "[{Group}] failed to index {Topic} offset {Offset}",
                            GroupId, topic, result.Offset);
                    }
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

using ContentEventHub.Contracts;
using ContentEventHub.Contracts.Events;
using ContentEventHub.Shared;
using Microsoft.Extensions.Options;

namespace ContentEventHub.Consumers;

/// <summary>
/// Replay-tolerant, throughput-oriented consumer group: a larger bounded buffer and
/// infrequent offset commits trade latency for throughput (ADR 0006, ADR 0007).
/// Supports rebuilding the index from scratch by replaying topics from the beginning.
/// Messages that fail processing after retry are routed to the DLQ rather than
/// blocking the worker pool indefinitely (ADR 0005).
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
            var loop = new BackpressureConsumerLoop<TData>(consumer, options.Value.Backpressure);

            await loop.RunAsync(
                async result =>
                {
                    var envelope = result.Message.Value;
                    var outcome = await RetryPolicy.ExecuteAsync(() =>
                    {
                        failureInjector.MaybeThrow($"{envelope.Type} id={envelope.Id}");
                        apply(envelope);
                        return Task.CompletedTask;
                    });

                    if (outcome.Outcome == ProcessingOutcome.Succeeded)
                    {
                        return;
                    }

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
                },
                stoppingToken);
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

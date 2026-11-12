using ContentEventHub.Contracts;
using ContentEventHub.Contracts.Events;
using ContentEventHub.Shared;
using Microsoft.Extensions.Options;

namespace ContentEventHub.Consumers;

/// <summary>
/// Near-real-time consumer group: forwards every event to a (simulated) webhook as soon
/// as it arrives, committing frequently rather than per-message now that offsets are
/// tracked out of order across a worker pool (ADR 0007). Tolerates at-least-once
/// duplicate delivery — webhook receivers are expected to dedup on the CloudEvents
/// <c>Id</c> (ADR 0006). Failures that exhaust retry are routed to the per-topic DLQ
/// rather than blocking the partition (ADR 0005).
/// </summary>
public sealed class NotificationForwarderService(
    KafkaOptions kafkaOptions,
    IOptions<NotificationForwarderOptions> options,
    IOptions<ChaosOptions> chaosOptions,
    ILogger<NotificationForwarderService> logger) : BackgroundService
{
    private const string GroupId = "notification-forwarder";

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var groupOptions = new ConsumerGroupOptions { GroupId = GroupId };
        var failureInjector = new FailureInjector(chaosOptions.Value);

        var tasks = new[]
        {
            RunTopicLoopAsync<ItemCreated>(Topics.Items, groupOptions, failureInjector, stoppingToken),
            RunTopicLoopAsync<PermissionsChanged>(Topics.Permissions, groupOptions, failureInjector, stoppingToken),
            RunTopicLoopAsync<WorkflowTransitioned>(Topics.Workflow, groupOptions, failureInjector, stoppingToken),
        };

        return Task.WhenAll(tasks);
    }

    private Task RunTopicLoopAsync<TData>(
        string topic,
        ConsumerGroupOptions groupOptions,
        FailureInjector failureInjector,
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
                    var outcome = await RetryPolicy.ExecuteAsync(
                        () => ForwardToWebhookAsync(envelope, failureInjector));

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

    private Task ForwardToWebhookAsync<TData>(CloudEventEnvelope<TData> envelope, FailureInjector failureInjector)
    {
        failureInjector.MaybeThrow($"{envelope.Type} id={envelope.Id}");

        logger.LogInformation(
            "[{Group}] webhook ← {Type} id={Id} key={Key}",
            GroupId, envelope.Type, envelope.Id, envelope.PartitionKey);

        return Task.CompletedTask;
    }
}

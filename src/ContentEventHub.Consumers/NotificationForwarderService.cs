using ContentEventHub.Contracts;
using ContentEventHub.Contracts.Events;
using ContentEventHub.Shared;

namespace ContentEventHub.Consumers;

/// <summary>
/// Near-real-time consumer group: forwards every event to a (simulated) webhook as soon
/// as it arrives, committing the offset immediately after each successful delivery.
/// Tolerates at-least-once duplicate delivery — webhook receivers are expected to dedup
/// on the CloudEvents <c>Id</c> (ADR 0006).
/// </summary>
public sealed class NotificationForwarderService(
    KafkaOptions kafkaOptions, ILogger<NotificationForwarderService> logger) : BackgroundService
{
    private const string GroupId = "notification-forwarder";

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var groupOptions = new ConsumerGroupOptions { GroupId = GroupId };

        var tasks = new[]
        {
            RunTopicLoopAsync<ItemCreated>(Topics.Items, groupOptions, stoppingToken),
            RunTopicLoopAsync<PermissionsChanged>(Topics.Permissions, groupOptions, stoppingToken),
            RunTopicLoopAsync<WorkflowTransitioned>(Topics.Workflow, groupOptions, stoppingToken),
        };

        return Task.WhenAll(tasks);
    }

    private Task RunTopicLoopAsync<TData>(
        string topic, ConsumerGroupOptions groupOptions, CancellationToken stoppingToken) =>
        Task.Run(() =>
        {
            using var consumer = new EventConsumer<TData>(kafkaOptions, groupOptions, topic);

            while (!stoppingToken.IsCancellationRequested)
            {
                var result = consumer.Consume(TimeSpan.FromMilliseconds(500));
                if (result is null)
                {
                    continue;
                }

                try
                {
                    ForwardToWebhook(result.Message.Value);
                    consumer.Commit(result);
                }
                catch (Exception ex)
                {
                    // Retry/DLQ routing for failed deliveries is handled in the
                    // dead-letter pipeline (ADR 0005) — not yet wired into this loop.
                    logger.LogError(ex, "[{Group}] failed to forward {Topic} offset {Offset}",
                        GroupId, topic, result.Offset);
                }
            }
        }, stoppingToken);

    private void ForwardToWebhook<TData>(CloudEventEnvelope<TData> envelope)
    {
        logger.LogInformation(
            "[{Group}] webhook ← {Type} id={Id} key={Key}",
            GroupId, envelope.Type, envelope.Id, envelope.PartitionKey);
    }
}

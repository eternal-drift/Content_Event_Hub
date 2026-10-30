using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using ContentEventHub.Contracts;

namespace ContentEventHub.Shared;

public sealed class ConsumerGroupOptions
{
    public required string GroupId { get; init; }

    /// <summary>
    /// When true, every assigned partition is seeked to the beginning on startup,
    /// ignoring any previously committed offsets — used for the search-indexer's
    /// replay-from-scratch mode (ADR 0006).
    /// </summary>
    public bool ReplayFromBeginning { get; init; }
}

/// <summary>
/// Wraps a single-topic Kafka consumer deserializing schema-registry-enforced
/// CloudEvents envelopes. Offset commits are always manual and explicit — callers
/// decide commit timing (per-message vs. batched) per ADR 0006.
/// </summary>
public sealed class EventConsumer<TData> : IDisposable
{
    private readonly IConsumer<string, CloudEventEnvelope<TData>> _consumer;

    public EventConsumer(KafkaOptions kafkaOptions, ConsumerGroupOptions groupOptions, string topic)
    {
        var schemaRegistry = new CachedSchemaRegistryClient(
            new SchemaRegistryConfig { Url = kafkaOptions.SchemaRegistryUrl });

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = kafkaOptions.BootstrapServers,
            GroupId = groupOptions.GroupId,
            EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Earliest,
        };

        _consumer = new ConsumerBuilder<string, CloudEventEnvelope<TData>>(consumerConfig)
            .SetValueDeserializer(new JsonDeserializer<CloudEventEnvelope<TData>>().AsSyncOverAsync())
            .SetPartitionsAssignedHandler((c, partitions) =>
            {
                if (!groupOptions.ReplayFromBeginning)
                {
                    return partitions.Select(p => new TopicPartitionOffset(p, Offset.Unset));
                }

                return partitions.Select(p => new TopicPartitionOffset(p, Offset.Beginning));
            })
            .Build();

        _consumer.Subscribe(topic);
    }

    public ConsumeResult<string, CloudEventEnvelope<TData>>? Consume(TimeSpan timeout) =>
        _consumer.Consume(timeout) is { IsPartitionEOF: false } result ? result : null;

    public void Commit(ConsumeResult<string, CloudEventEnvelope<TData>> result) => _consumer.Commit(result);

    public void Commit(IEnumerable<TopicPartitionOffset> offsets) => _consumer.Commit(offsets);

    public void Dispose()
    {
        _consumer.Close();
        _consumer.Dispose();
    }
}

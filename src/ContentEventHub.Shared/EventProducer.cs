using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using ContentEventHub.Contracts;

namespace ContentEventHub.Shared;

/// <summary>
/// Publishes CloudEvents envelopes to a Kafka topic with schema-enforced JSON
/// serialization (ADR 0003, 0004). The message key is the envelope's partition key
/// so events for the same source+item stay ordered on one partition.
/// </summary>
public sealed class EventProducer<TData> : IAsyncDisposable
{
    private readonly IProducer<string, CloudEventEnvelope<TData>> _producer;

    public EventProducer(KafkaOptions options)
    {
        var schemaRegistry = new CachedSchemaRegistryClient(
            new SchemaRegistryConfig { Url = options.SchemaRegistryUrl });

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = options.BootstrapServers,
            Acks = Acks.All,
            EnableIdempotence = true,
            MessageSendMaxRetries = 3,
        };

        _producer = new ProducerBuilder<string, CloudEventEnvelope<TData>>(producerConfig)
            .SetValueSerializer(new JsonSerializer<CloudEventEnvelope<TData>>(
                schemaRegistry,
                new JsonSerializerConfig { BufferBytes = 1024 }))
            .Build();
    }

    public async Task<DeliveryResult<string, CloudEventEnvelope<TData>>> PublishAsync(
        string topic,
        CloudEventEnvelope<TData> envelope,
        CancellationToken cancellationToken = default)
    {
        var message = new Message<string, CloudEventEnvelope<TData>>
        {
            Key = envelope.PartitionKey,
            Value = envelope,
        };

        return await _producer.ProduceAsync(topic, message, cancellationToken);
    }

    public ValueTask DisposeAsync()
    {
        _producer.Flush(TimeSpan.FromSeconds(5));
        _producer.Dispose();
        return ValueTask.CompletedTask;
    }
}

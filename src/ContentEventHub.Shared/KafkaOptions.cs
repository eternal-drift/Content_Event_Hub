namespace ContentEventHub.Shared;

public sealed class KafkaOptions
{
    public const string SectionName = "Kafka";

    public required string BootstrapServers { get; init; }
    public required string SchemaRegistryUrl { get; init; }
}

namespace ContentEventHub.DlqOperator;

/// <summary>Looks up the handler for a given DLQ topic name, as used by the operator API routes.</summary>
public sealed class DlqRegistry(IReadOnlyList<IDlqTopicHandler> handlers)
{
    public IReadOnlyList<IDlqTopicHandler> Handlers { get; } = handlers;

    public IDlqTopicHandler? Find(string dlqTopic) =>
        Handlers.FirstOrDefault(h => h.DlqTopic == dlqTopic);
}

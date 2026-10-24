namespace ContentEventHub.Contracts;

/// <summary>
/// Canonical CloudEvents `type` values and Kafka topic names for each event category (ADR 0003, 0004).
/// </summary>
public static class EventTypes
{
    public const string ItemCreated = "com.contenthub.item.created.v1";
    public const string PermissionsChanged = "com.contenthub.permissions.changed.v1";
    public const string WorkflowTransitioned = "com.contenthub.workflow.transitioned.v1";
}

public static class Topics
{
    public const string Items = "content.items.v1";
    public const string Permissions = "content.permissions.v1";
    public const string Workflow = "content.workflow.v1";

    public static string DlqFor(string topic) => $"{topic}.dlq";
}

using ContentEventHub.Contracts;
using ContentEventHub.Contracts.Events;
using ContentEventHub.Shared;

namespace ContentEventHub.Producers;

/// <summary>
/// Simulates one independent content/document-management system emitting domain
/// events over time. Each simulated system has its own identity (<see cref="SourceSystem"/>)
/// and tracks the items it has "created" so later permission/workflow events reference
/// real prior items — producers never coordinate with each other (ADR 0003, 0004).
/// </summary>
public sealed class ContentSystemSimulator(
    string sourceSystem,
    EventProducer<ItemCreated> itemProducer,
    EventProducer<PermissionsChanged> permissionsProducer,
    EventProducer<WorkflowTransitioned> workflowProducer,
    ILogger<ContentSystemSimulator> logger)
{
    private static readonly string[] Users = ["alice", "bob", "carol", "dave"];
    private static readonly string[] ContentTypes = ["document", "spreadsheet", "image", "video"];
    private static readonly string[] WorkflowStates = ["draft", "in_review", "approved", "published"];

    private readonly List<string> _knownItemIds = [];
    private readonly Random _random = new();

    public string SourceSystem => sourceSystem;

    public async Task RunOnceAsync(CancellationToken cancellationToken)
    {
        // No items yet, or occasionally, create a new one; otherwise mutate an existing item.
        if (_knownItemIds.Count == 0 || _random.NextDouble() < 0.4)
        {
            await CreateItemAsync(cancellationToken);
            return;
        }

        var itemId = _knownItemIds[_random.Next(_knownItemIds.Count)];
        if (_random.NextDouble() < 0.5)
        {
            await ChangePermissionsAsync(itemId, cancellationToken);
        }
        else
        {
            await TransitionWorkflowAsync(itemId, cancellationToken);
        }
    }

    private async Task CreateItemAsync(CancellationToken cancellationToken)
    {
        var itemId = Guid.NewGuid().ToString("n");
        var now = DateTimeOffset.UtcNow;

        var envelope = new CloudEventEnvelope<ItemCreated>
        {
            Id = Guid.NewGuid().ToString(),
            Source = $"urn:content-system:{sourceSystem}",
            Type = EventTypes.ItemCreated,
            Time = now,
            DataSchema = "item-created.v1.schema.json",
            PartitionKey = $"{sourceSystem}:{itemId}",
            Data = new ItemCreated
            {
                ItemId = itemId,
                SourceSystem = sourceSystem,
                Title = $"Item {itemId[..8]}",
                ContentType = ContentTypes[_random.Next(ContentTypes.Length)],
                CreatedBy = Users[_random.Next(Users.Length)],
                CreatedAt = now,
            },
        };

        await itemProducer.PublishAsync(Topics.Items, envelope, cancellationToken);
        _knownItemIds.Add(itemId);
        logger.LogInformation("[{Source}] published ItemCreated {ItemId}", sourceSystem, itemId);
    }

    private async Task ChangePermissionsAsync(string itemId, CancellationToken cancellationToken)
    {
        var now = DateTimeOffset.UtcNow;
        var granted = Users.OrderBy(_ => _random.Next()).Take(_random.Next(1, 3)).ToArray();
        var revoked = Users.Except(granted).OrderBy(_ => _random.Next()).Take(_random.Next(0, 2)).ToArray();

        var envelope = new CloudEventEnvelope<PermissionsChanged>
        {
            Id = Guid.NewGuid().ToString(),
            Source = $"urn:content-system:{sourceSystem}",
            Type = EventTypes.PermissionsChanged,
            Time = now,
            DataSchema = "permissions-changed.v1.schema.json",
            PartitionKey = $"{sourceSystem}:{itemId}",
            Data = new PermissionsChanged
            {
                ItemId = itemId,
                SourceSystem = sourceSystem,
                ChangedBy = Users[_random.Next(Users.Length)],
                GrantedTo = granted,
                RevokedFrom = revoked,
                ChangedAt = now,
            },
        };

        await permissionsProducer.PublishAsync(Topics.Permissions, envelope, cancellationToken);
        logger.LogInformation("[{Source}] published PermissionsChanged {ItemId}", sourceSystem, itemId);
    }

    private async Task TransitionWorkflowAsync(string itemId, CancellationToken cancellationToken)
    {
        var now = DateTimeOffset.UtcNow;
        var fromIndex = _random.Next(WorkflowStates.Length - 1);

        var envelope = new CloudEventEnvelope<WorkflowTransitioned>
        {
            Id = Guid.NewGuid().ToString(),
            Source = $"urn:content-system:{sourceSystem}",
            Type = EventTypes.WorkflowTransitioned,
            Time = now,
            DataSchema = "workflow-transitioned.v1.schema.json",
            PartitionKey = $"{sourceSystem}:{itemId}",
            Data = new WorkflowTransitioned
            {
                ItemId = itemId,
                SourceSystem = sourceSystem,
                FromState = WorkflowStates[fromIndex],
                ToState = WorkflowStates[fromIndex + 1],
                TransitionedBy = Users[_random.Next(Users.Length)],
                TransitionedAt = now,
            },
        };

        await workflowProducer.PublishAsync(Topics.Workflow, envelope, cancellationToken);
        logger.LogInformation("[{Source}] published WorkflowTransitioned {ItemId}", sourceSystem, itemId);
    }
}

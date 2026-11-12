using ContentEventHub.Contracts;
using ContentEventHub.Contracts.Events;
using ContentEventHub.DlqOperator;
using ContentEventHub.Shared;

var builder = WebApplication.CreateBuilder(args);

var kafkaOptions = builder.Configuration.GetSection(KafkaOptions.SectionName).Get<KafkaOptions>()
    ?? throw new InvalidOperationException($"Missing '{KafkaOptions.SectionName}' configuration section.");
builder.Services.AddSingleton(kafkaOptions);

builder.Services.AddSingleton<DlqRegistry>(sp =>
{
    var loggerFactory = sp.GetRequiredService<ILoggerFactory>();
    IDlqTopicHandler[] handlers =
    [
        new DlqTopicHandler<ItemCreated>(Topics.Items, kafkaOptions, loggerFactory.CreateLogger<DlqTopicHandler<ItemCreated>>()),
        new DlqTopicHandler<PermissionsChanged>(Topics.Permissions, kafkaOptions, loggerFactory.CreateLogger<DlqTopicHandler<PermissionsChanged>>()),
        new DlqTopicHandler<WorkflowTransitioned>(Topics.Workflow, kafkaOptions, loggerFactory.CreateLogger<DlqTopicHandler<WorkflowTransitioned>>()),
    ];
    return new DlqRegistry(handlers);
});
builder.Services.AddHostedService<DlqTailerHostedService>();

var app = builder.Build();

app.MapGet("/dlq", (DlqRegistry registry) =>
    registry.Handlers.Select(h => new { h.DlqTopic, h.OriginalTopic, Count = h.List().Count }));

app.MapGet("/dlq/{topic}", (string topic, DlqRegistry registry) =>
{
    var handler = registry.Find(topic);
    return handler is null ? Results.NotFound() : Results.Ok(handler.List());
});

app.MapPost("/dlq/{topic}/{id}/replay", async (string topic, string id, DlqRegistry registry, CancellationToken ct) =>
{
    var handler = registry.Find(topic);
    if (handler is null)
    {
        return Results.NotFound();
    }

    var outcome = await handler.ReplayAsync(id, ct);
    return outcome switch
    {
        DlqOperationOutcome.Success => Results.Ok(new { status = "replayed" }),
        DlqOperationOutcome.NotFound => Results.NotFound(),
        DlqOperationOutcome.AlreadyResolved => Results.Conflict(new { status = "already resolved" }),
        _ => Results.Problem(),
    };
});

app.MapPost("/dlq/{topic}/{id}/discard", (string topic, string id, DlqRegistry registry) =>
{
    var handler = registry.Find(topic);
    if (handler is null)
    {
        return Results.NotFound();
    }

    var outcome = handler.Discard(id);
    return outcome switch
    {
        DlqOperationOutcome.Success => Results.Ok(new { status = "discarded" }),
        DlqOperationOutcome.NotFound => Results.NotFound(),
        DlqOperationOutcome.AlreadyResolved => Results.Conflict(new { status = "already resolved" }),
        _ => Results.Problem(),
    };
});

app.Run();

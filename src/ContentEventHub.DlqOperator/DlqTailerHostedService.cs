namespace ContentEventHub.DlqOperator;

/// <summary>Runs every registered DLQ topic handler's tailing loop concurrently.</summary>
public sealed class DlqTailerHostedService(DlqRegistry registry) : BackgroundService
{
    protected override Task ExecuteAsync(CancellationToken stoppingToken) =>
        Task.WhenAll(registry.Handlers.Select(h => h.RunAsync(stoppingToken)));
}

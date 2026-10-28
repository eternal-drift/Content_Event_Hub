using ContentEventHub.Contracts.Events;
using ContentEventHub.Shared;

namespace ContentEventHub.Producers;

/// <summary>
/// Runs several independent <see cref="ContentSystemSimulator"/> instances concurrently,
/// each emitting events on its own schedule — demonstrating that producers publish once
/// with no coordination or knowledge of each other or of any consumer (ADR 0003).
/// </summary>
public sealed class Worker(
    EventProducer<ItemCreated> itemProducer,
    EventProducer<PermissionsChanged> permissionsProducer,
    EventProducer<WorkflowTransitioned> workflowProducer,
    ILoggerFactory loggerFactory,
    ILogger<Worker> logger) : BackgroundService
{
    private static readonly string[] SourceSystems = ["alpha", "beta", "gamma"];

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var simulators = SourceSystems
            .Select(name => new ContentSystemSimulator(
                name,
                itemProducer,
                permissionsProducer,
                workflowProducer,
                loggerFactory.CreateLogger<ContentSystemSimulator>()))
            .ToList();

        logger.LogInformation(
            "Starting {Count} simulated content systems: {Systems}",
            simulators.Count, string.Join(", ", SourceSystems));

        var random = new Random();
        var tasks = simulators.Select(simulator => RunSimulatorLoopAsync(simulator, random, stoppingToken));

        await Task.WhenAll(tasks);
    }

    private async Task RunSimulatorLoopAsync(
        ContentSystemSimulator simulator, Random random, CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await simulator.RunOnceAsync(stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "[{Source}] failed to publish an event", simulator.SourceSystem);
            }

            var delayMs = random.Next(800, 2500);
            try
            {
                await Task.Delay(delayMs, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }
}

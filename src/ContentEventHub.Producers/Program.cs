using ContentEventHub.Contracts.Events;
using ContentEventHub.Producers;
using ContentEventHub.Shared;

var builder = Host.CreateApplicationBuilder(args);

var kafkaOptions = builder.Configuration.GetSection(KafkaOptions.SectionName).Get<KafkaOptions>()
    ?? throw new InvalidOperationException($"Missing '{KafkaOptions.SectionName}' configuration section.");

builder.Services.AddSingleton(kafkaOptions);
builder.Services.AddSingleton(sp => new EventProducer<ItemCreated>(sp.GetRequiredService<KafkaOptions>()));
builder.Services.AddSingleton(sp => new EventProducer<PermissionsChanged>(sp.GetRequiredService<KafkaOptions>()));
builder.Services.AddSingleton(sp => new EventProducer<WorkflowTransitioned>(sp.GetRequiredService<KafkaOptions>()));
builder.Services.AddHostedService<Worker>();

var host = builder.Build();
host.Run();

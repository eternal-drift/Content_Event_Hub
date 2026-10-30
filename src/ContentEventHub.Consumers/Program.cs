using ContentEventHub.Consumers;
using ContentEventHub.Shared;

var builder = Host.CreateApplicationBuilder(args);

var kafkaOptions = builder.Configuration.GetSection(KafkaOptions.SectionName).Get<KafkaOptions>()
    ?? throw new InvalidOperationException($"Missing '{KafkaOptions.SectionName}' configuration section.");

builder.Services.AddSingleton(kafkaOptions);
builder.Services.Configure<SearchIndexerOptions>(
    builder.Configuration.GetSection(SearchIndexerOptions.SectionName));
builder.Services.AddSingleton<SearchIndexStore>();

builder.Services.AddHostedService<NotificationForwarderService>();
builder.Services.AddHostedService<SearchIndexerService>();

var host = builder.Build();
host.Run();

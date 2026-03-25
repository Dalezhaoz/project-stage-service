using Microsoft.Extensions.Hosting;
using StageAgentService;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddWindowsService(options => options.ServiceName = "StageAgent");
builder.Services.AddSingleton<SyncWorker>();
builder.Services.AddHostedService<HttpListenerService>();

var host = builder.Build();
host.Run();

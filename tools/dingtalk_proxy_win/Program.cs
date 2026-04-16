using DingTalkProxy;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.UseWindowsService(options => options.ServiceName = "DingTalkProxy");
builder.Services.AddHostedService<HeartbeatService>();
builder.Services.AddHostedService<ProxyListenerService>();
builder.Services.AddHttpClient();

var host = builder.Build();
host.Run();

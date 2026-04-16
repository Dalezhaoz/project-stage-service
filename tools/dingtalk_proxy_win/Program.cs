using DingTalkProxy;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddWindowsService(options => options.ServiceName = "DingTalkProxy");
builder.Services.AddHostedService<HeartbeatService>();
builder.Services.AddHostedService<ProxyListenerService>();

var host = builder.Build();
host.Run();

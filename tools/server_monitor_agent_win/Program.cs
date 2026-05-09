using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.DependencyInjection;

namespace ServerMonitorAgent;

internal static class Program
{
    private static void Main(string[] args)
    {
        var builder = Host.CreateApplicationBuilder(args);
        builder.Services.AddWindowsService(options => options.ServiceName = "ServerMonitorAgent");
        builder.Services.AddHttpClient();
        builder.Services.AddSingleton<PayloadProtector>();
        builder.Services.AddSingleton<SystemMetricCollector>();
        builder.Services.AddHostedService<HttpListenerService>();
        builder.Services.AddHostedService<AppServerSiteReportService>();

        var host = builder.Build();
        host.Run();
    }
}

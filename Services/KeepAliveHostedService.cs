using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;

namespace ProjectStageService.Services;

/// <summary>
/// Periodically pings the app's own health endpoint to prevent IIS app pool idle timeout
/// from killing the process and stopping background scheduled tasks.
/// </summary>
public sealed class KeepAliveHostedService(
    IServer server,
    IHttpClientFactory httpClientFactory,
    ILogger<KeepAliveHostedService> logger) : BackgroundService
{
    private static readonly TimeSpan Interval = TimeSpan.FromMinutes(10);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Wait for the app to fully start before pinging
        await Task.Delay(TimeSpan.FromSeconds(30), stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var address = server.Features
                    .Get<IServerAddressesFeature>()?.Addresses
                    .FirstOrDefault(a => a.StartsWith("http://", StringComparison.OrdinalIgnoreCase));

                if (!string.IsNullOrEmpty(address))
                {
                    var client = httpClientFactory.CreateClient();
                    client.Timeout = TimeSpan.FromSeconds(15);
                    await client.GetAsync(address.TrimEnd('/') + "/api/ping", stoppingToken);
                    logger.LogDebug("Keep-alive ping OK: {Address}", address);
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogWarning("Keep-alive ping failed: {Message}", ex.Message);
            }

            await Task.Delay(Interval, stoppingToken);
        }
    }
}

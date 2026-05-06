using ProjectStageService.Models;

namespace ProjectStageService.Services;

public sealed class ServerMonitorHostedService : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<ServerMonitorHostedService> _logger;

    public ServerMonitorHostedService(IServiceProvider serviceProvider, ILogger<ServerMonitorHostedService> logger)
    {
        _serviceProvider = serviceProvider;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                using var scope = _serviceProvider.CreateScope();
                var scheduleStore = scope.ServiceProvider.GetRequiredService<ScheduleConfigStore>();
                var monitorService = scope.ServiceProvider.GetRequiredService<ServerMonitorService>();
                var schedule = await scheduleStore.LoadAsync(stoppingToken);

                if (schedule.ServerMonitorEnabled)
                {
                    await monitorService.CheckAndNotifyAsync(stoppingToken);
                }

                var intervalMinutes = Math.Clamp(schedule.ServerMonitorIntervalMinutes, 1, 60);
                await Task.Delay(TimeSpan.FromMinutes(intervalMinutes), stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Server monitor hosted loop failed.");
                await Task.Delay(TimeSpan.FromMinutes(1), stoppingToken);
            }
        }
    }
}

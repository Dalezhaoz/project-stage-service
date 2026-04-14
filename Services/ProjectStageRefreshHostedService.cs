namespace ProjectStageService.Services;

public sealed class ProjectStageRefreshHostedService : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ScheduleConfigStore _scheduleConfigStore;
    private readonly ILogger<ProjectStageRefreshHostedService> _logger;

    public ProjectStageRefreshHostedService(
        IServiceProvider serviceProvider,
        ScheduleConfigStore scheduleConfigStore,
        ILogger<ProjectStageRefreshHostedService> logger)
    {
        _serviceProvider = serviceProvider;
        _scheduleConfigStore = scheduleConfigStore;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Stage refresh hosted service started.");
        DateTime? lastRefreshTime = null;

        while (!stoppingToken.IsCancellationRequested)
        {
            await WaitForNextMinuteAsync(stoppingToken);
            if (stoppingToken.IsCancellationRequested) break;

            var config = await _scheduleConfigStore.LoadAsync(stoppingToken);
            var now = DateTime.Now;
            var timeKey = now.ToString("HH:mm");
            var shouldRefresh = false;

            // ── Scheduled time points ────────────────────────────────────
            if (config.StageRefreshEnabled &&
                config.StageRefreshTimes.Any(t => string.Equals(t.Trim(), timeKey, StringComparison.Ordinal)))
            {
                _logger.LogInformation("Scheduled stage refresh triggered at {Time}.", timeKey);
                shouldRefresh = true;
            }

            // ── Interval-based ───────────────────────────────────────────
            if (!shouldRefresh && config.RefreshIntervalEnabled && config.RefreshIntervalMinutes > 0)
            {
                var elapsedMinutes = lastRefreshTime.HasValue
                    ? (now - lastRefreshTime.Value).TotalMinutes
                    : double.MaxValue; // first run: fire immediately

                if (elapsedMinutes >= config.RefreshIntervalMinutes)
                {
                    _logger.LogInformation("Interval stage refresh triggered at {Time} (interval={Interval}m, elapsed={Elapsed:F1}m).",
                        timeKey, config.RefreshIntervalMinutes, elapsedMinutes);
                    shouldRefresh = true;
                }
            }

            if (!shouldRefresh) continue;

            try
            {
                using var scope = _serviceProvider.CreateScope();
                var refreshService = scope.ServiceProvider.GetRequiredService<ProjectStageRefreshService>();
                await refreshService.RefreshAsync(null, stoppingToken);
                lastRefreshTime = DateTime.Now;
                _logger.LogInformation("Stage refresh completed at {Time}.", DateTime.Now.ToString("HH:mm:ss"));
            }
            catch (TaskCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Automatic stage cache refresh failed.");
            }
        }

        _logger.LogInformation("Stage refresh hosted service stopped.");
    }

    private static async Task WaitForNextMinuteAsync(CancellationToken cancellationToken)
    {
        var now = DateTime.Now;
        var msUntilNextMinute = (60 - now.Second) * 1000 - now.Millisecond;
        if (msUntilNextMinute < 200) msUntilNextMinute += 60_000;
        try
        {
            await Task.Delay(msUntilNextMinute, cancellationToken);
        }
        catch (TaskCanceledException)
        {
            // normal shutdown
        }
    }
}

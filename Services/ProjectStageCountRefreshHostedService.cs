namespace ProjectStageService.Services;

public sealed class ProjectStageCountRefreshHostedService : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ScheduleConfigStore _scheduleConfigStore;
    private readonly ILogger<ProjectStageCountRefreshHostedService> _logger;
    private CancellationTokenSource? _delayCts;

    public ProjectStageCountRefreshHostedService(
        IServiceProvider serviceProvider,
        ScheduleConfigStore scheduleConfigStore,
        ILogger<ProjectStageCountRefreshHostedService> logger)
    {
        _serviceProvider = serviceProvider;
        _scheduleConfigStore = scheduleConfigStore;
        _logger = logger;

        _scheduleConfigStore.OnChanged += () =>
        {
            _logger.LogInformation("Schedule config changed, recalculating next count refresh run.");
            _delayCts?.Cancel();
        };
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            var config = await _scheduleConfigStore.LoadAsync(stoppingToken);

            if (!config.CountRefreshEnabled || config.CountRefreshTimes.Count == 0)
            {
                _logger.LogInformation("Count refresh schedule is disabled. Waiting for config change.");
                try
                {
                    _delayCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
                    await Task.Delay(Timeout.Infinite, _delayCts.Token);
                }
                catch (TaskCanceledException) when (!stoppingToken.IsCancellationRequested)
                {
                    continue;
                }
                catch (TaskCanceledException)
                {
                    break;
                }
                continue;
            }

            var nextRun = GetNextRun(config.CountRefreshTimes);
            var delay = nextRun - DateTime.Now;

            _logger.LogInformation("Next automatic count refresh scheduled at {NextRun}.", nextRun);

            try
            {
                _delayCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
                await Task.Delay(delay, _delayCts.Token);
            }
            catch (TaskCanceledException) when (!stoppingToken.IsCancellationRequested)
            {
                continue;
            }
            catch (TaskCanceledException)
            {
                break;
            }

            try
            {
                using var scope = _serviceProvider.CreateScope();
                var refreshService = scope.ServiceProvider.GetRequiredService<ProjectStageCountRefreshService>();
                await refreshService.RefreshAsync(stoppingToken);
            }
            catch (TaskCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Automatic count refresh failed.");
            }
        }
    }

    private static DateTime GetNextRun(List<string> times)
    {
        var now = DateTime.Now;
        var candidates = new List<DateTime>();

        foreach (var t in times)
        {
            if (TimeSpan.TryParse(t, out var ts))
            {
                var candidate = DateTime.Today.Add(ts);
                if (candidate <= now) candidate = candidate.AddDays(1);
                candidates.Add(candidate);
            }
        }

        return candidates.Count > 0 ? candidates.Min() : DateTime.Today.AddDays(1).AddHours(6);
    }
}

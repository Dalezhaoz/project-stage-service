namespace ProjectStageService.Services;

public sealed class ProjectStageRefreshHostedService : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ScheduleConfigStore _scheduleConfigStore;
    private readonly ILogger<ProjectStageRefreshHostedService> _logger;
    private CancellationTokenSource? _delayCts;

    public ProjectStageRefreshHostedService(
        IServiceProvider serviceProvider,
        ScheduleConfigStore scheduleConfigStore,
        ILogger<ProjectStageRefreshHostedService> logger)
    {
        _serviceProvider = serviceProvider;
        _scheduleConfigStore = scheduleConfigStore;
        _logger = logger;

        _scheduleConfigStore.OnChanged += () =>
        {
            _logger.LogInformation("Schedule config changed, recalculating next run.");
            _delayCts?.Cancel();
        };
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            var config = await _scheduleConfigStore.LoadAsync(stoppingToken);

            if (!config.StageRefreshEnabled || config.StageRefreshTimes.Count == 0)
            {
                _logger.LogInformation("Stage refresh schedule is disabled. Waiting for config change.");
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

            var nextRun = GetNextRun(config.StageRefreshTimes);
            var delay = nextRun - DateTime.Now;

            _logger.LogInformation("Next automatic stage refresh scheduled at {NextRun}.", nextRun);

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
                var refreshService = scope.ServiceProvider.GetRequiredService<ProjectStageRefreshService>();
                await refreshService.RefreshAsync(null, stoppingToken);
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

        return candidates.Count > 0 ? candidates.Min() : DateTime.Today.AddDays(1).AddHours(4);
    }
}

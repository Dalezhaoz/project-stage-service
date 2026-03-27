namespace ProjectStageService.Services;

public sealed class DingTalkNotifyHostedService : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ScheduleConfigStore _scheduleConfigStore;
    private readonly ILogger<DingTalkNotifyHostedService> _logger;
    private CancellationTokenSource? _delayCts;

    public DingTalkNotifyHostedService(
        IServiceProvider serviceProvider,
        ScheduleConfigStore scheduleConfigStore,
        ILogger<DingTalkNotifyHostedService> logger)
    {
        _serviceProvider = serviceProvider;
        _scheduleConfigStore = scheduleConfigStore;
        _logger = logger;

        _scheduleConfigStore.OnChanged += () =>
        {
            _logger.LogInformation("Schedule config changed, recalculating next DingTalk notify.");
            _delayCts?.Cancel();
        };
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            var config = await _scheduleConfigStore.LoadAsync(stoppingToken);

            if (!config.DingTalkEnabled ||
                string.IsNullOrWhiteSpace(config.DingTalkConfig?.WebhookUrl) ||
                config.DingTalkNotifyTimes.Count == 0)
            {
                _logger.LogInformation("DingTalk notify is disabled. Waiting for config change.");
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

            var nextRun = GetNextRun(config.DingTalkNotifyTimes);
            var delay = nextRun - DateTime.Now;

            _logger.LogInformation("Next DingTalk notification scheduled at {NextRun}.", nextRun);

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
                var notifyService = scope.ServiceProvider.GetRequiredService<DingTalkNotifyService>();
                var summaryStoreConfigStore = scope.ServiceProvider.GetRequiredService<SummaryStoreConfigStore>();
                var summaryConfig = await summaryStoreConfigStore.LoadAsync(stoppingToken);

                if (!summaryConfig.Enabled)
                {
                    _logger.LogWarning("Summary store is not enabled, skipping DingTalk notification.");
                    continue;
                }

                var authService = scope.ServiceProvider.GetRequiredService<LocalAuthService>();
                var userDingTalkConfigs = await authService.GetAllDingTalkConfigsAsync(stoppingToken);
                await notifyService.SendDailyReportAsync(summaryConfig, config.DingTalkConfig!, userDingTalkConfigs, stoppingToken);
            }
            catch (TaskCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "DingTalk daily notification failed.");
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

        return candidates.Count > 0 ? candidates.Min() : DateTime.Today.AddDays(1).AddHours(8);
    }
}

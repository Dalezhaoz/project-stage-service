using ProjectStageService.Models;

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

            var todayTimes = config.DingTalkNotifyTimes ?? [];
            var nextDayTimes = config.DingTalkNextDayNotifyTimes ?? [];
            var unassignedTimes = config.UnassignedNotifyTimes ?? [];
            var hasTodayNotify = config.DingTalkEnabled && todayTimes.Count > 0;
            var hasNextDayNotify = config.DingTalkNextDayEnabled && nextDayTimes.Count > 0;
            var hasUnassignedNotify = config.UnassignedNotifyEnabled && unassignedTimes.Count > 0;

            if ((!hasTodayNotify && !hasNextDayNotify && !hasUnassignedNotify) ||
                string.IsNullOrWhiteSpace(config.DingTalkConfig?.WebhookUrl))
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

            var nextRun = GetNextRun(config);
            var delay = nextRun.Time - DateTime.Now;

            _logger.LogInformation("Next DingTalk notification scheduled at {NextRun}.", nextRun.Time);

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
                if (nextRun.Kind == DingTalkNotifyKind.NextDay)
                {
                    await notifyService.SendNextDayPreviewAsync(summaryConfig, config.DingTalkConfig!, userDingTalkConfigs, stoppingToken);
                }
                else if (nextRun.Kind == DingTalkNotifyKind.Unassigned)
                {
                    await notifyService.SendUnassignedProjectsReportAsync(summaryConfig, config.DingTalkConfig!, stoppingToken);
                }
                else
                {
                    await notifyService.SendDailyReportAsync(summaryConfig, config.DingTalkConfig!, userDingTalkConfigs, stoppingToken);
                }
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

    private static ScheduledRun GetNextRun(ScheduleConfig config)
    {
        var now = DateTime.Now;
        var candidates = new List<ScheduledRun>();

        foreach (var t in config.DingTalkNotifyTimes ?? [])
        {
            if (TimeSpan.TryParse(t, out var ts))
            {
                var candidate = DateTime.Today.Add(ts);
                if (candidate <= now) candidate = candidate.AddDays(1);
                candidates.Add(new ScheduledRun(candidate, DingTalkNotifyKind.Today));
            }
        }

        foreach (var t in config.DingTalkNextDayNotifyTimes ?? [])
        {
            if (TimeSpan.TryParse(t, out var ts))
            {
                var candidate = DateTime.Today.Add(ts);
                if (candidate <= now) candidate = candidate.AddDays(1);
                candidates.Add(new ScheduledRun(candidate, DingTalkNotifyKind.NextDay));
            }
        }

        foreach (var t in config.UnassignedNotifyTimes ?? [])
        {
            if (TimeSpan.TryParse(t, out var ts))
            {
                var candidate = DateTime.Today.Add(ts);
                if (candidate <= now) candidate = candidate.AddDays(1);
                candidates.Add(new ScheduledRun(candidate, DingTalkNotifyKind.Unassigned));
            }
        }

        return candidates.Count > 0
            ? candidates.OrderBy(item => item.Time).First()
            : new ScheduledRun(DateTime.Today.AddDays(1).AddHours(8), DingTalkNotifyKind.Today);
    }

    private sealed record ScheduledRun(DateTime Time, DingTalkNotifyKind Kind);

    private enum DingTalkNotifyKind
    {
        Today,
        NextDay,
        Unassigned
    }
}

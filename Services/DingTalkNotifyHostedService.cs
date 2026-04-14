using ProjectStageService.Models;

namespace ProjectStageService.Services;

public sealed class DingTalkNotifyHostedService : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ScheduleConfigStore _scheduleConfigStore;
    private readonly ILogger<DingTalkNotifyHostedService> _logger;

    public DingTalkNotifyHostedService(
        IServiceProvider serviceProvider,
        ScheduleConfigStore scheduleConfigStore,
        ILogger<DingTalkNotifyHostedService> logger)
    {
        _serviceProvider = serviceProvider;
        _scheduleConfigStore = scheduleConfigStore;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("DingTalk notify service started (minute-tick mode).");

        while (!stoppingToken.IsCancellationRequested)
        {
            // Wait until the start of next minute
            await WaitForNextMinuteAsync(stoppingToken);
            if (stoppingToken.IsCancellationRequested) break;

            var timeKey = DateTime.Now.ToString("HH:mm");

            try
            {
                using var scope = _serviceProvider.CreateScope();
                var notifyService = scope.ServiceProvider.GetRequiredService<DingTalkNotifyService>();
                var summaryStoreConfigStore = scope.ServiceProvider.GetRequiredService<SummaryStoreConfigStore>();
                var authService = scope.ServiceProvider.GetRequiredService<LocalAuthService>();

                var scheduleConfig = await _scheduleConfigStore.LoadAsync(stoppingToken);
                var summaryConfig = await summaryStoreConfigStore.LoadAsync(stoppingToken);

                if (!summaryConfig.Enabled)
                {
                    _logger.LogDebug("Summary store not enabled, skipping tick {Time}.", timeKey);
                    continue;
                }

                var mainConfig = scheduleConfig.DingTalkConfig;
                var hasMainWebhook = mainConfig is not null && !string.IsNullOrWhiteSpace(mainConfig.WebhookUrl);
                var proxyConfig = new DingTalkConfig { ProxyUrl = mainConfig?.ProxyUrl ?? "" };

                // ── Global: today report ──────────────────────────────────
                if (hasMainWebhook && scheduleConfig.DingTalkEnabled &&
                    ContainsTime(scheduleConfig.DingTalkNotifyTimes, timeKey))
                {
                    _logger.LogInformation("Firing global today report at {Time}.", timeKey);
                    var userConfigs = await authService.GetAllDingTalkConfigsAsync(stoppingToken);
                    await SafeRun(() => notifyService.SendDailyReportAsync(summaryConfig, mainConfig!, userConfigs, stoppingToken),
                        "global today report");
                }

                // ── Global: next-day preview ──────────────────────────────
                if (hasMainWebhook && scheduleConfig.DingTalkNextDayEnabled &&
                    ContainsTime(scheduleConfig.DingTalkNextDayNotifyTimes, timeKey))
                {
                    _logger.LogInformation("Firing global next-day preview at {Time}.", timeKey);
                    var userConfigs = await authService.GetAllDingTalkConfigsAsync(stoppingToken);
                    await SafeRun(() => notifyService.SendNextDayPreviewAsync(summaryConfig, mainConfig!, userConfigs, stoppingToken),
                        "global next-day preview");
                }

                // ── Global: unassigned notification ───────────────────────
                if (hasMainWebhook && scheduleConfig.UnassignedNotifyEnabled &&
                    ContainsTime(scheduleConfig.UnassignedNotifyTimes, timeKey))
                {
                    _logger.LogInformation("Firing unassigned projects report at {Time}.", timeKey);
                    await SafeRun(() => notifyService.SendUnassignedProjectsReportAsync(summaryConfig, mainConfig!, stoppingToken),
                        "unassigned report");
                }

                // ── Per-user personal times ───────────────────────────────
                var allUsers = await authService.GetAllDingTalkConfigsAsync(stoppingToken);
                foreach (var user in allUsers)
                {
                    if (ContainsTime(user.TodayNotifyTimes, timeKey))
                    {
                        _logger.LogInformation("Firing personal today push for {User} at {Time}.", user.Username, timeKey);
                        var u = user; // capture
                        await SafeRun(() => notifyService.SendPersonalDailyAsync(summaryConfig, proxyConfig, u, stoppingToken),
                            $"personal today for {user.Username}");
                    }

                    if (ContainsTime(user.NextDayNotifyTimes, timeKey))
                    {
                        _logger.LogInformation("Firing personal next-day push for {User} at {Time}.", user.Username, timeKey);
                        var u = user;
                        await SafeRun(() => notifyService.SendPersonalNextDayAsync(summaryConfig, proxyConfig, u, stoppingToken),
                            $"personal next-day for {user.Username}");
                    }
                }
            }
            catch (TaskCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unexpected error in DingTalk notify tick at {Time}.", timeKey);
            }
        }

        _logger.LogInformation("DingTalk notify service stopped.");
    }

    private static bool ContainsTime(IEnumerable<string>? times, string timeKey)
    {
        if (times is null) return false;
        return times.Any(t => string.Equals(t.Trim(), timeKey, StringComparison.Ordinal));
    }

    private async Task SafeRun(Func<Task> action, string label)
    {
        try
        {
            await action();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "DingTalk notify failed: {Label}.", label);
        }
    }

    private static async Task WaitForNextMinuteAsync(CancellationToken cancellationToken)
    {
        var now = DateTime.Now;
        var secondsUntilNextMinute = 60 - now.Second;
        var msUntilNextMinute = secondsUntilNextMinute * 1000 - now.Millisecond;
        if (msUntilNextMinute < 200) msUntilNextMinute += 60_000; // avoid firing twice in same minute
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

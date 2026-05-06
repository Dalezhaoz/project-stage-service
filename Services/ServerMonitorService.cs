using System.Collections.Concurrent;
using System.Text;
using ProjectStageService.Models;

namespace ProjectStageService.Services;

public sealed class ServerMonitorService
{
    private readonly ServerMetricClientService _metricClientService;
    private readonly MonitorServerConfigStore _monitorServerConfigStore;
    private readonly ScheduleConfigStore _scheduleConfigStore;
    private readonly DingTalkNotifyService _dingTalkNotifyService;
    private readonly ILogger<ServerMonitorService> _logger;
    private readonly ConcurrentDictionary<string, DateTime> _alertState = new(StringComparer.OrdinalIgnoreCase);

    public ServerMonitorService(
        ServerMetricClientService metricClientService,
        MonitorServerConfigStore monitorServerConfigStore,
        ScheduleConfigStore scheduleConfigStore,
        DingTalkNotifyService dingTalkNotifyService,
        ILogger<ServerMonitorService> logger)
    {
        _metricClientService = metricClientService;
        _monitorServerConfigStore = monitorServerConfigStore;
        _scheduleConfigStore = scheduleConfigStore;
        _dingTalkNotifyService = dingTalkNotifyService;
        _logger = logger;
    }

    public async Task<List<ServerMetricStatus>> QueryStatusesAsync(
        List<MonitorServerConfig>? inputServers,
        CancellationToken cancellationToken)
    {
        var servers = (inputServers?.Count > 0 ? inputServers : await _monitorServerConfigStore.LoadAsync(cancellationToken))
            .Where(s => s.Enabled)
            .ToList();

        var statuses = new List<ServerMetricStatus>();
        foreach (var server in servers)
        {
            try
            {
                var metric = await _metricClientService.QueryAsync(server, cancellationToken);
                statuses.Add(new ServerMetricStatus
                {
                    ServerName = server.Name,
                    Host = server.Host,
                    CollectedAt = metric.CollectedAt,
                    Success = true,
                    CpuUsagePercent = metric.CpuUsagePercent,
                    MemoryUsagePercent = metric.MemoryUsagePercent,
                    DiskUsagePercent = metric.DiskUsagePercent,
                    CpuAlert = metric.CpuUsagePercent >= server.CpuAlertThreshold,
                    MemoryAlert = metric.MemoryUsagePercent >= server.MemoryAlertThreshold,
                    DiskAlert = metric.DiskUsagePercent >= server.DiskAlertThreshold
                });
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Collect metrics failed for {ServerName}.", server.Name);
                statuses.Add(new ServerMetricStatus
                {
                    ServerName = server.Name,
                    Host = server.Host,
                    CollectedAt = DateTime.Now,
                    Success = false,
                    Error = ex.Message
                });
            }
        }

        return statuses;
    }

    public async Task<int> CheckAndNotifyAsync(CancellationToken cancellationToken)
    {
        var schedule = await _scheduleConfigStore.LoadAsync(cancellationToken);
        if (!schedule.ServerMonitorEnabled || schedule.DingTalkConfig is null || string.IsNullOrWhiteSpace(schedule.DingTalkConfig.WebhookUrl))
        {
            return 0;
        }

        var servers = await _monitorServerConfigStore.LoadAsync(cancellationToken);
        var statuses = await QueryStatusesAsync(servers, cancellationToken);
        var alerts = statuses.Where(s => s.Success && (s.CpuAlert || s.MemoryAlert || s.DiskAlert)).ToList();
        if (alerts.Count == 0)
        {
            return 0;
        }

        var cooldownMinutes = Math.Clamp(schedule.ServerMonitorAlertCooldownMinutes, 1, 24 * 60);
        var now = DateTime.Now;
        var pendingAlerts = new List<ServerMetricStatus>();
        foreach (var alert in alerts)
        {
            var key = BuildAlertKey(alert);
            if (_alertState.TryGetValue(key, out var lastSent) && now - lastSent < TimeSpan.FromMinutes(cooldownMinutes))
            {
                continue;
            }

            pendingAlerts.Add(alert);
            _alertState[key] = now;
        }

        if (pendingAlerts.Count == 0)
        {
            return 0;
        }

        var title = $"服务器状态预警 ({now:yyyy-MM-dd HH:mm})";
        var text = BuildAlertMarkdown(pendingAlerts, servers);
        await _dingTalkNotifyService.SendDirectMessageAsync(schedule.DingTalkConfig, title, text, cancellationToken);
        return pendingAlerts.Count;
    }

    private static string BuildAlertMarkdown(List<ServerMetricStatus> alerts, List<MonitorServerConfig> servers)
    {
        var map = servers.ToDictionary(s => s.Name, s => s, StringComparer.OrdinalIgnoreCase);
        var sb = new StringBuilder();
        sb.AppendLine("### 服务器资源预警");
        sb.AppendLine($"> 触发时间：**{DateTime.Now:yyyy-MM-dd HH:mm:ss}**");
        sb.AppendLine();
        foreach (var alert in alerts)
        {
            var server = map.GetValueOrDefault(alert.ServerName);
            sb.AppendLine($"**{alert.ServerName}** ({alert.Host})");
            sb.AppendLine($"- CPU：{alert.CpuUsagePercent:F1}% / 阈值 {server?.CpuAlertThreshold:F1}% {(alert.CpuAlert ? "[ALERT]" : "[OK]")}");
            sb.AppendLine($"- 内存：{alert.MemoryUsagePercent:F1}% / 阈值 {server?.MemoryAlertThreshold:F1}% {(alert.MemoryAlert ? "[ALERT]" : "[OK]")}");
            sb.AppendLine($"- 磁盘：{alert.DiskUsagePercent:F1}% / 阈值 {server?.DiskAlertThreshold:F1}% {(alert.DiskAlert ? "[ALERT]" : "[OK]")}");
            sb.AppendLine();
        }

        return sb.ToString();
    }

    private static string BuildAlertKey(ServerMetricStatus alert)
    {
        return $"{alert.ServerName}|{(alert.CpuAlert ? "1" : "0")}{(alert.MemoryAlert ? "1" : "0")}{(alert.DiskAlert ? "1" : "0")}";
    }
}

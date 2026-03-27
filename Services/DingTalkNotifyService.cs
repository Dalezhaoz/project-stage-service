using System.Net.Http.Json;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.Data.SqlClient;
using ProjectStageService.Models;

namespace ProjectStageService.Services;

public sealed class DingTalkNotifyService
{
    private readonly ILogger<DingTalkNotifyService> _logger;
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly DingTalkProxyRegistry _proxyRegistry;

    public DingTalkNotifyService(ILogger<DingTalkNotifyService> logger, IHttpClientFactory httpClientFactory, DingTalkProxyRegistry proxyRegistry)
    {
        _logger = logger;
        _httpClientFactory = httpClientFactory;
        _proxyRegistry = proxyRegistry;
    }

    public async Task SendDailyReportAsync(
        SummaryStoreConfig summaryConfig,
        DingTalkConfig dingTalkConfig,
        CancellationToken cancellationToken)
    {
        await SendDailyReportAsync(summaryConfig, dingTalkConfig, [], cancellationToken);
    }

    public async Task<DailyReportResult> SendDailyReportAsync(
        SummaryStoreConfig summaryConfig,
        DingTalkConfig dingTalkConfig,
        List<LocalAuthService.UserDingTalkConfig> userDingTalkConfigs,
        CancellationToken cancellationToken)
    {
        var result = new DailyReportResult();
        var todayStages = await QueryTodayStartingStagesAsync(summaryConfig, cancellationToken);
        result.TotalStages = todayStages.Count;

        if (todayStages.Count == 0)
        {
            _logger.LogInformation("No stages starting today, skipping DingTalk notification.");
            return result;
        }

        // Send overall report to main webhook
        if (!string.IsNullOrWhiteSpace(dingTalkConfig.WebhookUrl))
        {
            var markdown = BuildMarkdownMessage(todayStages);
            await SendDingTalkMessageAsync(dingTalkConfig, markdown.title, markdown.text, cancellationToken);
            _logger.LogInformation("DingTalk daily report sent: {Count} stages starting today.", todayStages.Count);
            result.MainSent = true;
        }

        // Send per-maintainer reports
        foreach (var userConfig in userDingTalkConfigs)
        {
            try
            {
                var userStages = todayStages
                    .Where(s => string.Equals(s.Maintainer, userConfig.Username, StringComparison.OrdinalIgnoreCase))
                    .ToList();

                if (userStages.Count == 0)
                {
                    result.SkippedUsers.Add(userConfig.Username);
                    continue;
                }

                var markdown = BuildMaintainerMarkdownMessage(userConfig.Username, userStages);
                var config = new DingTalkConfig
                {
                    WebhookUrl = userConfig.WebhookUrl,
                    Secret = userConfig.Secret,
                    ProxyUrl = dingTalkConfig.ProxyUrl
                };
                await SendDingTalkMessageAsync(config, markdown.title, markdown.text, cancellationToken);
                _logger.LogInformation("DingTalk personal report sent to {User}: {Count} stages.", userConfig.Username, userStages.Count);
                result.SentUsers.Add(userConfig.Username);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to send personal DingTalk report to {User}.", userConfig.Username);
                result.FailedUsers.Add(userConfig.Username);
            }
        }

        return result;
    }

    public class DailyReportResult
    {
        public int TotalStages { get; set; }
        public bool MainSent { get; set; }
        public List<string> SentUsers { get; set; } = [];
        public List<string> SkippedUsers { get; set; } = [];
        public List<string> FailedUsers { get; set; } = [];
    }

    private async Task<List<TodayStageInfo>> QueryTodayStartingStagesAsync(
        SummaryStoreConfig config,
        CancellationToken cancellationToken)
    {
        var builder = new SqlConnectionStringBuilder
        {
            DataSource = $"{config.Host},{config.Port}",
            InitialCatalog = config.DatabaseName,
            UserID = config.Username,
            Password = config.Password,
            TrustServerCertificate = true,
            Encrypt = false,
            ConnectTimeout = 20
        };

        await using var connection = new SqlConnection(builder.ConnectionString);
        await connection.OpenAsync(cancellationToken);

        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT s.project_name, s.stage_name, s.stage_start_time, s.stage_end_time,
                   s.registration_count, s.admission_ticket_count,
                   s.source_server_name, s.source_database_name, s.exam_code,
                   ISNULL(m.maintainer, '') AS maintainer,
                   ISNULL(m.app_servers, '') AS app_servers
            FROM dbo.project_stage_summary s
            LEFT JOIN dbo.project_metadata m
                ON s.source_server_name = m.server_name AND s.exam_code = m.exam_code
            WHERE CAST(s.stage_start_time AS DATE) = CAST(GETDATE() AS DATE)
            ORDER BY s.stage_start_time, s.project_name, s.stage_name
            """;

        var results = new List<TodayStageInfo>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            results.Add(new TodayStageInfo
            {
                ProjectName = reader.GetString(0),
                StageName = reader.GetString(1),
                StartTime = reader.GetDateTime(2),
                EndTime = reader.GetDateTime(3),
                RegistrationCount = reader.GetInt32(4),
                AdmissionTicketCount = reader.GetInt32(5),
                ServerName = reader.GetString(6),
                DatabaseName = reader.GetString(7),
                ExamCode = reader.GetString(8),
                Maintainer = reader.GetString(9),
                AppServers = reader.GetString(10)
            });
        }

        return results;
    }

    private static (string title, string text) BuildMarkdownMessage(List<TodayStageInfo> stages)
    {
        var today = DateTime.Today.ToString("yyyy-MM-dd");
        var title = $"今日开始的项目阶段 ({today})";

        var sb = new StringBuilder();

        // === Header ===
        sb.AppendLine($"### 📋 今日开始的项目阶段");
        sb.AppendLine($"> 日期：**{today}**，共 **{stages.Count}** 个阶段  ");
        sb.AppendLine();

        // === Overview ===
        var registrationStages = stages.Where(s => s.StageName.Contains("报名")).ToList();
        var admissionStages = stages.Where(s => s.StageName.Contains("准考证")).ToList();
        var scoreStages = stages.Where(s => s.StageName.Contains("成绩")).ToList();

        if (registrationStages.Count > 0 || admissionStages.Count > 0 || scoreStages.Count > 0)
        {
            sb.AppendLine("**📊 总览**  ");
            if (registrationStages.Count > 0)
                sb.AppendLine($"- 报名：**{registrationStages.Sum(s => s.RegistrationCount):N0}** 人（{registrationStages.Count} 个阶段）");
            if (admissionStages.Count > 0)
                sb.AppendLine($"- 准考证：**{admissionStages.Sum(s => s.AdmissionTicketCount):N0}** 人（{admissionStages.Count} 个阶段）");
            if (scoreStages.Count > 0)
                sb.AppendLine($"- 成绩查询：**{scoreStages.Sum(s => s.AdmissionTicketCount):N0}** 人（{scoreStages.Count} 个阶段）");
            sb.AppendLine();
        }

        // === Per App Server ===
        var appServerCounts = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        foreach (var stage in stages)
        {
            var servers = (stage.AppServers ?? "")
                .Split(['、', ',', '，'], StringSplitOptions.RemoveEmptyEntries)
                .Select(s => s.Trim())
                .Where(s => !string.IsNullOrWhiteSpace(s));
            var count = GetEffectiveCount(stage);
            foreach (var server in servers)
            {
                if (!appServerCounts.TryGetValue(server, out _))
                    appServerCounts[server] = 0;
                appServerCounts[server] += count;
            }
        }
        if (appServerCounts.Count > 0)
        {
            sb.AppendLine("**🖥 应用服务器**  ");
            foreach (var kv in appServerCounts.OrderByDescending(kv => kv.Value))
                sb.AppendLine($"- {kv.Key}：**{kv.Value:N0}** 人");
            sb.AppendLine();
        }

        // === Per Database Server ===
        var dbServerGroups = stages
            .GroupBy(s => s.ServerName)
            .Select(g => new { Server = g.Key, Count = g.Sum(s => GetEffectiveCount(s)) })
            .Where(g => g.Count > 0)
            .OrderByDescending(g => g.Count)
            .ToList();
        if (dbServerGroups.Count > 0)
        {
            sb.AppendLine("**💾 数据库服务器**  ");
            foreach (var g in dbServerGroups)
                sb.AppendLine($"- {g.Server}：**{g.Count:N0}** 人");
            sb.AppendLine();
        }

        // === Details ===
        var groups = stages
            .GroupBy(s => new { s.ProjectName, s.ServerName, s.ExamCode })
            .OrderBy(g => g.Min(s => s.StartTime))
            .ToList();

        sb.AppendLine("**📝 明细**  ");
        var index = 0;
        foreach (var group in groups)
        {
            index++;
            var first = group.First();
            sb.AppendLine($"**{index}. {first.ProjectName}**");
            sb.AppendLine($"> {first.ServerName} / 考试代码：{first.ExamCode}  ");

            foreach (var stage in group.OrderBy(s => s.StartTime))
            {
                var startStr = stage.StartTime.ToString("HH:mm");
                var endStr = stage.EndTime.ToString("MM-dd HH:mm");
                sb.AppendLine($"- **{stage.StageName}** {startStr} 至 {endStr}");

                var counts = new List<string>();
                if (stage.StageName.Contains("报名") && stage.RegistrationCount > 0)
                    counts.Add($"报名 {stage.RegistrationCount:N0} 人");
                if (stage.StageName.Contains("准考证") && stage.AdmissionTicketCount > 0)
                    counts.Add($"准考证 {stage.AdmissionTicketCount:N0} 人");
                if (stage.StageName.Contains("成绩") && stage.AdmissionTicketCount > 0)
                    counts.Add($"预估查询 {stage.AdmissionTicketCount:N0} 人");
                if (counts.Count > 0)
                    sb.AppendLine($"  - {string.Join("，", counts)}");
            }

            sb.AppendLine();
        }

        return (title, sb.ToString());
    }

    private static (string title, string text) BuildMaintainerMarkdownMessage(string maintainer, List<TodayStageInfo> stages)
    {
        var today = DateTime.Today.ToString("yyyy-MM-dd");
        var title = $"今日项目提醒 - {maintainer} ({today})";

        var sb = new StringBuilder();
        sb.AppendLine($"### 📋 {maintainer}，你今日有 **{stages.Count}** 个阶段开始");
        sb.AppendLine($"> 日期：**{today}**  ");
        sb.AppendLine();

        var groups = stages
            .GroupBy(s => new { s.ProjectName, s.ServerName, s.ExamCode })
            .OrderBy(g => g.Min(s => s.StartTime))
            .ToList();

        var index = 0;
        foreach (var group in groups)
        {
            index++;
            var first = group.First();
            sb.AppendLine($"**{index}. {first.ProjectName}**");
            sb.AppendLine($"> {first.ServerName} / 考试代码：{first.ExamCode}  ");

            foreach (var stage in group.OrderBy(s => s.StartTime))
            {
                var startStr = stage.StartTime.ToString("HH:mm");
                var endStr = stage.EndTime.ToString("MM-dd HH:mm");
                sb.AppendLine($"- **{stage.StageName}** {startStr} 至 {endStr}");

                var counts = new List<string>();
                if (stage.StageName.Contains("报名") && stage.RegistrationCount > 0)
                    counts.Add($"报名 {stage.RegistrationCount:N0} 人");
                if (stage.StageName.Contains("准考证") && stage.AdmissionTicketCount > 0)
                    counts.Add($"准考证 {stage.AdmissionTicketCount:N0} 人");
                if (stage.StageName.Contains("成绩") && stage.AdmissionTicketCount > 0)
                    counts.Add($"预估查询 {stage.AdmissionTicketCount:N0} 人");
                if (counts.Count > 0)
                    sb.AppendLine($"  - {string.Join("，", counts)}");
            }

            sb.AppendLine();
        }

        return (title, sb.ToString());
    }

    private static int GetEffectiveCount(TodayStageInfo stage)
    {
        var count = 0;
        if (stage.StageName.Contains("报名"))
            count += stage.RegistrationCount;
        if (stage.StageName.Contains("准考证"))
            count += stage.AdmissionTicketCount;
        if (stage.StageName.Contains("成绩"))
            count += stage.AdmissionTicketCount;
        return count;
    }

    private async Task SendDingTalkMessageAsync(
        DingTalkConfig config,
        string title,
        string markdownText,
        CancellationToken cancellationToken)
    {
        var targetUrl = config.WebhookUrl;

        // Sign if secret is provided
        if (!string.IsNullOrWhiteSpace(config.Secret))
        {
            var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var stringToSign = $"{timestamp}\n{config.Secret}";
            using var hmac = new HMACSHA256(Encoding.UTF8.GetBytes(config.Secret));
            var hash = hmac.ComputeHash(Encoding.UTF8.GetBytes(stringToSign));
            var sign = Uri.EscapeDataString(Convert.ToBase64String(hash));
            var separator = targetUrl.Contains('?') ? "&" : "?";
            targetUrl = $"{targetUrl}{separator}timestamp={timestamp}&sign={sign}";
        }

        var dingTalkMessage = new
        {
            msgtype = "markdown",
            markdown = new
            {
                title,
                text = markdownText
            }
        };

        var client = _httpClientFactory.CreateClient();
        HttpResponseMessage response;

        // Priority: registered proxy (heartbeat) > static config proxy
        var proxyUrl = _proxyRegistry.GetActiveProxyUrl() ?? config.ProxyUrl;

        // If proxy is configured, send via proxy; otherwise send directly
        if (!string.IsNullOrWhiteSpace(proxyUrl))
        {
            var proxyPayload = new
            {
                targetUrl,
                message = dingTalkMessage
            };
            _logger.LogDebug("Sending via proxy: {ProxyUrl}", proxyUrl);
            response = await client.PostAsJsonAsync(proxyUrl.TrimEnd('/'), proxyPayload, cancellationToken);
        }
        else
        {
            response = await client.PostAsJsonAsync(targetUrl, dingTalkMessage, cancellationToken);
        }

        var responseBody = await response.Content.ReadAsStringAsync(cancellationToken);

        if (!response.IsSuccessStatusCode)
        {
            _logger.LogError("DingTalk API returned {StatusCode}: {Body}", response.StatusCode, responseBody);
            throw new InvalidOperationException($"钉钉消息发送失败: {response.StatusCode}");
        }

        // Check DingTalk error code
        try
        {
            using var doc = JsonDocument.Parse(responseBody);
            var errCode = doc.RootElement.GetProperty("errcode").GetInt32();
            if (errCode != 0)
            {
                var errMsg = doc.RootElement.GetProperty("errmsg").GetString();
                _logger.LogError("DingTalk returned error: {Code} - {Message}", errCode, errMsg);
                throw new InvalidOperationException($"钉钉返回错误: {errCode} - {errMsg}");
            }
        }
        catch (JsonException)
        {
            // ignore parse errors
        }
    }

    private sealed class TodayStageInfo
    {
        public string ProjectName { get; set; } = "";
        public string StageName { get; set; } = "";
        public DateTime StartTime { get; set; }
        public DateTime EndTime { get; set; }
        public int RegistrationCount { get; set; }
        public int AdmissionTicketCount { get; set; }
        public string ServerName { get; set; } = "";
        public string DatabaseName { get; set; } = "";
        public string ExamCode { get; set; } = "";
        public string Maintainer { get; set; } = "";
        public string AppServers { get; set; } = "";
    }
}

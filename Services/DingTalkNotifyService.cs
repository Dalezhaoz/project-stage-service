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

    public DingTalkNotifyService(ILogger<DingTalkNotifyService> logger, IHttpClientFactory httpClientFactory)
    {
        _logger = logger;
        _httpClientFactory = httpClientFactory;
    }

    public async Task SendDailyReportAsync(
        SummaryStoreConfig summaryConfig,
        DingTalkConfig dingTalkConfig,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(dingTalkConfig.WebhookUrl))
        {
            _logger.LogWarning("DingTalk webhook URL is empty, skipping notification.");
            return;
        }

        var todayStages = await QueryTodayStartingStagesAsync(summaryConfig, cancellationToken);
        if (todayStages.Count == 0)
        {
            _logger.LogInformation("No stages starting today, skipping DingTalk notification.");
            return;
        }

        var markdown = BuildMarkdownMessage(todayStages);
        await SendDingTalkMessageAsync(dingTalkConfig, markdown.title, markdown.text, cancellationToken);
        _logger.LogInformation("DingTalk daily report sent: {Count} stages starting today.", todayStages.Count);
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
            SELECT project_name, stage_name, stage_start_time, stage_end_time,
                   registration_count, admission_ticket_count,
                   source_server_name, source_database_name, exam_code
            FROM dbo.project_stage_summary
            WHERE CAST(stage_start_time AS DATE) = CAST(GETDATE() AS DATE)
            ORDER BY stage_start_time, project_name, stage_name
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
                ExamCode = reader.GetString(8)
            });
        }

        return results;
    }

    private static (string title, string text) BuildMarkdownMessage(List<TodayStageInfo> stages)
    {
        var today = DateTime.Today.ToString("yyyy-MM-dd");
        var title = $"今日开始的项目阶段 ({today})";

        var totalRegistration = stages.Sum(s => s.RegistrationCount);
        var totalAdmission = stages.Sum(s => s.AdmissionTicketCount);

        var sb = new StringBuilder();
        sb.AppendLine($"### 📋 今日开始的项目阶段");
        sb.Append($"> 日期：**{today}**，共 **{stages.Count}** 个阶段");
        if (totalRegistration > 0 || totalAdmission > 0)
        {
            var totals = new List<string>();
            if (totalRegistration > 0) totals.Add($"报名 **{totalRegistration:N0}** 人");
            if (totalAdmission > 0) totals.Add($"准考证 **{totalAdmission:N0}** 人");
            sb.Append($"，合计 {string.Join("、", totals)}");
        }
        sb.AppendLine("  ");
        sb.AppendLine();

        // Group by project
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
                var isScoreStage = stage.StageName.Contains("成绩", StringComparison.OrdinalIgnoreCase);
                if (stage.RegistrationCount > 0)
                    counts.Add($"报名 {stage.RegistrationCount} 人");
                if (stage.AdmissionTicketCount > 0)
                    counts.Add($"准考证 {stage.AdmissionTicketCount} 人");
                if (isScoreStage && stage.AdmissionTicketCount > 0)
                    counts.Add($"预估查询 {stage.AdmissionTicketCount} 人");
                if (counts.Count > 0)
                    sb.AppendLine($"  - {string.Join("，", counts)}");
            }

            sb.AppendLine();
        }

        return (title, sb.ToString());
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

        // If proxy is configured, send via proxy; otherwise send directly
        if (!string.IsNullOrWhiteSpace(config.ProxyUrl))
        {
            var proxyPayload = new
            {
                targetUrl,
                message = dingTalkMessage
            };
            response = await client.PostAsJsonAsync(config.ProxyUrl.TrimEnd('/'), proxyPayload, cancellationToken);
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
    }
}

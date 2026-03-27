using System.Text;
using System.Text.Json;
using ProjectStageService.Models;

namespace ProjectStageService.Services;

public sealed class AgentClientService
{
    private static readonly HttpClient HttpClient = new() { Timeout = TimeSpan.FromMinutes(10) };
    private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };

    private readonly ILogger<AgentClientService> _logger;

    public AgentClientService(ILogger<AgentClientService> logger)
    {
        _logger = logger;
    }

    public async Task<object> TestAsync(StageServerConfig server, CancellationToken cancellationToken)
    {
        ValidateServer(server);

        var payload = new AgentTestPayload
        {
            ServerName = server.Name,
            Source = BuildSource(server),
            Definition = BuildDefinition(server)
        };

        var response = await PostEncryptedAsync<AgentTestResult>(server, "/test", payload, cancellationToken);
        return new
        {
            mode = "agent",
            visited_databases = response.VisitedDatabases,
            matched_databases = response.MatchedDatabases
        };
    }

    public async Task<AgentQueryResponse> QueryAsync(StageServerConfig server, CancellationToken cancellationToken)
    {
        ValidateServer(server);

        var payload = new AgentQueryPayload
        {
            ServerName = server.Name,
            Source = BuildSource(server),
            Definition = BuildDefinition(server)
        };

        return await PostEncryptedAsync<AgentQueryResponse>(server, "/query", payload, cancellationToken);
    }

    public async Task<AgentRefreshResult> SyncAsync(StageServerConfig server, SummaryStoreConfig summaryStoreConfig, CancellationToken cancellationToken)
    {
        ValidateServer(server);

        if (!summaryStoreConfig.Enabled)
        {
            throw new InvalidOperationException("请先启用中心库。");
        }

        var payload = new AgentSyncPayload
        {
            ServerName = server.Name,
            Source = BuildSource(server),
            Target = BuildTarget(summaryStoreConfig)
        };

        try
        {
            var result = await PostEncryptedAsync<AgentSyncResult>(server, "/sync", payload, cancellationToken);
            return new AgentRefreshResult
            {
                ServerName = server.Name,
                Success = true,
                Databases = result.Databases,
                Records = result.Records
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "调用 Agent 失败：{ServerName}", server.Name);
            return new AgentRefreshResult
            {
                ServerName = server.Name,
                Success = false,
                Error = ex.Message
            };
        }
    }

    private async Task<TResponse> PostEncryptedAsync<TResponse>(
        StageServerConfig server,
        string path,
        object payload,
        CancellationToken cancellationToken)
    {
        var envelope = AgentPayloadProtector.Encrypt(payload, server.AgentSecret);
        var json = JsonSerializer.Serialize(envelope);
        using var content = new StringContent(json, Encoding.UTF8, "application/json");

        var url = $"http://{server.Host}:{server.AgentPort}{path}";
        _logger.LogInformation("Calling agent endpoint {Url} for server {ServerName}.", url, server.Name);

        using var response = await HttpClient.PostAsync(url, content, cancellationToken);
        var responseBody = await response.Content.ReadAsStringAsync(cancellationToken);

        if (!response.IsSuccessStatusCode)
        {
            throw new InvalidOperationException($"HTTP {(int)response.StatusCode}: {responseBody}");
        }

        var result = JsonSerializer.Deserialize<TResponse>(responseBody, JsonOptions);
        if (result is null)
        {
            throw new InvalidOperationException("Agent 返回内容无效。");
        }

        return result;
    }

    private static AgentSourceConfig BuildSource(StageServerConfig server)
    {
        return new AgentSourceConfig
        {
            DatabaseType = server.DatabaseType,
            Host = "localhost",
            Port = server.Port,
            Username = server.Username,
            Password = server.Password
        };
    }

    private static AgentQueryDefinition BuildDefinition(StageServerConfig server)
    {
        return string.Equals(server.DatabaseType, "MySQL", StringComparison.OrdinalIgnoreCase)
            ? new AgentQueryDefinition
            {
                RequiredTables = ["mgt_exam_organize", "mgt_exam_step"],
                StageQuerySql = """
                    SELECT
                        CAST(a.id AS CHAR) AS exam_code,
                        a.name AS project_name,
                        b.name AS stage_name,
                        b.start_date AS start_time,
                        b.end_date AS end_time
                    FROM mgt_exam_organize a
                    JOIN mgt_exam_step b ON a.id = b.exam_id
                    ORDER BY b.start_date ASC
                    """,
                ExistingTablesSql = "SHOW TABLES",
                RegistrationTablePattern = "tb_ks_a001_{exam_code}",
                AdmissionTicketTablePattern = "tb_ks_kc_{exam_code}"
            }
            : new AgentQueryDefinition
            {
                RequiredTables = ["EI_ExamTreeDesc", "web_SR_CodeItem", "WEB_SR_SetTime"],
                StageQuerySql = """
                    SELECT
                        CONVERT(NVARCHAR(50), A.Code) AS exam_code,
                        CONVERT(NVARCHAR(500), A.NAME) AS project_name,
                        CONVERT(NVARCHAR(200), B.Description) AS stage_name,
                        C.KDate AS start_time,
                        C.ZDate AS end_time
                    FROM dbo.EI_ExamTreeDesc A
                    JOIN dbo.WEB_SR_SetTime C ON A.Code = C.ExamSort
                    JOIN dbo.web_SR_CodeItem B ON B.Codeid = 'WT' AND B.Code = C.Kind
                    WHERE A.CodeLen = '2' AND C.Kind <> '06'
                    ORDER BY C.KDate ASC
                    """,
                ExistingTablesSql = "SELECT name FROM sys.tables",
                RegistrationTablePattern = "考生表{exam_code}",
                AdmissionTicketTablePattern = "考场表{exam_code}"
            };
    }

    private static AgentTargetConfig BuildTarget(SummaryStoreConfig config)
    {
        return new AgentTargetConfig
        {
            Host = config.Host,
            Port = config.Port,
            DatabaseName = config.DatabaseName,
            Username = config.Username,
            Password = config.Password
        };
    }

    private static void ValidateServer(StageServerConfig server)
    {
        if (!server.Enabled)
        {
            throw new InvalidOperationException($"服务器 {server.Name} 未启用。");
        }

        if (string.IsNullOrWhiteSpace(server.Name) || string.IsNullOrWhiteSpace(server.Host))
        {
            throw new InvalidOperationException("服务器名称或地址不能为空。");
        }

        if (server.AgentPort <= 0)
        {
            throw new InvalidOperationException($"服务器 {server.Name} 未配置 Agent 端口。当前版本仅支持 Agent 模式。");
        }

        if (string.IsNullOrWhiteSpace(server.AgentSecret))
        {
            throw new InvalidOperationException($"服务器 {server.Name} 未配置 Agent 密钥。");
        }
    }

    private sealed class AgentSyncResult
    {
        public string ServerName { get; set; } = "";
        public int Databases { get; set; }
        public int Records { get; set; }
    }

    private sealed class AgentTestResult
    {
        public int VisitedDatabases { get; set; }
        public int MatchedDatabases { get; set; }
    }
}

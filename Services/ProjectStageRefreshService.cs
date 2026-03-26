using System.Text;
using System.Text.Json;
using ProjectStageService.Models;

namespace ProjectStageService.Services;

public sealed class ProjectStageRefreshService
{
    private static readonly HttpClient HttpClient = new() { Timeout = TimeSpan.FromMinutes(5) };
    private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };

    private readonly ServerConfigStore _serverConfigStore;
    private readonly ProjectStageQueryService _queryService;
    private readonly ProjectStageCacheStore _cacheStore;
    private readonly SummaryStoreConfigStore _summaryStoreConfigStore;
    private readonly SummaryStoreService _summaryStoreService;
    private readonly ILogger<ProjectStageRefreshService> _logger;
    private readonly SemaphoreSlim _refreshLock = new(1, 1);

    public ProjectStageRefreshService(
        ServerConfigStore serverConfigStore,
        ProjectStageQueryService queryService,
        ProjectStageCacheStore cacheStore,
        SummaryStoreConfigStore summaryStoreConfigStore,
        SummaryStoreService summaryStoreService,
        ILogger<ProjectStageRefreshService> logger)
    {
        _serverConfigStore = serverConfigStore;
        _queryService = queryService;
        _cacheStore = cacheStore;
        _summaryStoreConfigStore = summaryStoreConfigStore;
        _summaryStoreService = summaryStoreService;
        _logger = logger;
    }

    public async Task<ProjectStageRefreshResult> RefreshAsync(List<StageServerConfig>? servers, CancellationToken cancellationToken)
    {
        if (!await _refreshLock.WaitAsync(0, cancellationToken))
        {
            throw new InvalidOperationException("正在更新数据，请稍后再试。");
        }

        try
        {
            var sourceServers = servers?.Where(item => item.Enabled).ToList()
                ?? (await _serverConfigStore.LoadAsync(cancellationToken)).Where(item => item.Enabled).ToList();

            if (sourceServers.Count == 0)
            {
                throw new InvalidOperationException("请至少启用一台服务器后再更新数据。");
            }

            _logger.LogInformation("Starting stage cache refresh for {Count} enabled servers.", sourceServers.Count);

            // 分成两组：有 Agent 的走远程调用，没有的走直连查询
            var agentServers = sourceServers.Where(s => s.AgentPort > 0).ToList();
            var directServers = sourceServers.Where(s => s.AgentPort <= 0).ToList();

            var summaryStoreConfig = await _summaryStoreConfigStore.LoadAsync(cancellationToken);

            // 并行调用所有 Agent
            var agentResultList = new List<AgentRefreshResult>();
            if (agentServers.Count > 0 && summaryStoreConfig.Enabled)
            {
                var agentTasks = agentServers.Select(server => CallAgentAsync(server, summaryStoreConfig, cancellationToken));
                agentResultList.AddRange(await Task.WhenAll(agentTasks));
            }
            else if (agentServers.Count > 0)
            {
                _logger.LogWarning("有 {Count} 台服务器配置了 Agent 端口，但中心库未启用，已跳过 Agent 同步。", agentServers.Count);
                agentResultList.AddRange(agentServers.Select(s => new AgentRefreshResult
                {
                    ServerName = s.Name, Success = false, Error = "中心库未启用，已跳过"
                }));
            }

            // 直连查询（没有 Agent 的服务器，走原有逻辑）
            ProjectStageSummary summary;
            if (directServers.Count > 0)
            {
                summary = await _queryService.QueryAsync(new ProjectStageQueryRequest
                {
                    Servers = directServers,
                    StatusFilters = [],
                    StageKeyword = "",
                    StageNames = [],
                    ProjectKeyword = "",
                    RangeStart = null,
                    RangeEnd = null
                }, cancellationToken);
            }
            else
            {
                summary = new ProjectStageSummary { EnabledServers = 0 };
            }

            // 用实际启用的服务器总数覆盖 summary 中的值（包含 Agent 服务器）
            summary.EnabledServers = sourceServers.Count;

            // 汇总 Agent 结果到 summary
            var agentTotalRecords = agentResultList.Where(r => r.Success).Sum(r => r.Records);
            var agentTotalDatabases = agentResultList.Where(r => r.Success).Sum(r => r.Databases);
            summary.VisitedDatabases += agentTotalDatabases;
            summary.MatchedDatabases += agentTotalDatabases;

            await _cacheStore.SaveSnapshotAsync(summary, cancellationToken);

            if (summaryStoreConfig.Enabled && directServers.Count > 0)
            {
                await _summaryStoreService.SyncSnapshotAsync(summaryStoreConfig, summary, cancellationToken);
            }

            var totalRecords = summary.Records.Count + agentTotalRecords;
            var refreshedAt = DateTime.Now;
            _logger.LogInformation(
                "Stage cache refresh completed. DirectRecords={Direct}, AgentRecords={Agent}, Total={Total}.",
                summary.Records.Count, agentTotalRecords, totalRecords);

            return new ProjectStageRefreshResult
            {
                RefreshedAt = refreshedAt,
                EnabledServers = sourceServers.Count,
                VisitedDatabases = summary.VisitedDatabases,
                MatchedDatabases = summary.MatchedDatabases,
                RecordCount = totalRecords,
                AgentResults = agentResultList
            };
        }
        finally
        {
            _refreshLock.Release();
        }
    }

    private async Task<AgentRefreshResult> CallAgentAsync(StageServerConfig server, SummaryStoreConfig summaryStoreConfig, CancellationToken cancellationToken)
    {
        var url = $"http://{server.Host}:{server.AgentPort}/sync";
        _logger.LogInformation("Calling agent at {Url} for server {Name}.", url, server.Name);

        var payload = new
        {
            serverName = server.Name,
            source = new
            {
                databaseType = server.DatabaseType,
                host = "localhost",
                port = server.Port,
                username = server.Username,
                password = server.Password
            },
            target = new
            {
                host = summaryStoreConfig.Host,
                port = summaryStoreConfig.Port,
                databaseName = summaryStoreConfig.DatabaseName,
                username = summaryStoreConfig.Username,
                password = summaryStoreConfig.Password
            }
        };

        var json = JsonSerializer.Serialize(payload);
        var content = new StringContent(json, Encoding.UTF8, "application/json");

        try
        {
            var response = await HttpClient.PostAsync(url, content, cancellationToken);
            var responseBody = await response.Content.ReadAsStringAsync(cancellationToken);

            if (!response.IsSuccessStatusCode)
            {
                _logger.LogError("Agent {Name} returned {Code}: {Body}", server.Name, (int)response.StatusCode, responseBody);
                return new AgentRefreshResult
                {
                    ServerName = server.Name, Success = false,
                    Error = $"HTTP {(int)response.StatusCode}: {responseBody}"
                };
            }

            var result = JsonSerializer.Deserialize<AgentSyncResult>(responseBody, JsonOptions);
            _logger.LogInformation("Agent {Name} synced {Records} records from {Databases} databases.",
                server.Name, result?.Records ?? 0, result?.Databases ?? 0);
            return new AgentRefreshResult
            {
                ServerName = server.Name, Success = true,
                Databases = result?.Databases ?? 0, Records = result?.Records ?? 0
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to call agent for server {Name} at {Url}.", server.Name, url);
            return new AgentRefreshResult
            {
                ServerName = server.Name, Success = false, Error = ex.Message
            };
        }
    }

    private sealed class AgentSyncResult
    {
        public string ServerName { get; set; } = "";
        public int Databases { get; set; }
        public int Records { get; set; }
    }
}

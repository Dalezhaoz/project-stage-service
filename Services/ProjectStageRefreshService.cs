using ProjectStageService.Models;

namespace ProjectStageService.Services;

public sealed class ProjectStageRefreshService
{
    private readonly ServerConfigStore _serverConfigStore;
    private readonly AgentClientService _agentClientService;
    private readonly ProjectStageCacheStore _cacheStore;
    private readonly SummaryStoreConfigStore _summaryStoreConfigStore;
    private readonly SummaryStoreService _summaryStoreService;
    private readonly ProjectStageSummaryBuilder _summaryBuilder;
    private readonly ILogger<ProjectStageRefreshService> _logger;
    private readonly SemaphoreSlim _refreshLock = new(1, 1);

    public ProjectStageRefreshService(
        ServerConfigStore serverConfigStore,
        AgentClientService agentClientService,
        ProjectStageCacheStore cacheStore,
        SummaryStoreConfigStore summaryStoreConfigStore,
        SummaryStoreService summaryStoreService,
        ProjectStageSummaryBuilder summaryBuilder,
        ILogger<ProjectStageRefreshService> logger)
    {
        _serverConfigStore = serverConfigStore;
        _agentClientService = agentClientService;
        _cacheStore = cacheStore;
        _summaryStoreConfigStore = summaryStoreConfigStore;
        _summaryStoreService = summaryStoreService;
        _summaryBuilder = summaryBuilder;
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

            if (sourceServers.Any(item => item.AgentPort <= 0))
            {
                var invalidServers = string.Join("、", sourceServers.Where(item => item.AgentPort <= 0).Select(item => item.Name));
                throw new InvalidOperationException($"当前版本仅支持 Agent 模式，以下服务器未配置 Agent 端口：{invalidServers}");
            }

            _logger.LogInformation("Starting stage cache refresh for {Count} enabled servers.", sourceServers.Count);

            var summaryStoreConfig = await _summaryStoreConfigStore.LoadAsync(cancellationToken);
            if (!summaryStoreConfig.Enabled)
            {
                throw new InvalidOperationException("当前版本仅支持 Agent 写入中心表，请先启用中心库。");
            }

            var agentQueryTasks = sourceServers.Select(server => QueryServerThroughAgentAsync(server, cancellationToken));
            var agentQueryResults = (await Task.WhenAll(agentQueryTasks)).ToList();

            var records = agentQueryResults
                .SelectMany(item => item.Records)
                .ToList();

            var summary = _summaryBuilder.BuildSummary(
                records,
                sourceServers.Count,
                agentQueryResults.Sum(item => item.VisitedDatabases),
                agentQueryResults.Sum(item => item.MatchedDatabases));

            await _cacheStore.SaveSnapshotAsync(summary, cancellationToken);
            await _summaryStoreService.SyncSnapshotAsync(summaryStoreConfig, summary, cancellationToken);

            var totalRecords = summary.Records.Count;
            var refreshedAt = DateTime.Now;
            _logger.LogInformation(
                "Stage cache refresh completed through agents. Total={Total}.",
                totalRecords);

            return new ProjectStageRefreshResult
            {
                RefreshedAt = refreshedAt,
                EnabledServers = sourceServers.Count,
                VisitedDatabases = summary.VisitedDatabases,
                MatchedDatabases = summary.MatchedDatabases,
                RecordCount = totalRecords,
                AgentResults = agentQueryResults.Select(item => item.Result).ToList()
            };
        }
        finally
        {
            _refreshLock.Release();
        }
    }

    private async Task<AgentServerQueryAggregate> QueryServerThroughAgentAsync(StageServerConfig server, CancellationToken cancellationToken)
    {
        var agentResult = new AgentRefreshResult { ServerName = server.Name };

        try
        {
            var response = await _agentClientService.QueryAsync(server, cancellationToken);
            var now = DateTime.Now;
            var records = response.Records
                .Select(item => MapRecord(server.Name, item, now))
                .Where(item => item is not null)
                .Cast<ProjectStageRecord>()
                .ToList();

            agentResult.Success = true;
            agentResult.Databases = response.MatchedDatabases;
            agentResult.Records = records.Count;

            return new AgentServerQueryAggregate(
                response.VisitedDatabases,
                response.MatchedDatabases,
                records,
                agentResult);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Agent query failed for server {ServerName}.", server.Name);
            agentResult.Success = false;
            agentResult.Error = ex.Message;
            return new AgentServerQueryAggregate(0, 0, [], agentResult);
        }
    }

    private ProjectStageRecord? MapRecord(string serverName, AgentQueryRecord record, DateTime now)
    {
        if (!record.Values.TryGetValue("exam_code", out var examCode) ||
            !record.Values.TryGetValue("project_name", out var projectName) ||
            !record.Values.TryGetValue("stage_name", out var stageName) ||
            !record.Values.TryGetValue("start_time", out var startText) ||
            !record.Values.TryGetValue("end_time", out var endText))
        {
            return null;
        }

        if (string.IsNullOrWhiteSpace(examCode) ||
            string.IsNullOrWhiteSpace(projectName) ||
            string.IsNullOrWhiteSpace(stageName) ||
            !DateTime.TryParse(startText, out var startTime) ||
            !DateTime.TryParse(endText, out var endTime))
        {
            return null;
        }

        var status = now < startTime
            ? ProjectStageStatuses.Upcoming
            : now > endTime
                ? ProjectStageStatuses.Ended
                : ProjectStageStatuses.Ongoing;

        return new ProjectStageRecord
        {
            ServerName = serverName,
            DatabaseName = record.DatabaseName ?? "",
            ExamCode = examCode.Trim(),
            ProjectName = projectName.Trim(),
            StageName = stageName.Trim(),
            StartTime = startTime,
            EndTime = endTime,
            Status = status,
            RegistrationCount = record.Metrics.TryGetValue("registration_count", out var registrationCount) ? registrationCount : 0,
            AdmissionTicketCount = record.Metrics.TryGetValue("admission_ticket_count", out var admissionTicketCount) ? admissionTicketCount : 0
        };
    }

    private sealed record AgentServerQueryAggregate(
        int VisitedDatabases,
        int MatchedDatabases,
        List<ProjectStageRecord> Records,
        AgentRefreshResult Result);
}

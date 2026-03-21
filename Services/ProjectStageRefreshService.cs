using ProjectStageService.Models;

namespace ProjectStageService.Services;

public sealed class ProjectStageRefreshService
{
    private readonly ServerConfigStore _serverConfigStore;
    private readonly ProjectStageQueryService _queryService;
    private readonly ProjectStageCacheStore _cacheStore;
    private readonly ILogger<ProjectStageRefreshService> _logger;
    private readonly SemaphoreSlim _refreshLock = new(1, 1);

    public ProjectStageRefreshService(
        ServerConfigStore serverConfigStore,
        ProjectStageQueryService queryService,
        ProjectStageCacheStore cacheStore,
        ILogger<ProjectStageRefreshService> logger)
    {
        _serverConfigStore = serverConfigStore;
        _queryService = queryService;
        _cacheStore = cacheStore;
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

            var summary = await _queryService.QueryAsync(new ProjectStageQueryRequest
            {
                Servers = sourceServers,
                StatusFilters = [],
                StageKeyword = "",
                StageNames = [],
                ProjectKeyword = "",
                RangeStart = null,
                RangeEnd = null
            }, cancellationToken);

            await _cacheStore.SaveSnapshotAsync(summary, cancellationToken);

            var refreshedAt = DateTime.Now;
            _logger.LogInformation(
                "Stage cache refresh completed. Records={Records}, VisitedDatabases={Visited}, MatchedDatabases={Matched}.",
                summary.Records.Count,
                summary.VisitedDatabases,
                summary.MatchedDatabases);

            return new ProjectStageRefreshResult
            {
                RefreshedAt = refreshedAt,
                EnabledServers = summary.EnabledServers,
                VisitedDatabases = summary.VisitedDatabases,
                MatchedDatabases = summary.MatchedDatabases,
                RecordCount = summary.Records.Count
            };
        }
        finally
        {
            _refreshLock.Release();
        }
    }
}

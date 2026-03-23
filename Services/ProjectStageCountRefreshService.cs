using ProjectStageService.Models;

namespace ProjectStageService.Services;

public sealed class ProjectStageCountRefreshService
{
    private readonly ServerConfigStore _serverConfigStore;
    private readonly ProjectStageCacheStore _cacheStore;
    private readonly ProjectStageQueryService _queryService;
    private readonly ILogger<ProjectStageCountRefreshService> _logger;
    private readonly SemaphoreSlim _refreshLock = new(1, 1);

    public ProjectStageCountRefreshService(
        ServerConfigStore serverConfigStore,
        ProjectStageCacheStore cacheStore,
        ProjectStageQueryService queryService,
        ILogger<ProjectStageCountRefreshService> logger)
    {
        _serverConfigStore = serverConfigStore;
        _cacheStore = cacheStore;
        _queryService = queryService;
        _logger = logger;
    }

    public async Task RefreshAsync(CancellationToken cancellationToken)
    {
        if (!await _refreshLock.WaitAsync(0, cancellationToken))
        {
            _logger.LogInformation("Count refresh is already running. Skipping this schedule.");
            return;
        }

        try
        {
            var servers = (await _serverConfigStore.LoadAsync(cancellationToken))
                .Where(item => item.Enabled)
                .ToList();

            if (servers.Count == 0)
            {
                _logger.LogInformation("Skipping count refresh because no enabled server is configured.");
                return;
            }

            _logger.LogInformation("Starting scheduled count refresh for {Count} enabled servers.", servers.Count);

            foreach (var server in servers)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var targets = await _cacheStore.LoadCountTargetsAsync(server.Name, cancellationToken);
                if (targets.Count == 0)
                {
                    _logger.LogInformation("Skipping server {ServerName} because cached targets are empty.", server.Name);
                    continue;
                }

                _logger.LogInformation(
                    "Refreshing registration counts for server {ServerName}. Targets={TargetCount}.",
                    server.Name,
                    targets.Count);

                var registrationUpdates = await _queryService.QueryServerCountsAsync(
                    server,
                    targets,
                    includeRegistrationCount: true,
                    includeAdmissionTicketCount: false,
                    cancellationToken: cancellationToken);
                await _cacheStore.SaveServerCountsAsync(server.Name, registrationUpdates, cancellationToken);

                _logger.LogInformation(
                    "Refreshing admission ticket counts for server {ServerName}. Targets={TargetCount}.",
                    server.Name,
                    targets.Count);

                var admissionUpdates = await _queryService.QueryServerCountsAsync(
                    server,
                    targets,
                    includeRegistrationCount: false,
                    includeAdmissionTicketCount: true,
                    cancellationToken: cancellationToken);
                await _cacheStore.SaveServerCountsAsync(server.Name, admissionUpdates, cancellationToken);

                _logger.LogInformation("Scheduled count refresh completed for server {ServerName}.", server.Name);
            }

            _logger.LogInformation("Scheduled count refresh completed for all enabled servers.");
        }
        finally
        {
            _refreshLock.Release();
        }
    }
}

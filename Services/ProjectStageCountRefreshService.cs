namespace ProjectStageService.Services;

public sealed class ProjectStageCountRefreshService
{
    private readonly ProjectStageRefreshService _refreshService;
    private readonly ILogger<ProjectStageCountRefreshService> _logger;
    private readonly SemaphoreSlim _refreshLock = new(1, 1);

    public ProjectStageCountRefreshService(
        ProjectStageRefreshService refreshService,
        ILogger<ProjectStageCountRefreshService> logger)
    {
        _refreshService = refreshService;
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
            _logger.LogInformation("Starting scheduled count refresh through agents.");
            await _refreshService.RefreshAsync(null, cancellationToken);
            _logger.LogInformation("Scheduled count refresh through agents completed.");
        }
        finally
        {
            _refreshLock.Release();
        }
    }
}

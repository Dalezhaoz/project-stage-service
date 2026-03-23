namespace ProjectStageService.Services;

public sealed class ProjectStageRefreshHostedService : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<ProjectStageRefreshHostedService> _logger;

    public ProjectStageRefreshHostedService(
        IServiceProvider serviceProvider,
        ILogger<ProjectStageRefreshHostedService> logger)
    {
        _serviceProvider = serviceProvider;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            var nextRun = DateTime.Today.AddHours(4);
            if (nextRun <= DateTime.Now)
            {
                nextRun = nextRun.AddDays(1);
            }

            var delay = nextRun - DateTime.Now;

            _logger.LogInformation("Next automatic stage cache refresh scheduled at {NextRun}.", nextRun);

            try
            {
                await Task.Delay(delay, stoppingToken);
            }
            catch (TaskCanceledException)
            {
                break;
            }

            try
            {
                using var scope = _serviceProvider.CreateScope();
                var refreshService = scope.ServiceProvider.GetRequiredService<ProjectStageRefreshService>();
                await refreshService.RefreshAsync(null, stoppingToken);
            }
            catch (TaskCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Automatic stage cache refresh failed.");
            }
        }
    }
}

namespace ProjectStageService.Services;

public sealed class ProjectStageCountRefreshHostedService : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<ProjectStageCountRefreshHostedService> _logger;

    public ProjectStageCountRefreshHostedService(
        IServiceProvider serviceProvider,
        ILogger<ProjectStageCountRefreshHostedService> logger)
    {
        _serviceProvider = serviceProvider;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            var nextRun = DateTime.Today.AddHours(6).AddMinutes(30);
            if (nextRun <= DateTime.Now)
            {
                nextRun = nextRun.AddDays(1);
            }

            var delay = nextRun - DateTime.Now;

            _logger.LogInformation("Next automatic count refresh scheduled at {NextRun}.", nextRun);

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
                var refreshService = scope.ServiceProvider.GetRequiredService<ProjectStageCountRefreshService>();
                await refreshService.RefreshAsync(stoppingToken);
            }
            catch (TaskCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Automatic count refresh failed.");
            }
        }
    }
}

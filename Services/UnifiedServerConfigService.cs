using ProjectStageService.Models;

namespace ProjectStageService.Services;

public sealed class UnifiedServerConfigService
{
    private readonly ServerConfigStore _serverStore;
    private readonly MonitorServerConfigStore _monitorStore;
    private readonly AppServerNodeStore _appServerNodeStore;
    private readonly SummaryStoreConfigStore _summaryStoreConfigStore;
    private readonly SummaryStoreService _summaryStoreService;
    private readonly ProjectMetadataService _projectMetadataService;

    public UnifiedServerConfigService(
        ServerConfigStore serverStore,
        MonitorServerConfigStore monitorStore,
        AppServerNodeStore appServerNodeStore,
        SummaryStoreConfigStore summaryStoreConfigStore,
        SummaryStoreService summaryStoreService,
        ProjectMetadataService projectMetadataService)
    {
        _serverStore = serverStore;
        _monitorStore = monitorStore;
        _appServerNodeStore = appServerNodeStore;
        _summaryStoreConfigStore = summaryStoreConfigStore;
        _summaryStoreService = summaryStoreService;
        _projectMetadataService = projectMetadataService;
    }

    public async Task<UnifiedServerConfigPayload> LoadAsync(CancellationToken cancellationToken)
    {
        var databaseServers = await _serverStore.LoadAsync(cancellationToken);
        var monitorServers = await _monitorStore.LoadAsync(cancellationToken);
        var appServers = await _appServerNodeStore.LoadAsync(cancellationToken);
        var summaryStore = await _summaryStoreConfigStore.LoadAsync(cancellationToken);

        if (appServers.Count == 0)
        {
            var options = await _projectMetadataService.GetAppServerOptionsAsync(cancellationToken);
            appServers = options
                .Select(item => new AppServerNode
                {
                    Name = item.Name,
                    Enabled = true,
                    FrontSiteName = item.Name,
                    BackSiteName = item.Name.EndsWith("gl", StringComparison.OrdinalIgnoreCase) ? item.Name : $"{item.Name}gl"
                })
                .ToList();
        }

        return new UnifiedServerConfigPayload
        {
            DatabaseServers = databaseServers,
            MonitorServers = monitorServers,
            AppServers = appServers,
            SummaryStore = summaryStore
        };
    }

    public async Task SaveAsync(UnifiedServerConfigPayload payload, CancellationToken cancellationToken)
    {
        payload ??= new UnifiedServerConfigPayload();
        payload.DatabaseServers ??= [];
        payload.MonitorServers ??= [];
        payload.AppServers ??= [];
        payload.SummaryStore ??= new SummaryStoreConfig();

        await _serverStore.SaveAsync(payload.DatabaseServers, cancellationToken);
        await _monitorStore.SaveAsync(payload.MonitorServers, cancellationToken);
        await _appServerNodeStore.SaveAsync(payload.AppServers, cancellationToken);
        await _summaryStoreConfigStore.SaveAsync(payload.SummaryStore, cancellationToken);

        var optionNames = payload.AppServers
            .Where(item => item.Enabled && !string.IsNullOrWhiteSpace(item.Name))
            .Select(item => item.Name.Trim())
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(item => item, StringComparer.OrdinalIgnoreCase)
            .ToList();
        await _projectMetadataService.SaveAppServerOptionsAsync(optionNames, cancellationToken);

        if (payload.SummaryStore.Enabled)
        {
            await _summaryStoreService.EnsureSchemaAsync(payload.SummaryStore, cancellationToken);
        }
    }
}


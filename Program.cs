using ProjectStageService.Models;
using ProjectStageService.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSingleton<ProjectStageQueryService>();
builder.Services.AddSingleton<ProjectStageExportService>();
builder.Services.AddSingleton<ServerConfigStore>();
builder.Services.AddSingleton<ProjectStageCacheStore>();
builder.Services.AddSingleton<ProjectStageRefreshService>();
builder.Services.AddHostedService<ProjectStageRefreshHostedService>();

var app = builder.Build();

await app.Services.GetRequiredService<ProjectStageCacheStore>().InitializeAsync(CancellationToken.None);

app.UseDefaultFiles();
app.UseStaticFiles();

app.MapGet("/health", () => Results.Ok(new { status = "ok" }));

app.MapGet("/api/servers", async (ServerConfigStore store, CancellationToken cancellationToken) =>
{
    var servers = await store.LoadAsync(cancellationToken);
    return Results.Ok(servers);
});

app.MapPost("/api/servers", async (List<StageServerConfig> servers, ServerConfigStore store, CancellationToken cancellationToken) =>
{
    await store.SaveAsync(servers, cancellationToken);
    return Results.Ok(new { saved = servers.Count });
});

app.MapPost("/api/test", async (TestConnectionRequest request, ProjectStageQueryService queryService, CancellationToken cancellationToken) =>
{
    try
    {
        var result = await queryService.TestConnectionAsync(request.Server, cancellationToken);
        return Results.Ok(result);
    }
    catch (Exception ex)
    {
        return Results.BadRequest(new { detail = ex.Message });
    }
});

app.MapGet("/api/cache-info", async (ProjectStageCacheStore cacheStore, CancellationToken cancellationToken) =>
{
    var cacheInfo = await cacheStore.GetCacheInfoAsync(cancellationToken);
    return Results.Ok(cacheInfo);
});

app.MapPost("/api/refresh", async (ProjectStageRefreshRequest request, ProjectStageRefreshService refreshService, CancellationToken cancellationToken) =>
{
    try
    {
        var result = await refreshService.RefreshAsync(request.Servers, cancellationToken);
        return Results.Ok(result);
    }
    catch (Exception ex)
    {
        return Results.BadRequest(new { detail = ex.Message });
    }
});

app.MapPost("/api/query", async (ProjectStageQueryRequest request, ProjectStageCacheStore cacheStore, CancellationToken cancellationToken) =>
{
    try
    {
        var summary = await cacheStore.QueryAsync(request, cancellationToken);
        return Results.Ok(summary);
    }
    catch (Exception ex)
    {
        return Results.BadRequest(new { detail = ex.Message });
    }
});

app.MapPost("/api/stages", async (ProjectStageQueryRequest request, ProjectStageCacheStore cacheStore, CancellationToken cancellationToken) =>
{
    try
    {
        var stageNames = await cacheStore.QueryStageNamesAsync(request, cancellationToken);
        return Results.Ok(stageNames);
    }
    catch (Exception ex)
    {
        return Results.BadRequest(new { detail = ex.Message });
    }
});

app.MapPost("/api/export", async (ProjectStageQueryRequest request, ProjectStageCacheStore cacheStore, ProjectStageExportService exportService, CancellationToken cancellationToken) =>
{
    try
    {
        var summary = await cacheStore.QueryAsync(request, cancellationToken);
        var content = exportService.Export(summary);
        return Results.File(
            content,
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "项目阶段汇总.xlsx");
    }
    catch (Exception ex)
    {
        return Results.BadRequest(new { detail = ex.Message });
    }
});

app.Run();

using ProjectStageService.Models;
using ProjectStageService.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSingleton<ProjectStageQueryService>();
builder.Services.AddSingleton<ProjectStageExportService>();

var app = builder.Build();

app.UseDefaultFiles();
app.UseStaticFiles();

app.MapGet("/health", () => Results.Ok(new { status = "ok" }));

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

app.MapPost("/api/query", async (ProjectStageQueryRequest request, ProjectStageQueryService queryService, CancellationToken cancellationToken) =>
{
    try
    {
        var summary = await queryService.QueryAsync(request, cancellationToken);
        return Results.Ok(summary);
    }
    catch (Exception ex)
    {
        return Results.BadRequest(new { detail = ex.Message });
    }
});

app.MapPost("/api/export", async (ProjectStageQueryRequest request, ProjectStageQueryService queryService, ProjectStageExportService exportService, CancellationToken cancellationToken) =>
{
    try
    {
        var summary = await queryService.QueryAsync(request, cancellationToken);
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

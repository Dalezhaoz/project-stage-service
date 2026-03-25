using ProjectStageService.Models;
using ProjectStageService.Services;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Cookies;
using System.Security.Claims;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddAuthentication(CookieAuthenticationDefaults.AuthenticationScheme)
    .AddCookie(options =>
    {
        options.LoginPath = "/";
        options.Cookie.Name = "ProjectStageService.Auth";
        options.ExpireTimeSpan = TimeSpan.FromHours(12);
        options.SlidingExpiration = true;
        options.Events.OnRedirectToLogin = context =>
        {
            if (context.Request.Path.StartsWithSegments("/api"))
            {
                context.Response.StatusCode = StatusCodes.Status401Unauthorized;
                return Task.CompletedTask;
            }

            context.Response.Redirect(context.RedirectUri);
            return Task.CompletedTask;
        };
    });
builder.Services.AddAuthorization(options =>
{
    options.AddPolicy("AdminOnly", policy => policy.RequireClaim("is_admin", "true"));
});
builder.Services.AddSingleton<ProjectStageQueryService>();
builder.Services.AddSingleton<ProjectStageExportService>();
builder.Services.AddSingleton<ServerConfigStore>();
builder.Services.AddSingleton<ProjectStageCacheStore>();
builder.Services.AddSingleton<ProjectStageRefreshService>();
builder.Services.AddSingleton<ProjectStageCountRefreshService>();
builder.Services.AddSingleton<LocalAuthService>();
builder.Services.AddSingleton<SummaryStoreConfigStore>();
builder.Services.AddSingleton<SummaryStoreService>();
builder.Services.AddSingleton<ScheduleConfigStore>();
builder.Services.AddHostedService<ProjectStageRefreshHostedService>();
builder.Services.AddHostedService<ProjectStageCountRefreshHostedService>();

var app = builder.Build();

await app.Services.GetRequiredService<ProjectStageCacheStore>().InitializeAsync(CancellationToken.None);

app.UseDefaultFiles();
app.UseStaticFiles();
app.UseAuthentication();
app.Use(async (context, next) =>
{
    if (context.User.Identity?.IsAuthenticated == true &&
        context.User.HasClaim("force_password_change", "true") &&
        context.Request.Path.StartsWithSegments("/api") &&
        !context.Request.Path.StartsWithSegments("/api/auth/status") &&
        !context.Request.Path.StartsWithSegments("/api/auth/change-password") &&
        !context.Request.Path.StartsWithSegments("/api/auth/logout"))
    {
        context.Response.StatusCode = StatusCodes.Status403Forbidden;
        await context.Response.WriteAsJsonAsync(new { detail = "首次登录必须先修改密码。" });
        return;
    }

    await next();
});
app.UseAuthorization();

app.MapGet("/health", () => Results.Ok(new { status = "ok" }));

app.MapGet("/api/auth/status", async (HttpContext httpContext, LocalAuthService authService, CancellationToken cancellationToken) =>
{
    var currentUsername = httpContext.User.Identity?.IsAuthenticated == true ? httpContext.User.Identity?.Name : null;
    var (hasAccount, username, isAdmin, forcePasswordChange) = await authService.GetStatusAsync(currentUsername, cancellationToken);
    return Results.Ok(new
    {
        authenticated = currentUsername is not null,
        hasAccount,
        username,
        isAdmin,
        forcePasswordChange
    });
});

app.MapPost("/api/auth/setup", async (SetupAuthRequest request, HttpContext httpContext, LocalAuthService authService, CancellationToken cancellationToken) =>
{
    if (request.Password != request.ConfirmPassword)
    {
        return Results.BadRequest(new { detail = "两次输入的密码不一致。" });
    }

    try
    {
        await authService.SetupAsync(request.Username, request.Password, cancellationToken);

        var claims = new List<Claim>
        {
            new(ClaimTypes.Name, request.Username.Trim()),
            new("is_admin", "true"),
            new("force_password_change", "false")
        };
        var identity = new ClaimsIdentity(claims, CookieAuthenticationDefaults.AuthenticationScheme);
        await httpContext.SignInAsync(
            CookieAuthenticationDefaults.AuthenticationScheme,
            new ClaimsPrincipal(identity));

        return Results.Ok(new { username = request.Username.Trim() });
    }
    catch (Exception ex)
    {
        return Results.BadRequest(new { detail = ex.Message });
    }
});

app.MapPost("/api/auth/login", async (LoginRequest request, HttpContext httpContext, LocalAuthService authService, CancellationToken cancellationToken) =>
{
    var (hasAccount, _, _, _) = await authService.GetStatusAsync(null, cancellationToken);
    if (!hasAccount)
    {
        return Results.BadRequest(new { detail = "系统尚未初始化账号，请先设置登录账号。" });
    }

    var result = await authService.ValidateAsync(request.Username, request.Password, cancellationToken);
    if (!result.Success)
    {
        return Results.BadRequest(new { detail = "用户名或密码错误。" });
    }

    var claims = new List<Claim>
    {
        new(ClaimTypes.Name, request.Username.Trim()),
        new("is_admin", result.IsAdmin ? "true" : "false"),
        new("force_password_change", result.ForcePasswordChange ? "true" : "false")
    };
    var identity = new ClaimsIdentity(claims, CookieAuthenticationDefaults.AuthenticationScheme);
    await httpContext.SignInAsync(
        CookieAuthenticationDefaults.AuthenticationScheme,
        new ClaimsPrincipal(identity));

    return Results.Ok(new { username = request.Username.Trim() });
});

app.MapPost("/api/auth/users", async (CreateUserRequest request, LocalAuthService authService, CancellationToken cancellationToken) =>
{
    try
    {
        await authService.CreateUserAsync(request.Username, cancellationToken);
        return Results.Ok(new { username = request.Username.Trim(), defaultPassword = LocalAuthService.DefaultPassword });
    }
    catch (Exception ex)
    {
        return Results.BadRequest(new { detail = ex.Message });
    }
}).RequireAuthorization("AdminOnly");

app.MapGet("/api/auth/users", async (LocalAuthService authService, CancellationToken cancellationToken) =>
{
    return Results.Ok(await authService.GetUsersAsync(cancellationToken));
}).RequireAuthorization("AdminOnly");

app.MapPost("/api/auth/change-password", async (ChangePasswordRequest request, HttpContext httpContext, LocalAuthService authService, CancellationToken cancellationToken) =>
{
    if (request.NewPassword != request.ConfirmPassword)
    {
        return Results.BadRequest(new { detail = "两次输入的新密码不一致。" });
    }

    var username = httpContext.User.Identity?.Name;
    if (string.IsNullOrWhiteSpace(username))
    {
        return Results.Unauthorized();
    }

    try
    {
        await authService.ChangePasswordAsync(username, request.CurrentPassword, request.NewPassword, cancellationToken);
        var isAdmin = await authService.IsAdminAsync(username, cancellationToken);
        var claims = new List<Claim>
        {
            new(ClaimTypes.Name, username),
            new("is_admin", isAdmin ? "true" : "false"),
            new("force_password_change", "false")
        };
        var identity = new ClaimsIdentity(claims, CookieAuthenticationDefaults.AuthenticationScheme);
        await httpContext.SignInAsync(
            CookieAuthenticationDefaults.AuthenticationScheme,
            new ClaimsPrincipal(identity));
        return Results.Ok(new { ok = true });
    }
    catch (Exception ex)
    {
        return Results.BadRequest(new { detail = ex.Message });
    }
}).RequireAuthorization();

app.MapPost("/api/auth/reset-password", async (ResetPasswordRequest request, LocalAuthService authService, CancellationToken cancellationToken) =>
{
    try
    {
        await authService.ResetPasswordAsync(request.Username, cancellationToken);
        return Results.Ok(new { username = request.Username.Trim(), defaultPassword = LocalAuthService.DefaultPassword });
    }
    catch (Exception ex)
    {
        return Results.BadRequest(new { detail = ex.Message });
    }
}).RequireAuthorization("AdminOnly");

app.MapPost("/api/auth/logout", async (HttpContext httpContext) =>
{
    await httpContext.SignOutAsync(CookieAuthenticationDefaults.AuthenticationScheme);
    return Results.Ok(new { ok = true });
}).RequireAuthorization();

app.MapGet("/api/summary-store", async (SummaryStoreConfigStore store, CancellationToken cancellationToken) =>
{
    return Results.Ok(await store.LoadAsync(cancellationToken));
}).RequireAuthorization("AdminOnly");

app.MapPost("/api/summary-store", async (SummaryStoreConfig config, SummaryStoreConfigStore store, SummaryStoreService service, CancellationToken cancellationToken) =>
{
    await store.SaveAsync(config, cancellationToken);
    if (config.Enabled)
    {
        await service.EnsureSchemaAsync(config, cancellationToken);
    }

    return Results.Ok(new { saved = true });
}).RequireAuthorization("AdminOnly");

app.MapPost("/api/summary-store/test", async (SummaryStoreTestRequest request, SummaryStoreService service, CancellationToken cancellationToken) =>
{
    try
    {
        var result = await service.TestConnectionAsync(request.Config, cancellationToken);
        return Results.Ok(result);
    }
    catch (Exception ex)
    {
        return Results.BadRequest(new { detail = ex.Message });
    }
}).RequireAuthorization("AdminOnly");

app.MapGet("/api/schedule", async (ScheduleConfigStore store, CancellationToken cancellationToken) =>
{
    return Results.Ok(await store.LoadAsync(cancellationToken));
}).RequireAuthorization("AdminOnly");

app.MapPost("/api/schedule", async (ScheduleConfig config, ScheduleConfigStore store, CancellationToken cancellationToken) =>
{
    await store.SaveAsync(config, cancellationToken);
    return Results.Ok(new { saved = true });
}).RequireAuthorization("AdminOnly");

app.MapGet("/api/servers", async (ServerConfigStore store, CancellationToken cancellationToken) =>
{
    var servers = await store.LoadAsync(cancellationToken);
    return Results.Ok(servers);
}).RequireAuthorization();

app.MapPost("/api/servers", async (List<StageServerConfig> servers, ServerConfigStore store, CancellationToken cancellationToken) =>
{
    await store.SaveAsync(servers, cancellationToken);
    return Results.Ok(new { saved = servers.Count });
}).RequireAuthorization();

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
}).RequireAuthorization();

app.MapGet("/api/cache-info", async (ProjectStageCacheStore cacheStore, CancellationToken cancellationToken) =>
{
    var cacheInfo = await cacheStore.GetCacheInfoAsync(cancellationToken);
    return Results.Ok(cacheInfo);
}).RequireAuthorization();

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
}).RequireAuthorization();

app.MapPost("/api/query", async (ProjectStageQueryRequest request, CancellationToken cancellationToken) =>
{
    try
    {
        var summaryStoreConfigStore = app.Services.GetRequiredService<SummaryStoreConfigStore>();
        var summaryStoreService = app.Services.GetRequiredService<SummaryStoreService>();
        var summaryStoreConfig = await summaryStoreConfigStore.LoadAsync(cancellationToken);

        if (!summaryStoreConfig.Enabled)
            throw new InvalidOperationException("请先在"中心库"中配置并启用中心表。");

        var summary = await summaryStoreService.QueryAsync(summaryStoreConfig, request, cancellationToken);
        return Results.Ok(summary);
    }
    catch (Exception ex)
    {
        return Results.BadRequest(new { detail = ex.Message });
    }
}).RequireAuthorization();

app.MapPost("/api/board-counts", async (BoardCountRequest request, ProjectStageQueryService queryService, CancellationToken cancellationToken) =>
{
    try
    {
        var summary = await queryService.QueryAsync(
            request.Query,
            request.IncludeRegistrationCount,
            request.IncludeAdmissionTicketCount,
            request.Targets,
            cancellationToken);
        return Results.Ok(summary);
    }
    catch (Exception ex)
    {
        return Results.BadRequest(new { detail = ex.Message });
    }
}).RequireAuthorization();

app.MapPost("/api/stages", async (ProjectStageQueryRequest request, CancellationToken cancellationToken) =>
{
    try
    {
        var summaryStoreConfigStore = app.Services.GetRequiredService<SummaryStoreConfigStore>();
        var summaryStoreService = app.Services.GetRequiredService<SummaryStoreService>();
        var summaryStoreConfig = await summaryStoreConfigStore.LoadAsync(cancellationToken);

        if (!summaryStoreConfig.Enabled)
            throw new InvalidOperationException("请先在"中心库"中配置并启用中心表。");

        var stageNames = await summaryStoreService.QueryStageNamesAsync(summaryStoreConfig, request, cancellationToken);
        return Results.Ok(stageNames);
    }
    catch (Exception ex)
    {
        return Results.BadRequest(new { detail = ex.Message });
    }
}).RequireAuthorization();

app.MapPost("/api/export", async (ProjectStageQueryRequest request, ProjectStageExportService exportService, CancellationToken cancellationToken) =>
{
    try
    {
        var summaryStoreConfigStore = app.Services.GetRequiredService<SummaryStoreConfigStore>();
        var summaryStoreService = app.Services.GetRequiredService<SummaryStoreService>();
        var summaryStoreConfig = await summaryStoreConfigStore.LoadAsync(cancellationToken);

        if (!summaryStoreConfig.Enabled)
            throw new InvalidOperationException("请先在"中心库"中配置并启用中心表。");

        var summary = await summaryStoreService.QueryAsync(summaryStoreConfig, request, cancellationToken);
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
}).RequireAuthorization();

app.Run();

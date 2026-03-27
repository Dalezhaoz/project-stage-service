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
    options.AddPolicy("AdminOnly", policy => policy.RequireClaim("role", "admin"));
    options.AddPolicy("InternalOrAbove", policy => policy.RequireClaim("role", "admin", "internal"));
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
builder.Services.AddSingleton<DingTalkNotifyService>();
builder.Services.AddSingleton<DingTalkProxyRegistry>();
builder.Services.AddSingleton<ProjectMetadataService>();
builder.Services.AddHttpClient();
builder.Services.AddHostedService<ProjectStageRefreshHostedService>();
builder.Services.AddHostedService<ProjectStageCountRefreshHostedService>();
builder.Services.AddHostedService<DingTalkNotifyHostedService>();

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
    var (hasAccount, username, role, forcePasswordChange) = await authService.GetStatusAsync(currentUsername, cancellationToken);
    var allowUserRefresh = await authService.GetAllowUserRefreshAsync(cancellationToken);
    return Results.Ok(new
    {
        authenticated = currentUsername is not null,
        hasAccount,
        username,
        role,
        isAdmin = role == "admin",
        forcePasswordChange,
        allowUserRefresh
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
            new("role", "admin"),
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
        new("role", result.Role),
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
        await authService.CreateUserAsync(request.Username, request.Role, cancellationToken);
        return Results.Ok(new { username = request.Username.Trim(), role = request.Role, defaultPassword = LocalAuthService.DefaultPassword });
    }
    catch (Exception ex)
    {
        return Results.BadRequest(new { detail = ex.Message });
    }
}).RequireAuthorization("AdminOnly");

app.MapGet("/api/auth/users", async (LocalAuthService authService, CancellationToken cancellationToken) =>
{
    return Results.Ok(await authService.GetUsersAsync(cancellationToken));
}).RequireAuthorization("InternalOrAbove");

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
        var role = await authService.GetRoleAsync(username, cancellationToken);
        var claims = new List<Claim>
        {
            new(ClaimTypes.Name, username),
            new("role", role),
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

app.MapPost("/api/auth/user-dingtalk", async (UpdateUserDingTalkRequest request, LocalAuthService authService, CancellationToken cancellationToken) =>
{
    try
    {
        await authService.UpdateUserDingTalkAsync(request.Username, request.WebhookUrl, request.Secret, cancellationToken);
        return Results.Ok(new { ok = true });
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

app.MapGet("/api/auth/my-dingtalk", async (HttpContext httpContext, LocalAuthService authService, CancellationToken cancellationToken) =>
{
    var username = httpContext.User.Identity?.Name;
    if (string.IsNullOrWhiteSpace(username)) return Results.Unauthorized();
    var users = await authService.GetUsersAsync(cancellationToken);
    var me = users.FirstOrDefault(u => string.Equals(u.Username, username, StringComparison.Ordinal));
    return Results.Ok(new { webhookUrl = me?.DingTalkWebhook ?? "", secret = me?.DingTalkSecret ?? "" });
}).RequireAuthorization();

app.MapPost("/api/auth/my-dingtalk", async (UpdateUserDingTalkRequest request, HttpContext httpContext, LocalAuthService authService, CancellationToken cancellationToken) =>
{
    var username = httpContext.User.Identity?.Name;
    if (string.IsNullOrWhiteSpace(username)) return Results.Unauthorized();
    try
    {
        await authService.UpdateUserDingTalkAsync(username, request.WebhookUrl, request.Secret, cancellationToken);
        return Results.Ok(new { ok = true });
    }
    catch (Exception ex)
    {
        return Results.BadRequest(new { detail = ex.Message });
    }
}).RequireAuthorization();

app.MapPost("/api/dingtalk/test-personal", async (HttpContext httpContext, DingTalkNotifyService notifyService, SummaryStoreConfigStore summaryStoreConfigStore, LocalAuthService authService, ScheduleConfigStore scheduleConfigStore, CancellationToken cancellationToken) =>
{
    var username = httpContext.User.Identity?.Name;
    if (string.IsNullOrWhiteSpace(username)) return Results.Unauthorized();

    try
    {
        var users = await authService.GetUsersAsync(cancellationToken);
        var me = users.FirstOrDefault(u => string.Equals(u.Username, username, StringComparison.Ordinal));
        if (me is null || string.IsNullOrWhiteSpace(me.DingTalkWebhook))
            return Results.BadRequest(new { detail = "请先配置你的钉钉 Webhook 地址。" });

        var summaryConfig = await summaryStoreConfigStore.LoadAsync(cancellationToken);
        if (!summaryConfig.Enabled)
            return Results.BadRequest(new { detail = "请联系管理员先启用中心库。" });

        var scheduleConfig = await scheduleConfigStore.LoadAsync(cancellationToken);
        var proxyUrl = scheduleConfig.DingTalkConfig?.ProxyUrl ?? "";

        var userConfigs = new List<LocalAuthService.UserDingTalkConfig>
        {
            new(me.Username, me.DingTalkWebhook, me.DingTalkSecret)
        };
        var dummyMainConfig = new DingTalkConfig { ProxyUrl = proxyUrl };
        var result = await notifyService.SendDailyReportAsync(summaryConfig, dummyMainConfig, userConfigs, cancellationToken);

        if (result.TotalStages == 0)
            return Results.Ok(new { sent = false, detail = "今天没有开始的阶段，无内容可推送。" });
        if (result.SkippedUsers.Contains(username))
            return Results.Ok(new { sent = false, detail = $"今天有 {result.TotalStages} 个阶段，但没有分配给你的项目。请联系管理员在看板中设置负责人。" });
        if (result.SentUsers.Contains(username))
            return Results.Ok(new { sent = true });
        return Results.Ok(new { sent = false, detail = "发送失败，请检查 Webhook 配置。" });
    }
    catch (Exception ex)
    {
        return Results.BadRequest(new { detail = ex.Message });
    }
}).RequireAuthorization();

app.MapPost("/api/auth/allow-user-refresh", async (AllowUserRefreshRequest request, LocalAuthService authService, CancellationToken cancellationToken) =>
{
    await authService.SetAllowUserRefreshAsync(request.Allow, cancellationToken);
    return Results.Ok(new { allowUserRefresh = request.Allow });
}).RequireAuthorization("AdminOnly");

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

app.MapPost("/api/dingtalk/register-proxy", (DingTalkProxyRegistrationRequest request, DingTalkProxyRegistry registry, ScheduleConfigStore scheduleConfigStore) =>
{
    // Simple token auth - proxy must send the same secret configured in DingTalkConfig
    var config = scheduleConfigStore.LoadAsync(CancellationToken.None).GetAwaiter().GetResult();
    var expectedSecret = config.DingTalkConfig?.Secret ?? "";
    if (string.IsNullOrWhiteSpace(request.Token) || request.Token != expectedSecret)
    {
        return Results.Unauthorized();
    }

    registry.Register(request.ProxyUrl);
    return Results.Ok(new { ok = true });
});

app.MapGet("/api/dingtalk/proxy-status", (DingTalkProxyRegistry registry) =>
{
    var status = registry.GetStatus();
    return Results.Ok(new
    {
        proxyUrl = status.url,
        lastHeartbeat = status.lastHeartbeat,
        alive = status.alive
    });
}).RequireAuthorization("AdminOnly");

app.MapPost("/api/dingtalk/test", async (DingTalkNotifyService notifyService, SummaryStoreConfigStore summaryStoreConfigStore, ScheduleConfigStore scheduleConfigStore, LocalAuthService authService, CancellationToken cancellationToken) =>
{
    try
    {
        var scheduleConfig = await scheduleConfigStore.LoadAsync(cancellationToken);
        if (scheduleConfig.DingTalkConfig is null || string.IsNullOrWhiteSpace(scheduleConfig.DingTalkConfig.WebhookUrl))
        {
            return Results.BadRequest(new { detail = "请先配置钉钉 Webhook 地址。" });
        }

        var summaryConfig = await summaryStoreConfigStore.LoadAsync(cancellationToken);
        if (!summaryConfig.Enabled)
        {
            return Results.BadRequest(new { detail = "请先启用中心库。" });
        }

        // Also send to all users who have DingTalk configured
        var users = await authService.GetUsersAsync(cancellationToken);
        var userConfigs = users
            .Where(u => !string.IsNullOrWhiteSpace(u.DingTalkWebhook))
            .Select(u => new LocalAuthService.UserDingTalkConfig(u.Username, u.DingTalkWebhook, u.DingTalkSecret))
            .ToList();

        var result = await notifyService.SendDailyReportAsync(summaryConfig, scheduleConfig.DingTalkConfig, userConfigs, cancellationToken);
        return Results.Ok(new
        {
            sent = result.MainSent || result.SentUsers.Count > 0,
            totalStages = result.TotalStages,
            mainSent = result.MainSent,
            sentUsers = result.SentUsers,
            skippedUsers = result.SkippedUsers,
            failedUsers = result.FailedUsers
        });
    }
    catch (Exception ex)
    {
        return Results.BadRequest(new { detail = ex.Message });
    }
}).RequireAuthorization("AdminOnly");

app.MapPost("/api/dingtalk/test-user", async (TestUserDingTalkRequest request, DingTalkNotifyService notifyService, SummaryStoreConfigStore summaryStoreConfigStore, ScheduleConfigStore scheduleConfigStore, LocalAuthService authService, CancellationToken cancellationToken) =>
{
    try
    {
        var users = await authService.GetUsersAsync(cancellationToken);
        var target = users.FirstOrDefault(u => string.Equals(u.Username, request.Username, StringComparison.Ordinal));
        if (target is null)
            return Results.BadRequest(new { detail = $"用户 {request.Username} 不存在。" });
        if (string.IsNullOrWhiteSpace(target.DingTalkWebhook))
            return Results.BadRequest(new { detail = $"用户 {request.Username} 未配置钉钉 Webhook。" });

        var summaryConfig = await summaryStoreConfigStore.LoadAsync(cancellationToken);
        if (!summaryConfig.Enabled)
            return Results.BadRequest(new { detail = "请先启用中心库。" });

        var scheduleConfig = await scheduleConfigStore.LoadAsync(cancellationToken);
        var proxyUrl = scheduleConfig.DingTalkConfig?.ProxyUrl ?? "";

        var userConfigs = new List<LocalAuthService.UserDingTalkConfig>
        {
            new(target.Username, target.DingTalkWebhook, target.DingTalkSecret)
        };
        var dummyMainConfig = new DingTalkConfig { ProxyUrl = proxyUrl };
        var result = await notifyService.SendDailyReportAsync(summaryConfig, dummyMainConfig, userConfigs, cancellationToken);

        if (result.TotalStages == 0)
            return Results.Ok(new { sent = false, detail = "今天没有开始的阶段，无内容可推送。" });
        if (result.SkippedUsers.Contains(target.Username))
            return Results.Ok(new { sent = false, detail = $"今天有 {result.TotalStages} 个阶段，但没有分配给 {target.Username} 的项目。" });
        if (result.SentUsers.Contains(target.Username))
            return Results.Ok(new { sent = true });
        return Results.Ok(new { sent = false, detail = "发送失败，请检查 Webhook 配置。" });
    }
    catch (Exception ex)
    {
        return Results.BadRequest(new { detail = ex.Message });
    }
}).RequireAuthorization("AdminOnly");

app.MapGet("/api/servers", async (ServerConfigStore store, CancellationToken cancellationToken) =>
{
    var servers = await store.LoadAsync(cancellationToken);
    return Results.Ok(servers);
}).RequireAuthorization("InternalOrAbove");

app.MapPost("/api/servers", async (List<StageServerConfig> servers, ServerConfigStore store, CancellationToken cancellationToken) =>
{
    await store.SaveAsync(servers, cancellationToken);
    return Results.Ok(new { saved = servers.Count });
}).RequireAuthorization("InternalOrAbove");

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
}).RequireAuthorization("InternalOrAbove");

app.MapGet("/api/cache-info", async (ProjectStageCacheStore cacheStore, CancellationToken cancellationToken) =>
{
    var cacheInfo = await cacheStore.GetCacheInfoAsync(cancellationToken);
    return Results.Ok(cacheInfo);
}).RequireAuthorization();

app.MapPost("/api/refresh", async (HttpContext httpContext, ProjectStageRefreshRequest request, ProjectStageRefreshService refreshService, ServerConfigStore serverConfigStore, LocalAuthService authService, CancellationToken cancellationToken) =>
{
    try
    {
        var role = GetCurrentRole(httpContext);
        List<StageServerConfig>? servers = request.Servers;

        if (role == LocalAuthService.RoleExternal)
        {
            var allowUserRefresh = await authService.GetAllowUserRefreshAsync(cancellationToken);
            if (!allowUserRefresh)
            {
                return Results.Forbid();
            }

            servers = await serverConfigStore.LoadAsync(cancellationToken);
        }

        var result = await refreshService.RefreshAsync(servers, cancellationToken);
        return Results.Ok(result);
    }
    catch (Exception ex)
    {
        return Results.BadRequest(new { detail = ex.Message });
    }
}).RequireAuthorization();

app.MapPost("/api/query", async (HttpContext httpContext, ProjectStageQueryRequest request, ProjectMetadataService metadataService, CancellationToken cancellationToken) =>
{
    try
    {
        var summaryStoreConfigStore = app.Services.GetRequiredService<SummaryStoreConfigStore>();
        var summaryStoreService = app.Services.GetRequiredService<SummaryStoreService>();
        var summaryStoreConfig = await summaryStoreConfigStore.LoadAsync(cancellationToken);

        if (!summaryStoreConfig.Enabled)
            throw new InvalidOperationException("请先在中心库中配置并启用中心表。");

        var summary = await summaryStoreService.QueryAsync(summaryStoreConfig, request, cancellationToken);
        summary = await FilterSummaryForCurrentUserAsync(httpContext, metadataService, summary, cancellationToken);
        return Results.Ok(summary);
    }
    catch (Exception ex)
    {
        return Results.BadRequest(new { detail = ex.Message });
    }
}).RequireAuthorization();

app.MapPost("/api/board-counts", async (HttpContext httpContext, BoardCountRequest request, ProjectMetadataService metadataService, CancellationToken cancellationToken) =>
{
    try
    {
        var summaryStoreConfigStore = app.Services.GetRequiredService<SummaryStoreConfigStore>();
        var summaryStoreService = app.Services.GetRequiredService<SummaryStoreService>();
        var summaryStoreConfig = await summaryStoreConfigStore.LoadAsync(cancellationToken);

        if (!summaryStoreConfig.Enabled)
            throw new InvalidOperationException("请先在中心库中配置并启用中心表。");

        var summary = await summaryStoreService.QueryAsync(summaryStoreConfig, request.Query, cancellationToken);
        summary = await FilterSummaryForCurrentUserAsync(httpContext, metadataService, summary, cancellationToken);
        return Results.Ok(summary);
    }
    catch (Exception ex)
    {
        return Results.BadRequest(new { detail = ex.Message });
    }
}).RequireAuthorization();

app.MapPost("/api/stages", async (HttpContext httpContext, ProjectStageQueryRequest request, ProjectMetadataService metadataService, CancellationToken cancellationToken) =>
{
    try
    {
        var summaryStoreConfigStore = app.Services.GetRequiredService<SummaryStoreConfigStore>();
        var summaryStoreService = app.Services.GetRequiredService<SummaryStoreService>();
        var summaryStoreConfig = await summaryStoreConfigStore.LoadAsync(cancellationToken);

        if (!summaryStoreConfig.Enabled)
            throw new InvalidOperationException("请先在中心库中配置并启用中心表。");

        var summary = await summaryStoreService.QueryAsync(summaryStoreConfig, request, cancellationToken);
        summary = await FilterSummaryForCurrentUserAsync(httpContext, metadataService, summary, cancellationToken);
        var stageNames = summary.Records
            .Select(item => item.StageName?.Trim() ?? "")
            .Where(item => !string.IsNullOrWhiteSpace(item))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(item => item, StringComparer.OrdinalIgnoreCase)
            .ToList();
        return Results.Ok(stageNames);
    }
    catch (Exception ex)
    {
        return Results.BadRequest(new { detail = ex.Message });
    }
}).RequireAuthorization();

app.MapPost("/api/export", async (HttpContext httpContext, ProjectStageQueryRequest request, ProjectStageExportService exportService, ProjectMetadataService metadataService, CancellationToken cancellationToken) =>
{
    try
    {
        var summaryStoreConfigStore = app.Services.GetRequiredService<SummaryStoreConfigStore>();
        var summaryStoreService = app.Services.GetRequiredService<SummaryStoreService>();
        var summaryStoreConfig = await summaryStoreConfigStore.LoadAsync(cancellationToken);

        if (!summaryStoreConfig.Enabled)
            throw new InvalidOperationException("请先在中心库中配置并启用中心表。");

        var summary = await summaryStoreService.QueryAsync(summaryStoreConfig, request, cancellationToken);
        summary = await FilterSummaryForCurrentUserAsync(httpContext, metadataService, summary, cancellationToken);
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

app.MapGet("/api/project-metadata", async (HttpContext httpContext, ProjectMetadataService metadataService, CancellationToken cancellationToken) =>
{
    try
    {
        var role = httpContext.User.FindFirst("role")?.Value ?? "external";
        if (role == "admin" || role == "internal")
        {
            return Results.Ok(await metadataService.GetAllAsync(cancellationToken));
        }

        var username = httpContext.User.Identity?.Name ?? "";
        return Results.Ok(await metadataService.GetByMaintainerAsync(username, cancellationToken));
    }
    catch (Exception ex)
    {
        return Results.BadRequest(new { detail = ex.Message });
    }
}).RequireAuthorization();

app.MapPost("/api/project-metadata", async (HttpContext httpContext, ProjectMetadataRecord request, ProjectMetadataService metadataService, CancellationToken cancellationToken) =>
{
    try
    {
        var role = httpContext.User.FindFirst("role")?.Value ?? "external";
        var username = httpContext.User.Identity?.Name ?? "";

        if (role == "admin" || role == "internal")
        {
            await metadataService.SaveAsync(request.ServerName, request.ExamCode, request.Maintainer, request.AppServers, cancellationToken);
            return Results.Ok(new { saved = true });
        }

        // External users can only update app_servers for their assigned projects
        var existing = await metadataService.GetByMaintainerAsync(username, cancellationToken);
        var match = existing.FirstOrDefault(m =>
            string.Equals(m.ServerName, request.ServerName, StringComparison.OrdinalIgnoreCase) &&
            string.Equals(m.ExamCode, request.ExamCode, StringComparison.OrdinalIgnoreCase));

        if (match is null)
        {
            return Results.Forbid();
        }

        // External can only change app_servers, keep existing maintainer
        await metadataService.SaveAsync(request.ServerName, request.ExamCode, match.Maintainer, request.AppServers, cancellationToken);
        return Results.Ok(new { saved = true });
    }
    catch (Exception ex)
    {
        return Results.BadRequest(new { detail = ex.Message });
    }
}).RequireAuthorization();

app.MapGet("/api/app-server-options", async (ProjectMetadataService metadataService, CancellationToken cancellationToken) =>
{
    try
    {
        return Results.Ok(await metadataService.GetAppServerOptionsAsync(cancellationToken));
    }
    catch (Exception ex)
    {
        return Results.BadRequest(new { detail = ex.Message });
    }
}).RequireAuthorization();

app.MapPost("/api/app-server-options", async (AppServerOptionRequest request, ProjectMetadataService metadataService, CancellationToken cancellationToken) =>
{
    try
    {
        await metadataService.SaveAppServerOptionsAsync(request.Options, cancellationToken);
        return Results.Ok(new { saved = true, count = request.Options.Count });
    }
    catch (Exception ex)
    {
        return Results.BadRequest(new { detail = ex.Message });
    }
}).RequireAuthorization("InternalOrAbove");

static string GetCurrentRole(HttpContext httpContext)
{
    return httpContext.User.FindFirst("role")?.Value ?? LocalAuthService.RoleExternal;
}

static async Task<ProjectStageSummary> FilterSummaryForCurrentUserAsync(
    HttpContext httpContext,
    ProjectMetadataService metadataService,
    ProjectStageSummary summary,
    CancellationToken cancellationToken)
{
    var role = GetCurrentRole(httpContext);
    if (role == LocalAuthService.RoleAdmin || role == LocalAuthService.RoleInternal)
    {
        return summary;
    }

    var username = httpContext.User.Identity?.Name ?? "";
    if (string.IsNullOrWhiteSpace(username))
    {
        return BuildFilteredSummary(summary, new HashSet<string>(StringComparer.OrdinalIgnoreCase));
    }

    var metadata = await metadataService.GetByMaintainerAsync(username, cancellationToken);
    var allowedKeys = metadata
        .Select(item => BuildProjectKey(item.ServerName, item.ExamCode))
        .ToHashSet(StringComparer.OrdinalIgnoreCase);

    return BuildFilteredSummary(summary, allowedKeys);
}

static ProjectStageSummary BuildFilteredSummary(ProjectStageSummary source, HashSet<string> allowedKeys)
{
    var groups = (source.Groups ?? [])
        .Where(item => allowedKeys.Contains(BuildProjectKey(item.ServerName, item.ExamCode)))
        .ToList();

    var records = (source.Records ?? [])
        .Where(item => allowedKeys.Contains(BuildProjectKey(item.ServerName, item.ExamCode)))
        .ToList();

    return new ProjectStageSummary
    {
        Records = records,
        Groups = groups,
        EnabledServers = groups.Select(item => item.ServerName).Where(item => !string.IsNullOrWhiteSpace(item)).Distinct(StringComparer.OrdinalIgnoreCase).Count(),
        VisitedDatabases = groups.Select(item => $"{item.ServerName}|{item.DatabaseName}").Distinct(StringComparer.OrdinalIgnoreCase).Count(),
        MatchedDatabases = records.Select(item => $"{item.ServerName}|{item.DatabaseName}").Distinct(StringComparer.OrdinalIgnoreCase).Count(),
        EndedCount = groups.Count(item => item.Statuses.Any(status => string.Equals(status, "已结束", StringComparison.OrdinalIgnoreCase) || string.Equals(status, "已经结束", StringComparison.OrdinalIgnoreCase))),
        OngoingCount = groups.Count(item => item.Statuses.Any(status => string.Equals(status, "正在进行", StringComparison.OrdinalIgnoreCase))),
        UpcomingCount = groups.Count(item => item.Statuses.Any(status => string.Equals(status, "即将开始", StringComparison.OrdinalIgnoreCase)))
    };
}

static string BuildProjectKey(string serverName, string examCode)
{
    return $"{serverName}|{examCode}";
}

app.Run();

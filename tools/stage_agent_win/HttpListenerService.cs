using System.Net;
using System.Text;
using System.Text.Json;

namespace StageAgentService;

public sealed class HttpListenerService : BackgroundService
{
    private static readonly JsonSerializerOptions JsonOpts = new() { PropertyNameCaseInsensitive = true };

    private readonly SyncWorker _syncWorker;
    private readonly ILogger<HttpListenerService> _logger;
    private readonly int _port;

    public HttpListenerService(SyncWorker syncWorker, ILogger<HttpListenerService> logger, IConfiguration configuration)
    {
        _syncWorker = syncWorker;
        _logger = logger;
        _port = configuration.GetValue("Port", 5100);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var prefix = $"http://+:{_port}/";
        var listener = new HttpListener();
        listener.Prefixes.Add(prefix);
        listener.Start();

        _logger.LogInformation("StageAgent 已启动，监听端口 {Port}", _port);
        _logger.LogInformation("  健康检查: GET  http://localhost:{Port}/health", _port);
        _logger.LogInformation("  触发同步: POST http://localhost:{Port}/sync", _port);

        stoppingToken.Register(() => listener.Stop());

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var ctx = await listener.GetContextAsync();
                _ = Task.Run(() => HandleRequest(ctx), CancellationToken.None);
            }
            catch (HttpListenerException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (ObjectDisposedException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
        }
    }

    private async Task HandleRequest(HttpListenerContext ctx)
    {
        try
        {
            var path = ctx.Request.Url?.AbsolutePath?.TrimEnd('/') ?? "";

            if (ctx.Request.HttpMethod == "GET" && path == "/health")
            {
                await WriteJson(ctx, 200, new { status = "ok" });
                return;
            }

            if (ctx.Request.HttpMethod == "POST" && path == "/sync")
            {
                using var reader = new StreamReader(ctx.Request.InputStream, Encoding.UTF8);
                var body = await reader.ReadToEndAsync();
                var request = JsonSerializer.Deserialize<SyncRequest>(body, JsonOpts);

                if (request is null)
                {
                    await WriteJson(ctx, 400, new { detail = "invalid request body" });
                    return;
                }

                var result = await _syncWorker.RunSyncAsync(request);
                await WriteJson(ctx, 200, result);
                return;
            }

            await WriteJson(ctx, 404, new { detail = "not found" });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "请求处理失败");
            try { await WriteJson(ctx, 500, new { detail = ex.Message }); } catch { }
        }
    }

    private static async Task WriteJson(HttpListenerContext ctx, int code, object data)
    {
        var json = JsonSerializer.Serialize(data, JsonOpts);
        var bytes = Encoding.UTF8.GetBytes(json);
        ctx.Response.StatusCode = code;
        ctx.Response.ContentType = "application/json; charset=utf-8";
        ctx.Response.ContentLength64 = bytes.Length;
        await ctx.Response.OutputStream.WriteAsync(bytes);
        ctx.Response.Close();
    }
}

using System.Net;
using System.Text;
using System.Text.Json;

namespace DingTalkProxy;

public sealed class ProxyListenerService(IConfiguration config, ILogger<ProxyListenerService> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var port = config.GetValue<int>("Port", 9100);
        var listener = new HttpListener();
        listener.Prefixes.Add($"http://+:{port}/");

        try
        {
            listener.Start();
            logger.LogInformation("[转发] 钉钉转发代理已启动，监听端口 {Port}", port);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "[转发] 启动失败，端口 {Port} 可能被占用", port);
            return;
        }

        using var client = new HttpClient { Timeout = TimeSpan.FromSeconds(15) };

        while (!stoppingToken.IsCancellationRequested)
        {
            HttpListenerContext ctx;
            try
            {
                ctx = await listener.GetContextAsync().WaitAsync(stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "[转发] 接受请求失败");
                continue;
            }

            _ = Task.Run(() => HandleRequestAsync(ctx, client, stoppingToken), stoppingToken);
        }

        listener.Stop();
    }

    private async Task HandleRequestAsync(HttpListenerContext ctx, HttpClient client, CancellationToken ct)
    {
        var req = ctx.Request;
        var resp = ctx.Response;

        if (req.HttpMethod == "GET")
        {
            await WriteJsonAsync(resp, 200, new { status = "ok", message = "钉钉转发代理运行中" });
            return;
        }

        if (req.HttpMethod != "POST")
        {
            await WriteJsonAsync(resp, 405, new { error = "仅支持 POST" });
            return;
        }

        try
        {
            using var reader = new StreamReader(req.InputStream, req.ContentEncoding);
            var body = await reader.ReadToEndAsync(ct);

            if (string.IsNullOrWhiteSpace(body))
            {
                await WriteJsonAsync(resp, 400, new { error = "请求体为空" });
                return;
            }

            using var doc = JsonDocument.Parse(body);
            var root = doc.RootElement;

            if (!root.TryGetProperty("targetUrl", out var targetUrlEl) || string.IsNullOrWhiteSpace(targetUrlEl.GetString()))
            {
                await WriteJsonAsync(resp, 400, new { error = "缺少 targetUrl" });
                return;
            }

            var targetUrl = targetUrlEl.GetString()!;
            var message = root.TryGetProperty("message", out var msgEl)
                ? msgEl.GetRawText()
                : "{}";

            logger.LogInformation("[转发] -> {Url}", targetUrl.Length > 80 ? targetUrl[..80] + "..." : targetUrl);

            var content = new StringContent(message, Encoding.UTF8, "application/json");
            var dingResp = await client.PostAsync(targetUrl, content, ct);
            var resultBody = await dingResp.Content.ReadAsStringAsync(ct);

            resp.StatusCode = (int)dingResp.StatusCode;
            var bytes = Encoding.UTF8.GetBytes(resultBody);
            resp.ContentType = "application/json; charset=utf-8";
            resp.ContentLength64 = bytes.Length;
            await resp.OutputStream.WriteAsync(bytes, ct);

            try
            {
                using var resultDoc = JsonDocument.Parse(resultBody);
                var errCode = resultDoc.RootElement.GetProperty("errcode").GetInt32();
                if (errCode == 0)
                    logger.LogInformation("[转发] 成功");
                else
                    logger.LogWarning("[转发] 钉钉返回 errcode={Code}: {Msg}",
                        errCode, resultDoc.RootElement.GetProperty("errmsg").GetString());
            }
            catch { /* ignore parse errors */ }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            logger.LogError(ex, "[转发] 处理请求失败");
            try { await WriteJsonAsync(resp, 500, new { error = ex.Message }); } catch { }
        }
        finally
        {
            try { resp.Close(); } catch { }
        }
    }

    private static async Task WriteJsonAsync(HttpListenerResponse resp, int statusCode, object data)
    {
        var bytes = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(data, new JsonSerializerOptions { Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping }));
        resp.StatusCode = statusCode;
        resp.ContentType = "application/json; charset=utf-8";
        resp.ContentLength64 = bytes.Length;
        await resp.OutputStream.WriteAsync(bytes);
        resp.Close();
    }
}

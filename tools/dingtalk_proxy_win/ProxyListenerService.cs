using System.Net;
using System.Text;
using System.Text.Json;

namespace DingTalkProxy;

public sealed class ProxyListenerService(AppConfig config, Action<string> log)
{
    private CancellationTokenSource? _cts;
    private HttpListener? _listener;
    private readonly HttpClient _client = new() { Timeout = TimeSpan.FromSeconds(15) };

    public void Start()
    {
        _cts = new CancellationTokenSource();
        _listener = new HttpListener();
        _listener.Prefixes.Add($"http://+:{config.Port}/");
        try
        {
            _listener.Start();
            log($"✅ 监听端口 {config.Port} 已启动");
        }
        catch (HttpListenerException ex) when (ex.ErrorCode == 5)
        {
            log($"❌ 端口 {config.Port} 拒绝访问。请右键运行「注册端口权限.bat」后重启程序。");
            return;
        }
        catch (Exception ex)
        {
            log($"❌ 监听启动失败（端口 {config.Port}）：{ex.Message}");
            return;
        }
        Task.Run(() => AcceptLoopAsync(_cts.Token));
    }

    public void Stop()
    {
        _cts?.Cancel();
        try { _listener?.Stop(); } catch { }
    }

    private async Task AcceptLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested && _listener!.IsListening)
        {
            try
            {
                var ctx = await _listener.GetContextAsync().WaitAsync(ct);
                _ = Task.Run(() => HandleAsync(ctx, ct), ct);
            }
            catch (OperationCanceledException) { break; }
            catch { }
        }
    }

    private async Task HandleAsync(HttpListenerContext ctx, CancellationToken ct)
    {
        var req = ctx.Request;
        var resp = ctx.Response;
        try
        {
            if (req.HttpMethod == "GET")
            {
                await WriteJsonAsync(resp, 200, new { status = "ok" });
                return;
            }
            if (req.HttpMethod != "POST")
            {
                await WriteJsonAsync(resp, 405, new { error = "Method Not Allowed" });
                return;
            }

            using var reader = new StreamReader(req.InputStream, req.ContentEncoding);
            var body = await reader.ReadToEndAsync(ct);
            using var doc = JsonDocument.Parse(body);
            var root = doc.RootElement;

            if (!root.TryGetProperty("targetUrl", out var urlEl) || string.IsNullOrWhiteSpace(urlEl.GetString()))
            {
                await WriteJsonAsync(resp, 400, new { error = "缺少 targetUrl" });
                return;
            }

            var targetUrl = urlEl.GetString()!;
            var message = root.TryGetProperty("message", out var msgEl) ? msgEl.GetRawText() : "{}";

            var content = new StringContent(message, Encoding.UTF8, "application/json");
            var dingResp = await _client.PostAsync(targetUrl, content, ct);
            var resultBody = await dingResp.Content.ReadAsStringAsync(ct);

            resp.StatusCode = (int)dingResp.StatusCode;
            var bytes = Encoding.UTF8.GetBytes(resultBody);
            resp.ContentType = "application/json; charset=utf-8";
            resp.ContentLength64 = bytes.Length;
            await resp.OutputStream.WriteAsync(bytes, ct);

            try
            {
                using var r = JsonDocument.Parse(resultBody);
                var code = r.RootElement.GetProperty("errcode").GetInt32();
                log(code == 0 ? "✅ 转发成功" : $"⚠️ 钉钉返回 errcode={code}");
            }
            catch { log("✅ 转发完成"); }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex) { log($"❌ 转发失败：{ex.Message}"); try { await WriteJsonAsync(resp, 500, new { error = ex.Message }); } catch { } }
        finally { try { resp.Close(); } catch { } }
    }

    private static async Task WriteJsonAsync(HttpListenerResponse resp, int code, object data)
    {
        var bytes = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(data));
        resp.StatusCode = code;
        resp.ContentType = "application/json; charset=utf-8";
        resp.ContentLength64 = bytes.Length;
        await resp.OutputStream.WriteAsync(bytes);
        resp.Close();
    }
}

using System.Net;
using System.Text;
using System.Text.Json;

namespace DingTalkProxy;

public sealed class ProxyListenerService(AppConfig config, Action<string> log)
{
    private readonly HttpClient _client = new() { Timeout = TimeSpan.FromSeconds(15) };
    private readonly object _sync = new();

    private CancellationTokenSource? _cts;
    private HttpListener? _listener;

    public bool IsListening
    {
        get
        {
            lock (_sync)
            {
                return _listener?.IsListening == true;
            }
        }
    }

    public void Start()
    {
        lock (_sync)
        {
            if (_listener?.IsListening == true)
                return;

            _cts?.Cancel();
            _cts = new CancellationTokenSource();
            _listener = new HttpListener();
            _listener.Prefixes.Add($"http://+:{config.Port}/");
        }

        try
        {
            _listener!.Start();
            log($"Proxy listener started on port {config.Port}.");
        }
        catch (HttpListenerException ex) when (ex.ErrorCode == 5)
        {
            log($"Port {config.Port} access denied. Please run 注册端口权限.bat as admin.");
            return;
        }
        catch (Exception ex)
        {
            log($"Proxy listener failed to start on port {config.Port}: {ex.Message}");
            return;
        }

        Task.Run(() => AcceptLoopAsync(_cts!.Token));
    }

    public void Stop()
    {
        _cts?.Cancel();
        try { _listener?.Stop(); } catch { }
        try { _listener?.Close(); } catch { }
    }

    private async Task AcceptLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                if (_listener?.IsListening != true)
                    break;

                var ctx = await _listener.GetContextAsync().WaitAsync(ct);
                _ = Task.Run(() => HandleAsync(ctx, ct), ct);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (HttpListenerException ex)
            {
                if (!ct.IsCancellationRequested)
                    log($"Listener loop stopped: {ex.Message}");
                break;
            }
            catch (ObjectDisposedException)
            {
                break;
            }
            catch (Exception ex)
            {
                log($"Listener loop error: {ex.Message}");
                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(2), ct);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
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
                await WriteJsonAsync(resp, 400, new { error = "missing targetUrl" });
                return;
            }

            var targetUrl = urlEl.GetString()!;
            var message = root.TryGetProperty("message", out var msgEl) ? msgEl.GetRawText() : "{}";

            using var content = new StringContent(message, Encoding.UTF8, "application/json");
            using var dingResp = await _client.PostAsync(targetUrl, content, ct);
            var resultBody = await dingResp.Content.ReadAsStringAsync(ct);

            resp.StatusCode = (int)dingResp.StatusCode;
            var bytes = Encoding.UTF8.GetBytes(resultBody);
            resp.ContentType = "application/json; charset=utf-8";
            resp.ContentLength64 = bytes.Length;
            await resp.OutputStream.WriteAsync(bytes, ct);

            try
            {
                using var resultJson = JsonDocument.Parse(resultBody);
                var code = resultJson.RootElement.GetProperty("errcode").GetInt32();
                log(code == 0 ? "Forwarded to DingTalk successfully." : $"DingTalk returned errcode={code}");
            }
            catch
            {
                log("Forward completed.");
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception ex)
        {
            log($"Forward failed: {ex.Message}");
            try
            {
                await WriteJsonAsync(resp, 500, new { error = ex.Message });
            }
            catch
            {
            }
        }
        finally
        {
            try { resp.Close(); } catch { }
        }
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

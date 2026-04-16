using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;

namespace DingTalkProxy;

public sealed class HeartbeatService(AppConfig config, Action<string> log)
{
    private const int Interval = 30;
    private CancellationTokenSource? _cts;

    public string? CurrentIp { get; private set; }

    public void Start()
    {
        _cts = new CancellationTokenSource();
        Task.Run(() => RunAsync(_cts.Token));
    }

    public void Stop() => _cts?.Cancel();

    private async Task RunAsync(CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(config.MainServer))
        {
            log("未配置 MainServer，跳过心跳注册。");
            return;
        }

        var registerUrl = config.MainServer.TrimEnd('/') + "/api/dingtalk/register-proxy";
        string? lastIp = null;
        var failCount = 0;
        using var client = new HttpClient { Timeout = TimeSpan.FromSeconds(10) };

        while (!ct.IsCancellationRequested)
        {
            try
            {
                var localIp =
                    !string.IsNullOrWhiteSpace(config.ProxyIp) ? config.ProxyIp
                    : !string.IsNullOrWhiteSpace(config.VpnPrefix) ? FindIpByPrefix(config.VpnPrefix)
                    : GetLocalIp(config.MainServer);

                if (localIp is null)
                {
                    log("无法获取本机 IP，等待重试...");
                    await Task.Delay(TimeSpan.FromSeconds(Interval), ct);
                    continue;
                }

                var proxyUrl = $"http://{localIp}:{config.Port}";
                var payload = JsonSerializer.Serialize(new { proxyUrl, token = "" });
                var content = new StringContent(payload, Encoding.UTF8, "application/json");
                var resp = await client.PostAsync(registerUrl, content, ct);
                resp.EnsureSuccessStatusCode();

                if (localIp != lastIp)
                {
                    log(lastIp is null
                        ? $"✅ 已注册到主服务：{proxyUrl}"
                        : $"🔄 IP 变更 {lastIp} → {localIp}，已更新");
                    lastIp = localIp;
                }

                CurrentIp = localIp;
                failCount = 0;
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                failCount++;
                if (failCount <= 3 || failCount % 10 == 0)
                    log($"❌ 注册失败 #{failCount}：{ex.Message}");
                CurrentIp = null;
            }

            try { await Task.Delay(TimeSpan.FromSeconds(Interval), ct); }
            catch (OperationCanceledException) { break; }
        }
    }

    private static string? GetLocalIp(string serverUrl)
    {
        try
        {
            var uri = new Uri(serverUrl);
            using var socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
            socket.Connect(uri.Host, uri.Port > 0 ? uri.Port : 80);
            return (socket.LocalEndPoint as IPEndPoint)?.Address.ToString();
        }
        catch { return null; }
    }

    /// <summary>
    /// 扫描所有启用的网络接口，找出 IPv4 地址以指定前缀开头的一个（如 "10.10.11."）。
    /// 优先匹配 VPN 适配器（TAP/TUN/OpenVPN/Wintun/WireGuard）；
    /// 当机器上有多个同网段 IP 时避免选到本地虚拟网卡。
    /// </summary>
    private static string? FindIpByPrefix(string prefix)
    {
        string[] vpnKeywords = ["tap-windows", "openvpn", "wintun", "wireguard", " tap", " tun", "vpn"];
        try
        {
            var vpnHit = (string?)null;
            var otherHit = (string?)null;

            foreach (var nic in NetworkInterface.GetAllNetworkInterfaces())
            {
                if (nic.OperationalStatus != OperationalStatus.Up) continue;

                var desc = " " + (nic.Description ?? "").ToLowerInvariant();
                var name = " " + (nic.Name ?? "").ToLowerInvariant();
                var isVpn = vpnKeywords.Any(k => desc.Contains(k) || name.Contains(k));

                foreach (var addr in nic.GetIPProperties().UnicastAddresses)
                {
                    if (addr.Address.AddressFamily != AddressFamily.InterNetwork) continue;
                    var ip = addr.Address.ToString();
                    if (!ip.StartsWith(prefix)) continue;

                    if (isVpn) { vpnHit = ip; break; }
                    otherHit ??= ip;
                }

                if (vpnHit != null) break;
            }

            return vpnHit ?? otherHit;
        }
        catch { }
        return null;
    }
}

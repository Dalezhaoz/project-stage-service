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
    private string? _lastLoggedSelection;

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
    /// 扫描网卡找出 IPv4 匹配前缀的地址。综合打分：
    /// DHCP 来源 + VPN 关键字 &gt; DHCP 来源 &gt; VPN 关键字 &gt; 非虚拟机 &gt; 兜底。
    /// OpenVPN 分配的 IP 会是 DHCP 来源，本机静态或 VMware 虚拟的是 Manual 来源。
    /// </summary>
    private string? FindIpByPrefix(string prefix)
    {
        string[] vpnKeywords = ["tap-windows", "openvpn", "wintun", "wireguard", " tap ", " tun ", "vpn"];
        string[] vmKeywords  = ["vmware", "virtualbox", "hyper-v", "hyper v", "vmnet", "vethernet", "virtual ethernet", "loopback", "pseudo", "bluetooth", "docker"];

        try
        {
            var all = new List<Candidate>();

            foreach (var nic in NetworkInterface.GetAllNetworkInterfaces())
            {
                if (nic.OperationalStatus != OperationalStatus.Up) continue;

                var desc = " " + (nic.Description ?? "").ToLowerInvariant() + " ";
                var name = " " + (nic.Name ?? "").ToLowerInvariant() + " ";
                var isVpn = vpnKeywords.Any(k => desc.Contains(k) || name.Contains(k));
                var isVm  = vmKeywords.Any(k => desc.Contains(k) || name.Contains(k));

                foreach (var addr in nic.GetIPProperties().UnicastAddresses)
                {
                    if (addr.Address.AddressFamily != AddressFamily.InterNetwork) continue;
                    var ip = addr.Address.ToString();
                    if (!ip.StartsWith(prefix)) continue;

                    // 跳过已弃用/无效地址（系统标记为失效的残留 IP）
                    if (addr.DuplicateAddressDetectionState is DuplicateAddressDetectionState.Invalid
                        or DuplicateAddressDetectionState.Deprecated) continue;

                    var isDhcp = addr.PrefixOrigin == PrefixOrigin.Dhcp;
                    var desc2 = nic.Description ?? nic.Name ?? "unknown";
                    all.Add(new Candidate(ip, desc2, isVpn, isVm, isDhcp));
                }
            }

            // 评分：DHCP+VPN 最高，Manual+VM 最低
            int Score(Candidate c) =>
                (c.IsDhcp ? 8 : 0) +
                (c.IsVpn  ? 4 : 0) +
                (c.IsVm   ? 0 : 2);

            var picked = all.OrderByDescending(Score).FirstOrDefault();

            // 变化时才打日志
            var key = picked is null ? $"NONE|{all.Count}" : $"{picked.Ip}|{all.Count}";
            if (key != _lastLoggedSelection)
            {
                _lastLoggedSelection = key;
                if (all.Count == 0)
                {
                    log($"⚠️ 未找到 {prefix}x 网段的网卡，VPN 可能未连接");
                }
                else
                {
                    foreach (var c in all)
                    {
                        var tag = c.IsDhcp ? "DHCP" : "Manual";
                        var extra = c.IsVpn ? " [VPN]" : c.IsVm ? " [VM]" : "";
                        log($"🔍 候选 {c.Ip} ({c.Desc}) {tag}{extra}");
                    }
                    if (picked != null)
                        log($"✓ 已选用 {picked.Ip} ({picked.Desc})");
                }
            }
            return picked?.Ip;
        }
        catch (Exception ex)
        {
            log($"⚠️ 扫描网卡失败：{ex.Message}");
        }
        return null;
    }

    private sealed record Candidate(string Ip, string Desc, bool IsVpn, bool IsVm, bool IsDhcp);
}

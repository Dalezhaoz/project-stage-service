using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;

namespace DingTalkProxy;

public sealed class HeartbeatService(AppConfig config, Action<string> log)
{
    private CancellationTokenSource? _cts;
    private Task? _runTask;
    private string? _lastLoggedSelection;

    public string? CurrentIp { get; private set; }
    public DateTimeOffset? LastSuccessAt { get; private set; }
    public DateTimeOffset? LastAttemptAt { get; private set; }
    public int ConsecutiveFailures { get; private set; }
    public bool IsRunning => _runTask is { IsCompleted: false };

    public void Start()
    {
        if (IsRunning)
            return;

        _cts = new CancellationTokenSource();
        _runTask = Task.Run(() => RunSupervisorAsync(_cts.Token));
    }

    public void Stop()
    {
        _cts?.Cancel();
        _runTask = null;
    }

    public bool IsStale()
    {
        if (LastSuccessAt is null)
            return true;

        return DateTimeOffset.Now - LastSuccessAt.Value > TimeSpan.FromSeconds(config.MaxHeartbeatSilenceSeconds);
    }

    private async Task RunSupervisorAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await RunAsync(ct);
                break;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                log($"Heartbeat worker stopped unexpectedly: {ex.Message}. Restarting in 5s.");
                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(5), ct);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }
    }

    private async Task RunAsync(CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(config.MainServer))
        {
            log("MainServer is empty, skip heartbeat registration.");
            return;
        }

        var registerUrl = config.MainServer.TrimEnd('/') + "/api/dingtalk/register-proxy";
        string? lastIp = null;
        using var client = new HttpClient { Timeout = TimeSpan.FromSeconds(10) };

        while (!ct.IsCancellationRequested)
        {
            try
            {
                LastAttemptAt = DateTimeOffset.Now;

                var localIp =
                    !string.IsNullOrWhiteSpace(config.ProxyIp) ? config.ProxyIp
                    : !string.IsNullOrWhiteSpace(config.VpnPrefix) ? FindIpByPrefix(config.VpnPrefix)
                    : GetLocalIp(config.MainServer);

                if (localIp is null)
                {
                    log("Cannot determine local IP, will retry.");
                    await Task.Delay(TimeSpan.FromSeconds(config.HeartbeatIntervalSeconds), ct);
                    continue;
                }

                var proxyUrl = $"http://{localIp}:{config.Port}";
                var payload = JsonSerializer.Serialize(new { proxyUrl, token = "" });
                using var content = new StringContent(payload, Encoding.UTF8, "application/json");
                using var resp = await client.PostAsync(registerUrl, content, ct);
                resp.EnsureSuccessStatusCode();

                if (localIp != lastIp)
                {
                    log(lastIp is null
                        ? $"Registered proxy to server: {proxyUrl}"
                        : $"Proxy IP changed {lastIp} -> {localIp}, registration updated.");
                    lastIp = localIp;
                }

                CurrentIp = localIp;
                LastSuccessAt = DateTimeOffset.Now;
                ConsecutiveFailures = 0;
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                ConsecutiveFailures++;
                CurrentIp = null;

                if (ConsecutiveFailures <= 3 || ConsecutiveFailures % 10 == 0)
                {
                    log($"Heartbeat register failed #{ConsecutiveFailures}: {ex.Message}");
                }
            }

            try
            {
                await Task.Delay(TimeSpan.FromSeconds(config.HeartbeatIntervalSeconds), ct);
            }
            catch (OperationCanceledException)
            {
                break;
            }
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
        catch
        {
            return null;
        }
    }

    private string? FindIpByPrefix(string prefix)
    {
        string[] vpnKeywords = ["tap-windows", "openvpn", "wintun", "wireguard", " tap ", " tun ", "vpn"];
        string[] vmKeywords = ["vmware", "virtualbox", "hyper-v", "hyper v", "vmnet", "vethernet", "virtual ethernet", "loopback", "pseudo", "bluetooth", "docker"];

        try
        {
            var all = new List<Candidate>();

            foreach (var nic in NetworkInterface.GetAllNetworkInterfaces())
            {
                if (nic.OperationalStatus != OperationalStatus.Up)
                    continue;

                var desc = " " + (nic.Description ?? "").ToLowerInvariant() + " ";
                var name = " " + (nic.Name ?? "").ToLowerInvariant() + " ";
                var isVpn = vpnKeywords.Any(k => desc.Contains(k) || name.Contains(k));
                var isVm = vmKeywords.Any(k => desc.Contains(k) || name.Contains(k));

                foreach (var addr in nic.GetIPProperties().UnicastAddresses)
                {
                    if (addr.Address.AddressFamily != AddressFamily.InterNetwork)
                        continue;

                    var ip = addr.Address.ToString();
                    if (!ip.StartsWith(prefix, StringComparison.Ordinal))
                        continue;

                    if (addr.DuplicateAddressDetectionState is DuplicateAddressDetectionState.Invalid or DuplicateAddressDetectionState.Deprecated)
                        continue;

                    var isDhcp = addr.PrefixOrigin == PrefixOrigin.Dhcp;
                    var desc2 = nic.Description ?? nic.Name ?? "unknown";
                    all.Add(new Candidate(ip, desc2, isVpn, isVm, isDhcp));
                }
            }

            var picked = all
                .OrderByDescending(Score)
                .FirstOrDefault();

            var key = picked is null ? $"NONE|{all.Count}" : $"{picked.Ip}|{all.Count}";
            if (key != _lastLoggedSelection)
            {
                _lastLoggedSelection = key;

                if (all.Count == 0)
                {
                    log($"No NIC found in {prefix}*, VPN may be disconnected.");
                }
                else
                {
                    foreach (var candidate in all)
                    {
                        var source = candidate.IsDhcp ? "DHCP" : "Manual";
                        var extra = candidate.IsVpn ? " [VPN]" : candidate.IsVm ? " [VM]" : "";
                        log($"Candidate IP {candidate.Ip} ({candidate.Desc}) {source}{extra}");
                    }

                    if (picked is not null)
                    {
                        log($"Selected IP {picked.Ip} ({picked.Desc})");
                    }
                }
            }

            return picked?.Ip;
        }
        catch (Exception ex)
        {
            log($"NIC scan failed: {ex.Message}");
            return null;
        }
    }

    private static int Score(Candidate candidate) =>
        (candidate.IsDhcp ? 8 : 0) +
        (candidate.IsVpn ? 4 : 0) +
        (candidate.IsVm ? 0 : 2);

    private sealed record Candidate(string Ip, string Desc, bool IsVpn, bool IsVm, bool IsDhcp);
}

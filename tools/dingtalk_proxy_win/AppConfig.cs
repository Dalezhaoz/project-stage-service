using System.Text.Json;

namespace DingTalkProxy;

public sealed class AppConfig
{
    public int Port { get; set; } = 9100;
    public string MainServer { get; set; } = "";
    public string ProxyIp { get; set; } = "";
    public string VpnPrefix { get; set; } = "";
    public int HeartbeatIntervalSeconds { get; set; } = 30;
    public int WatchdogIntervalSeconds { get; set; } = 5;
    public int MaxHeartbeatSilenceSeconds { get; set; } = 120;

    public AppConfig()
    {
        var path = Path.Combine(AppContext.BaseDirectory, "appsettings.json");
        if (!File.Exists(path))
            return;

        try
        {
            var doc = JsonDocument.Parse(File.ReadAllText(path));
            var root = doc.RootElement;

            if (root.TryGetProperty("Port", out var p)) Port = p.GetInt32();
            if (root.TryGetProperty("MainServer", out var s)) MainServer = s.GetString() ?? "";
            if (root.TryGetProperty("ProxyIp", out var ip)) ProxyIp = ip.GetString() ?? "";
            if (root.TryGetProperty("VpnPrefix", out var vp)) VpnPrefix = vp.GetString() ?? "";
            if (root.TryGetProperty("HeartbeatIntervalSeconds", out var h)) HeartbeatIntervalSeconds = Math.Max(5, h.GetInt32());
            if (root.TryGetProperty("WatchdogIntervalSeconds", out var w)) WatchdogIntervalSeconds = Math.Max(2, w.GetInt32());
            if (root.TryGetProperty("MaxHeartbeatSilenceSeconds", out var m))
            {
                MaxHeartbeatSilenceSeconds = Math.Max(HeartbeatIntervalSeconds * 2, m.GetInt32());
            }
        }
        catch
        {
        }
    }
}

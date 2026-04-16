using System.Text.Json;

namespace DingTalkProxy;

public sealed class AppConfig
{
    public int Port { get; set; } = 9100;
    public string MainServer { get; set; } = "";
    public string ProxyIp { get; set; } = "";
    /// <summary>VPN 网段前缀，例如 "10.10.11."，留空则走默认路由探测。</summary>
    public string VpnPrefix { get; set; } = "";

    public AppConfig()
    {
        var path = Path.Combine(AppContext.BaseDirectory, "appsettings.json");
        if (!File.Exists(path)) return;
        try
        {
            var doc = JsonDocument.Parse(File.ReadAllText(path));
            var root = doc.RootElement;
            if (root.TryGetProperty("Port", out var p)) Port = p.GetInt32();
            if (root.TryGetProperty("MainServer", out var s)) MainServer = s.GetString() ?? "";
            if (root.TryGetProperty("ProxyIp", out var ip)) ProxyIp = ip.GetString() ?? "";
            if (root.TryGetProperty("VpnPrefix", out var vp)) VpnPrefix = vp.GetString() ?? "";
        }
        catch { }
    }
}

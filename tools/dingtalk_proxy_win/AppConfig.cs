using System.Text.Json;

namespace DingTalkProxy;

public sealed class AppConfig
{
    public int Port { get; set; } = 9100;
    public string MainServer { get; set; } = "";
    public string ProxyIp { get; set; } = "";

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
        }
        catch { }
    }
}

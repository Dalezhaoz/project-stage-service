namespace ProjectStageService.Services;

/// <summary>
/// Stores the latest proxy URL registered by the DingTalk forwarding service via heartbeat.
/// When a proxy is registered and alive, it takes priority over the static ProxyUrl in config.
/// </summary>
public sealed class DingTalkProxyRegistry
{
    private string? _proxyUrl;
    private DateTime _lastHeartbeat;
    private readonly TimeSpan _timeout = TimeSpan.FromMinutes(3); // consider dead if no heartbeat for 3 min

    public void Register(string proxyUrl)
    {
        _proxyUrl = proxyUrl.TrimEnd('/');
        _lastHeartbeat = DateTime.UtcNow;
    }

    /// <summary>
    /// Returns the registered proxy URL if alive, otherwise null.
    /// </summary>
    public string? GetActiveProxyUrl()
    {
        if (_proxyUrl is null) return null;
        if (DateTime.UtcNow - _lastHeartbeat > _timeout)
        {
            return null; // proxy is considered dead
        }
        return _proxyUrl;
    }

    public (string? url, DateTime lastHeartbeat, bool alive) GetStatus()
    {
        var alive = _proxyUrl is not null && DateTime.UtcNow - _lastHeartbeat <= _timeout;
        return (_proxyUrl, _lastHeartbeat, alive);
    }
}

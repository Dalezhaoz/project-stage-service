namespace ServerMonitorAgent;

public sealed class AgentEncryptedEnvelope
{
    public int Version { get; set; } = 1;
    public string Nonce { get; set; } = "";
    public string Ciphertext { get; set; } = "";
    public string Tag { get; set; } = "";
}

public sealed class MetricRequest
{
    public string ServerName { get; set; } = "";
}

public sealed class MetricResponse
{
    public DateTime CollectedAt { get; set; } = DateTime.Now;
    public double CpuUsagePercent { get; set; }
    public double MemoryUsagePercent { get; set; }
    public double DiskUsagePercent { get; set; }
    public double NetworkInMbps { get; set; }
    public double NetworkOutMbps { get; set; }
    public double NetworkTotalMbps { get; set; }
}

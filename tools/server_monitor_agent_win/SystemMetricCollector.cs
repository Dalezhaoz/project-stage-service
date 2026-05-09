using System.Diagnostics;
using System.Net.NetworkInformation;
using System.Runtime.InteropServices;

namespace ServerMonitorAgent;

public sealed class SystemMetricCollector
{
    public async Task<MetricResponse> CollectAsync(CancellationToken cancellationToken)
    {
        var cpuTask = GetCpuUsagePercentAsync(cancellationToken);
        var mem = GetMemoryUsagePercent();
        var disk = GetDiskUsagePercent();
        var (inMbps, outMbps) = await GetNetworkMbpsAsync(cancellationToken);
        var cpu = await cpuTask;

        return new MetricResponse
        {
            CollectedAt = DateTime.Now,
            CpuUsagePercent = Math.Round(cpu, 2),
            MemoryUsagePercent = Math.Round(mem, 2),
            DiskUsagePercent = Math.Round(disk, 2),
            NetworkInMbps = Math.Round(inMbps, 2),
            NetworkOutMbps = Math.Round(outMbps, 2),
            NetworkTotalMbps = Math.Round(inMbps + outMbps, 2)
        };
    }

    private static async Task<double> GetCpuUsagePercentAsync(CancellationToken cancellationToken)
    {
        var start = DateTime.UtcNow;
        var startCpu = Process.GetProcesses().Sum(p =>
        {
            try { return p.TotalProcessorTime.TotalMilliseconds; }
            catch { return 0; }
            finally { p.Dispose(); }
        });

        await Task.Delay(600, cancellationToken);

        var end = DateTime.UtcNow;
        var endCpu = Process.GetProcesses().Sum(p =>
        {
            try { return p.TotalProcessorTime.TotalMilliseconds; }
            catch { return 0; }
            finally { p.Dispose(); }
        });

        var elapsedMs = (end - start).TotalMilliseconds;
        if (elapsedMs <= 0) return 0;
        var logicalCores = Math.Max(1, Environment.ProcessorCount);
        var usage = (endCpu - startCpu) / (elapsedMs * logicalCores) * 100.0;
        return Math.Clamp(usage, 0, 100);
    }

    private static double GetMemoryUsagePercent()
    {
        var status = new MemoryStatusEx();
        if (!GlobalMemoryStatusEx(status) || status.TotalPhys == 0) return 0;
        var used = status.TotalPhys - status.AvailPhys;
        return (double)used / status.TotalPhys * 100.0;
    }

    private static double GetDiskUsagePercent()
    {
        var drives = DriveInfo.GetDrives()
            .Where(d => d.IsReady && d.DriveType == DriveType.Fixed && d.TotalSize > 0)
            .ToList();
        if (drives.Count == 0) return 0;

        return drives.Max(d =>
        {
            var used = d.TotalSize - d.AvailableFreeSpace;
            return (double)used / d.TotalSize * 100.0;
        });
    }

    private static async Task<(double inMbps, double outMbps)> GetNetworkMbpsAsync(CancellationToken cancellationToken)
    {
        var adapters = NetworkInterface.GetAllNetworkInterfaces()
            .Where(n =>
                n.OperationalStatus == OperationalStatus.Up &&
                n.NetworkInterfaceType != NetworkInterfaceType.Loopback &&
                n.NetworkInterfaceType != NetworkInterfaceType.Tunnel)
            .ToList();

        if (adapters.Count == 0) return (0, 0);

        long inStart = 0;
        long outStart = 0;
        foreach (var adapter in adapters)
        {
            var stats = adapter.GetIPv4Statistics();
            inStart += stats.BytesReceived;
            outStart += stats.BytesSent;
        }

        var started = DateTime.UtcNow;
        await Task.Delay(1000, cancellationToken);

        long inEnd = 0;
        long outEnd = 0;
        foreach (var adapter in adapters)
        {
            var stats = adapter.GetIPv4Statistics();
            inEnd += stats.BytesReceived;
            outEnd += stats.BytesSent;
        }

        var seconds = Math.Max((DateTime.UtcNow - started).TotalSeconds, 0.2);
        var inMbps = ((inEnd - inStart) * 8.0 / 1_000_000.0) / seconds;
        var outMbps = ((outEnd - outStart) * 8.0 / 1_000_000.0) / seconds;
        return (Math.Max(0, inMbps), Math.Max(0, outMbps));
    }

    [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Auto)]
    private sealed class MemoryStatusEx
    {
        public uint Length = (uint)Marshal.SizeOf<MemoryStatusEx>();
        public uint MemoryLoad;
        public ulong TotalPhys;
        public ulong AvailPhys;
        public ulong TotalPageFile;
        public ulong AvailPageFile;
        public ulong TotalVirtual;
        public ulong AvailVirtual;
        public ulong AvailExtendedVirtual;
    }

    [DllImport("kernel32.dll", CharSet = CharSet.Auto, SetLastError = true)]
    private static extern bool GlobalMemoryStatusEx([In, Out] MemoryStatusEx buffer);
}

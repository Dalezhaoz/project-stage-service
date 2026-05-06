using ProjectStageService.Models;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace ProjectStageService.Services;

public sealed class MonitorServerConfigStore
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true
    };

    private readonly string _configPath;

    public MonitorServerConfigStore()
    {
        var baseDir = AppDataPath.GetBaseDirectory();
        Directory.CreateDirectory(baseDir);
        _configPath = Path.Combine(baseDir, "monitor_servers.dat");
    }

    public async Task<List<MonitorServerConfig>> LoadAsync(CancellationToken cancellationToken)
    {
        if (!File.Exists(_configPath))
        {
            return [];
        }

        var encrypted = await File.ReadAllBytesAsync(_configPath, cancellationToken);
        if (encrypted.Length == 0)
        {
            return [];
        }

        var plain = Unprotect(encrypted);
        var json = Encoding.UTF8.GetString(plain);
        return JsonSerializer.Deserialize<List<MonitorServerConfig>>(json) ?? [];
    }

    public async Task SaveAsync(List<MonitorServerConfig> servers, CancellationToken cancellationToken)
    {
        var json = JsonSerializer.Serialize(servers, JsonOptions);
        var plain = Encoding.UTF8.GetBytes(json);
        var encrypted = Protect(plain);
        await File.WriteAllBytesAsync(_configPath, encrypted, cancellationToken);
    }

    private static byte[] Protect(byte[] plain)
    {
        if (OperatingSystem.IsWindows())
        {
            return ProtectedData.Protect(plain, null, DataProtectionScope.LocalMachine);
        }

        return plain;
    }

    private static byte[] Unprotect(byte[] encrypted)
    {
        if (OperatingSystem.IsWindows())
        {
            return ProtectedData.Unprotect(encrypted, null, DataProtectionScope.LocalMachine);
        }

        return encrypted;
    }
}

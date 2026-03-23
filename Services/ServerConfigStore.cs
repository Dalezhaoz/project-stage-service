using ProjectStageService.Models;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace ProjectStageService.Services;

public sealed class ServerConfigStore
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true
    };

    private readonly string _configPath;

    public ServerConfigStore()
    {
        var baseDir = AppDataPath.GetBaseDirectory();
        Directory.CreateDirectory(baseDir);
        _configPath = Path.Combine(baseDir, "servers.dat");
    }

    public async Task<List<StageServerConfig>> LoadAsync(CancellationToken cancellationToken)
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
        return JsonSerializer.Deserialize<List<StageServerConfig>>(json) ?? [];
    }

    public async Task SaveAsync(List<StageServerConfig> servers, CancellationToken cancellationToken)
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

        // Non-Windows fallback for local development only.
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

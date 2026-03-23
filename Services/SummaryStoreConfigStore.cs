using ProjectStageService.Models;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace ProjectStageService.Services;

public sealed class SummaryStoreConfigStore
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true
    };

    private readonly string _configPath;

    public SummaryStoreConfigStore()
    {
        var baseDir = AppDataPath.GetBaseDirectory();
        Directory.CreateDirectory(baseDir);
        _configPath = Path.Combine(baseDir, "summary_store.dat");
    }

    public async Task<SummaryStoreConfig> LoadAsync(CancellationToken cancellationToken)
    {
        if (!File.Exists(_configPath))
        {
            return new SummaryStoreConfig();
        }

        var encrypted = await File.ReadAllBytesAsync(_configPath, cancellationToken);
        if (encrypted.Length == 0)
        {
            return new SummaryStoreConfig();
        }

        var plain = Unprotect(encrypted);
        var json = Encoding.UTF8.GetString(plain);
        return JsonSerializer.Deserialize<SummaryStoreConfig>(json) ?? new SummaryStoreConfig();
    }

    public async Task SaveAsync(SummaryStoreConfig config, CancellationToken cancellationToken)
    {
        var json = JsonSerializer.Serialize(config, JsonOptions);
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

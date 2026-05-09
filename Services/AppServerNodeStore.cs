using ProjectStageService.Models;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace ProjectStageService.Services;

public sealed class AppServerNodeStore
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true
    };

    private readonly string _configPath;

    public AppServerNodeStore()
    {
        var baseDir = AppDataPath.GetBaseDirectory();
        Directory.CreateDirectory(baseDir);
        _configPath = Path.Combine(baseDir, "app_server_nodes.dat");
    }

    public async Task<List<AppServerNode>> LoadAsync(CancellationToken cancellationToken)
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
        return JsonSerializer.Deserialize<List<AppServerNode>>(json) ?? [];
    }

    public async Task SaveAsync(List<AppServerNode> nodes, CancellationToken cancellationToken)
    {
        var normalized = (nodes ?? [])
            .Where(item => !string.IsNullOrWhiteSpace(item.Name))
            .GroupBy(item => item.Name.Trim(), StringComparer.OrdinalIgnoreCase)
            .Select(group =>
            {
                var first = group.First();
                first.Name = first.Name.Trim();
                first.Protocol = string.IsNullOrWhiteSpace(first.Protocol) ? "http" : first.Protocol.Trim().ToLowerInvariant();
                first.Port = first.Port <= 0 ? 80 : first.Port;
                first.FrontSiteName = first.FrontSiteName?.Trim() ?? "";
                first.BackSiteName = first.BackSiteName?.Trim() ?? "";
                first.Remark = first.Remark?.Trim() ?? "";
                return first;
            })
            .OrderBy(item => item.Name, StringComparer.OrdinalIgnoreCase)
            .ToList();

        var json = JsonSerializer.Serialize(normalized, JsonOptions);
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


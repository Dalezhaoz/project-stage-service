using ProjectStageService.Models;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace ProjectStageService.Services;

public sealed class AppServerSiteInventoryStore
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true
    };

    private readonly string _path;

    public AppServerSiteInventoryStore()
    {
        var baseDir = AppDataPath.GetBaseDirectory();
        Directory.CreateDirectory(baseDir);
        _path = Path.Combine(baseDir, "app_server_site_inventory.dat");
    }

    public async Task<List<AppServerSiteSnapshot>> LoadAsync(CancellationToken cancellationToken)
    {
        if (!File.Exists(_path))
        {
            return [];
        }

        var encrypted = await File.ReadAllBytesAsync(_path, cancellationToken);
        if (encrypted.Length == 0)
        {
            return [];
        }

        var plain = Unprotect(encrypted);
        var json = Encoding.UTF8.GetString(plain);
        return JsonSerializer.Deserialize<List<AppServerSiteSnapshot>>(json) ?? [];
    }

    public async Task SaveAsync(List<AppServerSiteSnapshot> snapshots, CancellationToken cancellationToken)
    {
        var normalized = (snapshots ?? [])
            .Where(item => !string.IsNullOrWhiteSpace(item.ServerName))
            .GroupBy(item => item.ServerName.Trim(), StringComparer.OrdinalIgnoreCase)
            .Select(group =>
            {
                var first = group.OrderByDescending(item => item.CollectedAt).First();
                return new AppServerSiteSnapshot
                {
                    ServerName = first.ServerName.Trim(),
                    SiteNames = (first.SiteNames ?? [])
                        .Select(item => item?.Trim() ?? "")
                        .Where(item => !string.IsNullOrWhiteSpace(item))
                        .Distinct(StringComparer.OrdinalIgnoreCase)
                        .OrderBy(item => item, StringComparer.OrdinalIgnoreCase)
                        .ToList(),
                    CollectedAt = first.CollectedAt == default ? DateTime.Now : first.CollectedAt
                };
            })
            .OrderBy(item => item.ServerName, StringComparer.OrdinalIgnoreCase)
            .ToList();

        var json = JsonSerializer.Serialize(normalized, JsonOptions);
        var plain = Encoding.UTF8.GetBytes(json);
        var encrypted = Protect(plain);
        await File.WriteAllBytesAsync(_path, encrypted, cancellationToken);
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


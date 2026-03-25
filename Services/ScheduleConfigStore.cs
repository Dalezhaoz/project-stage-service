using ProjectStageService.Models;
using System.Text.Json;

namespace ProjectStageService.Services;

public sealed class ScheduleConfigStore
{
    private static readonly JsonSerializerOptions JsonOptions = new() { WriteIndented = true };

    private readonly string _configPath;
    private ScheduleConfig? _cached;

    public event Action? OnChanged;

    public ScheduleConfigStore()
    {
        var baseDir = AppDataPath.GetBaseDirectory();
        Directory.CreateDirectory(baseDir);
        _configPath = Path.Combine(baseDir, "schedule.json");
    }

    public async Task<ScheduleConfig> LoadAsync(CancellationToken cancellationToken)
    {
        if (_cached is not null) return _cached;

        if (!File.Exists(_configPath))
        {
            _cached = new ScheduleConfig();
            return _cached;
        }

        var json = await File.ReadAllTextAsync(_configPath, cancellationToken);
        _cached = JsonSerializer.Deserialize<ScheduleConfig>(json) ?? new ScheduleConfig();
        return _cached;
    }

    public async Task SaveAsync(ScheduleConfig config, CancellationToken cancellationToken)
    {
        var json = JsonSerializer.Serialize(config, JsonOptions);
        await File.WriteAllTextAsync(_configPath, json, cancellationToken);
        _cached = config;
        OnChanged?.Invoke();
    }
}

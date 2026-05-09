using System.Diagnostics;
using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace ServerMonitorAgent;

public sealed class AppServerSiteReportService : BackgroundService
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    private readonly ILogger<AppServerSiteReportService> _logger;
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly bool _enabled;
    private readonly string _mainServer;
    private readonly string _reportPath;
    private readonly string _token;
    private readonly string _serverName;
    private readonly int _intervalSeconds;
    private readonly bool _ignoreStoppedSites;

    public AppServerSiteReportService(
        ILogger<AppServerSiteReportService> logger,
        IHttpClientFactory httpClientFactory,
        IConfiguration configuration)
    {
        _logger = logger;
        _httpClientFactory = httpClientFactory;
        _enabled = configuration.GetValue("SiteReport:Enabled", false);
        _mainServer = configuration["SiteReport:MainServer"]?.Trim() ?? "";
        _reportPath = configuration["SiteReport:ReportPath"]?.Trim() ?? "/api/app-server-sites/report-agent";
        _token = configuration["SiteReport:Token"]?.Trim() ?? "";
        _serverName = configuration["SiteReport:ServerName"]?.Trim() ?? Environment.MachineName;
        _intervalSeconds = Math.Max(30, configuration.GetValue("SiteReport:IntervalSeconds", 300));
        _ignoreStoppedSites = configuration.GetValue("SiteReport:IgnoreStoppedSites", false);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_enabled)
        {
            _logger.LogInformation("Site reporter disabled.");
            return;
        }

        if (string.IsNullOrWhiteSpace(_mainServer) || string.IsNullOrWhiteSpace(_token))
        {
            _logger.LogWarning("Site reporter config incomplete (MainServer/Token).");
            return;
        }

        _logger.LogInformation("Site reporter started. Interval={IntervalSeconds}s Server={ServerName}", _intervalSeconds, _serverName);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var siteNames = CollectIisSiteNames();
                await ReportAsync(siteNames, stoppingToken);
                _logger.LogInformation("Site report sent. Count={Count}", siteNames.Count);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Site report failed.");
            }

            try
            {
                await Task.Delay(TimeSpan.FromSeconds(_intervalSeconds), stoppingToken);
            }
            catch (TaskCanceledException)
            {
                break;
            }
        }
    }

    private List<string> CollectIisSiteNames()
    {
        var appcmd = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.Windows), "System32", "inetsrv", "appcmd.exe");
        if (!File.Exists(appcmd))
        {
            throw new InvalidOperationException($"appcmd not found: {appcmd}");
        }

        var siteOutput = RunProcess(appcmd, "list site");
        var appOutput = RunProcess(appcmd, "list app");
        var vdirOutput = RunProcess(appcmd, "list vdir");

        var stoppedSites = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var result = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var line in siteOutput.Split(['\r', '\n'], StringSplitOptions.RemoveEmptyEntries))
        {
            var siteName = ExtractQuotedValue(line);
            var state = ExtractNamedValue(line, "state");
            if (string.IsNullOrWhiteSpace(siteName)) continue;
            if (_ignoreStoppedSites && !string.Equals(state, "Started", StringComparison.OrdinalIgnoreCase))
            {
                stoppedSites.Add(siteName);
                continue;
            }
            result.Add(siteName);
        }

        foreach (var line in appOutput.Split(['\r', '\n'], StringSplitOptions.RemoveEmptyEntries))
        {
            var sitePath = ExtractQuotedValue(line);
            if (string.IsNullOrWhiteSpace(sitePath)) continue;
            var slashIndex = sitePath.IndexOf('/');
            if (slashIndex <= 0) continue;
            var siteName = sitePath[..slashIndex];
            var appPath = sitePath[(slashIndex + 1)..];
            if (string.IsNullOrWhiteSpace(siteName) || string.IsNullOrWhiteSpace(appPath)) continue;
            if (_ignoreStoppedSites && stoppedSites.Contains(siteName)) continue;
            var path = appPath.Trim().Trim('/');
            if (string.IsNullOrWhiteSpace(path)) continue;
            result.Add($"{siteName}/{path}");
        }

        foreach (var line in vdirOutput.Split(['\r', '\n'], StringSplitOptions.RemoveEmptyEntries))
        {
            var sitePath = ExtractQuotedValue(line);
            if (string.IsNullOrWhiteSpace(sitePath)) continue;
            var slashIndex = sitePath.IndexOf('/');
            if (slashIndex <= 0) continue;
            var siteName = sitePath[..slashIndex];
            var vdirPath = sitePath[(slashIndex + 1)..];
            if (string.IsNullOrWhiteSpace(siteName) || string.IsNullOrWhiteSpace(vdirPath)) continue;
            if (_ignoreStoppedSites && stoppedSites.Contains(siteName)) continue;
            var path = vdirPath.Trim().Trim('/');
            if (string.IsNullOrWhiteSpace(path)) continue;
            result.Add($"{siteName}/{path}");
        }

        return result.OrderBy(item => item, StringComparer.OrdinalIgnoreCase).ToList();
    }

    private async Task ReportAsync(List<string> siteNames, CancellationToken cancellationToken)
    {
        var uri = new Uri(new Uri(_mainServer.TrimEnd('/') + "/"), _reportPath.TrimStart('/'));
        var payload = new
        {
            serverName = _serverName,
            siteNames,
            collectedAt = DateTimeOffset.Now,
            token = _token
        };

        var client = _httpClientFactory.CreateClient(nameof(AppServerSiteReportService));
        client.Timeout = TimeSpan.FromSeconds(20);
        using var response = await client.PostAsJsonAsync(uri, payload, JsonOptions, cancellationToken);
        if (!response.IsSuccessStatusCode)
        {
            var body = await response.Content.ReadAsStringAsync(cancellationToken);
            throw new InvalidOperationException($"Report failed: {(int)response.StatusCode} {body}");
        }
    }

    private static string RunProcess(string fileName, string arguments)
    {
        using var process = new Process
        {
            StartInfo = new ProcessStartInfo
            {
                FileName = fileName,
                Arguments = arguments,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                CreateNoWindow = true,
                UseShellExecute = false
            }
        };
        process.Start();
        var output = process.StandardOutput.ReadToEnd();
        var error = process.StandardError.ReadToEnd();
        process.WaitForExit(10000);
        if (process.ExitCode != 0)
        {
            throw new InvalidOperationException($"appcmd exit {process.ExitCode}: {error}");
        }
        return output ?? "";
    }

    private static string ExtractQuotedValue(string line)
    {
        if (string.IsNullOrWhiteSpace(line)) return "";
        var first = line.IndexOf('"');
        if (first < 0) return "";
        var second = line.IndexOf('"', first + 1);
        if (second <= first) return "";
        return line.Substring(first + 1, second - first - 1).Trim();
    }

    private static string ExtractNamedValue(string line, string name)
    {
        if (string.IsNullOrWhiteSpace(line) || string.IsNullOrWhiteSpace(name)) return "";
        var token = $"{name}:";
        var idx = line.IndexOf(token, StringComparison.OrdinalIgnoreCase);
        if (idx < 0) return "";
        var start = idx + token.Length;
        var end = line.IndexOfAny([',', ')'], start);
        if (end < 0) end = line.Length;
        return line[start..end].Trim();
    }
}

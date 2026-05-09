using ProjectStageService.Models;

namespace ProjectStageService.Services;

public sealed class AppServerAutoAssignService
{
    private readonly UnifiedServerConfigService _unifiedServerConfigService;
    private readonly AppServerSiteInventoryStore _inventoryStore;
    private readonly ProjectStageCacheStore _cacheStore;
    private readonly ProjectMetadataService _metadataService;

    public AppServerAutoAssignService(
        UnifiedServerConfigService unifiedServerConfigService,
        AppServerSiteInventoryStore inventoryStore,
        ProjectStageCacheStore cacheStore,
        ProjectMetadataService metadataService)
    {
        _unifiedServerConfigService = unifiedServerConfigService;
        _inventoryStore = inventoryStore;
        _cacheStore = cacheStore;
        _metadataService = metadataService;
    }

    public async Task<List<AppServerSiteSnapshot>> GetInventoryAsync(CancellationToken cancellationToken)
    {
        return await _inventoryStore.LoadAsync(cancellationToken);
    }

    public async Task UpsertInventoryAsync(AppServerSiteReportRequest request, CancellationToken cancellationToken)
    {
        var serverName = request.ServerName?.Trim() ?? "";
        if (string.IsNullOrWhiteSpace(serverName))
        {
            throw new InvalidOperationException("ServerName 不能为空。");
        }

        var all = await _inventoryStore.LoadAsync(cancellationToken);
        var existing = all.FirstOrDefault(item => string.Equals(item.ServerName, serverName, StringComparison.OrdinalIgnoreCase));
        var collectedAt = request.CollectedAt ?? DateTime.Now;
        var siteNames = (request.SiteNames ?? [])
            .Select(item => item?.Trim() ?? "")
            .Where(item => !string.IsNullOrWhiteSpace(item))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

        if (existing is null)
        {
            all.Add(new AppServerSiteSnapshot
            {
                ServerName = serverName,
                SiteNames = siteNames,
                CollectedAt = collectedAt
            });
        }
        else
        {
            existing.SiteNames = siteNames;
            existing.CollectedAt = collectedAt;
        }

        await _inventoryStore.SaveAsync(all, cancellationToken);
    }

    public async Task<List<AutoAssignAppServersResultItem>> AutoAssignAsync(AutoAssignAppServersRequest request, CancellationToken cancellationToken)
    {
        request ??= new AutoAssignAppServersRequest();
        var config = await _unifiedServerConfigService.LoadAsync(cancellationToken);
        var inventory = await _inventoryStore.LoadAsync(cancellationToken);
        var inventoryMap = inventory.ToDictionary(item => item.ServerName, StringComparer.OrdinalIgnoreCase);

        var summary = await _cacheStore.QueryAsync(new ProjectStageQueryRequest(), cancellationToken);
        var groups = summary.Groups
            .Where(item => !string.IsNullOrWhiteSpace(item.DatabaseName))
            .ToList();

        var existingMetadata = await _metadataService.GetAllAsync(cancellationToken);
        var metadataMap = existingMetadata.ToDictionary(
            item => $"{item.ServerName}|{item.DatabaseName}|{item.ExamCode}",
            item => item,
            StringComparer.OrdinalIgnoreCase);

        var results = new List<AutoAssignAppServersResultItem>();
        foreach (var group in groups)
        {
            var matched = MatchAppServers(group.DatabaseName, config.AppServers, inventoryMap);
            var status = matched.Count switch
            {
                0 => "unmatched",
                1 => "single",
                _ => "multiple"
            };
            var matchedValue = string.Join("、", matched);
            results.Add(new AutoAssignAppServersResultItem
            {
                ServerName = group.ServerName,
                DatabaseName = group.DatabaseName,
                ExamCode = group.ExamCode,
                MatchedAppServers = matchedValue,
                MatchStatus = status
            });

            if (request.DryRun)
            {
                continue;
            }

            var key = $"{group.ServerName}|{group.DatabaseName}|{group.ExamCode}";
            metadataMap.TryGetValue(key, out var meta);
            var existingAppServers = meta?.AppServers?.Trim() ?? "";
            if (!request.OverwriteExisting && !string.IsNullOrWhiteSpace(existingAppServers))
            {
                continue;
            }

            await _metadataService.SaveAsync(
                group.ServerName,
                group.DatabaseName,
                group.ExamCode,
                meta?.Maintainer ?? "",
                matchedValue,
                cancellationToken,
                meta?.AllowOthersView);
        }

        return results;
    }

    private static List<string> MatchAppServers(
        string databaseName,
        List<AppServerNode> appServers,
        Dictionary<string, AppServerSiteSnapshot> inventoryMap)
    {
        var dbCandidates = BuildDatabaseCandidates(databaseName);
        if (dbCandidates.Count == 0)
        {
            return [];
        }

        var matches = new List<string>();
        foreach (var node in appServers.Where(item => item.Enabled && !string.IsNullOrWhiteSpace(item.Name)))
        {
            var hit = false;
            if (inventoryMap.TryGetValue(node.Name, out var snapshot))
            {
                hit = (snapshot.SiteNames ?? []).Any(site => MatchSiteToken(site, dbCandidates));
            }

            if (!hit)
            {
                var rules = ParseRuleTokens(node.FrontSiteName)
                    .Concat(ParseRuleTokens(node.BackSiteName))
                    .ToList();
                hit = rules.Any(rule => MatchRuleToken(rule, dbCandidates));
            }

            if (!hit && MatchSiteToken(node.Name, dbCandidates))
            {
                hit = true;
            }

            if (hit && !matches.Contains(node.Name, StringComparer.OrdinalIgnoreCase))
            {
                matches.Add(node.Name);
            }
        }

        return matches
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(item => item, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    private static bool MatchRuleToken(string rule, IReadOnlyList<string> dbCandidates)
    {
        if (string.IsNullOrWhiteSpace(rule))
        {
            return false;
        }

        var normalizedRule = NormalizeIdentity(rule);
        if (string.IsNullOrWhiteSpace(normalizedRule))
        {
            return false;
        }

        foreach (var db in dbCandidates)
        {
            var candidate = normalizedRule.Replace("{db}", db, StringComparison.OrdinalIgnoreCase);
            if (WildcardMatch(db, candidate))
            {
                return true;
            }
        }

        return dbCandidates.Any(db => WildcardMatch(db, normalizedRule));
    }

    private static bool MatchSiteToken(string siteName, IReadOnlyList<string> dbCandidates)
    {
        var token = NormalizeIdentity(siteName);
        if (string.IsNullOrWhiteSpace(token))
        {
            return false;
        }

        return dbCandidates.Any(db => string.Equals(token, db, StringComparison.OrdinalIgnoreCase));
    }

    private static List<string> ParseRuleTokens(string value)
    {
        return (value ?? "")
            .Split(['\r', '\n', ',', '，', ';', '；'], StringSplitOptions.RemoveEmptyEntries)
            .Select(item => item.Trim())
            .Where(item => !string.IsNullOrWhiteSpace(item))
            .ToList();
    }

    private static List<string> BuildDatabaseCandidates(string databaseName)
    {
        var db = (databaseName ?? "").Trim().ToLowerInvariant();
        if (string.IsNullOrWhiteSpace(db))
        {
            return [];
        }

        var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { db };
        if (db.EndsWith("gl", StringComparison.OrdinalIgnoreCase))
        {
            if (db.Length > 2)
            {
                set.Add(db[..^2]);
            }
        }
        else
        {
            set.Add($"{db}gl");
        }

        return set.ToList();
    }

    private static string NormalizeIdentity(string value)
    {
        var raw = (value ?? "").Trim().ToLowerInvariant();
        if (string.IsNullOrWhiteSpace(raw))
        {
            return "";
        }

        var normalized = raw.Replace("\\", "/");
        while (normalized.Contains("//", StringComparison.Ordinal))
        {
            normalized = normalized.Replace("//", "/", StringComparison.Ordinal);
        }
        normalized = normalized.Trim('/');
        if (string.IsNullOrWhiteSpace(normalized))
        {
            return "";
        }

        var parts = normalized.Split('/', StringSplitOptions.RemoveEmptyEntries);
        return parts.Length == 0 ? normalized : parts[^1];
    }

    private static bool WildcardMatch(string text, string pattern)
    {
        if (string.IsNullOrWhiteSpace(text) || string.IsNullOrWhiteSpace(pattern))
        {
            return false;
        }

        if (!pattern.Contains('*', StringComparison.Ordinal))
        {
            return string.Equals(text, pattern, StringComparison.OrdinalIgnoreCase);
        }

        var escaped = System.Text.RegularExpressions.Regex.Escape(pattern).Replace("\\*", ".*");
        return System.Text.RegularExpressions.Regex.IsMatch(text, $"^{escaped}$", System.Text.RegularExpressions.RegexOptions.IgnoreCase);
    }
}


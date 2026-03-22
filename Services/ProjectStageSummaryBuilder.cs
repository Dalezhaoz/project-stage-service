using ProjectStageService.Models;

namespace ProjectStageService.Services;

public sealed class ProjectStageSummaryBuilder
{
    public ProjectStageSummary BuildSummary(
        IEnumerable<ProjectStageRecord> records,
        int enabledServers,
        int visitedDatabases,
        int matchedDatabases)
    {
        var orderedRecords = records
            .OrderBy(item => item.ServerName, StringComparer.OrdinalIgnoreCase)
            .ThenBy(item => item.DatabaseName, StringComparer.OrdinalIgnoreCase)
            .ThenBy(item => item.ProjectName, StringComparer.OrdinalIgnoreCase)
            .ThenBy(item => item.ExamCode, StringComparer.OrdinalIgnoreCase)
            .ThenBy(item => item.StartTime)
            .ThenBy(item => item.EndTime)
            .ThenBy(item => item.StageName, StringComparer.OrdinalIgnoreCase)
            .ToList();

        return new ProjectStageSummary
        {
            Records = orderedRecords,
            Groups = BuildGroups(orderedRecords),
            EnabledServers = enabledServers,
            VisitedDatabases = visitedDatabases,
            MatchedDatabases = matchedDatabases,
            EndedCount = orderedRecords.Count(item => IsEndedStatus(item.Status)),
            OngoingCount = orderedRecords.Count(item => string.Equals(item.Status, ProjectStageStatuses.Ongoing, StringComparison.OrdinalIgnoreCase)),
            UpcomingCount = orderedRecords.Count(item => string.Equals(item.Status, ProjectStageStatuses.Upcoming, StringComparison.OrdinalIgnoreCase))
        };
    }

    public List<string> BuildStageNames(IEnumerable<ProjectStageRecord> records, ProjectStageQueryRequest request)
    {
        var stageFilterRequest = CreateStageFilterRequest(request);
        return records
            .Where(item => Matches(item, stageFilterRequest))
            .Select(item => item.StageName)
            .Where(item => !string.IsNullOrWhiteSpace(item))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(item => item, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    public IEnumerable<ProjectStageRecord> ApplyFilters(IEnumerable<ProjectStageRecord> records, ProjectStageQueryRequest request) =>
        records.Where(item => Matches(item, request));

    public bool Matches(ProjectStageRecord record, ProjectStageQueryRequest request)
    {
        if (!MatchesSelectedValues(record.ServerName, request.ServerNames))
        {
            return false;
        }

        if (!ContainsIgnoreCase(record.ServerName, request.ServerKeyword))
        {
            return false;
        }

        if (!ContainsIgnoreCase(record.DatabaseName, request.DatabaseKeyword))
        {
            return false;
        }

        if (!ContainsIgnoreCase(record.ExamCode, request.ExamCodeKeyword))
        {
            return false;
        }

        if (!ContainsIgnoreCase(record.ProjectName, request.ProjectKeyword))
        {
            return false;
        }

        if (!ContainsIgnoreCase(record.StageName, request.StageKeyword))
        {
            return false;
        }

        if (!MatchesSelectedValues(record.StageName, request.StageNames, allowContains: true))
        {
            return false;
        }

        if (!MatchesStatus(record.Status, request.StatusFilters))
        {
            return false;
        }

        if (!MatchesRange(record.StartTime, record.EndTime, request.RangeStart, request.RangeEnd))
        {
            return false;
        }

        return true;
    }

    public string NormalizeStatus(string? status)
    {
        if (string.IsNullOrWhiteSpace(status))
        {
            return string.Empty;
        }

        return status.Trim() switch
        {
            "已经结束" => ProjectStageStatuses.Ended,
            "已结束" => ProjectStageStatuses.Ended,
            "正在进行" => ProjectStageStatuses.Ongoing,
            "即将开始" => ProjectStageStatuses.Upcoming,
            var value => value
        };
    }

    private List<ProjectStageGroup> BuildGroups(IEnumerable<ProjectStageRecord> records)
    {
        return records
            .GroupBy(item => new { item.ServerName, item.DatabaseName, item.ExamCode })
            .Select(group =>
            {
                var stages = group
                    .OrderBy(item => item.StartTime)
                    .ThenBy(item => item.EndTime)
                    .ThenBy(item => item.StageName, StringComparer.OrdinalIgnoreCase)
                    .ToList();

                return new ProjectStageGroup
                {
                    ServerName = group.Key.ServerName,
                    DatabaseName = group.Key.DatabaseName,
                    ExamCode = group.Key.ExamCode,
                    ProjectName = stages.FirstOrDefault()?.ProjectName ?? string.Empty,
                    StartTime = stages.Min(item => item.StartTime),
                    EndTime = stages.Max(item => item.EndTime),
                    Statuses = stages
                        .Select(item => NormalizeStatus(item.Status))
                        .Distinct(StringComparer.OrdinalIgnoreCase)
                        .OrderBy(GetStatusSortOrder)
                        .ToList(),
                    Stages = stages
                };
            })
            .OrderBy(item => item.ServerName, StringComparer.OrdinalIgnoreCase)
            .ThenBy(item => item.DatabaseName, StringComparer.OrdinalIgnoreCase)
            .ThenBy(item => item.ProjectName, StringComparer.OrdinalIgnoreCase)
            .ThenBy(item => item.ExamCode, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    private ProjectStageQueryRequest CreateStageFilterRequest(ProjectStageQueryRequest request)
    {
        return new ProjectStageQueryRequest
        {
            Servers = request.Servers,
            StatusFilters = request.StatusFilters,
            ServerNames = request.ServerNames,
            ProjectKeyword = request.ProjectKeyword,
            ServerKeyword = request.ServerKeyword,
            DatabaseKeyword = request.DatabaseKeyword,
            ExamCodeKeyword = request.ExamCodeKeyword,
            RangeStart = request.RangeStart,
            RangeEnd = request.RangeEnd
        };
    }

    private bool MatchesStatus(string recordStatus, List<string> requestedStatuses)
    {
        if (requestedStatuses.Count == 0)
        {
            return true;
        }

        var normalizedRecordStatus = NormalizeStatus(recordStatus);
        return requestedStatuses
            .Select(NormalizeStatus)
            .Any(item => string.Equals(item, normalizedRecordStatus, StringComparison.OrdinalIgnoreCase));
    }

    private static bool MatchesSelectedValues(string source, List<string> selectedValues, bool allowContains = false)
    {
        if (selectedValues.Count == 0)
        {
            return true;
        }

        return selectedValues.Any(item =>
        {
            if (string.IsNullOrWhiteSpace(item))
            {
                return false;
            }

            var candidate = item.Trim();
            return allowContains
                ? source.Contains(candidate, StringComparison.OrdinalIgnoreCase)
                : string.Equals(source, candidate, StringComparison.OrdinalIgnoreCase);
        });
    }

    private static bool MatchesRange(DateTime startTime, DateTime endTime, DateTime? rangeStart, DateTime? rangeEnd)
    {
        if (!rangeStart.HasValue && !rangeEnd.HasValue)
        {
            return true;
        }

        var effectiveStart = rangeStart ?? DateTime.MinValue;
        var effectiveEnd = rangeEnd ?? DateTime.MaxValue;
        return endTime >= effectiveStart && startTime <= effectiveEnd;
    }

    private static bool ContainsIgnoreCase(string source, string keyword)
    {
        if (string.IsNullOrWhiteSpace(keyword))
        {
            return true;
        }

        return source.Contains(keyword.Trim(), StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsEndedStatus(string status) =>
        string.Equals(status, ProjectStageStatuses.Ended, StringComparison.OrdinalIgnoreCase) ||
        string.Equals(status, "已经结束", StringComparison.OrdinalIgnoreCase);

    private static int GetStatusSortOrder(string status) => status switch
    {
        ProjectStageStatuses.Ongoing => 0,
        ProjectStageStatuses.Upcoming => 1,
        ProjectStageStatuses.Ended => 2,
        _ => 9
    };
}

using Microsoft.Data.Sqlite;
using ProjectStageService.Models;

namespace ProjectStageService.Services;

public sealed class ProjectStageCacheStore
{
    private readonly string _dbPath;

    public ProjectStageCacheStore()
    {
        var baseDir = Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
            "ProjectStageService");
        Directory.CreateDirectory(baseDir);
        _dbPath = Path.Combine(baseDir, "stage_cache.db");
    }

    public async Task InitializeAsync(CancellationToken cancellationToken)
    {
        await using var connection = OpenConnection();
        await connection.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            CREATE TABLE IF NOT EXISTS cache_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                refreshed_at TEXT NULL,
                enabled_servers INTEGER NOT NULL DEFAULT 0,
                visited_databases INTEGER NOT NULL DEFAULT 0,
                matched_databases INTEGER NOT NULL DEFAULT 0,
                ended_count INTEGER NOT NULL DEFAULT 0,
                ongoing_count INTEGER NOT NULL DEFAULT 0,
                upcoming_count INTEGER NOT NULL DEFAULT 0,
                record_count INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS stage_record (
                server_name TEXT NOT NULL,
                database_name TEXT NOT NULL,
                exam_code TEXT NOT NULL,
                project_name TEXT NOT NULL,
                stage_name TEXT NOT NULL,
                start_time TEXT NOT NULL,
                end_time TEXT NOT NULL,
                status TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_stage_record_server ON stage_record(server_name);
            CREATE INDEX IF NOT EXISTS idx_stage_record_exam ON stage_record(server_name, database_name, exam_code);
            CREATE INDEX IF NOT EXISTS idx_stage_record_stage_name ON stage_record(stage_name);
            CREATE INDEX IF NOT EXISTS idx_stage_record_status ON stage_record(status);
            CREATE INDEX IF NOT EXISTS idx_stage_record_time ON stage_record(start_time, end_time);
            """;
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<ProjectStageCacheInfo> GetCacheInfoAsync(CancellationToken cancellationToken)
    {
        await using var connection = OpenConnection();
        await connection.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT refreshed_at, enabled_servers, visited_databases, matched_databases, record_count
            FROM cache_state
            WHERE id = 1
            """;

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return new ProjectStageCacheInfo { HasData = false };
        }

        return new ProjectStageCacheInfo
        {
            HasData = !reader.IsDBNull(0),
            RefreshedAt = reader.IsDBNull(0) ? null : DateTime.Parse(reader.GetString(0)),
            EnabledServers = reader.GetInt32(1),
            VisitedDatabases = reader.GetInt32(2),
            MatchedDatabases = reader.GetInt32(3),
            RecordCount = reader.GetInt32(4)
        };
    }

    public async Task SaveSnapshotAsync(ProjectStageSummary summary, CancellationToken cancellationToken)
    {
        await using var connection = OpenConnection();
        await connection.OpenAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        await using (var deleteRecords = connection.CreateCommand())
        {
            deleteRecords.Transaction = transaction;
            deleteRecords.CommandText = "DELETE FROM stage_record;";
            await deleteRecords.ExecuteNonQueryAsync(cancellationToken);
        }

        await using (var upsertState = connection.CreateCommand())
        {
            upsertState.Transaction = transaction;
            upsertState.CommandText = """
                INSERT INTO cache_state (
                    id, refreshed_at, enabled_servers, visited_databases, matched_databases,
                    ended_count, ongoing_count, upcoming_count, record_count
                )
                VALUES (1, $refreshed_at, $enabled_servers, $visited_databases, $matched_databases, $ended_count, $ongoing_count, $upcoming_count, $record_count)
                ON CONFLICT(id) DO UPDATE SET
                    refreshed_at = excluded.refreshed_at,
                    enabled_servers = excluded.enabled_servers,
                    visited_databases = excluded.visited_databases,
                    matched_databases = excluded.matched_databases,
                    ended_count = excluded.ended_count,
                    ongoing_count = excluded.ongoing_count,
                    upcoming_count = excluded.upcoming_count,
                    record_count = excluded.record_count;
                """;
            upsertState.Parameters.AddWithValue("$refreshed_at", DateTime.Now.ToString("O"));
            upsertState.Parameters.AddWithValue("$enabled_servers", summary.EnabledServers);
            upsertState.Parameters.AddWithValue("$visited_databases", summary.VisitedDatabases);
            upsertState.Parameters.AddWithValue("$matched_databases", summary.MatchedDatabases);
            upsertState.Parameters.AddWithValue("$ended_count", summary.EndedCount);
            upsertState.Parameters.AddWithValue("$ongoing_count", summary.OngoingCount);
            upsertState.Parameters.AddWithValue("$upcoming_count", summary.UpcomingCount);
            upsertState.Parameters.AddWithValue("$record_count", summary.Records.Count);
            await upsertState.ExecuteNonQueryAsync(cancellationToken);
        }

        await using (var insertRecord = connection.CreateCommand())
        {
            insertRecord.Transaction = transaction;
            insertRecord.CommandText = """
                INSERT INTO stage_record (
                    server_name, database_name, exam_code, project_name, stage_name, start_time, end_time, status
                )
                VALUES ($server_name, $database_name, $exam_code, $project_name, $stage_name, $start_time, $end_time, $status);
                """;

            var serverName = insertRecord.Parameters.Add("$server_name", SqliteType.Text);
            var databaseName = insertRecord.Parameters.Add("$database_name", SqliteType.Text);
            var examCode = insertRecord.Parameters.Add("$exam_code", SqliteType.Text);
            var projectName = insertRecord.Parameters.Add("$project_name", SqliteType.Text);
            var stageName = insertRecord.Parameters.Add("$stage_name", SqliteType.Text);
            var startTime = insertRecord.Parameters.Add("$start_time", SqliteType.Text);
            var endTime = insertRecord.Parameters.Add("$end_time", SqliteType.Text);
            var status = insertRecord.Parameters.Add("$status", SqliteType.Text);

            foreach (var record in summary.Records)
            {
                serverName.Value = record.ServerName;
                databaseName.Value = record.DatabaseName;
                examCode.Value = record.ExamCode;
                projectName.Value = record.ProjectName;
                stageName.Value = record.StageName;
                startTime.Value = record.StartTime.ToString("O");
                endTime.Value = record.EndTime.ToString("O");
                status.Value = record.Status;
                await insertRecord.ExecuteNonQueryAsync(cancellationToken);
            }
        }

        await transaction.CommitAsync(cancellationToken);
    }

    public async Task<List<string>> QueryStageNamesAsync(ProjectStageQueryRequest request, CancellationToken cancellationToken)
    {
        var records = await LoadRecordsAsync(cancellationToken);
        return ApplyServerFilter(records, request.Servers)
            .Select(item => item.StageName)
            .Where(item => !string.IsNullOrWhiteSpace(item))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(item => item, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    public async Task<ProjectStageSummary> QueryAsync(ProjectStageQueryRequest request, CancellationToken cancellationToken)
    {
        var cacheInfo = await GetCacheInfoAsync(cancellationToken);
        var records = await LoadRecordsAsync(cancellationToken);
        var filtered = ApplyServerFilter(records, request.Servers)
            .Where(item => AllowRecord(item, request))
            .ToList();

        var summary = new ProjectStageSummary
        {
            Records = filtered,
            EnabledServers = cacheInfo.EnabledServers,
            VisitedDatabases = cacheInfo.VisitedDatabases,
            MatchedDatabases = cacheInfo.MatchedDatabases,
            EndedCount = filtered.Count(item => item.Status == "已经结束"),
            OngoingCount = filtered.Count(item => item.Status == "正在进行"),
            UpcomingCount = filtered.Count(item => item.Status == "即将开始")
        };

        summary.Groups = filtered
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
                        .Select(item => item.Status)
                        .Distinct(StringComparer.OrdinalIgnoreCase)
                        .OrderBy(item => item == "正在进行" ? 0 : item == "即将开始" ? 1 : 2)
                        .ToList(),
                    Stages = stages
                };
            })
            .OrderBy(item => item.ServerName, StringComparer.OrdinalIgnoreCase)
            .ThenBy(item => item.DatabaseName, StringComparer.OrdinalIgnoreCase)
            .ThenBy(item => item.ProjectName, StringComparer.OrdinalIgnoreCase)
            .ThenBy(item => item.ExamCode, StringComparer.OrdinalIgnoreCase)
            .ToList();

        return summary;
    }

    private async Task<List<ProjectStageRecord>> LoadRecordsAsync(CancellationToken cancellationToken)
    {
        await using var connection = OpenConnection();
        await connection.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT server_name, database_name, exam_code, project_name, stage_name, start_time, end_time, status
            FROM stage_record
            ORDER BY server_name, database_name, exam_code, start_time, end_time
            """;

        var records = new List<ProjectStageRecord>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            records.Add(new ProjectStageRecord
            {
                ServerName = reader.GetString(0),
                DatabaseName = reader.GetString(1),
                ExamCode = reader.GetString(2),
                ProjectName = reader.GetString(3),
                StageName = reader.GetString(4),
                StartTime = DateTime.Parse(reader.GetString(5)),
                EndTime = DateTime.Parse(reader.GetString(6)),
                Status = reader.GetString(7)
            });
        }

        return records;
    }

    private static IEnumerable<ProjectStageRecord> ApplyServerFilter(IEnumerable<ProjectStageRecord> records, List<StageServerConfig> servers)
    {
        var enabledNames = servers
            .Where(item => item.Enabled && !string.IsNullOrWhiteSpace(item.Name))
            .Select(item => item.Name.Trim())
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

        if (enabledNames.Count == 0)
        {
            return records;
        }

        return records.Where(item => enabledNames.Contains(item.ServerName, StringComparer.OrdinalIgnoreCase));
    }

    private static bool AllowRecord(ProjectStageRecord record, ProjectStageQueryRequest request)
    {
        if (!ContainsIgnoreCase(record.ProjectName, request.ProjectKeyword))
        {
            return false;
        }

        if (!ContainsIgnoreCase(record.StageName, request.StageKeyword))
        {
            return false;
        }

        if (request.StageNames.Count > 0 &&
            !request.StageNames.Any(item => string.Equals(item, record.StageName, StringComparison.OrdinalIgnoreCase)))
        {
            return false;
        }

        if (request.StatusFilters.Count > 0 &&
            !request.StatusFilters.Any(item => string.Equals(item, record.Status, StringComparison.OrdinalIgnoreCase)))
        {
            return false;
        }

        if (request.RangeStart.HasValue || request.RangeEnd.HasValue)
        {
            var rangeStart = request.RangeStart ?? DateTime.MinValue;
            var rangeEnd = request.RangeEnd ?? DateTime.MaxValue;
            if (record.EndTime < rangeStart || record.StartTime > rangeEnd)
            {
                return false;
            }
        }

        return true;
    }

    private static bool ContainsIgnoreCase(string source, string keyword)
    {
        if (string.IsNullOrWhiteSpace(keyword))
        {
            return true;
        }

        return source.Contains(keyword.Trim(), StringComparison.OrdinalIgnoreCase);
    }

    private SqliteConnection OpenConnection() => new($"Data Source={_dbPath}");
}

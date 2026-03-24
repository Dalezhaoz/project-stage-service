using Microsoft.Data.Sqlite;
using ProjectStageService.Models;

namespace ProjectStageService.Services;

public sealed class ProjectStageCacheStore
{
    private readonly string _dbPath;

    public ProjectStageCacheStore()
    {
        var baseDir = AppDataPath.GetBaseDirectory();
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
                status TEXT NOT NULL,
                registration_count INTEGER NOT NULL DEFAULT 0,
                admission_ticket_count INTEGER NOT NULL DEFAULT 0
            );

            CREATE INDEX IF NOT EXISTS idx_stage_record_server ON stage_record(server_name);
            CREATE INDEX IF NOT EXISTS idx_stage_record_exam ON stage_record(server_name, database_name, exam_code);
            CREATE INDEX IF NOT EXISTS idx_stage_record_stage_name ON stage_record(stage_name);
            CREATE INDEX IF NOT EXISTS idx_stage_record_status ON stage_record(status);
            CREATE INDEX IF NOT EXISTS idx_stage_record_time ON stage_record(start_time, end_time);
            """;
        await command.ExecuteNonQueryAsync(cancellationToken);

        await EnsureColumnAsync(connection, "stage_record", "registration_count", "INTEGER NOT NULL DEFAULT 0", cancellationToken);
        await EnsureColumnAsync(connection, "stage_record", "admission_ticket_count", "INTEGER NOT NULL DEFAULT 0", cancellationToken);
    }

    public async Task<ProjectStageCacheInfo> GetCacheInfoAsync(CancellationToken cancellationToken)
    {
        await using var connection = OpenConnection();
        await connection.OpenAsync(cancellationToken);
        return await GetCacheInfoAsync(connection, cancellationToken);
    }

    private static async Task<ProjectStageCacheInfo> GetCacheInfoAsync(SqliteConnection connection, CancellationToken cancellationToken)
    {
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
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);

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
                    server_name, database_name, exam_code, project_name, stage_name, start_time, end_time, status, registration_count, admission_ticket_count
                )
                VALUES ($server_name, $database_name, $exam_code, $project_name, $stage_name, $start_time, $end_time, $status, $registration_count, $admission_ticket_count);
                """;

            var serverName = insertRecord.Parameters.Add("$server_name", SqliteType.Text);
            var databaseName = insertRecord.Parameters.Add("$database_name", SqliteType.Text);
            var examCode = insertRecord.Parameters.Add("$exam_code", SqliteType.Text);
            var projectName = insertRecord.Parameters.Add("$project_name", SqliteType.Text);
            var stageName = insertRecord.Parameters.Add("$stage_name", SqliteType.Text);
            var startTime = insertRecord.Parameters.Add("$start_time", SqliteType.Text);
            var endTime = insertRecord.Parameters.Add("$end_time", SqliteType.Text);
            var status = insertRecord.Parameters.Add("$status", SqliteType.Text);
            var registrationCount = insertRecord.Parameters.Add("$registration_count", SqliteType.Integer);
            var admissionTicketCount = insertRecord.Parameters.Add("$admission_ticket_count", SqliteType.Integer);

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
                registrationCount.Value = record.RegistrationCount;
                admissionTicketCount.Value = record.AdmissionTicketCount;
                await insertRecord.ExecuteNonQueryAsync(cancellationToken);
            }
        }

        await transaction.CommitAsync(cancellationToken);
    }

    public async Task<List<string>> QueryStageNamesAsync(ProjectStageQueryRequest request, CancellationToken cancellationToken)
    {
        var stageFilterRequest = new ProjectStageQueryRequest
        {
            Servers = request.Servers,
            StatusFilters = request.StatusFilters,
            ServerNames = request.ServerNames,
            ProjectKeyword = request.ProjectKeyword,
            ServerKeyword = request.ServerKeyword,
            DatabaseKeyword = request.DatabaseKeyword,
            ExamCodeKeyword = request.ExamCodeKeyword,
            RangeStart = request.RangeStart,
            RangeEnd = request.RangeEnd,
            DayOffsets = request.DayOffsets,
            TimeMatchMode = request.TimeMatchMode
        };

        await using var connection = OpenConnection();
        await connection.OpenAsync(cancellationToken);
        var records = await LoadFilteredRecordsAsync(connection, stageFilterRequest, cancellationToken);

        return records
            .Select(item => item.StageName)
            .Where(item => !string.IsNullOrWhiteSpace(item))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(item => item, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    public async Task<ProjectStageSummary> QueryAsync(ProjectStageQueryRequest request, CancellationToken cancellationToken)
    {
        await using var connection = OpenConnection();
        await connection.OpenAsync(cancellationToken);

        var cacheInfo = await GetCacheInfoAsync(connection, cancellationToken);
        var filtered = await LoadFilteredRecordsAsync(connection, request, cancellationToken);

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
                    RegistrationCount = stages.FirstOrDefault()?.RegistrationCount ?? 0,
                    AdmissionTicketCount = stages.FirstOrDefault()?.AdmissionTicketCount ?? 0,
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

    public async Task<List<BoardCountTarget>> LoadCountTargetsAsync(string serverName, CancellationToken cancellationToken)
    {
        await using var connection = OpenConnection();
        await connection.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT DISTINCT server_name, database_name, exam_code
            FROM stage_record
            WHERE server_name = $server_name
              AND exam_code IS NOT NULL
              AND TRIM(exam_code) <> ''
              AND status IN ('正在进行', '即将开始')
            ORDER BY database_name, exam_code
            """;
        command.Parameters.AddWithValue("$server_name", serverName);

        var targets = new List<BoardCountTarget>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            targets.Add(new BoardCountTarget
            {
                ServerName = reader.GetString(0),
                DatabaseName = reader.GetString(1),
                ExamCode = reader.GetString(2)
            });
        }

        return targets;
    }

    public async Task SaveServerCountsAsync(
        string serverName,
        IReadOnlyCollection<ServerCountUpdate> updates,
        CancellationToken cancellationToken)
    {
        if (updates.Count == 0)
        {
            return;
        }

        await using var connection = OpenConnection();
        await connection.OpenAsync(cancellationToken);
        await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            UPDATE stage_record
            SET
                registration_count = COALESCE($registration_count, registration_count),
                admission_ticket_count = COALESCE($admission_ticket_count, admission_ticket_count)
            WHERE server_name = $server_name
              AND database_name = $database_name
              AND exam_code = $exam_code
            """;

        var serverNameParameter = command.Parameters.Add("$server_name", SqliteType.Text);
        var databaseNameParameter = command.Parameters.Add("$database_name", SqliteType.Text);
        var examCodeParameter = command.Parameters.Add("$exam_code", SqliteType.Text);
        var registrationCountParameter = command.Parameters.Add("$registration_count", SqliteType.Integer);
        var admissionTicketCountParameter = command.Parameters.Add("$admission_ticket_count", SqliteType.Integer);

        foreach (var update in updates)
        {
            serverNameParameter.Value = serverName;
            databaseNameParameter.Value = update.DatabaseName;
            examCodeParameter.Value = update.ExamCode;
            registrationCountParameter.Value = update.RegistrationCount.HasValue
                ? update.RegistrationCount.Value
                : DBNull.Value;
            admissionTicketCountParameter.Value = update.AdmissionTicketCount.HasValue
                ? update.AdmissionTicketCount.Value
                : DBNull.Value;
            await command.ExecuteNonQueryAsync(cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
    }

    private static async Task<List<ProjectStageRecord>> LoadFilteredRecordsAsync(
        SqliteConnection connection,
        ProjectStageQueryRequest request,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        var conditions = new List<string>();
        var paramIndex = 0;

        // Server filter (enabled servers)
        var enabledNames = request.Servers
            .Where(item => item.Enabled && !string.IsNullOrWhiteSpace(item.Name))
            .Select(item => item.Name.Trim())
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

        if (enabledNames.Count > 0)
        {
            var placeholders = new List<string>();
            foreach (var name in enabledNames)
            {
                var p = $"$sv{paramIndex++}";
                placeholders.Add(p);
                command.Parameters.AddWithValue(p, name);
            }
            conditions.Add($"server_name COLLATE NOCASE IN ({string.Join(", ", placeholders)})");
        }

        // ServerNames filter
        if (request.ServerNames.Count > 0)
        {
            var placeholders = new List<string>();
            foreach (var name in request.ServerNames.Where(item => !string.IsNullOrWhiteSpace(item)))
            {
                var p = $"$sn{paramIndex++}";
                placeholders.Add(p);
                command.Parameters.AddWithValue(p, name.Trim());
            }
            if (placeholders.Count > 0)
            {
                conditions.Add($"server_name COLLATE NOCASE IN ({string.Join(", ", placeholders)})");
            }
        }

        // Status filter
        if (request.StatusFilters.Count > 0)
        {
            var placeholders = new List<string>();
            foreach (var status in request.StatusFilters)
            {
                var p = $"$st{paramIndex++}";
                placeholders.Add(p);
                command.Parameters.AddWithValue(p, status);
            }
            conditions.Add($"status COLLATE NOCASE IN ({string.Join(", ", placeholders)})");
        }

        // Keyword filters (LIKE)
        if (!string.IsNullOrWhiteSpace(request.ServerKeyword))
        {
            var p = $"$kw{paramIndex++}";
            conditions.Add($"server_name LIKE {p}");
            command.Parameters.AddWithValue(p, $"%{request.ServerKeyword.Trim()}%");
        }
        if (!string.IsNullOrWhiteSpace(request.DatabaseKeyword))
        {
            var p = $"$kw{paramIndex++}";
            conditions.Add($"database_name LIKE {p}");
            command.Parameters.AddWithValue(p, $"%{request.DatabaseKeyword.Trim()}%");
        }
        if (!string.IsNullOrWhiteSpace(request.ExamCodeKeyword))
        {
            var p = $"$kw{paramIndex++}";
            conditions.Add($"exam_code LIKE {p}");
            command.Parameters.AddWithValue(p, $"%{request.ExamCodeKeyword.Trim()}%");
        }
        if (!string.IsNullOrWhiteSpace(request.ProjectKeyword))
        {
            var p = $"$kw{paramIndex++}";
            conditions.Add($"project_name LIKE {p}");
            command.Parameters.AddWithValue(p, $"%{request.ProjectKeyword.Trim()}%");
        }
        if (!string.IsNullOrWhiteSpace(request.StageKeyword))
        {
            var p = $"$kw{paramIndex++}";
            conditions.Add($"stage_name LIKE {p}");
            command.Parameters.AddWithValue(p, $"%{request.StageKeyword.Trim()}%");
        }

        // StageNames filter (OR of LIKE)
        if (request.StageNames.Count > 0)
        {
            var stageConditions = new List<string>();
            foreach (var name in request.StageNames.Where(item => !string.IsNullOrWhiteSpace(item)))
            {
                var p = $"$sgn{paramIndex++}";
                stageConditions.Add($"stage_name LIKE {p}");
                command.Parameters.AddWithValue(p, $"%{name.Trim()}%");
            }
            if (stageConditions.Count > 0)
            {
                conditions.Add($"({string.Join(" OR ", stageConditions)})");
            }
        }

        // Time range filter
        if (request.RangeStart.HasValue || request.RangeEnd.HasValue)
        {
            var rangeStart = request.RangeStart ?? DateTime.MinValue;
            var rangeEnd = request.RangeEnd ?? DateTime.MaxValue;
            AddTimeRangeCondition(conditions, command, ref paramIndex, rangeStart, rangeEnd, request.TimeMatchMode, "tr");
        }

        // DayOffsets filter
        if (request.DayOffsets.Count > 0)
        {
            var today = DateTime.Today;
            var dayConditions = new List<string>();
            foreach (var offset in request.DayOffsets.Distinct())
            {
                var dayStart = today.AddDays(offset);
                var dayEnd = dayStart.AddDays(1).AddTicks(-1);
                var subConditions = new List<string>();
                AddTimeRangeCondition(subConditions, command, ref paramIndex, dayStart, dayEnd, request.TimeMatchMode, $"do{offset}");
                if (subConditions.Count > 0)
                {
                    dayConditions.Add($"({string.Join(" AND ", subConditions)})");
                }
            }
            if (dayConditions.Count > 0)
            {
                conditions.Add($"({string.Join(" OR ", dayConditions)})");
            }
        }

        var whereClause = conditions.Count > 0 ? $"WHERE {string.Join(" AND ", conditions)}" : "";
        command.CommandText = $"""
            SELECT server_name, database_name, exam_code, project_name, stage_name, start_time, end_time, status, registration_count, admission_ticket_count
            FROM stage_record
            {whereClause}
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
                Status = reader.GetString(7),
                RegistrationCount = reader.GetInt32(8),
                AdmissionTicketCount = reader.GetInt32(9)
            });
        }

        return records;
    }

    private static void AddTimeRangeCondition(
        List<string> conditions,
        SqliteCommand command,
        ref int paramIndex,
        DateTime rangeStart,
        DateTime rangeEnd,
        string timeMatchMode,
        string prefix)
    {
        var pStart = $"${prefix}s{paramIndex}";
        var pEnd = $"${prefix}e{paramIndex}";
        paramIndex++;
        command.Parameters.AddWithValue(pStart, rangeStart.ToString("O"));
        command.Parameters.AddWithValue(pEnd, rangeEnd.ToString("O"));

        switch (timeMatchMode)
        {
            case "start":
                conditions.Add($"start_time >= {pStart} AND start_time <= {pEnd}");
                break;
            case "end":
                conditions.Add($"end_time >= {pStart} AND end_time <= {pEnd}");
                break;
            default:
                conditions.Add($"end_time >= {pStart} AND start_time <= {pEnd}");
                break;
        }
    }

    private SqliteConnection OpenConnection() => new($"Data Source={_dbPath}");

    private static async Task EnsureColumnAsync(
        SqliteConnection connection,
        string tableName,
        string columnName,
        string columnDefinition,
        CancellationToken cancellationToken)
    {
        await using var existsCommand = connection.CreateCommand();
        existsCommand.CommandText = $"PRAGMA table_info({tableName});";

        var exists = false;
        await using (var reader = await existsCommand.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                if (string.Equals(reader.GetString(1), columnName, StringComparison.OrdinalIgnoreCase))
                {
                    exists = true;
                    break;
                }
            }
        }

        if (exists)
        {
            return;
        }

        await using var alterCommand = connection.CreateCommand();
        alterCommand.CommandText = $"ALTER TABLE {tableName} ADD COLUMN {columnName} {columnDefinition};";
        await alterCommand.ExecuteNonQueryAsync(cancellationToken);
    }
}

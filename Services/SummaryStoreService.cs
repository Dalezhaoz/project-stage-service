using Microsoft.Data.SqlClient;
using ProjectStageService.Models;

namespace ProjectStageService.Services;

public sealed class SummaryStoreService
{
    private const string TableName = "project_stage_summary";

    public async Task<object> TestConnectionAsync(SummaryStoreConfig config, CancellationToken cancellationToken)
    {
        await using var connection = OpenConnection(config);
        await connection.OpenAsync(cancellationToken);
        await EnsureSchemaAsync(connection, cancellationToken);

        await using var command = connection.CreateCommand();
        command.CommandText = $"SELECT COUNT(*) FROM dbo.{TableName};";
        var count = Convert.ToInt32(await command.ExecuteScalarAsync(cancellationToken));
        return new { record_count = count };
    }

    public async Task SyncSnapshotAsync(SummaryStoreConfig config, ProjectStageSummary summary, CancellationToken cancellationToken)
    {
        await using var connection = OpenConnection(config);
        await connection.OpenAsync(cancellationToken);
        await EnsureSchemaAsync(connection, cancellationToken);

        var groups = summary.Records
            .GroupBy(item => new { item.ServerName, item.DatabaseName })
            .ToList();

        await using var transaction = (SqlTransaction)await connection.BeginTransactionAsync(cancellationToken);

        foreach (var group in groups)
        {
            var distinctRecords = group
                .GroupBy(item => new
                {
                    item.ServerName,
                    item.DatabaseName,
                    item.ExamCode,
                    item.StageName,
                    item.StartTime,
                    item.EndTime
                })
                .Select(recordGroup => recordGroup
                    .OrderByDescending(item => item.RegistrationCount)
                    .ThenByDescending(item => item.AdmissionTicketCount)
                    .First())
                .ToList();

            await using (var deleteCommand = connection.CreateCommand())
            {
                deleteCommand.Transaction = transaction;
                deleteCommand.CommandText = $"""
                    DELETE FROM dbo.{TableName}
                    WHERE source_server_name = @server_name
                      AND source_database_name = @database_name;
                    """;
                deleteCommand.Parameters.AddWithValue("@server_name", group.Key.ServerName);
                deleteCommand.Parameters.AddWithValue("@database_name", group.Key.DatabaseName);
                await deleteCommand.ExecuteNonQueryAsync(cancellationToken);
            }

            await using (var insertCommand = connection.CreateCommand())
            {
                insertCommand.Transaction = transaction;
                insertCommand.CommandText = $"""
                    INSERT INTO dbo.{TableName} (
                        source_server_name, source_database_name, source_database_type, exam_code,
                        project_name, stage_name, stage_start_time, stage_end_time, stage_status,
                        registration_count, admission_ticket_count, synced_at
                    )
                    VALUES (
                        @source_server_name, @source_database_name, @source_database_type, @exam_code,
                        @project_name, @stage_name, @stage_start_time, @stage_end_time, @stage_status,
                        @registration_count, @admission_ticket_count, @synced_at
                    );
                    """;

                var sourceServerName = insertCommand.Parameters.Add("@source_server_name", System.Data.SqlDbType.NVarChar, 100);
                var sourceDatabaseName = insertCommand.Parameters.Add("@source_database_name", System.Data.SqlDbType.NVarChar, 200);
                var sourceDatabaseType = insertCommand.Parameters.Add("@source_database_type", System.Data.SqlDbType.NVarChar, 20);
                var examCode = insertCommand.Parameters.Add("@exam_code", System.Data.SqlDbType.NVarChar, 50);
                var projectName = insertCommand.Parameters.Add("@project_name", System.Data.SqlDbType.NVarChar, 500);
                var stageName = insertCommand.Parameters.Add("@stage_name", System.Data.SqlDbType.NVarChar, 200);
                var stageStartTime = insertCommand.Parameters.Add("@stage_start_time", System.Data.SqlDbType.DateTime2);
                var stageEndTime = insertCommand.Parameters.Add("@stage_end_time", System.Data.SqlDbType.DateTime2);
                var stageStatus = insertCommand.Parameters.Add("@stage_status", System.Data.SqlDbType.NVarChar, 20);
                var registrationCount = insertCommand.Parameters.Add("@registration_count", System.Data.SqlDbType.Int);
                var admissionTicketCount = insertCommand.Parameters.Add("@admission_ticket_count", System.Data.SqlDbType.Int);
                var syncedAt = insertCommand.Parameters.Add("@synced_at", System.Data.SqlDbType.DateTime2);

                var syncedAtValue = DateTime.Now;
                foreach (var record in distinctRecords)
                {
                    sourceServerName.Value = record.ServerName;
                    sourceDatabaseName.Value = record.DatabaseName;
                    sourceDatabaseType.Value = ResolveDatabaseType(summary, record.ServerName);
                    examCode.Value = record.ExamCode;
                    projectName.Value = record.ProjectName;
                    stageName.Value = record.StageName;
                    stageStartTime.Value = record.StartTime;
                    stageEndTime.Value = record.EndTime;
                    stageStatus.Value = record.Status;
                    registrationCount.Value = record.RegistrationCount;
                    admissionTicketCount.Value = record.AdmissionTicketCount;
                    syncedAt.Value = syncedAtValue;
                    await insertCommand.ExecuteNonQueryAsync(cancellationToken);
                }
            }
        }

        await transaction.CommitAsync(cancellationToken);
    }

    public async Task<List<string>> QueryStageNamesAsync(SummaryStoreConfig config, ProjectStageQueryRequest request, CancellationToken cancellationToken)
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

        await using var connection = OpenConnection(config);
        await connection.OpenAsync(cancellationToken);
        var records = await LoadFilteredRecordsAsync(connection, stageFilterRequest, cancellationToken);

        return records
            .Select(item => item.StageName)
            .Where(item => !string.IsNullOrWhiteSpace(item))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(item => item, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    public async Task<ProjectStageSummary> QueryAsync(SummaryStoreConfig config, ProjectStageQueryRequest request, CancellationToken cancellationToken)
    {
        await using var connection = OpenConnection(config);
        await connection.OpenAsync(cancellationToken);
        var filtered = await LoadFilteredRecordsAsync(connection, request, cancellationToken);

        var enabledServerCount = request.Servers.Count(item => item.Enabled);
        var matchedDatabaseCount = filtered
            .Select(item => $"{item.ServerName}::{item.DatabaseName}")
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .Count();

        var summary = new ProjectStageSummary
        {
            Records = filtered,
            EnabledServers = enabledServerCount,
            VisitedDatabases = matchedDatabaseCount,
            MatchedDatabases = matchedDatabaseCount,
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

    public async Task EnsureSchemaAsync(SummaryStoreConfig config, CancellationToken cancellationToken)
    {
        await using var connection = OpenConnection(config);
        await connection.OpenAsync(cancellationToken);
        await EnsureSchemaAsync(connection, cancellationToken);
    }

    private static async Task EnsureSchemaAsync(SqlConnection connection, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = $"""
            IF OBJECT_ID(N'dbo.{TableName}', N'U') IS NULL
            BEGIN
                CREATE TABLE dbo.{TableName} (
                    id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                    source_server_name NVARCHAR(100) NOT NULL,
                    source_database_name NVARCHAR(200) NOT NULL,
                    source_database_type NVARCHAR(20) NOT NULL,
                    exam_code NVARCHAR(50) NOT NULL,
                    project_name NVARCHAR(500) NOT NULL,
                    stage_name NVARCHAR(200) NOT NULL,
                    stage_start_time DATETIME2 NOT NULL,
                    stage_end_time DATETIME2 NOT NULL,
                    stage_status NVARCHAR(20) NOT NULL,
                    registration_count INT NOT NULL DEFAULT 0,
                    admission_ticket_count INT NOT NULL DEFAULT 0,
                    synced_at DATETIME2 NOT NULL DEFAULT SYSDATETIME()
                );
            END;

            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'UX_project_stage_summary_unique' AND object_id = OBJECT_ID(N'dbo.{TableName}'))
                CREATE UNIQUE INDEX UX_project_stage_summary_unique
                ON dbo.{TableName}(source_server_name, source_database_name, exam_code, stage_name, stage_start_time, stage_end_time);

            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_project_stage_summary_status' AND object_id = OBJECT_ID(N'dbo.{TableName}'))
                CREATE INDEX IX_project_stage_summary_status ON dbo.{TableName}(stage_status);

            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_project_stage_summary_time' AND object_id = OBJECT_ID(N'dbo.{TableName}'))
                CREATE INDEX IX_project_stage_summary_time ON dbo.{TableName}(stage_start_time, stage_end_time);

            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_project_stage_summary_stage_name' AND object_id = OBJECT_ID(N'dbo.{TableName}'))
                CREATE INDEX IX_project_stage_summary_stage_name ON dbo.{TableName}(stage_name);

            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_project_stage_summary_project_name' AND object_id = OBJECT_ID(N'dbo.{TableName}'))
                CREATE INDEX IX_project_stage_summary_project_name ON dbo.{TableName}(project_name);

            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_project_stage_summary_exam_code' AND object_id = OBJECT_ID(N'dbo.{TableName}'))
                CREATE INDEX IX_project_stage_summary_exam_code ON dbo.{TableName}(exam_code);

            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_project_stage_summary_source' AND object_id = OBJECT_ID(N'dbo.{TableName}'))
                CREATE INDEX IX_project_stage_summary_source ON dbo.{TableName}(source_server_name, source_database_name);
            """;
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task<List<ProjectStageRecord>> LoadFilteredRecordsAsync(
        SqlConnection connection,
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
                var p = $"@sv{paramIndex++}";
                placeholders.Add(p);
                command.Parameters.AddWithValue(p, name);
            }
            conditions.Add($"source_server_name IN ({string.Join(", ", placeholders)})");
        }

        // ServerNames filter
        if (request.ServerNames.Count > 0)
        {
            var placeholders = new List<string>();
            foreach (var name in request.ServerNames.Where(item => !string.IsNullOrWhiteSpace(item)))
            {
                var p = $"@sn{paramIndex++}";
                placeholders.Add(p);
                command.Parameters.AddWithValue(p, name.Trim());
            }
            if (placeholders.Count > 0)
            {
                conditions.Add($"source_server_name IN ({string.Join(", ", placeholders)})");
            }
        }

        // Status filter
        if (request.StatusFilters.Count > 0)
        {
            var placeholders = new List<string>();
            foreach (var status in request.StatusFilters)
            {
                var p = $"@st{paramIndex++}";
                placeholders.Add(p);
                command.Parameters.AddWithValue(p, status);
            }
            conditions.Add($"stage_status IN ({string.Join(", ", placeholders)})");
        }

        // Keyword filters (LIKE)
        if (!string.IsNullOrWhiteSpace(request.ServerKeyword))
        {
            var p = $"@kw{paramIndex++}";
            conditions.Add($"source_server_name LIKE {p}");
            command.Parameters.AddWithValue(p, $"%{request.ServerKeyword.Trim()}%");
        }
        if (!string.IsNullOrWhiteSpace(request.DatabaseKeyword))
        {
            var p = $"@kw{paramIndex++}";
            conditions.Add($"source_database_name LIKE {p}");
            command.Parameters.AddWithValue(p, $"%{request.DatabaseKeyword.Trim()}%");
        }
        if (!string.IsNullOrWhiteSpace(request.ExamCodeKeyword))
        {
            var p = $"@kw{paramIndex++}";
            conditions.Add($"exam_code LIKE {p}");
            command.Parameters.AddWithValue(p, $"%{request.ExamCodeKeyword.Trim()}%");
        }
        if (!string.IsNullOrWhiteSpace(request.ProjectKeyword))
        {
            var p = $"@kw{paramIndex++}";
            conditions.Add($"project_name LIKE {p}");
            command.Parameters.AddWithValue(p, $"%{request.ProjectKeyword.Trim()}%");
        }
        if (!string.IsNullOrWhiteSpace(request.StageKeyword))
        {
            var p = $"@kw{paramIndex++}";
            conditions.Add($"stage_name LIKE {p}");
            command.Parameters.AddWithValue(p, $"%{request.StageKeyword.Trim()}%");
        }

        // StageNames filter (OR of LIKE)
        if (request.StageNames.Count > 0)
        {
            var stageConditions = new List<string>();
            foreach (var name in request.StageNames.Where(item => !string.IsNullOrWhiteSpace(item)))
            {
                var p = $"@sgn{paramIndex++}";
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
            SELECT source_server_name, source_database_name, exam_code, project_name, stage_name,
                   stage_start_time, stage_end_time, stage_status, registration_count, admission_ticket_count
            FROM dbo.{TableName}
            {whereClause}
            ORDER BY source_server_name, source_database_name, exam_code, stage_start_time, stage_end_time
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
                StartTime = reader.GetDateTime(5),
                EndTime = reader.GetDateTime(6),
                Status = reader.GetString(7),
                RegistrationCount = reader.GetInt32(8),
                AdmissionTicketCount = reader.GetInt32(9)
            });
        }

        return records;
    }

    private static void AddTimeRangeCondition(
        List<string> conditions,
        SqlCommand command,
        ref int paramIndex,
        DateTime rangeStart,
        DateTime rangeEnd,
        string timeMatchMode,
        string prefix)
    {
        var pStart = $"@{prefix}s{paramIndex}";
        var pEnd = $"@{prefix}e{paramIndex}";
        paramIndex++;
        command.Parameters.AddWithValue(pStart, rangeStart);
        command.Parameters.AddWithValue(pEnd, rangeEnd);

        switch (timeMatchMode)
        {
            case "start":
                conditions.Add($"stage_start_time >= {pStart} AND stage_start_time <= {pEnd}");
                break;
            case "end":
                conditions.Add($"stage_end_time >= {pStart} AND stage_end_time <= {pEnd}");
                break;
            default:
                conditions.Add($"stage_end_time >= {pStart} AND stage_start_time <= {pEnd}");
                break;
        }
    }

    private static SqlConnection OpenConnection(SummaryStoreConfig config)
    {
        if (string.IsNullOrWhiteSpace(config.Host) ||
            string.IsNullOrWhiteSpace(config.DatabaseName) ||
            string.IsNullOrWhiteSpace(config.Username) ||
            string.IsNullOrWhiteSpace(config.Password))
        {
            throw new InvalidOperationException("请先完成中心库配置。");
        }

        var builder = new SqlConnectionStringBuilder
        {
            DataSource = $"{config.Host},{config.Port}",
            InitialCatalog = config.DatabaseName,
            UserID = config.Username,
            Password = config.Password,
            TrustServerCertificate = true,
            Encrypt = false,
            ConnectTimeout = 20
        };

        return new SqlConnection(builder.ConnectionString);
    }

    private static string ResolveDatabaseType(ProjectStageSummary summary, string serverName)
    {
        return summary.Records
            .FirstOrDefault(item => string.Equals(item.ServerName, serverName, StringComparison.OrdinalIgnoreCase))
            is not null
            ? "Mixed"
            : "Unknown";
    }

}

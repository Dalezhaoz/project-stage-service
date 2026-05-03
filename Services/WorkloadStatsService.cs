using Microsoft.Data.SqlClient;
using ProjectStageService.Models;

namespace ProjectStageService.Services;

public sealed class WorkloadStatsService
{
    private const string StageSummaryTableName = "project_stage_summary";
    private const string MetadataTableName = "project_metadata";
    private const string ConfigTableName = "stage_workload_config";
    private readonly SummaryStoreConfigStore _configStore;

    public WorkloadStatsService(SummaryStoreConfigStore configStore)
    {
        _configStore = configStore;
    }

    public async Task<List<StageWorkloadConfigRecord>> GetStageConfigsAsync(WorkloadStatsRequest request, CancellationToken cancellationToken)
    {
        var config = await LoadEnabledConfigAsync(cancellationToken);
        await using var connection = OpenConnection(config);
        await connection.OpenAsync(cancellationToken);
        await EnsureSchemaAsync(connection, cancellationToken);

        await using var command = connection.CreateCommand();
        var conditions = BuildDateConditions(command, request);
        AddMaintainerCondition(command, conditions, request);
        var whereClause = conditions.Count > 0
            ? $"WHERE LTRIM(RTRIM(stage_name)) <> '' AND {string.Join(" AND ", conditions)}"
            : "WHERE LTRIM(RTRIM(stage_name)) <> ''";
        command.CommandText = $"""
            SELECT s.project_name,
                   s.stage_name,
                   CAST(ISNULL(c.hours, 0) AS DECIMAL(10,2)) AS hours,
                   c.updated_at
            FROM (
                SELECT DISTINCT s.project_name, s.stage_name
                FROM dbo.{StageSummaryTableName} s
                LEFT JOIN dbo.{MetadataTableName} m
                  ON m.server_name = s.source_server_name
                 AND m.database_name = s.source_database_name
                 AND m.exam_code = s.exam_code
                {whereClause}
            ) s
            LEFT JOIN dbo.{ConfigTableName} c ON c.stage_name = s.stage_name
            ORDER BY s.project_name, s.stage_name;
            """;

        var items = new List<StageWorkloadConfigRecord>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new StageWorkloadConfigRecord
            {
                ProjectName = reader.GetString(0),
                StageName = reader.GetString(1),
                Hours = reader.GetDecimal(2),
                UpdatedAt = reader.IsDBNull(3) ? null : reader.GetDateTime(3)
            });
        }

        return items;
    }

    public async Task SaveStageConfigsAsync(List<StageWorkloadConfigRecord> items, CancellationToken cancellationToken)
    {
        var config = await LoadEnabledConfigAsync(cancellationToken);
        var normalized = (items ?? [])
            .Select(item => new StageWorkloadConfigRecord
            {
                StageName = item.StageName?.Trim() ?? "",
                Hours = item.Hours < 0 ? 0 : Math.Round(item.Hours, 2)
            })
            .Where(item => !string.IsNullOrWhiteSpace(item.StageName))
            .GroupBy(item => item.StageName, StringComparer.OrdinalIgnoreCase)
            .Select(group => group.Last())
            .ToList();

        await using var connection = OpenConnection(config);
        await connection.OpenAsync(cancellationToken);
        await EnsureSchemaAsync(connection, cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        try
        {
            foreach (var item in normalized)
            {
                await using var command = connection.CreateCommand();
                command.Transaction = (SqlTransaction)transaction;
                command.CommandText = $"""
                    MERGE dbo.{ConfigTableName} AS target
                    USING (SELECT @stage_name AS stage_name) AS source
                    ON target.stage_name = source.stage_name
                    WHEN MATCHED THEN
                        UPDATE SET hours = @hours, updated_at = GETDATE()
                    WHEN NOT MATCHED THEN
                        INSERT (stage_name, hours, updated_at)
                        VALUES (@stage_name, @hours, GETDATE());
                    """;
                command.Parameters.AddWithValue("@stage_name", item.StageName);
                command.Parameters.AddWithValue("@hours", item.Hours);
                await command.ExecuteNonQueryAsync(cancellationToken);
            }

            await transaction.CommitAsync(cancellationToken);
        }
        catch
        {
            await transaction.RollbackAsync(cancellationToken);
            throw;
        }
    }

    public async Task<WorkloadStatsResponse> GetStatsAsync(WorkloadStatsRequest request, CancellationToken cancellationToken)
    {
        var config = await LoadEnabledConfigAsync(cancellationToken);
        await using var connection = OpenConnection(config);
        await connection.OpenAsync(cancellationToken);
        await EnsureSchemaAsync(connection, cancellationToken);

        var rows = await LoadWorkloadRowsAsync(connection, request, cancellationToken);
        var totalHours = rows.Sum(item => item.Hours);
        var totalStages = rows.Count;
        var maintainer = request.Maintainer?.Trim() ?? "";
        var scopedRows = string.IsNullOrWhiteSpace(maintainer)
            ? rows
            : rows.Where(item => string.Equals(item.Maintainer, maintainer, StringComparison.OrdinalIgnoreCase)).ToList();

        return new WorkloadStatsResponse
        {
            TotalHours = Round(totalHours),
            TotalStages = totalStages,
            People = rows
                .GroupBy(item => item.Maintainer, StringComparer.OrdinalIgnoreCase)
                .Select(group => new WorkloadPersonSummary
                {
                    Maintainer = group.Key,
                    Hours = Round(group.Sum(item => item.Hours)),
                    StageCount = group.Count(),
                    Percent = totalHours <= 0 ? 0 : Round(group.Sum(item => item.Hours) * 100 / totalHours)
                })
                .OrderByDescending(item => item.Hours)
                .ThenBy(item => item.Maintainer, StringComparer.OrdinalIgnoreCase)
                .ToList(),
            Periods = scopedRows
                .GroupBy(item => new { item.PeriodKey, item.PeriodLabel })
                .Select(group => new WorkloadPeriodSummary
                {
                    PeriodKey = group.Key.PeriodKey,
                    PeriodLabel = group.Key.PeriodLabel,
                    Hours = Round(group.Sum(item => item.Hours)),
                    StageCount = group.Count()
                })
                .OrderBy(item => item.PeriodKey, StringComparer.Ordinal)
                .ToList(),
            Stages = scopedRows
                .GroupBy(item => new { item.ProjectName, item.StageName, item.ConfiguredHours })
                .Select(group => new WorkloadStageSummary
                {
                    ProjectName = group.Key.ProjectName,
                    StageName = group.Key.StageName,
                    ConfiguredHours = Round(group.Key.ConfiguredHours),
                    StageCount = group.Count(),
                    Hours = Round(group.Sum(item => item.Hours))
                })
                .OrderByDescending(item => item.Hours)
                .ThenBy(item => item.StageName, StringComparer.OrdinalIgnoreCase)
                .ToList()
        };
    }

    private static async Task<List<WorkloadRow>> LoadWorkloadRowsAsync(
        SqlConnection connection,
        WorkloadStatsRequest request,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        var conditions = BuildDateConditions(command, request);
        var whereClause = conditions.Count > 0 ? $"WHERE {string.Join(" AND ", conditions)}" : "";
        command.CommandText = $"""
            SELECT ISNULL(NULLIF(LTRIM(RTRIM(m.maintainer)), ''), N'未分配') AS maintainer,
                   s.project_name,
                   s.stage_name,
                   s.stage_start_time,
                   CAST(ISNULL(c.hours, 0) AS DECIMAL(10,2)) AS configured_hours
            FROM dbo.{StageSummaryTableName} s
            LEFT JOIN dbo.{MetadataTableName} m
              ON m.server_name = s.source_server_name
             AND m.database_name = s.source_database_name
             AND m.exam_code = s.exam_code
            LEFT JOIN dbo.{ConfigTableName} c
              ON c.stage_name = s.stage_name
            {whereClause};
            """;

        var granularity = NormalizeGranularity(request.Granularity);
        var rows = new List<WorkloadRow>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var stageStart = reader.GetDateTime(3);
            var configuredHours = reader.GetDecimal(4);
            var (periodKey, periodLabel) = BuildPeriod(stageStart, granularity);
            rows.Add(new WorkloadRow(
                reader.GetString(0),
                reader.GetString(1),
                reader.GetString(2),
                configuredHours,
                configuredHours,
                periodKey,
                periodLabel));
        }

        return rows;
    }

    private static List<string> BuildDateConditions(SqlCommand command, WorkloadStatsRequest request)
    {
        var conditions = new List<string>();
        var rangeStart = request.RangeStart?.Date;
        var rangeEnd = request.RangeEnd?.Date.AddDays(1);

        if (rangeStart.HasValue)
        {
            conditions.Add("s.stage_start_time >= @range_start");
            command.Parameters.AddWithValue("@range_start", rangeStart.Value);
        }

        if (rangeEnd.HasValue)
        {
            conditions.Add("s.stage_start_time < @range_end");
            command.Parameters.AddWithValue("@range_end", rangeEnd.Value);
        }

        return conditions;
    }

    private static void AddMaintainerCondition(SqlCommand command, List<string> conditions, WorkloadStatsRequest request)
    {
        var maintainer = request.Maintainer?.Trim() ?? "";
        if (string.IsNullOrWhiteSpace(maintainer))
            return;

        conditions.Add("ISNULL(NULLIF(LTRIM(RTRIM(m.maintainer)), ''), N'未分配') = @maintainer");
        command.Parameters.AddWithValue("@maintainer", maintainer);
    }

    private async Task<SummaryStoreConfig> LoadEnabledConfigAsync(CancellationToken cancellationToken)
    {
        var config = await _configStore.LoadAsync(cancellationToken);
        if (!config.Enabled)
            throw new InvalidOperationException("请先启用中心库。");
        return config;
    }

    private static async Task EnsureSchemaAsync(SqlConnection connection, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = $"""
            IF OBJECT_ID(N'dbo.{ConfigTableName}', N'U') IS NULL
            BEGIN
                CREATE TABLE dbo.{ConfigTableName} (
                    stage_name NVARCHAR(200) NOT NULL,
                    hours DECIMAL(10, 2) NOT NULL DEFAULT 0,
                    updated_at DATETIME NOT NULL DEFAULT GETDATE(),
                    CONSTRAINT PK_{ConfigTableName} PRIMARY KEY (stage_name)
                );
            END;

            IF OBJECT_ID(N'dbo.{MetadataTableName}', N'U') IS NULL
            BEGIN
                CREATE TABLE dbo.{MetadataTableName} (
                    server_name NVARCHAR(100) NOT NULL,
                    database_name NVARCHAR(200) NOT NULL DEFAULT '',
                    exam_code NVARCHAR(50) NOT NULL,
                    maintainer NVARCHAR(100) NOT NULL DEFAULT '',
                    app_servers NVARCHAR(500) NOT NULL DEFAULT '',
                    allow_others_view BIT NOT NULL DEFAULT 1,
                    updated_at DATETIME NOT NULL DEFAULT GETDATE(),
                    CONSTRAINT PK_{MetadataTableName} PRIMARY KEY (server_name, database_name, exam_code)
                );
            END;
            """;
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static (string Key, string Label) BuildPeriod(DateTime value, string granularity)
    {
        return granularity switch
        {
            "day" => (value.ToString("yyyy-MM-dd"), value.ToString("yyyy-MM-dd")),
            "week" => BuildWeekPeriod(value),
            "year" => (value.ToString("yyyy"), $"{value:yyyy}年"),
            _ => (value.ToString("yyyy-MM"), $"{value:yyyy-MM}")
        };
    }

    private static (string Key, string Label) BuildWeekPeriod(DateTime value)
    {
        var dayOffset = ((int)value.DayOfWeek + 6) % 7;
        var monday = value.Date.AddDays(-dayOffset);
        return ($"{monday:yyyy-MM-dd}", $"{monday:yyyy-MM-dd}周");
    }

    private static string NormalizeGranularity(string? value)
    {
        value = value?.Trim().ToLowerInvariant();
        return value is "day" or "week" or "month" or "year" ? value : "month";
    }

    private static decimal Round(decimal value) => Math.Round(value, 2);

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

    private sealed record WorkloadRow(
        string Maintainer,
        string ProjectName,
        string StageName,
        decimal ConfiguredHours,
        decimal Hours,
        string PeriodKey,
        string PeriodLabel);
}

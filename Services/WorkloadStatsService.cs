using ClosedXML.Excel;
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
                   CAST(ISNULL(c.complexity, 1) AS DECIMAL(10,2)) AS complexity,
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
            LEFT JOIN dbo.{ConfigTableName} c
              ON c.project_name = s.project_name
             AND c.stage_name = s.stage_name
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
                Complexity = reader.GetDecimal(3),
                UpdatedAt = reader.IsDBNull(4) ? null : reader.GetDateTime(4)
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
                ProjectName = item.ProjectName?.Trim() ?? "",
                StageName = item.StageName?.Trim() ?? "",
                Hours = item.Hours < 0 ? 0 : Math.Round(item.Hours, 2),
                Complexity = item.Complexity < 0 ? 0 : Math.Round(item.Complexity, 2)
            })
            .Where(item => !string.IsNullOrWhiteSpace(item.ProjectName) && !string.IsNullOrWhiteSpace(item.StageName))
            .GroupBy(item => new { ProjectName = item.ProjectName.ToLowerInvariant(), StageName = item.StageName.ToLowerInvariant() })
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
                    USING (SELECT @project_name AS project_name, @stage_name AS stage_name) AS source
                    ON target.project_name = source.project_name
                    AND target.stage_name = source.stage_name
                    WHEN MATCHED THEN
                        UPDATE SET hours = @hours, complexity = @complexity, updated_at = GETDATE()
                    WHEN NOT MATCHED THEN
                        INSERT (project_name, stage_name, hours, complexity, updated_at)
                        VALUES (@project_name, @stage_name, @hours, @complexity, GETDATE());
                    """;
                command.Parameters.AddWithValue("@project_name", item.ProjectName);
                command.Parameters.AddWithValue("@stage_name", item.StageName);
                command.Parameters.AddWithValue("@hours", item.Hours);
                command.Parameters.AddWithValue("@complexity", item.Complexity);
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
                .GroupBy(item => new { item.ProjectName, item.StageName, item.ConfiguredHours, item.Complexity })
                .Select(group => new WorkloadStageSummary
                {
                    ProjectName = group.Key.ProjectName,
                    StageName = group.Key.StageName,
                    ConfiguredHours = Round(group.Key.ConfiguredHours),
                    Complexity = Round(group.Key.Complexity),
                    StageCount = group.Count(),
                    Hours = Round(group.Sum(item => item.Hours))
                })
                .OrderByDescending(item => item.Hours)
                .ThenBy(item => item.StageName, StringComparer.OrdinalIgnoreCase)
                .ToList()
        };
    }

    public async Task<byte[]> ExportAsync(WorkloadStatsRequest request, CancellationToken cancellationToken)
    {
        var stats = await GetStatsAsync(request, cancellationToken);
        var configs = await GetStageConfigsAsync(request, cancellationToken);
        configs = FilterStageConfigs(configs, request);
        var stageStats = stats.Stages.ToDictionary(
            item => BuildStageKey(item.ProjectName, item.StageName),
            item => item,
            StringComparer.OrdinalIgnoreCase);

        using var workbook = new XLWorkbook();
        AddPeopleSheet(workbook, stats);
        AddPeriodSheet(workbook, stats);
        AddStageConfigSheet(workbook, configs, stageStats);

        using var stream = new MemoryStream();
        workbook.SaveAs(stream);
        return stream.ToArray();
    }

    private static List<StageWorkloadConfigRecord> FilterStageConfigs(List<StageWorkloadConfigRecord> configs, WorkloadStatsRequest request)
    {
        var keyword = request.ConfigKeyword?.Trim() ?? "";
        var status = (request.ConfigStatus?.Trim() ?? "all").ToLowerInvariant();

        return configs
            .Where(item => string.IsNullOrWhiteSpace(keyword) ||
                (item.ProjectName?.Contains(keyword, StringComparison.OrdinalIgnoreCase) ?? false) ||
                (item.StageName?.Contains(keyword, StringComparison.OrdinalIgnoreCase) ?? false))
            .Where(item =>
            {
                var configured = item.UpdatedAt.HasValue;
                return status switch
                {
                    "review" => NeedsReview(item),
                    "unset" => !configured,
                    "set" => configured,
                    _ => true
                };
            })
            .ToList();
    }

    private static bool NeedsReview(StageWorkloadConfigRecord item) =>
        !item.UpdatedAt.HasValue ||
        item.Hours <= 0 ||
        item.Complexity < 0.5m ||
        item.Complexity > 2m;

    private static void AddPeopleSheet(XLWorkbook workbook, WorkloadStatsResponse stats)
    {
        var sheet = workbook.Worksheets.Add("人员汇总");
        WriteHeader(sheet, ["负责人", "工时", "阶段次数", "占比"]);
        var row = 2;
        foreach (var item in stats.People)
        {
            sheet.Cell(row, 1).Value = item.Maintainer;
            sheet.Cell(row, 2).Value = item.Hours;
            sheet.Cell(row, 3).Value = item.StageCount;
            sheet.Cell(row, 4).Value = $"{item.Percent}%";
            row++;
        }
        FormatSheet(sheet);
    }

    private static void AddPeriodSheet(XLWorkbook workbook, WorkloadStatsResponse stats)
    {
        var sheet = workbook.Worksheets.Add("周期汇总");
        WriteHeader(sheet, ["周期", "工时", "阶段次数"]);
        var row = 2;
        foreach (var item in stats.Periods)
        {
            sheet.Cell(row, 1).Value = item.PeriodLabel;
            sheet.Cell(row, 2).Value = item.Hours;
            sheet.Cell(row, 3).Value = item.StageCount;
            row++;
        }
        FormatSheet(sheet);
    }

    private static void AddStageConfigSheet(
        XLWorkbook workbook,
        List<StageWorkloadConfigRecord> configs,
        Dictionary<string, WorkloadStageSummary> stageStats)
    {
        var sheet = workbook.Worksheets.Add("阶段工时配置");
        WriteHeader(sheet, ["项目名称", "阶段名称", "基础单次工时", "复杂度", "折算单次工时", "出现次数", "合计工时", "工时状态", "核对建议", "更新时间"]);
        var row = 2;
        foreach (var item in configs)
        {
            stageStats.TryGetValue(BuildStageKey(item.ProjectName, item.StageName), out var stat);
            sheet.Cell(row, 1).Value = item.ProjectName;
            sheet.Cell(row, 2).Value = item.StageName;
            sheet.Cell(row, 3).Value = item.Hours;
            sheet.Cell(row, 4).Value = item.Complexity;
            sheet.Cell(row, 5).Value = Round(item.Hours * item.Complexity);
            sheet.Cell(row, 6).Value = stat?.StageCount ?? 0;
            sheet.Cell(row, 7).Value = stat?.Hours ?? 0;
            sheet.Cell(row, 8).Value = item.UpdatedAt.HasValue ? "已设置" : "未设置";
            sheet.Cell(row, 9).Value = BuildReviewHint(item);
            if (item.UpdatedAt.HasValue)
                sheet.Cell(row, 10).Value = item.UpdatedAt.Value;
            row++;
        }
        FormatSheet(sheet);
    }

    private static string BuildReviewHint(StageWorkloadConfigRecord item)
    {
        var hints = new List<string>();
        if (!item.UpdatedAt.HasValue) hints.Add("未设置工时");
        if (item.Hours <= 0) hints.Add("工时为0");
        if (item.Complexity < 0.5m) hints.Add("复杂度偏低");
        if (item.Complexity > 2m) hints.Add("复杂度偏高");
        return hints.Count == 0 ? "正常" : string.Join("、", hints);
    }

    private static void WriteHeader(IXLWorksheet sheet, string[] headers)
    {
        for (var index = 0; index < headers.Length; index++)
            sheet.Cell(1, index + 1).Value = headers[index];

        var header = sheet.Range(1, 1, 1, headers.Length);
        header.Style.Font.Bold = true;
        header.Style.Fill.BackgroundColor = XLColor.FromHtml("#EDF2FF");
        header.Style.Font.FontColor = XLColor.FromHtml("#1A1D26");
    }

    private static void FormatSheet(IXLWorksheet sheet)
    {
        sheet.SheetView.FreezeRows(1);
        sheet.Columns().AdjustToContents();
    }

    private static string BuildStageKey(string? projectName, string? stageName) =>
        $"{projectName ?? ""}||{stageName ?? ""}";

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
                   CAST(ISNULL(c.hours, 0) AS DECIMAL(10,2)) AS configured_hours,
                   CAST(ISNULL(c.complexity, 1) AS DECIMAL(10,2)) AS complexity
            FROM dbo.{StageSummaryTableName} s
            LEFT JOIN dbo.{MetadataTableName} m
              ON m.server_name = s.source_server_name
             AND m.database_name = s.source_database_name
             AND m.exam_code = s.exam_code
            LEFT JOIN dbo.{ConfigTableName} c
              ON c.project_name = s.project_name
             AND c.stage_name = s.stage_name
            {whereClause};
            """;

        var granularity = NormalizeGranularity(request.Granularity);
        var rows = new List<WorkloadRow>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var stageStart = reader.GetDateTime(3);
            var configuredHours = reader.GetDecimal(4);
            var complexity = reader.GetDecimal(5);
            var (periodKey, periodLabel) = BuildPeriod(stageStart, granularity);
            rows.Add(new WorkloadRow(
                reader.GetString(0),
                reader.GetString(1),
                reader.GetString(2),
                configuredHours,
                complexity,
                configuredHours * complexity,
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
                    project_name NVARCHAR(500) NOT NULL,
                    stage_name NVARCHAR(200) NOT NULL,
                    hours DECIMAL(10, 2) NOT NULL DEFAULT 0,
                    complexity DECIMAL(10, 2) NOT NULL DEFAULT 1,
                    updated_at DATETIME NOT NULL DEFAULT GETDATE(),
                    CONSTRAINT PK_{ConfigTableName} PRIMARY KEY (project_name, stage_name)
                );
            END;

            IF COL_LENGTH(N'dbo.{ConfigTableName}', N'project_name') IS NULL
            BEGIN
                ALTER TABLE dbo.{ConfigTableName}
                ADD project_name NVARCHAR(500) NOT NULL CONSTRAINT DF_{ConfigTableName}_project_name DEFAULT N'';
            END;

            IF COL_LENGTH(N'dbo.{ConfigTableName}', N'complexity') IS NULL
            BEGIN
                ALTER TABLE dbo.{ConfigTableName}
                ADD complexity DECIMAL(10, 2) NOT NULL CONSTRAINT DF_{ConfigTableName}_complexity DEFAULT 1;
            END;

            DECLARE @workloadHasCompositePk BIT = 0;

            IF EXISTS (
                SELECT 1
                FROM sys.key_constraints
                WHERE parent_object_id = OBJECT_ID(N'dbo.{ConfigTableName}')
                  AND [type] = N'PK'
            )
            BEGIN
                SELECT @workloadHasCompositePk =
                    CASE WHEN COUNT(*) = 2
                           AND SUM(CASE WHEN c.name = N'project_name' THEN 1 ELSE 0 END) = 1
                           AND SUM(CASE WHEN c.name = N'stage_name' THEN 1 ELSE 0 END) = 1
                         THEN 1 ELSE 0 END
                FROM sys.index_columns ic
                INNER JOIN sys.columns c
                  ON c.object_id = ic.object_id
                 AND c.column_id = ic.column_id
                INNER JOIN sys.key_constraints kc
                  ON kc.parent_object_id = ic.object_id
                 AND kc.unique_index_id = ic.index_id
                WHERE kc.parent_object_id = OBJECT_ID(N'dbo.{ConfigTableName}')
                  AND kc.[type] = N'PK';

                IF @workloadHasCompositePk = 0
                   AND EXISTS (
                       SELECT 1
                       FROM sys.key_constraints
                       WHERE parent_object_id = OBJECT_ID(N'dbo.{ConfigTableName}')
                         AND [type] = N'PK'
                         AND name = N'PK_{ConfigTableName}'
                   )
                BEGIN
                    ALTER TABLE dbo.{ConfigTableName} DROP CONSTRAINT PK_{ConfigTableName};
                END;
            END;

            IF NOT EXISTS (
                SELECT 1
                FROM sys.key_constraints
                WHERE parent_object_id = OBJECT_ID(N'dbo.{ConfigTableName}')
                  AND [type] = N'PK'
            )
            BEGIN
                ALTER TABLE dbo.{ConfigTableName}
                ADD CONSTRAINT PK_{ConfigTableName} PRIMARY KEY (project_name, stage_name);
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
        decimal Complexity,
        decimal Hours,
        string PeriodKey,
        string PeriodLabel);
}

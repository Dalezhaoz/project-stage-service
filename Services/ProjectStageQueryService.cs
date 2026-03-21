using Microsoft.Data.SqlClient;
using MySqlConnector;
using ProjectStageService.Models;

namespace ProjectStageService.Services;

public sealed class ProjectStageQueryService
{
    public async Task<object> TestConnectionAsync(StageServerConfig server, CancellationToken cancellationToken)
    {
        var databaseNames = await LoadDatabaseNamesAsync(server, cancellationToken);
        return new
        {
            visited_databases = databaseNames.Count,
            matched_databases = 0
        };
    }

    public async Task<List<string>> QueryStageNamesAsync(List<StageServerConfig> servers, CancellationToken cancellationToken)
    {
        var enabledServers = servers.Where(item => item.Enabled).ToList();
        var stageNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var server in enabledServers)
        {
            var databaseNames = await LoadDatabaseNamesAsync(server, cancellationToken);
            foreach (var databaseName in databaseNames)
            {
                if (!await HasBusinessTablesAsync(server, databaseName, cancellationToken))
                {
                    continue;
                }

                foreach (var stageName in await LoadStageNamesAsync(server, databaseName, cancellationToken))
                {
                    stageNames.Add(stageName);
                }
            }
        }

        return stageNames.OrderBy(item => item, StringComparer.OrdinalIgnoreCase).ToList();
    }

    public async Task<ProjectStageSummary> QueryAsync(ProjectStageQueryRequest request, CancellationToken cancellationToken)
    {
        var enabledServers = request.Servers.Where(item => item.Enabled).ToList();
        if (enabledServers.Count == 0)
        {
            throw new InvalidOperationException("请至少启用一台服务器。");
        }

        var now = DateTime.Now;
        var summary = new ProjectStageSummary
        {
            EnabledServers = enabledServers.Count
        };

        foreach (var server in enabledServers)
        {
            var databaseNames = await LoadDatabaseNamesAsync(server, cancellationToken);
            summary.VisitedDatabases += databaseNames.Count;

            foreach (var databaseName in databaseNames)
            {
                if (!await HasBusinessTablesAsync(server, databaseName, cancellationToken))
                {
                    continue;
                }

                summary.MatchedDatabases += 1;
                var records = await QueryDatabaseAsync(server, databaseName, now, request, cancellationToken);
                summary.Records.AddRange(records);
            }
        }

        summary.EndedCount = summary.Records.Count(item => item.Status == "已经结束");
        summary.OngoingCount = summary.Records.Count(item => item.Status == "正在进行");
        summary.UpcomingCount = summary.Records.Count(item => item.Status == "即将开始");
        return summary;
    }

    private static async Task<List<string>> LoadDatabaseNamesAsync(StageServerConfig server, CancellationToken cancellationToken)
    {
        return IsMySql(server)
            ? await LoadMySqlDatabaseNamesAsync(server, cancellationToken)
            : await LoadSqlServerDatabaseNamesAsync(server, cancellationToken);
    }

    private static async Task<List<string>> LoadSqlServerDatabaseNamesAsync(StageServerConfig server, CancellationToken cancellationToken)
    {
        await using var connection = new SqlConnection(BuildSqlServerConnectionString(server, "master"));
        await connection.OpenAsync(cancellationToken);

        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT name
            FROM sys.databases
            WHERE state_desc = 'ONLINE'
              AND name NOT IN ('master', 'model', 'msdb', 'tempdb')
            ORDER BY name
            """;

        var names = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            names.Add(reader.GetString(0));
        }

        return names;
    }

    private static async Task<List<string>> LoadMySqlDatabaseNamesAsync(StageServerConfig server, CancellationToken cancellationToken)
    {
        await using var connection = new MySqlConnection(BuildMySqlConnectionString(server, null));
        await connection.OpenAsync(cancellationToken);

        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
            ORDER BY schema_name
            """;

        var names = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            names.Add(reader.GetString(0));
        }

        return names;
    }

    private static async Task<bool> HasBusinessTablesAsync(StageServerConfig server, string databaseName, CancellationToken cancellationToken)
    {
        return IsMySql(server)
            ? await HasMySqlBusinessTablesAsync(server, databaseName, cancellationToken)
            : await HasSqlServerBusinessTablesAsync(server, databaseName, cancellationToken);
    }

    private static async Task<bool> HasSqlServerBusinessTablesAsync(StageServerConfig server, string databaseName, CancellationToken cancellationToken)
    {
        var escaped = EscapeDbName(databaseName);
        await using var connection = new SqlConnection(BuildSqlServerConnectionString(server, "master"));
        await connection.OpenAsync(cancellationToken);

        await using var command = connection.CreateCommand();
        command.CommandText = $"""
            SELECT COUNT(*)
            FROM [{escaped}].sys.tables
            WHERE name IN ('EI_ExamTreeDesc', 'web_SR_CodeItem', 'WEB_SR_SetTime')
            """;

        var result = await command.ExecuteScalarAsync(cancellationToken);
        return Convert.ToInt32(result) == 3;
    }

    private static async Task<bool> HasMySqlBusinessTablesAsync(StageServerConfig server, string databaseName, CancellationToken cancellationToken)
    {
        await using var connection = new MySqlConnection(BuildMySqlConnectionString(server, null));
        await connection.OpenAsync(cancellationToken);

        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = @schema
              AND table_name IN ('mgt_exam_organize', 'mgt_exam_step')
            """;
        command.Parameters.AddWithValue("@schema", databaseName);

        var result = await command.ExecuteScalarAsync(cancellationToken);
        return Convert.ToInt32(result) == 2;
    }

    private static async Task<List<string>> LoadStageNamesAsync(StageServerConfig server, string databaseName, CancellationToken cancellationToken)
    {
        return IsMySql(server)
            ? await LoadMySqlStageNamesAsync(server, databaseName, cancellationToken)
            : await LoadSqlServerStageNamesAsync(server, databaseName, cancellationToken);
    }

    private static async Task<List<string>> LoadSqlServerStageNamesAsync(StageServerConfig server, string databaseName, CancellationToken cancellationToken)
    {
        var escaped = EscapeDbName(databaseName);
        await using var connection = new SqlConnection(BuildSqlServerConnectionString(server, databaseName));
        await connection.OpenAsync(cancellationToken);

        await using var command = connection.CreateCommand();
        command.CommandText = $"""
            SELECT DISTINCT B.Description
            FROM [{escaped}].[dbo].[web_SR_CodeItem] B
            JOIN [{escaped}].[dbo].[WEB_SR_SetTime] C
                ON B.Codeid = 'WT'
               AND B.Code = C.Kind
            WHERE B.Description IS NOT NULL AND LTRIM(RTRIM(B.Description)) <> ''
            ORDER BY B.Description
            """;

        var names = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            names.Add(reader.GetString(0).Trim());
        }

        return names;
    }

    private static async Task<List<string>> LoadMySqlStageNamesAsync(StageServerConfig server, string databaseName, CancellationToken cancellationToken)
    {
        await using var connection = new MySqlConnection(BuildMySqlConnectionString(server, databaseName));
        await connection.OpenAsync(cancellationToken);

        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT DISTINCT b.name
            FROM mgt_exam_step b
            WHERE b.name IS NOT NULL AND TRIM(b.name) <> ''
            ORDER BY b.name
            """;

        var names = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            names.Add(reader.GetString(0).Trim());
        }

        return names;
    }

    private static async Task<List<ProjectStageRecord>> QueryDatabaseAsync(
        StageServerConfig server,
        string databaseName,
        DateTime now,
        ProjectStageQueryRequest request,
        CancellationToken cancellationToken)
    {
        return IsMySql(server)
            ? await QueryMySqlDatabaseAsync(server, databaseName, now, request, cancellationToken)
            : await QuerySqlServerDatabaseAsync(server, databaseName, now, request, cancellationToken);
    }

    private static async Task<List<ProjectStageRecord>> QuerySqlServerDatabaseAsync(
        StageServerConfig server,
        string databaseName,
        DateTime now,
        ProjectStageQueryRequest request,
        CancellationToken cancellationToken)
    {
        var escaped = EscapeDbName(databaseName);
        await using var connection = new SqlConnection(BuildSqlServerConnectionString(server, databaseName));
        await connection.OpenAsync(cancellationToken);

        await using var command = connection.CreateCommand();
        command.CommandText = $"""
            SELECT
                A.NAME,
                B.Description,
                C.KDate,
                C.ZDate
            FROM [{escaped}].[dbo].[EI_ExamTreeDesc] A
            JOIN [{escaped}].[dbo].[WEB_SR_SetTime] C
                ON A.Code = C.ExamSort
            JOIN [{escaped}].[dbo].[web_SR_CodeItem] B
                ON B.Codeid = 'WT'
               AND B.Code = C.Kind
            WHERE A.CodeLen = '2'
            ORDER BY C.KDate ASC
            """;

        return await ReadSqlServerRecordsAsync(command, server.Name, databaseName, now, request, cancellationToken);
    }

    private static async Task<List<ProjectStageRecord>> QueryMySqlDatabaseAsync(
        StageServerConfig server,
        string databaseName,
        DateTime now,
        ProjectStageQueryRequest request,
        CancellationToken cancellationToken)
    {
        await using var connection = new MySqlConnection(BuildMySqlConnectionString(server, databaseName));
        await connection.OpenAsync(cancellationToken);

        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT
                a.name,
                b.name,
                b.start_date,
                b.end_date
            FROM mgt_exam_organize a
            JOIN mgt_exam_step b
              ON 1 = 1
            ORDER BY b.start_date ASC
            """;

        return await ReadMySqlRecordsAsync(command, server.Name, databaseName, now, request, cancellationToken);
    }

    private static async Task<List<ProjectStageRecord>> ReadSqlServerRecordsAsync(
        SqlCommand command,
        string serverName,
        string databaseName,
        DateTime now,
        ProjectStageQueryRequest request,
        CancellationToken cancellationToken)
    {
        var records = new List<ProjectStageRecord>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var projectName = reader.IsDBNull(0) ? "" : reader.GetString(0).Trim();
            var stageName = reader.IsDBNull(1) ? "" : reader.GetString(1).Trim();
            if (string.IsNullOrWhiteSpace(projectName) || string.IsNullOrWhiteSpace(stageName))
            {
                continue;
            }

            var startTime = reader.GetDateTime(2);
            var endTime = reader.GetDateTime(3);
            var record = BuildRecord(serverName, databaseName, projectName, stageName, startTime, endTime, now);
            if (AllowRecord(record, request))
            {
                records.Add(record);
            }
        }

        return records;
    }

    private static async Task<List<ProjectStageRecord>> ReadMySqlRecordsAsync(
        MySqlCommand command,
        string serverName,
        string databaseName,
        DateTime now,
        ProjectStageQueryRequest request,
        CancellationToken cancellationToken)
    {
        var records = new List<ProjectStageRecord>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var projectName = reader.IsDBNull(0) ? "" : reader.GetString(0).Trim();
            var stageName = reader.IsDBNull(1) ? "" : reader.GetString(1).Trim();
            if (string.IsNullOrWhiteSpace(projectName) || string.IsNullOrWhiteSpace(stageName))
            {
                continue;
            }

            var startTime = reader.GetDateTime(2);
            var endTime = reader.GetDateTime(3);
            var record = BuildRecord(serverName, databaseName, projectName, stageName, startTime, endTime, now);
            if (AllowRecord(record, request))
            {
                records.Add(record);
            }
        }

        return records;
    }

    private static ProjectStageRecord BuildRecord(
        string serverName,
        string databaseName,
        string projectName,
        string stageName,
        DateTime startTime,
        DateTime endTime,
        DateTime now)
    {
        return new ProjectStageRecord
        {
            ServerName = serverName,
            DatabaseName = databaseName,
            ProjectName = projectName,
            StageName = stageName,
            StartTime = startTime,
            EndTime = endTime,
            Status = GetStatus(now, startTime, endTime)
        };
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

    private static string BuildSqlServerConnectionString(StageServerConfig server, string databaseName)
    {
        var builder = new SqlConnectionStringBuilder
        {
            DataSource = $"{server.Host},{server.Port}",
            InitialCatalog = databaseName,
            UserID = server.Username,
            Password = server.Password,
            TrustServerCertificate = true,
            Encrypt = false,
            ConnectTimeout = 8
        };
        return builder.ConnectionString;
    }

    private static string BuildMySqlConnectionString(StageServerConfig server, string? databaseName)
    {
        var builder = new MySqlConnectionStringBuilder
        {
            Server = server.Host,
            Port = (uint)server.Port,
            UserID = server.Username,
            Password = server.Password,
            Database = databaseName ?? string.Empty,
            ConnectionTimeout = 8,
            AllowUserVariables = true
        };
        return builder.ConnectionString;
    }

    private static bool IsMySql(StageServerConfig server) =>
        string.Equals(server.DatabaseType, "MySQL", StringComparison.OrdinalIgnoreCase);

    private static string EscapeDbName(string databaseName) => databaseName.Replace("]", "]]");

    private static string GetStatus(DateTime now, DateTime startTime, DateTime endTime)
    {
        if (now < startTime)
        {
            return "即将开始";
        }

        if (now > endTime)
        {
            return "已经结束";
        }

        return "正在进行";
    }

    private static bool ContainsIgnoreCase(string source, string keyword)
    {
        if (string.IsNullOrWhiteSpace(keyword))
        {
            return true;
        }

        return source.Contains(keyword.Trim(), StringComparison.OrdinalIgnoreCase);
    }
}

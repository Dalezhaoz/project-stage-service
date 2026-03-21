using Microsoft.Data.SqlClient;
using ProjectStageService.Models;
using System.Data;

namespace ProjectStageService.Services;

public sealed class ProjectStageQueryService
{
    public async Task<object> TestConnectionAsync(StageServerConfig server, CancellationToken cancellationToken)
    {
        await using var connection = new SqlConnection(BuildConnectionString(server, "master"));
        await connection.OpenAsync(cancellationToken);

        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT name
            FROM sys.databases
            WHERE state_desc = 'ONLINE'
              AND name NOT IN ('master', 'model', 'msdb', 'tempdb')
            """;

        var count = 0;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            count++;
        }

        return new
        {
            visited_databases = count,
            matched_databases = 0
        };
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
            await using var connection = new SqlConnection(BuildConnectionString(server, "master"));
            await connection.OpenAsync(cancellationToken);

            var databaseNames = await LoadDatabaseNamesAsync(connection, cancellationToken);
            summary.VisitedDatabases += databaseNames.Count;

            foreach (var databaseName in databaseNames)
            {
                if (!await HasBusinessTablesAsync(connection, databaseName, cancellationToken))
                {
                    continue;
                }

                summary.MatchedDatabases += 1;
                var records = await QueryDatabaseAsync(
                    connection,
                    server.Name,
                    databaseName,
                    now,
                    request.StatusFilter,
                    request.StageKeyword,
                    request.ProjectKeyword,
                    cancellationToken);

                summary.Records.AddRange(records);
            }
        }

        summary.OngoingCount = summary.Records.Count(item => item.Status == "正在进行");
        summary.UpcomingCount = summary.Records.Count(item => item.Status == "即将开始");
        return summary;
    }

    private static string BuildConnectionString(StageServerConfig server, string databaseName)
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

    private static async Task<List<string>> LoadDatabaseNamesAsync(SqlConnection connection, CancellationToken cancellationToken)
    {
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

    private static async Task<bool> HasBusinessTablesAsync(SqlConnection connection, string databaseName, CancellationToken cancellationToken)
    {
        var escaped = EscapeDbName(databaseName);
        await using var command = connection.CreateCommand();
        command.CommandText = $"""
            SELECT COUNT(*)
            FROM [{escaped}].sys.tables
            WHERE name IN ('EI_ExamTreeDesc', 'web_SR_CodeItem', 'WEB_SR_SetTime')
            """;

        var result = await command.ExecuteScalarAsync(cancellationToken);
        return Convert.ToInt32(result) == 3;
    }

    private static async Task<List<ProjectStageRecord>> QueryDatabaseAsync(
        SqlConnection connection,
        string serverName,
        string databaseName,
        DateTime now,
        string statusFilter,
        string stageKeyword,
        string projectKeyword,
        CancellationToken cancellationToken)
    {
        var escaped = EscapeDbName(databaseName);
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
            if (!ContainsIgnoreCase(stageName, stageKeyword) || !ContainsIgnoreCase(projectName, projectKeyword))
            {
                continue;
            }

            var status = GetStatus(now, startTime, endTime);
            if (!AllowStatus(statusFilter, status))
            {
                continue;
            }

            records.Add(new ProjectStageRecord
            {
                ServerName = serverName,
                DatabaseName = databaseName,
                ProjectName = projectName,
                StageName = stageName,
                StartTime = startTime,
                EndTime = endTime,
                Status = status
            });
        }

        return records;
    }

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

    private static bool AllowStatus(string filter, string status)
    {
        return filter switch
        {
            "全部" => true,
            "只看正在进行" => status == "正在进行",
            "只看即将开始" => status == "即将开始",
            _ => status == "正在进行" || status == "即将开始",
        };
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

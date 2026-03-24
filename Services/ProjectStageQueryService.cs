using Microsoft.Data.SqlClient;
using MySqlConnector;
using ProjectStageService.Models;

namespace ProjectStageService.Services;

public sealed class ProjectStageQueryService
{
    public async Task<List<ServerCountUpdate>> QueryServerCountsAsync(
        StageServerConfig server,
        IReadOnlyCollection<BoardCountTarget> targets,
        bool includeRegistrationCount,
        bool includeAdmissionTicketCount,
        CancellationToken cancellationToken)
    {
        if (!server.Enabled || targets.Count == 0 || (!includeRegistrationCount && !includeAdmissionTicketCount))
        {
            return [];
        }

        var groupedTargets = targets
            .Where(item =>
                !string.IsNullOrWhiteSpace(item.DatabaseName) &&
                !string.IsNullOrWhiteSpace(item.ExamCode) &&
                string.Equals(item.ServerName, server.Name, StringComparison.OrdinalIgnoreCase))
            .GroupBy(item => item.DatabaseName, StringComparer.OrdinalIgnoreCase)
            .ToList();

        if (groupedTargets.Count == 0)
        {
            return [];
        }

        return IsMySql(server)
            ? await QueryMySqlServerCountsAsync(server, groupedTargets, includeRegistrationCount, includeAdmissionTicketCount, cancellationToken)
            : await QuerySqlServerServerCountsAsync(server, groupedTargets, includeRegistrationCount, includeAdmissionTicketCount, cancellationToken);
    }

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

    public async Task<List<string>> QueryStageNamesAsync(ProjectStageQueryRequest request, CancellationToken cancellationToken)
    {
        var enabledServers = request.Servers.Where(item => item.Enabled).ToList();
        var stageNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var stageFilterRequest = new ProjectStageQueryRequest
        {
            Servers = request.Servers,
            StatusFilters = request.StatusFilters,
            ProjectKeyword = request.ProjectKeyword,
            ServerKeyword = request.ServerKeyword,
            DatabaseKeyword = request.DatabaseKeyword,
            ExamCodeKeyword = request.ExamCodeKeyword,
            RangeStart = request.RangeStart,
            RangeEnd = request.RangeEnd,
            DayOffsets = request.DayOffsets,
            TimeMatchMode = request.TimeMatchMode
        };

        foreach (var server in enabledServers)
        {
            var databaseNames = await LoadDatabaseNamesAsync(server, cancellationToken);
            foreach (var databaseName in databaseNames)
            {
                if (!await HasBusinessTablesAsync(server, databaseName, cancellationToken))
                {
                    continue;
                }

                var records = await QueryDatabaseAsync(
                    server,
                    databaseName,
                    DateTime.Now,
                    stageFilterRequest,
                    includeRegistrationCount: false,
                    includeAdmissionTicketCount: false,
                    targets: null,
                    cancellationToken: cancellationToken);
                foreach (var stageName in records.Select(item => item.StageName))
                {
                    if (!string.IsNullOrWhiteSpace(stageName))
                    {
                        stageNames.Add(stageName);
                    }
                }
            }
        }

        return stageNames.OrderBy(item => item, StringComparer.OrdinalIgnoreCase).ToList();
    }

    public Task<ProjectStageSummary> QueryAsync(ProjectStageQueryRequest request, CancellationToken cancellationToken) =>
        QueryAsync(request, includeRegistrationCount: false, includeAdmissionTicketCount: false, cancellationToken: cancellationToken);

    public async Task<ProjectStageSummary> QueryAsync(
        ProjectStageQueryRequest request,
        bool includeRegistrationCount,
        bool includeAdmissionTicketCount,
        CancellationToken cancellationToken)
    {
        return await QueryAsync(request, includeRegistrationCount, includeAdmissionTicketCount, null, cancellationToken);
    }

    public async Task<ProjectStageSummary> QueryAsync(
        ProjectStageQueryRequest request,
        bool includeRegistrationCount,
        bool includeAdmissionTicketCount,
        IReadOnlyCollection<BoardCountTarget>? targets,
        CancellationToken cancellationToken)
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

        // 并行处理所有服务器
        var serverTasks = enabledServers.Select(server =>
            ProcessServerAsync(server, now, request, includeRegistrationCount, includeAdmissionTicketCount, targets, cancellationToken));

        var serverResults = await Task.WhenAll(serverTasks);

        foreach (var result in serverResults)
        {
            summary.VisitedDatabases += result.VisitedDatabases;
            summary.MatchedDatabases += result.MatchedDatabases;
            summary.Records.AddRange(result.Records);
        }

        summary.EndedCount = summary.Records.Count(item => item.Status == "已经结束");
        summary.OngoingCount = summary.Records.Count(item => item.Status == "正在进行");
        summary.UpcomingCount = summary.Records.Count(item => item.Status == "即将开始");
        summary.Groups = summary.Records
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

    private static async Task<ServerQueryResult> ProcessServerAsync(
        StageServerConfig server,
        DateTime now,
        ProjectStageQueryRequest request,
        bool includeRegistrationCount,
        bool includeAdmissionTicketCount,
        IReadOnlyCollection<BoardCountTarget>? targets,
        CancellationToken cancellationToken)
    {
        var (visitedCount, matchingDatabases) = await LoadMatchingDatabasesAsync(server, cancellationToken);

        const int maxConcurrency = 8;
        using var semaphore = new SemaphoreSlim(maxConcurrency);

        var tasks = matchingDatabases.Select(async databaseName =>
        {
            await semaphore.WaitAsync(cancellationToken);
            try
            {
                return await QueryDatabaseAsync(
                    server, databaseName, now, request,
                    includeRegistrationCount, includeAdmissionTicketCount,
                    targets, cancellationToken);
            }
            finally
            {
                semaphore.Release();
            }
        });

        var results = await Task.WhenAll(tasks);
        return new ServerQueryResult(visitedCount, matchingDatabases.Count, results.SelectMany(r => r).ToList());
    }

    private static Task<(int VisitedCount, List<string> MatchingNames)> LoadMatchingDatabasesAsync(
        StageServerConfig server, CancellationToken cancellationToken)
    {
        return IsMySql(server)
            ? LoadMySqlMatchingDatabasesAsync(server, cancellationToken)
            : LoadSqlServerMatchingDatabasesAsync(server, cancellationToken);
    }

    private static async Task<(int, List<string>)> LoadMySqlMatchingDatabasesAsync(
        StageServerConfig server, CancellationToken cancellationToken)
    {
        await using var connection = new MySqlConnection(BuildMySqlConnectionString(server, null));
        await connection.OpenAsync(cancellationToken);

        // 获取所有库名（用于 VisitedDatabases 计数）
        await using var allCmd = connection.CreateCommand();
        allCmd.CommandText = """
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
            ORDER BY schema_name
            """;
        var allDbs = new List<string>();
        await using (var reader = await allCmd.ExecuteReaderAsync(cancellationToken))
            while (await reader.ReadAsync(cancellationToken))
                allDbs.Add(reader.GetString(0));

        if (allDbs.Count == 0) return (0, []);

        // 一次批量查询替代 N 次逐库检查
        await using var matchCmd = connection.CreateCommand();
        matchCmd.CommandText = """
            SELECT table_schema
            FROM information_schema.tables
            WHERE table_schema NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
              AND table_name IN ('mgt_exam_organize', 'mgt_exam_step')
            GROUP BY table_schema
            HAVING COUNT(DISTINCT table_name) = 2
            """;
        var matchingSet = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        await using (var reader = await matchCmd.ExecuteReaderAsync(cancellationToken))
            while (await reader.ReadAsync(cancellationToken))
                matchingSet.Add(reader.GetString(0));

        return (allDbs.Count, allDbs.Where(db => matchingSet.Contains(db)).ToList());
    }

    private static async Task<(int, List<string>)> LoadSqlServerMatchingDatabasesAsync(
        StageServerConfig server, CancellationToken cancellationToken)
    {
        await using var listConnection = new SqlConnection(BuildSqlServerConnectionString(server, "master"));
        await listConnection.OpenAsync(cancellationToken);

        await using var allCmd = listConnection.CreateCommand();
        allCmd.CommandText = """
            SELECT name
            FROM sys.databases
            WHERE state_desc = 'ONLINE'
              AND name NOT IN ('master', 'model', 'msdb', 'tempdb')
            ORDER BY name
            """;
        var allDbs = new List<string>();
        await using (var reader = await allCmd.ExecuteReaderAsync(cancellationToken))
            while (await reader.ReadAsync(cancellationToken))
                allDbs.Add(reader.GetString(0));

        if (allDbs.Count == 0) return (0, []);

        // 并发 15 同时检查各库是否包含业务表
        const int checkConcurrency = 15;
        using var semaphore = new SemaphoreSlim(checkConcurrency);

        var checkTasks = allDbs.Select(async dbName =>
        {
            await semaphore.WaitAsync(cancellationToken);
            try
            {
                var escaped = EscapeDbName(dbName);
                await using var conn = new SqlConnection(BuildSqlServerConnectionString(server, "master"));
                await conn.OpenAsync(cancellationToken);
                await using var cmd = conn.CreateCommand();
                cmd.CommandText = $"""
                    SELECT COUNT(*) FROM [{escaped}].sys.tables
                    WHERE name IN ('EI_ExamTreeDesc', 'web_SR_CodeItem', 'WEB_SR_SetTime')
                    """;
                var count = Convert.ToInt32(await cmd.ExecuteScalarAsync(cancellationToken));
                return count == 3 ? dbName : null;
            }
            finally
            {
                semaphore.Release();
            }
        });

        var checkResults = await Task.WhenAll(checkTasks);
        var matching = checkResults.Where(db => db is not null).Cast<string>().ToList();
        return (allDbs.Count, matching);
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
        bool includeRegistrationCount,
        bool includeAdmissionTicketCount,
        IReadOnlyCollection<BoardCountTarget>? targets,
        CancellationToken cancellationToken)
    {
        return IsMySql(server)
            ? await QueryMySqlDatabaseAsync(server, databaseName, now, request, includeRegistrationCount, includeAdmissionTicketCount, targets, cancellationToken)
            : await QuerySqlServerDatabaseAsync(server, databaseName, now, request, includeRegistrationCount, includeAdmissionTicketCount, targets, cancellationToken);
    }

    private static async Task<List<ProjectStageRecord>> QuerySqlServerDatabaseAsync(
        StageServerConfig server,
        string databaseName,
        DateTime now,
        ProjectStageQueryRequest request,
        bool includeRegistrationCount,
        bool includeAdmissionTicketCount,
        IReadOnlyCollection<BoardCountTarget>? targets,
        CancellationToken cancellationToken)
    {
        var escaped = EscapeDbName(databaseName);
        await using var connection = new SqlConnection(BuildSqlServerConnectionString(server, databaseName));
        await connection.OpenAsync(cancellationToken);

        await using var command = connection.CreateCommand();
        command.CommandText = $"""
            SELECT
                A.Code,
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
              AND C.Kind <> '06'
            ORDER BY C.KDate ASC
            """;

        var rows = await ReadSqlServerRowsAsync(command, cancellationToken);
        var filteredRows = rows
            .Where(item => MatchesRow(server.Name, databaseName, item, now, request))
            .Where(item => MatchesTarget(server.Name, databaseName, item.ExamCode, targets))
            .ToList();

        var examStats = includeRegistrationCount || includeAdmissionTicketCount
            ? await LoadSqlServerExamStatsAsync(
                connection,
                databaseName,
                filteredRows.Select(item => item.ExamCode),
                includeRegistrationCount,
                includeAdmissionTicketCount,
                cancellationToken)
            : [];

        return filteredRows
            .Select(item => BuildRecord(server.Name, databaseName, item, now, examStats))
            .ToList();
    }

    private static async Task<List<ProjectStageRecord>> QueryMySqlDatabaseAsync(
        StageServerConfig server,
        string databaseName,
        DateTime now,
        ProjectStageQueryRequest request,
        bool includeRegistrationCount,
        bool includeAdmissionTicketCount,
        IReadOnlyCollection<BoardCountTarget>? targets,
        CancellationToken cancellationToken)
    {
        await using var connection = new MySqlConnection(BuildMySqlConnectionString(server, databaseName));
        await connection.OpenAsync(cancellationToken);

        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT
                a.id,
                a.name,
                b.name,
                b.start_date,
                b.end_date
            FROM mgt_exam_organize a
            JOIN mgt_exam_step b
              ON a.id = b.exam_id
            ORDER BY b.start_date ASC
            """;

        var rows = await ReadMySqlRowsAsync(command, cancellationToken);
        var filteredRows = rows
            .Where(item => MatchesRow(server.Name, databaseName, item, now, request))
            .Where(item => MatchesTarget(server.Name, databaseName, item.ExamCode, targets))
            .ToList();

        var examStats = includeRegistrationCount || includeAdmissionTicketCount
            ? await LoadMySqlExamStatsAsync(
                connection,
                filteredRows.Select(item => item.ExamCode),
                includeRegistrationCount,
                includeAdmissionTicketCount,
                cancellationToken)
            : [];

        return filteredRows
            .Select(item => BuildRecord(server.Name, databaseName, item, now, examStats))
            .ToList();
    }

    private static async Task<List<StageQueryRow>> ReadSqlServerRowsAsync(
        SqlCommand command,
        CancellationToken cancellationToken)
    {
        var rows = new List<StageQueryRow>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var examCode = reader.IsDBNull(0) ? "" : Convert.ToString(reader.GetValue(0))?.Trim() ?? "";
            var projectName = reader.IsDBNull(1) ? "" : reader.GetString(1).Trim();
            var stageName = reader.IsDBNull(2) ? "" : reader.GetString(2).Trim();
            if (string.IsNullOrWhiteSpace(examCode) || string.IsNullOrWhiteSpace(projectName) || string.IsNullOrWhiteSpace(stageName))
            {
                continue;
            }

            rows.Add(new StageQueryRow(
                examCode,
                projectName,
                stageName,
                reader.GetDateTime(3),
                reader.GetDateTime(4)));
        }

        return rows;
    }

    private static async Task<List<StageQueryRow>> ReadMySqlRowsAsync(
        MySqlCommand command,
        CancellationToken cancellationToken)
    {
        var rows = new List<StageQueryRow>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var examCode = reader.IsDBNull(0) ? "" : Convert.ToString(reader.GetValue(0))?.Trim() ?? "";
            var projectName = reader.IsDBNull(1) ? "" : reader.GetString(1).Trim();
            var stageName = reader.IsDBNull(2) ? "" : reader.GetString(2).Trim();
            if (string.IsNullOrWhiteSpace(examCode) || string.IsNullOrWhiteSpace(projectName) || string.IsNullOrWhiteSpace(stageName))
            {
                continue;
            }

            rows.Add(new StageQueryRow(
                examCode,
                projectName,
                stageName,
                reader.GetDateTime(3),
                reader.GetDateTime(4)));
        }

        return rows;
    }

    private static ProjectStageRecord BuildRecord(
        string serverName,
        string databaseName,
        StageQueryRow row,
        DateTime now,
        Dictionary<string, ExamStats> examStats)
    {
        examStats.TryGetValue(row.ExamCode, out var stats);

        return new ProjectStageRecord
        {
            ServerName = serverName,
            DatabaseName = databaseName,
            ExamCode = row.ExamCode,
            ProjectName = row.ProjectName,
            StageName = row.StageName,
            StartTime = row.StartTime,
            EndTime = row.EndTime,
            Status = GetStatus(now, row.StartTime, row.EndTime),
            RegistrationCount = stats?.RegistrationCount ?? 0,
            AdmissionTicketCount = stats?.AdmissionTicketCount ?? 0
        };
    }

    private static bool MatchesRow(
        string serverName,
        string databaseName,
        StageQueryRow row,
        DateTime now,
        ProjectStageQueryRequest request)
    {
        var record = new ProjectStageRecord
        {
            ServerName = serverName,
            DatabaseName = databaseName,
            ExamCode = row.ExamCode,
            ProjectName = row.ProjectName,
            StageName = row.StageName,
            StartTime = row.StartTime,
            EndTime = row.EndTime,
            Status = GetStatus(now, row.StartTime, row.EndTime)
        };

        return AllowRecord(record, request);
    }

    private static bool MatchesTarget(
        string serverName,
        string databaseName,
        string examCode,
        IReadOnlyCollection<BoardCountTarget>? targets)
    {
        if (targets is null || targets.Count == 0)
        {
            return true;
        }

        return targets.Any(item =>
            string.Equals(item.ServerName, serverName, StringComparison.OrdinalIgnoreCase) &&
            string.Equals(item.DatabaseName, databaseName, StringComparison.OrdinalIgnoreCase) &&
            string.Equals(item.ExamCode, examCode, StringComparison.OrdinalIgnoreCase));
    }

    private static async Task<Dictionary<string, ExamStats>> LoadSqlServerExamStatsAsync(
        SqlConnection connection,
        string databaseName,
        IEnumerable<string> examCodes,
        bool includeRegistrationCount,
        bool includeAdmissionTicketCount,
        CancellationToken cancellationToken)
    {
        var result = new Dictionary<string, ExamStats>(StringComparer.OrdinalIgnoreCase);
        var escapedDatabaseName = EscapeDbName(databaseName);
        var existingTableNames = await LoadSqlServerTableNamesAsync(connection, databaseName, cancellationToken);

        foreach (var examCode in examCodes
                     .Where(item => !string.IsNullOrWhiteSpace(item))
                     .Distinct(StringComparer.OrdinalIgnoreCase))
        {
            var registrationTableName = $"考生表{examCode}";
            var admissionTableName = $"考场表{examCode}";
            var registrationTable = EscapeSqlIdentifier(registrationTableName);
            var admissionTable = EscapeSqlIdentifier(admissionTableName);

            result[examCode] = new ExamStats(
                includeRegistrationCount && existingTableNames.Contains(registrationTableName)
                    ? await CountSqlServerTableRowsAsync(connection, escapedDatabaseName, registrationTable, cancellationToken)
                    : 0,
                includeAdmissionTicketCount && existingTableNames.Contains(admissionTableName)
                    ? await CountSqlServerTableRowsAsync(connection, escapedDatabaseName, admissionTable, cancellationToken)
                    : 0);
        }

        return result;
    }

    private static async Task<List<ServerCountUpdate>> QuerySqlServerServerCountsAsync(
        StageServerConfig server,
        IReadOnlyCollection<IGrouping<string, BoardCountTarget>> groupedTargets,
        bool includeRegistrationCount,
        bool includeAdmissionTicketCount,
        CancellationToken cancellationToken)
    {
        var result = new List<ServerCountUpdate>();

        foreach (var databaseGroup in groupedTargets)
        {
            var databaseName = databaseGroup.Key;
            await using var connection = new SqlConnection(BuildSqlServerConnectionString(server, databaseName));
            await connection.OpenAsync(cancellationToken);

            var existingTableNames = await LoadSqlServerTableNamesAsync(connection, databaseName, cancellationToken);
            foreach (var target in databaseGroup
                         .GroupBy(item => item.ExamCode, StringComparer.OrdinalIgnoreCase)
                         .Select(group => group.First()))
            {
                var registrationTableName = $"考生表{target.ExamCode}";
                var admissionTableName = $"考场表{target.ExamCode}";

                var registrationCount = includeRegistrationCount && existingTableNames.Contains(registrationTableName)
                    ? await CountSqlServerTableRowsAsync(connection, EscapeDbName(databaseName), EscapeSqlIdentifier(registrationTableName), cancellationToken)
                    : includeRegistrationCount
                        ? (int?)0
                        : null;

                var admissionTicketCount = includeAdmissionTicketCount && existingTableNames.Contains(admissionTableName)
                    ? await CountSqlServerTableRowsAsync(connection, EscapeDbName(databaseName), EscapeSqlIdentifier(admissionTableName), cancellationToken)
                    : includeAdmissionTicketCount
                        ? (int?)0
                        : null;

                result.Add(new ServerCountUpdate
                {
                    ServerName = server.Name,
                    DatabaseName = databaseName,
                    ExamCode = target.ExamCode,
                    RegistrationCount = registrationCount,
                    AdmissionTicketCount = admissionTicketCount
                });
            }
        }

        return result;
    }

    private static async Task<Dictionary<string, ExamStats>> LoadMySqlExamStatsAsync(
        MySqlConnection connection,
        IEnumerable<string> examCodes,
        bool includeRegistrationCount,
        bool includeAdmissionTicketCount,
        CancellationToken cancellationToken)
    {
        var result = new Dictionary<string, ExamStats>(StringComparer.OrdinalIgnoreCase);
        var existingTableNames = await LoadMySqlTableNamesAsync(connection, cancellationToken);

        foreach (var examCode in examCodes
                     .Where(item => !string.IsNullOrWhiteSpace(item))
                     .Distinct(StringComparer.OrdinalIgnoreCase))
        {
            var registrationTableName = $"tb_ks_a001_{examCode}";
            var admissionTableName = $"tb_ks_kc_{examCode}";
            result[examCode] = new ExamStats(
                includeRegistrationCount && existingTableNames.Contains(registrationTableName)
                    ? await CountMySqlTableRowsAsync(connection, registrationTableName, cancellationToken)
                    : 0,
                includeAdmissionTicketCount && existingTableNames.Contains(admissionTableName)
                    ? await CountMySqlTableRowsAsync(connection, admissionTableName, cancellationToken)
                    : 0);
        }

        return result;
    }

    private static async Task<List<ServerCountUpdate>> QueryMySqlServerCountsAsync(
        StageServerConfig server,
        IReadOnlyCollection<IGrouping<string, BoardCountTarget>> groupedTargets,
        bool includeRegistrationCount,
        bool includeAdmissionTicketCount,
        CancellationToken cancellationToken)
    {
        var result = new List<ServerCountUpdate>();

        foreach (var databaseGroup in groupedTargets)
        {
            var databaseName = databaseGroup.Key;
            await using var connection = new MySqlConnection(BuildMySqlConnectionString(server, databaseName));
            await connection.OpenAsync(cancellationToken);

            var existingTableNames = await LoadMySqlTableNamesAsync(connection, cancellationToken);
            foreach (var target in databaseGroup
                         .GroupBy(item => item.ExamCode, StringComparer.OrdinalIgnoreCase)
                         .Select(group => group.First()))
            {
                var registrationTableName = $"tb_ks_a001_{target.ExamCode}";
                var admissionTableName = $"tb_ks_kc_{target.ExamCode}";

                var registrationCount = includeRegistrationCount && existingTableNames.Contains(registrationTableName)
                    ? await CountMySqlTableRowsAsync(connection, registrationTableName, cancellationToken)
                    : includeRegistrationCount
                        ? (int?)0
                        : null;

                var admissionTicketCount = includeAdmissionTicketCount && existingTableNames.Contains(admissionTableName)
                    ? await CountMySqlTableRowsAsync(connection, admissionTableName, cancellationToken)
                    : includeAdmissionTicketCount
                        ? (int?)0
                        : null;

                result.Add(new ServerCountUpdate
                {
                    ServerName = server.Name,
                    DatabaseName = databaseName,
                    ExamCode = target.ExamCode,
                    RegistrationCount = registrationCount,
                    AdmissionTicketCount = admissionTicketCount
                });
            }
        }

        return result;
    }

    private static async Task<HashSet<string>> LoadSqlServerTableNamesAsync(
        SqlConnection connection,
        string databaseName,
        CancellationToken cancellationToken)
    {
        var escapedDatabaseName = EscapeDbName(databaseName);
        await using var command = connection.CreateCommand();
        command.CommandText = $"""
            SELECT name
            FROM [{escapedDatabaseName}].sys.tables
            """;

        var result = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            if (!reader.IsDBNull(0))
            {
                result.Add(reader.GetString(0));
            }
        }

        return result;
    }

    private static async Task<HashSet<string>> LoadMySqlTableNamesAsync(
        MySqlConnection connection,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = DATABASE()
            """;

        var result = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            if (!reader.IsDBNull(0))
            {
                result.Add(reader.GetString(0));
            }
        }

        return result;
    }

    private static async Task<int> CountSqlServerTableRowsAsync(
        SqlConnection connection,
        string escapedDatabaseName,
        string escapedTableName,
        CancellationToken cancellationToken)
    {
        await using var countCommand = connection.CreateCommand();
        countCommand.CommandText = $"""
            SELECT COUNT(*)
            FROM [{escapedDatabaseName}].[dbo].[{escapedTableName}]
            """;

        return Convert.ToInt32(await countCommand.ExecuteScalarAsync(cancellationToken));
    }

    private static async Task<int> CountMySqlTableRowsAsync(
        MySqlConnection connection,
        string tableName,
        CancellationToken cancellationToken)
    {
        await using var countCommand = connection.CreateCommand();
        countCommand.CommandText = $"SELECT COUNT(*) FROM `{EscapeMySqlIdentifier(tableName)}`";
        return Convert.ToInt32(await countCommand.ExecuteScalarAsync(cancellationToken));
    }

    private static bool AllowRecord(ProjectStageRecord record, ProjectStageQueryRequest request)
    {
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

        if (request.StageNames.Count > 0 &&
            !request.StageNames.Any(item =>
                !string.IsNullOrWhiteSpace(item) &&
                record.StageName.Contains(item.Trim(), StringComparison.OrdinalIgnoreCase)))
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
            if (!MatchesTimeRange(record, rangeStart, rangeEnd, request.TimeMatchMode))
            {
                return false;
            }
        }

        if (request.DayOffsets.Count > 0)
        {
            var today = DateTime.Today;
            var matched = request.DayOffsets
                .Distinct()
                .Any(offset =>
                {
                    var dayStart = today.AddDays(offset);
                    var dayEnd = dayStart.AddDays(1).AddTicks(-1);
                    return MatchesTimeRange(record, dayStart, dayEnd, request.TimeMatchMode);
                });

            if (!matched)
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
            ConnectTimeout = 60
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
            ConnectionTimeout = 20,
            AllowUserVariables = true
        };
        return builder.ConnectionString;
    }

    private static bool IsMySql(StageServerConfig server) =>
        string.Equals(server.DatabaseType, "MySQL", StringComparison.OrdinalIgnoreCase);

    private static string EscapeDbName(string databaseName) => databaseName.Replace("]", "]]");

    private static string EscapeSqlIdentifier(string value) => value.Replace("]", "]]");

    private static string EscapeMySqlIdentifier(string value) => value.Replace("`", "``");

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

    private static bool MatchesTimeRange(ProjectStageRecord record, DateTime rangeStart, DateTime rangeEnd, string timeMatchMode)
    {
        return timeMatchMode switch
        {
            "start" => record.StartTime >= rangeStart && record.StartTime <= rangeEnd,
            "end" => record.EndTime >= rangeStart && record.EndTime <= rangeEnd,
            _ => record.EndTime >= rangeStart && record.StartTime <= rangeEnd
        };
    }

    private sealed record StageQueryRow(
        string ExamCode,
        string ProjectName,
        string StageName,
        DateTime StartTime,
        DateTime EndTime);

    private sealed record ExamStats(
        int RegistrationCount,
        int AdmissionTicketCount);

    private sealed record ServerQueryResult(
        int VisitedDatabases,
        int MatchedDatabases,
        List<ProjectStageRecord> Records);
}

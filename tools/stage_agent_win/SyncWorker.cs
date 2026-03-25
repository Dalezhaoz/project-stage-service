using Microsoft.Data.SqlClient;

namespace StageAgentService;

public sealed class SyncWorker
{
    private readonly ILogger<SyncWorker> _logger;

    public SyncWorker(ILogger<SyncWorker> logger)
    {
        _logger = logger;
    }

    public async Task<object> RunSyncAsync(SyncRequest req)
    {
        _logger.LogInformation("同步开始 - {ServerName}", req.ServerName);

        var databases = await FindDatabasesAsync(req.Source);
        var allRecords = new List<StageRecord>();

        foreach (var dbName in databases)
        {
            var records = await QueryDatabaseAsync(req.Source, dbName, req.ServerName);
            allRecords.AddRange(records);
        }

        if (allRecords.Count > 0)
        {
            await WriteToCentralAsync(req.Target, req.ServerName, allRecords);
        }

        _logger.LogInformation("同步完成 - {Databases} 个库, {Records} 条记录", databases.Count, allRecords.Count);

        return new { serverName = req.ServerName, databases = databases.Count, records = allRecords.Count };
    }

    private static async Task<List<string>> FindDatabasesAsync(SourceConfig source)
    {
        var connStr = BuildSourceConnStr(source, "master");
        using var conn = new SqlConnection(connStr);
        await conn.OpenAsync();

        var allDbs = new List<string>();
        using (var cmd = new SqlCommand(
            "SELECT name FROM sys.databases WHERE state_desc = 'ONLINE' AND name NOT IN ('master','model','msdb','tempdb') ORDER BY name", conn))
        using (var rdr = await cmd.ExecuteReaderAsync())
        {
            while (await rdr.ReadAsync()) allDbs.Add(rdr.GetString(0));
        }

        var matched = new List<string>();
        foreach (var db in allDbs)
        {
            var escaped = db.Replace("]", "]]");
            using var cmd = new SqlCommand(
                $"SELECT COUNT(*) FROM [{escaped}].sys.tables WHERE name IN ('EI_ExamTreeDesc','web_SR_CodeItem','WEB_SR_SetTime')", conn);
            var count = (int)(await cmd.ExecuteScalarAsync() ?? 0);
            if (count == 3) matched.Add(db);
        }

        return matched;
    }

    private static async Task<List<StageRecord>> QueryDatabaseAsync(SourceConfig source, string dbName, string serverName)
    {
        var escaped = dbName.Replace("]", "]]");
        var connStr = BuildSourceConnStr(source, dbName);
        using var conn = new SqlConnection(connStr);
        await conn.OpenAsync();

        var sql = $@"
            SELECT A.Code, A.NAME, B.Description, C.KDate, C.ZDate
            FROM [{escaped}].[dbo].[EI_ExamTreeDesc] A
            JOIN [{escaped}].[dbo].[WEB_SR_SetTime] C ON A.Code = C.ExamSort
            JOIN [{escaped}].[dbo].[web_SR_CodeItem] B ON B.Codeid = 'WT' AND B.Code = C.Kind
            WHERE A.CodeLen = '2' AND C.Kind <> '06'
            ORDER BY C.KDate ASC";

        var records = new List<StageRecord>();
        var now = DateTime.Now;

        using (var cmd = new SqlCommand(sql, conn) { CommandTimeout = 120 })
        using (var rdr = await cmd.ExecuteReaderAsync())
        {
            while (await rdr.ReadAsync())
            {
                var examCode = rdr.IsDBNull(0) ? "" : rdr.GetValue(0).ToString()?.Trim() ?? "";
                var projectName = rdr.IsDBNull(1) ? "" : rdr.GetString(1).Trim();
                var stageName = rdr.IsDBNull(2) ? "" : rdr.GetString(2).Trim();
                if (string.IsNullOrEmpty(examCode) || string.IsNullOrEmpty(projectName) || string.IsNullOrEmpty(stageName))
                    continue;

                var startTime = rdr.GetDateTime(3);
                var endTime = rdr.GetDateTime(4);
                var status = now < startTime ? "即将开始" : now > endTime ? "已经结束" : "正在进行";

                records.Add(new StageRecord
                {
                    ServerName = serverName,
                    DatabaseName = dbName,
                    DatabaseType = "SQL Server",
                    ExamCode = examCode,
                    ProjectName = projectName,
                    StageName = stageName,
                    StartTime = startTime,
                    EndTime = endTime,
                    Status = status
                });
            }
        }

        // 查所有表名
        var existingTables = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        using (var cmd = new SqlCommand($"SELECT name FROM [{escaped}].sys.tables", conn))
        using (var rdr = await cmd.ExecuteReaderAsync())
        {
            while (await rdr.ReadAsync()) existingTables.Add(rdr.GetString(0));
        }

        // 按 exam_code 统计报名人数和准考证人数
        var examCodes = records.Select(r => r.ExamCode).Distinct(StringComparer.OrdinalIgnoreCase).ToList();
        var counts = new Dictionary<string, (int reg, int adm)>(StringComparer.OrdinalIgnoreCase);

        foreach (var ec in examCodes)
        {
            var regTable = $"考生表{ec}";
            var admTable = $"考场表{ec}";
            var regCount = existingTables.Contains(regTable)
                ? await CountTableRowsAsync(conn, escaped, regTable) : 0;
            var admCount = existingTables.Contains(admTable)
                ? await CountTableRowsAsync(conn, escaped, admTable) : 0;
            counts[ec] = (regCount, admCount);
        }

        foreach (var r in records)
        {
            if (counts.TryGetValue(r.ExamCode, out var c))
            {
                r.RegistrationCount = c.reg;
                r.AdmissionTicketCount = c.adm;
            }
        }

        return records;
    }

    private static async Task<int> CountTableRowsAsync(SqlConnection conn, string escapedDb, string tableName)
    {
        try
        {
            var escapedTable = tableName.Replace("]", "]]");
            using var cmd = new SqlCommand($"SELECT COUNT(*) FROM [{escapedDb}].[dbo].[{escapedTable}]", conn)
                { CommandTimeout = 120 };
            return (int)(await cmd.ExecuteScalarAsync() ?? 0);
        }
        catch
        {
            return 0;
        }
    }

    private static async Task WriteToCentralAsync(TargetConfig target, string serverName, List<StageRecord> records)
    {
        var connStr = $"Server={target.Host},{target.Port};Database={target.DatabaseName};" +
                      $"User Id={target.Username};Password={target.Password};TrustServerCertificate=True;Connection Timeout=20;";

        using var conn = new SqlConnection(connStr);
        await conn.OpenAsync();

        var groups = records.GroupBy(r => r.DatabaseName);
        foreach (var group in groups)
        {
            using (var delCmd = new SqlCommand(
                "DELETE FROM dbo.project_stage_summary WHERE source_server_name = @server AND source_database_name = @db", conn))
            {
                delCmd.Parameters.AddWithValue("@server", serverName);
                delCmd.Parameters.AddWithValue("@db", group.Key);
                await delCmd.ExecuteNonQueryAsync();
            }

            var seen = new HashSet<string>();
            var syncedAt = DateTime.Now;

            foreach (var r in group)
            {
                var key = $"{r.ExamCode}|{r.StageName}|{r.StartTime:O}|{r.EndTime:O}";
                if (!seen.Add(key)) continue;

                using var insCmd = new SqlCommand(
                    @"INSERT INTO dbo.project_stage_summary
                      (source_server_name, source_database_name, source_database_type, exam_code,
                       project_name, stage_name, stage_start_time, stage_end_time, stage_status,
                       registration_count, admission_ticket_count, synced_at)
                      VALUES (@sn, @db, @dt, @ec, @pn, @st, @s, @e, @ss, @rc, @ac, @sa)", conn);
                insCmd.Parameters.AddWithValue("@sn", r.ServerName);
                insCmd.Parameters.AddWithValue("@db", r.DatabaseName);
                insCmd.Parameters.AddWithValue("@dt", r.DatabaseType);
                insCmd.Parameters.AddWithValue("@ec", r.ExamCode);
                insCmd.Parameters.AddWithValue("@pn", r.ProjectName);
                insCmd.Parameters.AddWithValue("@st", r.StageName);
                insCmd.Parameters.AddWithValue("@s", r.StartTime);
                insCmd.Parameters.AddWithValue("@e", r.EndTime);
                insCmd.Parameters.AddWithValue("@ss", r.Status);
                insCmd.Parameters.AddWithValue("@rc", r.RegistrationCount);
                insCmd.Parameters.AddWithValue("@ac", r.AdmissionTicketCount);
                insCmd.Parameters.AddWithValue("@sa", syncedAt);
                await insCmd.ExecuteNonQueryAsync();
            }
        }
    }

    private static string BuildSourceConnStr(SourceConfig source, string database) =>
        $"Server={source.Host},{source.Port};Database={database};" +
        $"User Id={source.Username};Password={source.Password};TrustServerCertificate=True;Connection Timeout=60;";
}

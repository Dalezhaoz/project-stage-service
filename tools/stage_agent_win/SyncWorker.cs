using Microsoft.Data.SqlClient;
using MySqlConnector;
using System.Globalization;

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

        var response = await QueryInternalAsync(new QueryRequest
        {
            ServerName = req.ServerName,
            Source = req.Source,
            Definition = BuildLegacyDefinition(req.Source.DatabaseType)
        });

        var allRecords = response.Records.Select(record => ToLegacyStageRecord(req.ServerName, req.Source.DatabaseType, record)).ToList();
        if (allRecords.Count > 0)
        {
            await WriteToCentralAsync(req.Target, req.ServerName, allRecords);
        }

        _logger.LogInformation("同步完成 - {Databases} 个库, {Records} 条记录", response.MatchedDatabases, allRecords.Count);
        return new { serverName = req.ServerName, databases = response.MatchedDatabases, records = allRecords.Count };
    }

    public async Task<object> TestAsync(TestRequest req)
    {
        _logger.LogInformation("测试连接开始 - {ServerName}", req.ServerName);
        var databases = await FindDatabasesAsync(req.Source, req.Definition.RequiredTables);
        return new
        {
            visitedDatabases = databases.Count,
            matchedDatabases = databases.Count
        };
    }

    public Task<QueryResponse> QueryAsync(QueryRequest req) => QueryInternalAsync(req);

    private async Task<QueryResponse> QueryInternalAsync(QueryRequest req)
    {
        _logger.LogInformation("查询开始 - {ServerName}", req.ServerName);

        var databases = await FindDatabasesAsync(req.Source, req.Definition.RequiredTables);
        var records = new List<QueryRecord>();
        foreach (var dbName in databases)
        {
            var rows = await QueryDatabaseAsync(req.Source, dbName, req.Definition);
            records.AddRange(rows);
        }

        _logger.LogInformation("查询完成 - {Databases} 个库, {Records} 条记录", databases.Count, records.Count);
        return new QueryResponse
        {
            VisitedDatabases = databases.Count,
            MatchedDatabases = databases.Count,
            Records = records
        };
    }

    private static QueryDefinition BuildLegacyDefinition(string databaseType)
    {
        return string.Equals(databaseType, "MySQL", StringComparison.OrdinalIgnoreCase)
            ? new QueryDefinition
            {
                RequiredTables = ["mgt_exam_organize", "mgt_exam_step"],
                StageQuerySql = """
                    SELECT
                        CAST(a.id AS CHAR) AS exam_code,
                        a.name AS project_name,
                        b.name AS stage_name,
                        b.start_date AS start_time,
                        b.end_date AS end_time
                    FROM mgt_exam_organize a
                    JOIN mgt_exam_step b ON a.id = b.exam_id
                    ORDER BY b.start_date ASC
                    """,
                ExistingTablesSql = "SHOW TABLES",
                RegistrationTablePattern = "tb_ks_a001_{exam_code}",
                AdmissionTicketTablePattern = "tb_ks_kc_{exam_code}"
            }
            : new QueryDefinition
            {
                RequiredTables = ["EI_ExamTreeDesc", "web_SR_CodeItem", "WEB_SR_SetTime"],
                StageQuerySql = """
                    SELECT
                        CONVERT(NVARCHAR(50), A.Code) AS exam_code,
                        CONVERT(NVARCHAR(500), A.NAME) AS project_name,
                        CONVERT(NVARCHAR(200), B.Description) AS stage_name,
                        C.KDate AS start_time,
                        C.ZDate AS end_time
                    FROM dbo.EI_ExamTreeDesc A
                    JOIN dbo.WEB_SR_SetTime C ON A.Code = C.ExamSort
                    JOIN dbo.web_SR_CodeItem B ON B.Codeid = 'WT' AND B.Code = C.Kind
                    WHERE A.CodeLen = '2' AND C.Kind <> '06'
                    ORDER BY C.KDate ASC
                    """,
                ExistingTablesSql = "SELECT name FROM sys.tables",
                RegistrationTablePattern = "考生表{exam_code}",
                AdmissionTicketTablePattern = "考场表{exam_code}"
            };
    }

    private static async Task<List<string>> FindDatabasesAsync(SourceConfig source, List<string> requiredTables)
    {
        return string.Equals(source.DatabaseType, "MySQL", StringComparison.OrdinalIgnoreCase)
            ? await FindMySqlDatabasesAsync(source, requiredTables)
            : await FindSqlServerDatabasesAsync(source, requiredTables);
    }

    private static async Task<List<string>> FindSqlServerDatabasesAsync(SourceConfig source, List<string> requiredTables)
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
            var placeholders = string.Join(", ", requiredTables.Select((_, i) => $"@p{i}"));
            using var cmd = new SqlCommand(
                $"SELECT COUNT(*) FROM [{escaped}].sys.tables WHERE name IN ({placeholders})", conn);
            for (var i = 0; i < requiredTables.Count; i++)
            {
                cmd.Parameters.AddWithValue($"@p{i}", requiredTables[i]);
            }

            var count = Convert.ToInt32(await cmd.ExecuteScalarAsync() ?? 0);
            if (count == requiredTables.Count) matched.Add(db);
        }

        return matched;
    }

    private static async Task<List<string>> FindMySqlDatabasesAsync(SourceConfig source, List<string> requiredTables)
    {
        var connStr = BuildMySqlConnStr(source, null);
        await using var conn = new MySqlConnection(connStr);
        await conn.OpenAsync();

        var placeholders = string.Join(", ", requiredTables.Select((_, i) => $"@p{i}"));
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            SELECT table_schema
            FROM information_schema.tables
            WHERE table_schema NOT IN ('information_schema','mysql','performance_schema','sys')
              AND table_name IN ({placeholders})
            GROUP BY table_schema
            HAVING COUNT(DISTINCT table_name) = @tableCount
            """;
        for (var i = 0; i < requiredTables.Count; i++)
        {
            cmd.Parameters.AddWithValue($"@p{i}", requiredTables[i]);
        }
        cmd.Parameters.AddWithValue("@tableCount", requiredTables.Count);

        var databases = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            databases.Add(reader.GetString(0));
        }

        return databases;
    }

    private static async Task<List<QueryRecord>> QueryDatabaseAsync(SourceConfig source, string dbName, QueryDefinition definition)
    {
        return string.Equals(source.DatabaseType, "MySQL", StringComparison.OrdinalIgnoreCase)
            ? await QueryMySqlDatabaseAsync(source, dbName, definition)
            : await QuerySqlServerDatabaseAsync(source, dbName, definition);
    }

    private static async Task<List<QueryRecord>> QuerySqlServerDatabaseAsync(SourceConfig source, string dbName, QueryDefinition definition)
    {
        var connStr = BuildSourceConnStr(source, dbName);
        using var conn = new SqlConnection(connStr);
        await conn.OpenAsync();

        var records = new List<QueryRecord>();
        using (var cmd = new SqlCommand(definition.StageQuerySql, conn) { CommandTimeout = 120 })
        using (var reader = await cmd.ExecuteReaderAsync())
        {
            records.AddRange(await ReadQueryRecordsAsync(dbName, reader));
        }

        var existingTables = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        using (var cmd = new SqlCommand(definition.ExistingTablesSql, conn))
        using (var reader = await cmd.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
            {
                existingTables.Add(reader.GetString(0));
            }
        }

        foreach (var examCode in records.Select(item => item.Values.GetValueOrDefault("exam_code")).Where(item => !string.IsNullOrWhiteSpace(item)).Distinct(StringComparer.OrdinalIgnoreCase))
        {
            var registrationTable = definition.RegistrationTablePattern.Replace("{exam_code}", examCode);
            var admissionTable = definition.AdmissionTicketTablePattern.Replace("{exam_code}", examCode);

            var registrationCount = existingTables.Contains(registrationTable)
                ? await CountSqlServerTableAsync(conn, registrationTable)
                : 0;
            var admissionCount = existingTables.Contains(admissionTable)
                ? await CountSqlServerTableAsync(conn, admissionTable)
                : 0;

            foreach (var record in records.Where(item => string.Equals(item.Values.GetValueOrDefault("exam_code"), examCode, StringComparison.OrdinalIgnoreCase)))
            {
                record.Metrics["registration_count"] = registrationCount;
                record.Metrics["admission_ticket_count"] = admissionCount;
            }
        }

        return records;
    }

    private static async Task<List<QueryRecord>> QueryMySqlDatabaseAsync(SourceConfig source, string dbName, QueryDefinition definition)
    {
        var connStr = BuildMySqlConnStr(source, dbName);
        await using var conn = new MySqlConnection(connStr);
        await conn.OpenAsync();

        var records = new List<QueryRecord>();
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = definition.StageQuerySql;
            await using var reader = await cmd.ExecuteReaderAsync();
            records.AddRange(await ReadMySqlQueryRecordsAsync(dbName, reader));
        }

        var existingTables = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = definition.ExistingTablesSql;
            await using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                existingTables.Add(reader.GetString(0));
            }
        }

        foreach (var examCode in records.Select(item => item.Values.GetValueOrDefault("exam_code")).Where(item => !string.IsNullOrWhiteSpace(item)).Distinct(StringComparer.OrdinalIgnoreCase))
        {
            var registrationTable = definition.RegistrationTablePattern.Replace("{exam_code}", examCode);
            var admissionTable = definition.AdmissionTicketTablePattern.Replace("{exam_code}", examCode);

            var registrationCount = existingTables.Contains(registrationTable)
                ? await CountMySqlTableAsync(conn, registrationTable)
                : 0;
            var admissionCount = existingTables.Contains(admissionTable)
                ? await CountMySqlTableAsync(conn, admissionTable)
                : 0;

            foreach (var record in records.Where(item => string.Equals(item.Values.GetValueOrDefault("exam_code"), examCode, StringComparison.OrdinalIgnoreCase)))
            {
                record.Metrics["registration_count"] = registrationCount;
                record.Metrics["admission_ticket_count"] = admissionCount;
            }
        }

        return records;
    }

    private static async Task<List<QueryRecord>> ReadQueryRecordsAsync(string dbName, SqlDataReader reader)
    {
        var records = new List<QueryRecord>();
        while (await reader.ReadAsync())
        {
            var values = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            for (var index = 0; index < reader.FieldCount; index++)
            {
                var value = reader.IsDBNull(index) ? "" : ConvertValue(reader.GetValue(index));
                values[reader.GetName(index)] = value;
            }

            records.Add(new QueryRecord
            {
                DatabaseName = dbName,
                Values = values,
                Metrics = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase)
            });
        }

        return records;
    }

    private static async Task<List<QueryRecord>> ReadMySqlQueryRecordsAsync(string dbName, MySqlDataReader reader)
    {
        var records = new List<QueryRecord>();
        while (await reader.ReadAsync())
        {
            var values = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            for (var index = 0; index < reader.FieldCount; index++)
            {
                var value = await reader.IsDBNullAsync(index) ? "" : ConvertValue(reader.GetValue(index));
                values[reader.GetName(index)] = value;
            }

            records.Add(new QueryRecord
            {
                DatabaseName = dbName,
                Values = values,
                Metrics = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase)
            });
        }

        return records;
    }

    private static StageRecord ToLegacyStageRecord(string serverName, string databaseType, QueryRecord record)
    {
        var startText = record.Values.GetValueOrDefault("start_time") ?? "";
        var endText = record.Values.GetValueOrDefault("end_time") ?? "";
        var startTime = DateTime.Parse(startText, CultureInfo.InvariantCulture);
        var endTime = DateTime.Parse(endText, CultureInfo.InvariantCulture);
        var now = DateTime.Now;
        var status = now < startTime ? "即将开始" : now > endTime ? "已经结束" : "正在进行";

        return new StageRecord
        {
            ServerName = serverName,
            DatabaseName = record.DatabaseName,
            DatabaseType = databaseType,
            ExamCode = record.Values.GetValueOrDefault("exam_code") ?? "",
            ProjectName = record.Values.GetValueOrDefault("project_name") ?? "",
            StageName = record.Values.GetValueOrDefault("stage_name") ?? "",
            StartTime = startTime,
            EndTime = endTime,
            Status = status,
            RegistrationCount = record.Metrics.GetValueOrDefault("registration_count"),
            AdmissionTicketCount = record.Metrics.GetValueOrDefault("admission_ticket_count")
        };
    }

    private static async Task<int> CountSqlServerTableAsync(SqlConnection conn, string tableName)
    {
        try
        {
            var escapedTable = tableName.Replace("]", "]]");
            using var cmd = new SqlCommand($"SELECT COUNT(*) FROM [dbo].[{escapedTable}]", conn) { CommandTimeout = 120 };
            return Convert.ToInt32(await cmd.ExecuteScalarAsync() ?? 0);
        }
        catch
        {
            return 0;
        }
    }

    private static async Task<int> CountMySqlTableAsync(MySqlConnection conn, string tableName)
    {
        try
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"SELECT COUNT(*) FROM `{tableName.Replace("`", "``")}`";
            return Convert.ToInt32(await cmd.ExecuteScalarAsync() ?? 0);
        }
        catch
        {
            return 0;
        }
    }

    private static string ConvertValue(object value)
    {
        return value switch
        {
            DateTime dt => dt.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture),
            DateTimeOffset dto => dto.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture),
            null => "",
            _ => Convert.ToString(value, CultureInfo.InvariantCulture)?.Trim() ?? ""
        };
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

    private static string BuildMySqlConnStr(SourceConfig source, string? database) =>
        new MySqlConnectionStringBuilder
        {
            Server = source.Host,
            Port = (uint)source.Port,
            UserID = source.Username,
            Password = source.Password,
            Database = database ?? "",
            CharacterSet = "utf8mb4",
            ConnectionTimeout = 20
        }.ConnectionString;
}

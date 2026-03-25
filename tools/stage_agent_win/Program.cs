using System.Data;
using System.Net;
using System.Text;
using System.Text.Json;
using Microsoft.Data.SqlClient;

/// <summary>
/// StageAgent (Windows) - 部署在 Windows Server 上的轻量 HTTP 服务。
/// 接收中心服务的同步请求，本地查询 SQL Server，写入中心表。
///
/// 使用:
///   StageAgent.exe              默认监听 5100 端口
///   StageAgent.exe 5200         指定端口
/// </summary>

var JsonOpts = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };

var port = args.Length > 0 && int.TryParse(args[0], out var p) ? p : 5100;
var prefix = $"http://+:{port}/";

var listener = new HttpListener();
listener.Prefixes.Add(prefix);
listener.Start();

Log($"StageAgent 已启动，监听端口 {port}");
Log($"  健康检查: GET  http://localhost:{port}/health");
Log($"  触发同步: POST http://localhost:{port}/sync");

while (true)
{
    var ctx = await listener.GetContextAsync();
    _ = Task.Run(() => HandleRequest(ctx));
}

async Task HandleRequest(HttpListenerContext ctx)
{
    try
    {
        var path = ctx.Request.Url?.AbsolutePath?.TrimEnd('/') ?? "";

        if (ctx.Request.HttpMethod == "GET" && path == "/health")
        {
            await WriteJson(ctx, 200, new { status = "ok" });
            return;
        }

        if (ctx.Request.HttpMethod == "POST" && path == "/sync")
        {
            using var reader = new StreamReader(ctx.Request.InputStream, Encoding.UTF8);
            var body = await reader.ReadToEndAsync();
            var request = JsonSerializer.Deserialize<SyncRequest>(body, JsonOpts);

            if (request is null)
            {
                await WriteJson(ctx, 400, new { detail = "invalid request body" });
                return;
            }

            var result = await RunSync(request);
            await WriteJson(ctx, 200, result);
            return;
        }

        await WriteJson(ctx, 404, new { detail = "not found" });
    }
    catch (Exception ex)
    {
        Log($"错误: {ex.Message}");
        try { await WriteJson(ctx, 500, new { detail = ex.Message }); } catch { }
    }
}

async Task WriteJson(HttpListenerContext ctx, int code, object data)
{
    var json = JsonSerializer.Serialize(data, JsonOpts);
    var bytes = Encoding.UTF8.GetBytes(json);
    ctx.Response.StatusCode = code;
    ctx.Response.ContentType = "application/json; charset=utf-8";
    ctx.Response.ContentLength64 = bytes.Length;
    await ctx.Response.OutputStream.WriteAsync(bytes);
    ctx.Response.Close();
}

// ─── 同步逻辑 ────────────────────────────────────────────────

async Task<object> RunSync(SyncRequest req)
{
    Log($"同步开始 - {req.ServerName}");

    var databases = await FindDatabases(req.Source);
    var allRecords = new List<StageRecord>();

    foreach (var dbName in databases)
    {
        var records = await QueryDatabase(req.Source, dbName, req.ServerName);
        allRecords.AddRange(records);
    }

    if (allRecords.Count > 0)
    {
        await WriteToCentral(req.Target, req.ServerName, allRecords);
    }

    Log($"同步完成 - {databases.Count} 个库, {allRecords.Count} 条记录");

    return new { serverName = req.ServerName, databases = databases.Count, records = allRecords.Count };
}

async Task<List<string>> FindDatabases(SourceConfig source)
{
    var connStr = BuildSourceConnStr(source, "master");
    using var conn = new SqlConnection(connStr);
    await conn.OpenAsync();

    // 获取所有在线数据库
    var allDbs = new List<string>();
    using (var cmd = new SqlCommand(
        "SELECT name FROM sys.databases WHERE state_desc = 'ONLINE' AND name NOT IN ('master','model','msdb','tempdb') ORDER BY name", conn))
    using (var rdr = await cmd.ExecuteReaderAsync())
    {
        while (await rdr.ReadAsync()) allDbs.Add(rdr.GetString(0));
    }

    // 检查哪些库包含目标表
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

async Task<List<StageRecord>> QueryDatabase(SourceConfig source, string dbName, string serverName)
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

    using var cmd = new SqlCommand(sql, conn) { CommandTimeout = 120 };
    using var rdr = await cmd.ExecuteReaderAsync();
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

    return records;
}

// ─── 写入中心表 ──────────────────────────────────────────────

async Task WriteToCentral(TargetConfig target, string serverName, List<StageRecord> records)
{
    var connStr = $"Server={target.Host},{target.Port};Database={target.DatabaseName};" +
                  $"User Id={target.Username};Password={target.Password};TrustServerCertificate=True;Connection Timeout=20;";

    using var conn = new SqlConnection(connStr);
    await conn.OpenAsync();

    // 按 database_name 分组删除再插入
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
                  VALUES (@sn, @db, @dt, @ec, @pn, @st, @s, @e, @ss, 0, 0, @sa)", conn);
            insCmd.Parameters.AddWithValue("@sn", r.ServerName);
            insCmd.Parameters.AddWithValue("@db", r.DatabaseName);
            insCmd.Parameters.AddWithValue("@dt", r.DatabaseType);
            insCmd.Parameters.AddWithValue("@ec", r.ExamCode);
            insCmd.Parameters.AddWithValue("@pn", r.ProjectName);
            insCmd.Parameters.AddWithValue("@st", r.StageName);
            insCmd.Parameters.AddWithValue("@s", r.StartTime);
            insCmd.Parameters.AddWithValue("@e", r.EndTime);
            insCmd.Parameters.AddWithValue("@ss", r.Status);
            insCmd.Parameters.AddWithValue("@sa", syncedAt);
            await insCmd.ExecuteNonQueryAsync();
        }
    }
}

string BuildSourceConnStr(SourceConfig source, string database) =>
    $"Server={source.Host},{source.Port};Database={database};" +
    $"User Id={source.Username};Password={source.Password};TrustServerCertificate=True;Connection Timeout=60;";

void Log(string msg) => Console.WriteLine($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] {msg}");

// ─── 数据模型 ────────────────────────────────────────────────

class SyncRequest
{
    public string ServerName { get; set; } = "";
    public SourceConfig Source { get; set; } = new();
    public TargetConfig Target { get; set; } = new();
}

class SourceConfig
{
    public string DatabaseType { get; set; } = "SQL Server";
    public string Host { get; set; } = "localhost";
    public int Port { get; set; } = 1433;
    public string Username { get; set; } = "";
    public string Password { get; set; } = "";
}

class TargetConfig
{
    public string Host { get; set; } = "";
    public int Port { get; set; } = 1433;
    public string DatabaseName { get; set; } = "";
    public string Username { get; set; } = "";
    public string Password { get; set; } = "";
}

class StageRecord
{
    public string ServerName { get; set; } = "";
    public string DatabaseName { get; set; } = "";
    public string DatabaseType { get; set; } = "";
    public string ExamCode { get; set; } = "";
    public string ProjectName { get; set; } = "";
    public string StageName { get; set; } = "";
    public DateTime StartTime { get; set; }
    public DateTime EndTime { get; set; }
    public string Status { get; set; } = "";
}

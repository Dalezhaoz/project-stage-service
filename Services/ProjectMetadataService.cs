using Microsoft.Data.SqlClient;
using ProjectStageService.Models;

namespace ProjectStageService.Services;

public sealed class ProjectMetadataRecord
{
    public string ServerName { get; set; } = "";
    public string ExamCode { get; set; } = "";
    public string Maintainer { get; set; } = "";
    public string AppServers { get; set; } = "";
    public DateTime? UpdatedAt { get; set; }
}

public sealed class AppServerOptionRecord
{
    public string Name { get; set; } = "";
    public int SortOrder { get; set; }
    public DateTime? UpdatedAt { get; set; }
}

public sealed class ProjectMetadataService
{
    private const string TableName = "project_metadata";
    private const string AppServerOptionTableName = "app_server_options";
    private readonly SummaryStoreConfigStore _configStore;

    public ProjectMetadataService(SummaryStoreConfigStore configStore)
    {
        _configStore = configStore;
    }

    public async Task<List<ProjectMetadataRecord>> GetAllAsync(CancellationToken cancellationToken)
    {
        var config = await _configStore.LoadAsync(cancellationToken);
        if (!config.Enabled)
            return [];

        await using var connection = OpenConnection(config);
        await connection.OpenAsync(cancellationToken);
        await EnsureSchemaAsync(connection, cancellationToken);

        await using var command = connection.CreateCommand();
        command.CommandText = $"SELECT server_name, exam_code, maintainer, app_servers, updated_at FROM dbo.{TableName};";

        var records = new List<ProjectMetadataRecord>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            records.Add(new ProjectMetadataRecord
            {
                ServerName = reader.GetString(0),
                ExamCode = reader.GetString(1),
                Maintainer = reader.IsDBNull(2) ? "" : reader.GetString(2),
                AppServers = reader.IsDBNull(3) ? "" : reader.GetString(3),
                UpdatedAt = reader.IsDBNull(4) ? null : reader.GetDateTime(4)
            });
        }

        return records;
    }

    public async Task<List<ProjectMetadataRecord>> GetByMaintainerAsync(string username, CancellationToken cancellationToken)
    {
        var config = await _configStore.LoadAsync(cancellationToken);
        if (!config.Enabled)
            return [];

        await using var connection = OpenConnection(config);
        await connection.OpenAsync(cancellationToken);
        await EnsureSchemaAsync(connection, cancellationToken);

        await using var command = connection.CreateCommand();
        command.CommandText = $"SELECT server_name, exam_code, maintainer, app_servers, updated_at FROM dbo.{TableName} WHERE maintainer = @maintainer;";
        command.Parameters.AddWithValue("@maintainer", username);

        var records = new List<ProjectMetadataRecord>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            records.Add(new ProjectMetadataRecord
            {
                ServerName = reader.GetString(0),
                ExamCode = reader.GetString(1),
                Maintainer = reader.IsDBNull(2) ? "" : reader.GetString(2),
                AppServers = reader.IsDBNull(3) ? "" : reader.GetString(3),
                UpdatedAt = reader.IsDBNull(4) ? null : reader.GetDateTime(4)
            });
        }

        return records;
    }

    public async Task SaveAsync(string serverName, string examCode, string maintainer, string appServers, CancellationToken cancellationToken)
    {
        var config = await _configStore.LoadAsync(cancellationToken);
        if (!config.Enabled)
            throw new InvalidOperationException("请先启用中心库。");

        serverName = serverName?.Trim() ?? "";
        examCode = examCode?.Trim() ?? "";
        maintainer = maintainer?.Trim() ?? "";
        appServers = await NormalizeAppServersAsync(appServers, cancellationToken);

        if (string.IsNullOrWhiteSpace(serverName) || string.IsNullOrWhiteSpace(examCode))
            throw new InvalidOperationException("项目元数据缺少服务器或考试代码。");

        await using var connection = OpenConnection(config);
        await connection.OpenAsync(cancellationToken);
        await EnsureSchemaAsync(connection, cancellationToken);

        await using var command = connection.CreateCommand();
        command.CommandText = $"""
            MERGE dbo.{TableName} AS target
            USING (SELECT @server_name AS server_name, @exam_code AS exam_code) AS source
            ON target.server_name = source.server_name AND target.exam_code = source.exam_code
            WHEN MATCHED THEN
                UPDATE SET maintainer = @maintainer, app_servers = @app_servers, updated_at = GETDATE()
            WHEN NOT MATCHED THEN
                INSERT (server_name, exam_code, maintainer, app_servers, updated_at)
                VALUES (@server_name, @exam_code, @maintainer, @app_servers, GETDATE());
            """;
        command.Parameters.AddWithValue("@server_name", serverName);
        command.Parameters.AddWithValue("@exam_code", examCode);
        command.Parameters.AddWithValue("@maintainer", maintainer ?? "");
        command.Parameters.AddWithValue("@app_servers", appServers ?? "");
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<List<AppServerOptionRecord>> GetAppServerOptionsAsync(CancellationToken cancellationToken)
    {
        var config = await _configStore.LoadAsync(cancellationToken);
        if (!config.Enabled)
            return [];

        await using var connection = OpenConnection(config);
        await connection.OpenAsync(cancellationToken);
        await EnsureSchemaAsync(connection, cancellationToken);

        await using var command = connection.CreateCommand();
        command.CommandText = $"""
            SELECT name, sort_order, updated_at
            FROM dbo.{AppServerOptionTableName}
            ORDER BY sort_order, name;
            """;

        var records = new List<AppServerOptionRecord>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            records.Add(new AppServerOptionRecord
            {
                Name = reader.GetString(0),
                SortOrder = reader.GetInt32(1),
                UpdatedAt = reader.IsDBNull(2) ? null : reader.GetDateTime(2)
            });
        }

        return records;
    }

    public async Task SaveAppServerOptionsAsync(List<string> options, CancellationToken cancellationToken)
    {
        var config = await _configStore.LoadAsync(cancellationToken);
        if (!config.Enabled)
            throw new InvalidOperationException("请先启用中心库。");

        var normalized = (options ?? [])
            .Select(item => item?.Trim() ?? "")
            .Where(item => !string.IsNullOrWhiteSpace(item))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(item => item, StringComparer.OrdinalIgnoreCase)
            .ToList();

        await using var connection = OpenConnection(config);
        await connection.OpenAsync(cancellationToken);
        await EnsureSchemaAsync(connection, cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        try
        {
            await using (var deleteCommand = connection.CreateCommand())
            {
                deleteCommand.Transaction = (SqlTransaction)transaction;
                deleteCommand.CommandText = $"DELETE FROM dbo.{AppServerOptionTableName};";
                await deleteCommand.ExecuteNonQueryAsync(cancellationToken);
            }

            for (var index = 0; index < normalized.Count; index++)
            {
                await using var insertCommand = connection.CreateCommand();
                insertCommand.Transaction = (SqlTransaction)transaction;
                insertCommand.CommandText = $"""
                    INSERT INTO dbo.{AppServerOptionTableName} (name, sort_order, updated_at)
                    VALUES (@name, @sort_order, GETDATE());
                    """;
                insertCommand.Parameters.AddWithValue("@name", normalized[index]);
                insertCommand.Parameters.AddWithValue("@sort_order", index);
                await insertCommand.ExecuteNonQueryAsync(cancellationToken);
            }

            await transaction.CommitAsync(cancellationToken);
        }
        catch
        {
            await transaction.RollbackAsync(cancellationToken);
            throw;
        }
    }

    private async Task<string> NormalizeAppServersAsync(string appServers, CancellationToken cancellationToken)
    {
        var normalized = ParseDelimitedValues(appServers);
        if (normalized.Count == 0)
            return "";

        var configured = (await GetAppServerOptionsAsync(cancellationToken))
            .Select(item => item.Name)
            .Where(item => !string.IsNullOrWhiteSpace(item))
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        if (configured.Count == 0)
            throw new InvalidOperationException("请先配置可选应用服务器。");

        var invalid = normalized
            .Where(item => !configured.Contains(item))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

        if (invalid.Count > 0)
            throw new InvalidOperationException($"存在未配置的应用服务器：{string.Join("、", invalid)}");

        return string.Join("、", normalized);
    }

    private static List<string> ParseDelimitedValues(string? raw)
    {
        return (raw ?? "")
            .Split(['\r', '\n', ',', '，', ';', '；', '、'], StringSplitOptions.RemoveEmptyEntries)
            .Select(item => item.Trim())
            .Where(item => !string.IsNullOrWhiteSpace(item))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    private static async Task EnsureSchemaAsync(SqlConnection connection, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = $"""
            IF OBJECT_ID(N'dbo.{TableName}', N'U') IS NULL
            BEGIN
                CREATE TABLE dbo.{TableName} (
                    server_name NVARCHAR(100) NOT NULL,
                    exam_code NVARCHAR(50) NOT NULL,
                    maintainer NVARCHAR(100) NOT NULL DEFAULT '',
                    app_servers NVARCHAR(500) NOT NULL DEFAULT '',
                    updated_at DATETIME NOT NULL DEFAULT GETDATE(),
                    CONSTRAINT PK_{TableName} PRIMARY KEY (server_name, exam_code)
                );
            END;

            IF OBJECT_ID(N'dbo.{AppServerOptionTableName}', N'U') IS NULL
            BEGIN
                CREATE TABLE dbo.{AppServerOptionTableName} (
                    name NVARCHAR(100) NOT NULL,
                    sort_order INT NOT NULL DEFAULT 0,
                    updated_at DATETIME NOT NULL DEFAULT GETDATE(),
                    CONSTRAINT PK_{AppServerOptionTableName} PRIMARY KEY (name)
                );
            END;
            """;
        await command.ExecuteNonQueryAsync(cancellationToken);
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
}

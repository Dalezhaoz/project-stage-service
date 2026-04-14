using Microsoft.Data.SqlClient;
using ProjectStageService.Models;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace ProjectStageService.Services;

public sealed class LocalAuthService
{
    public const string DefaultPassword = "11111111";
    public const string RoleAdmin = "admin";
    public const string RoleInternal = "internal";
    public const string RoleExternal = "external";

    private static readonly HashSet<string> ValidRoles = [RoleAdmin, RoleInternal, RoleExternal];

    private readonly string _authPath;
    private readonly SummaryStoreConfigStore _summaryStoreConfigStore;

    public LocalAuthService(SummaryStoreConfigStore summaryStoreConfigStore)
    {
        _summaryStoreConfigStore = summaryStoreConfigStore;
        var baseDir = AppDataPath.GetBaseDirectory();
        Directory.CreateDirectory(baseDir);
        _authPath = Path.Combine(baseDir, "auth.dat");
    }

    public async Task<(bool HasAccount, string? Username, string Role, bool ForcePasswordChange)> GetStatusAsync(string? currentUsername, CancellationToken cancellationToken)
    {
        var store = await LoadStoreAsync(cancellationToken);
        var user = store.Users.FirstOrDefault(item => string.Equals(item.Username, currentUsername?.Trim(), StringComparison.Ordinal));
        return (store.Users.Count > 0, currentUsername, ResolveRole(user), user?.ForcePasswordChange ?? false);
    }

    public async Task SetupAsync(string username, string password, CancellationToken cancellationToken)
    {
        ValidateUsernameAndPassword(username, password);

        var store = await LoadStoreAsync(cancellationToken);
        if (store.Users.Count > 0)
        {
            throw new InvalidOperationException("登录账号已存在。");
        }

        store.Users.Add(BuildUser(username.Trim(), password, RoleAdmin, forcePasswordChange: false));
        await SaveStoreAsync(store, cancellationToken);
    }

    public async Task CreateUserAsync(string username, string role, CancellationToken cancellationToken)
    {
        ValidateUsernameAndPassword(username, DefaultPassword);

        if (!ValidRoles.Contains(role))
        {
            throw new InvalidOperationException($"无效的角色：{role}");
        }

        var store = await LoadStoreAsync(cancellationToken);
        if (store.Users.Any(item => string.Equals(item.Username, username.Trim(), StringComparison.OrdinalIgnoreCase)))
        {
            throw new InvalidOperationException("用户名已存在。");
        }

        store.Users.Add(BuildUser(username.Trim(), DefaultPassword, role, forcePasswordChange: true));
        await SaveStoreAsync(store, cancellationToken);
    }

    public async Task<List<UserInfo>> GetUsersAsync(CancellationToken cancellationToken)
    {
        var store = await LoadStoreAsync(cancellationToken);
        var roleOrder = new Dictionary<string, int> { [RoleAdmin] = 0, [RoleInternal] = 1, [RoleExternal] = 2 };
        return store.Users
            .OrderBy(item => roleOrder.GetValueOrDefault(ResolveRole(item), 9))
            .ThenBy(item => item.Username, StringComparer.OrdinalIgnoreCase)
            .Select(item => new UserInfo(item.Username, ResolveRole(item), item.DingTalkWebhook, item.DingTalkSecret,
                item.ParsedTodayTimes, item.ParsedNextDayTimes))
            .ToList();
    }

    public async Task<AuthValidationResult> ValidateAsync(string username, string password, CancellationToken cancellationToken)
    {
        var store = await LoadStoreAsync(cancellationToken);
        var user = store.Users.FirstOrDefault(item => string.Equals(item.Username, username?.Trim(), StringComparison.Ordinal));
        if (user is null)
        {
            return new AuthValidationResult(false, RoleExternal, false);
        }

        var salt = Convert.FromBase64String(user.Salt);
        var expectedHash = Convert.FromBase64String(user.PasswordHash);
        var actualHash = Rfc2898DeriveBytes.Pbkdf2(
            Encoding.UTF8.GetBytes(password ?? string.Empty),
            salt,
            user.Iterations,
            HashAlgorithmName.SHA256,
            expectedHash.Length);

        return new AuthValidationResult(
            CryptographicOperations.FixedTimeEquals(expectedHash, actualHash),
            ResolveRole(user),
            user.ForcePasswordChange);
    }

    public async Task<string> GetRoleAsync(string username, CancellationToken cancellationToken)
    {
        var store = await LoadStoreAsync(cancellationToken);
        var user = store.Users.FirstOrDefault(item =>
            string.Equals(item.Username, username?.Trim(), StringComparison.Ordinal));
        return ResolveRole(user);
    }

    public async Task ChangePasswordAsync(
        string username,
        string currentPassword,
        string newPassword,
        CancellationToken cancellationToken)
    {
        ValidateUsernameAndPassword(username, newPassword);

        var store = await LoadStoreAsync(cancellationToken);
        var user = store.Users.FirstOrDefault(item => string.Equals(item.Username, username.Trim(), StringComparison.Ordinal));
        if (user is null)
        {
            throw new InvalidOperationException("用户不存在。");
        }

        var validation = await ValidateAsync(username, currentPassword, cancellationToken);
        if (!validation.Success)
        {
            throw new InvalidOperationException("当前密码错误。");
        }

        ReplacePassword(user, newPassword);
        user.ForcePasswordChange = false;
        await SaveStoreAsync(store, cancellationToken);
    }

    public async Task ResetPasswordAsync(string username, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(username))
        {
            throw new InvalidOperationException("用户名不能为空。");
        }

        var store = await LoadStoreAsync(cancellationToken);
        var user = store.Users.FirstOrDefault(item => string.Equals(item.Username, username.Trim(), StringComparison.Ordinal));
        if (user is null)
        {
            throw new InvalidOperationException("用户不存在。");
        }

        ReplacePassword(user, DefaultPassword);
        user.ForcePasswordChange = true;
        await SaveStoreAsync(store, cancellationToken);
    }

    public async Task UpdateUserDingTalkAsync(string username, string webhookUrl, string secret,
        CancellationToken cancellationToken,
        string[]? todayNotifyTimes = null, string[]? nextDayNotifyTimes = null)
    {
        var store = await LoadStoreAsync(cancellationToken);
        var user = store.Users.FirstOrDefault(item => string.Equals(item.Username, username?.Trim(), StringComparison.Ordinal));
        if (user is null)
        {
            throw new InvalidOperationException("用户不存在。");
        }

        user.DingTalkWebhook = webhookUrl?.Trim() ?? "";
        user.DingTalkSecret = secret?.Trim() ?? "";
        if (todayNotifyTimes is not null)
            user.DingTalkTodayTimes = string.Join(",", todayNotifyTimes.Select(t => t.Trim()).Where(t => t.Length > 0));
        if (nextDayNotifyTimes is not null)
            user.DingTalkNextDayTimes = string.Join(",", nextDayNotifyTimes.Select(t => t.Trim()).Where(t => t.Length > 0));
        await SaveStoreAsync(store, cancellationToken);
    }

    public async Task DeleteUserAsync(string username, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(username))
            throw new InvalidOperationException("用户名不能为空。");

        var store = await LoadStoreAsync(cancellationToken);
        var user = store.Users.FirstOrDefault(item =>
            string.Equals(item.Username, username.Trim(), StringComparison.Ordinal));
        if (user is null)
            throw new InvalidOperationException("用户不存在。");
        if (user.IsAdmin)
            throw new InvalidOperationException("不能删除管理员账户。");

        store.Users.Remove(user);
        await SaveStoreAsync(store, cancellationToken);
    }

    public async Task<List<UserDingTalkConfig>> GetAllDingTalkConfigsAsync(CancellationToken cancellationToken)
    {
        var store = await LoadStoreAsync(cancellationToken);
        return store.Users
            .Where(item => !string.IsNullOrWhiteSpace(item.DingTalkWebhook))
            .Select(item => new UserDingTalkConfig(item.Username, item.DingTalkWebhook, item.DingTalkSecret,
                item.ParsedTodayTimes, item.ParsedNextDayTimes))
            .ToList();
    }

    public async Task<bool> GetAllowUserRefreshAsync(CancellationToken cancellationToken)
    {
        var store = await LoadStoreAsync(cancellationToken);
        return store.AllowUserRefresh;
    }

    public async Task SetAllowUserRefreshAsync(bool allow, CancellationToken cancellationToken)
    {
        var store = await LoadStoreAsync(cancellationToken);
        store.AllowUserRefresh = allow;
        await SaveStoreAsync(store, cancellationToken);
    }

    private async Task<AuthStore> LoadStoreAsync(CancellationToken cancellationToken)
    {
        var summaryStoreConfig = await TryGetSummaryStoreConfigAsync(cancellationToken);
        if (summaryStoreConfig is not null)
        {
            return await LoadStoreFromDatabaseAsync(summaryStoreConfig, cancellationToken);
        }

        return await LoadStoreFromFileAsync(cancellationToken);
    }

    private async Task<AuthStore> LoadStoreFromDatabaseAsync(SummaryStoreConfig config, CancellationToken cancellationToken)
    {
        await using var connection = OpenConnection(config);
        await connection.OpenAsync(cancellationToken);
        await EnsureSchemaAsync(connection, cancellationToken);

        var store = new AuthStore();

        await using (var command = connection.CreateCommand())
        {
            command.CommandText = """
                SELECT username, salt, password_hash, iterations, role, force_password_change, dingtalk_webhook, dingtalk_secret,
                       dingtalk_today_times, dingtalk_nextday_times
                FROM dbo.auth_users;
                """;

            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                store.Users.Add(new AuthUserRecord
                {
                    Username = reader.GetString(0),
                    Salt = reader.GetString(1),
                    PasswordHash = reader.GetString(2),
                    Iterations = reader.GetInt32(3),
                    Role = reader.IsDBNull(4) ? RoleExternal : reader.GetString(4),
                    IsAdmin = string.Equals(reader.IsDBNull(4) ? "" : reader.GetString(4), RoleAdmin, StringComparison.Ordinal),
                    ForcePasswordChange = !reader.IsDBNull(5) && reader.GetBoolean(5),
                    DingTalkWebhook = reader.IsDBNull(6) ? "" : reader.GetString(6),
                    DingTalkSecret = reader.IsDBNull(7) ? "" : reader.GetString(7),
                    DingTalkTodayTimes = reader.IsDBNull(8) ? "" : reader.GetString(8),
                    DingTalkNextDayTimes = reader.IsDBNull(9) ? "" : reader.GetString(9)
                });
            }
        }

        await using (var command = connection.CreateCommand())
        {
            command.CommandText = """
                SELECT setting_value
                FROM dbo.auth_settings
                WHERE setting_key = 'allow_user_refresh';
                """;

            var value = await command.ExecuteScalarAsync(cancellationToken);
            if (value is string raw)
            {
                store.AllowUserRefresh = string.Equals(raw, "true", StringComparison.OrdinalIgnoreCase);
            }
        }

        if (store.Users.Count == 0 && File.Exists(_authPath))
        {
            var legacyStore = await LoadStoreFromFileAsync(cancellationToken);
            if (legacyStore.Users.Count > 0)
            {
                await SaveStoreToDatabaseAsync(config, legacyStore, cancellationToken);
                return legacyStore;
            }
        }

        return store;
    }

    private async Task<AuthStore> LoadStoreFromFileAsync(CancellationToken cancellationToken)
    {
        if (!File.Exists(_authPath))
        {
            return new AuthStore();
        }

        var encrypted = await File.ReadAllBytesAsync(_authPath, cancellationToken);
        if (encrypted.Length == 0)
        {
            return new AuthStore();
        }

        var plain = Unprotect(encrypted);
        var json = Encoding.UTF8.GetString(plain);

        try
        {
            var store = JsonSerializer.Deserialize<AuthStore>(json);
            if (store?.Users is { Count: > 0 })
            {
                return store;
            }
        }
        catch (JsonException)
        {
        }

        var legacy = JsonSerializer.Deserialize<LegacyAuthRecord>(json);
        if (legacy is null || string.IsNullOrWhiteSpace(legacy.Username))
        {
            return new AuthStore();
        }

        return new AuthStore
        {
            Users =
            [
                new AuthUserRecord
                {
                    Username = legacy.Username,
                    Salt = legacy.Salt,
                    PasswordHash = legacy.PasswordHash,
                    Iterations = legacy.Iterations,
                    IsAdmin = true,
                    Role = RoleAdmin
                }
            ]
        };
    }

    private async Task SaveStoreAsync(AuthStore store, CancellationToken cancellationToken)
    {
        var summaryStoreConfig = await TryGetSummaryStoreConfigAsync(cancellationToken);
        if (summaryStoreConfig is not null)
        {
            await SaveStoreToDatabaseAsync(summaryStoreConfig, store, cancellationToken);
            return;
        }

        await SaveStoreToFileAsync(store, cancellationToken);
    }

    private async Task SaveStoreToDatabaseAsync(SummaryStoreConfig config, AuthStore store, CancellationToken cancellationToken)
    {
        await using var connection = OpenConnection(config);
        await connection.OpenAsync(cancellationToken);
        await EnsureSchemaAsync(connection, cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        try
        {
            await using (var deleteUsers = connection.CreateCommand())
            {
                deleteUsers.Transaction = (SqlTransaction)transaction;
                deleteUsers.CommandText = "DELETE FROM dbo.auth_users;";
                await deleteUsers.ExecuteNonQueryAsync(cancellationToken);
            }

            foreach (var user in store.Users)
            {
                await using var insertUser = connection.CreateCommand();
                insertUser.Transaction = (SqlTransaction)transaction;
                insertUser.CommandText = """
                    INSERT INTO dbo.auth_users
                    (
                        username,
                        salt,
                        password_hash,
                        iterations,
                        role,
                        force_password_change,
                        dingtalk_webhook,
                        dingtalk_secret,
                        dingtalk_today_times,
                        dingtalk_nextday_times,
                        updated_at
                    )
                    VALUES
                    (
                        @username,
                        @salt,
                        @password_hash,
                        @iterations,
                        @role,
                        @force_password_change,
                        @dingtalk_webhook,
                        @dingtalk_secret,
                        @dingtalk_today_times,
                        @dingtalk_nextday_times,
                        GETDATE()
                    );
                    """;
                insertUser.Parameters.AddWithValue("@username", user.Username);
                insertUser.Parameters.AddWithValue("@salt", user.Salt);
                insertUser.Parameters.AddWithValue("@password_hash", user.PasswordHash);
                insertUser.Parameters.AddWithValue("@iterations", user.Iterations);
                insertUser.Parameters.AddWithValue("@role", ResolveRole(user));
                insertUser.Parameters.AddWithValue("@force_password_change", user.ForcePasswordChange);
                insertUser.Parameters.AddWithValue("@dingtalk_webhook", user.DingTalkWebhook ?? "");
                insertUser.Parameters.AddWithValue("@dingtalk_secret", user.DingTalkSecret ?? "");
                insertUser.Parameters.AddWithValue("@dingtalk_today_times", user.DingTalkTodayTimes ?? "");
                insertUser.Parameters.AddWithValue("@dingtalk_nextday_times", user.DingTalkNextDayTimes ?? "");
                await insertUser.ExecuteNonQueryAsync(cancellationToken);
            }

            await using (var upsertSetting = connection.CreateCommand())
            {
                upsertSetting.Transaction = (SqlTransaction)transaction;
                upsertSetting.CommandText = """
                    MERGE dbo.auth_settings AS target
                    USING (SELECT @setting_key AS setting_key, @setting_value AS setting_value) AS source
                    ON target.setting_key = source.setting_key
                    WHEN MATCHED THEN
                        UPDATE SET setting_value = source.setting_value, updated_at = GETDATE()
                    WHEN NOT MATCHED THEN
                        INSERT (setting_key, setting_value, updated_at)
                        VALUES (source.setting_key, source.setting_value, GETDATE());
                    """;
                upsertSetting.Parameters.AddWithValue("@setting_key", "allow_user_refresh");
                upsertSetting.Parameters.AddWithValue("@setting_value", store.AllowUserRefresh ? "true" : "false");
                await upsertSetting.ExecuteNonQueryAsync(cancellationToken);
            }

            await transaction.CommitAsync(cancellationToken);
        }
        catch
        {
            await transaction.RollbackAsync(cancellationToken);
            throw;
        }
    }

    private async Task SaveStoreToFileAsync(AuthStore store, CancellationToken cancellationToken)
    {
        var json = JsonSerializer.Serialize(store);
        var plain = Encoding.UTF8.GetBytes(json);
        var encrypted = Protect(plain);
        await File.WriteAllBytesAsync(_authPath, encrypted, cancellationToken);
    }

    private async Task<SummaryStoreConfig?> TryGetSummaryStoreConfigAsync(CancellationToken cancellationToken)
    {
        var config = await _summaryStoreConfigStore.LoadAsync(cancellationToken);
        if (string.IsNullOrWhiteSpace(config.Host) ||
            string.IsNullOrWhiteSpace(config.DatabaseName) ||
            string.IsNullOrWhiteSpace(config.Username) ||
            string.IsNullOrWhiteSpace(config.Password))
        {
            return null;
        }

        return config;
    }

    private static AuthUserRecord BuildUser(string username, string password, string role, bool forcePasswordChange)
    {
        var salt = RandomNumberGenerator.GetBytes(16);
        const int iterations = 100_000;
        var hash = Rfc2898DeriveBytes.Pbkdf2(
            Encoding.UTF8.GetBytes(password),
            salt,
            iterations,
            HashAlgorithmName.SHA256,
            32);

        return new AuthUserRecord
        {
            Username = username,
            Salt = Convert.ToBase64String(salt),
            PasswordHash = Convert.ToBase64String(hash),
            Iterations = iterations,
            IsAdmin = role == RoleAdmin,
            Role = role,
            ForcePasswordChange = forcePasswordChange
        };
    }

    private static void ReplacePassword(AuthUserRecord user, string password)
    {
        var updated = BuildUser(user.Username, password, ResolveRole(user), user.ForcePasswordChange);
        user.Salt = updated.Salt;
        user.PasswordHash = updated.PasswordHash;
        user.Iterations = updated.Iterations;
    }

    private static string ResolveRole(AuthUserRecord? user)
    {
        if (user is null) return RoleExternal;
        if (!string.IsNullOrEmpty(user.Role)) return user.Role;
        return user.IsAdmin ? RoleAdmin : RoleExternal;
    }

    private static void ValidateUsernameAndPassword(string username, string password)
    {
        if (string.IsNullOrWhiteSpace(username))
        {
            throw new InvalidOperationException("用户名不能为空。");
        }

        if (string.IsNullOrWhiteSpace(password))
        {
            throw new InvalidOperationException("密码不能为空。");
        }
    }

    private static byte[] Protect(byte[] plain)
    {
        if (OperatingSystem.IsWindows())
        {
            return ProtectedData.Protect(plain, null, DataProtectionScope.LocalMachine);
        }

        return plain;
    }

    private static byte[] Unprotect(byte[] encrypted)
    {
        if (OperatingSystem.IsWindows())
        {
            return ProtectedData.Unprotect(encrypted, null, DataProtectionScope.LocalMachine);
        }

        return encrypted;
    }

    private static async Task EnsureSchemaAsync(SqlConnection connection, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = """
            IF OBJECT_ID(N'dbo.auth_users', N'U') IS NULL
            BEGIN
                CREATE TABLE dbo.auth_users (
                    username NVARCHAR(100) NOT NULL PRIMARY KEY,
                    salt NVARCHAR(200) NOT NULL,
                    password_hash NVARCHAR(200) NOT NULL,
                    iterations INT NOT NULL,
                    role NVARCHAR(20) NOT NULL DEFAULT 'external',
                    force_password_change BIT NOT NULL DEFAULT 0,
                    dingtalk_webhook NVARCHAR(1000) NOT NULL DEFAULT '',
                    dingtalk_secret NVARCHAR(500) NOT NULL DEFAULT '',
                    dingtalk_today_times NVARCHAR(500) NOT NULL DEFAULT '',
                    dingtalk_nextday_times NVARCHAR(500) NOT NULL DEFAULT '',
                    updated_at DATETIME NOT NULL DEFAULT GETDATE()
                );
            END
            ELSE
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'dbo.auth_users') AND name = 'dingtalk_today_times')
                    ALTER TABLE dbo.auth_users ADD dingtalk_today_times NVARCHAR(500) NOT NULL DEFAULT '';
                IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'dbo.auth_users') AND name = 'dingtalk_nextday_times')
                    ALTER TABLE dbo.auth_users ADD dingtalk_nextday_times NVARCHAR(500) NOT NULL DEFAULT '';
            END;

            IF OBJECT_ID(N'dbo.auth_settings', N'U') IS NULL
            BEGIN
                CREATE TABLE dbo.auth_settings (
                    setting_key NVARCHAR(100) NOT NULL PRIMARY KEY,
                    setting_value NVARCHAR(1000) NOT NULL DEFAULT '',
                    updated_at DATETIME NOT NULL DEFAULT GETDATE()
                );
            END;
            """;
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static SqlConnection OpenConnection(SummaryStoreConfig config)
    {
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

    public sealed record AuthValidationResult(bool Success, string Role, bool ForcePasswordChange);
    public sealed record UserInfo(string Username, string Role, string DingTalkWebhook = "", string DingTalkSecret = "",
        string[] TodayNotifyTimes = default!, string[] NextDayNotifyTimes = default!);
    public sealed record UserDingTalkConfig(string Username, string WebhookUrl, string Secret,
        string[]? TodayNotifyTimes = null, string[]? NextDayNotifyTimes = null);

    private sealed class AuthStore
    {
        public List<AuthUserRecord> Users { get; set; } = [];
        public bool AllowUserRefresh { get; set; } = true;
    }

    private sealed class AuthUserRecord
    {
        public string Username { get; set; } = "";
        public string Salt { get; set; } = "";
        public string PasswordHash { get; set; } = "";
        public int Iterations { get; set; }
        public bool IsAdmin { get; set; }
        public string Role { get; set; } = "";
        public bool ForcePasswordChange { get; set; }
        public string DingTalkWebhook { get; set; } = "";
        public string DingTalkSecret { get; set; } = "";
        public string DingTalkTodayTimes { get; set; } = "";
        public string DingTalkNextDayTimes { get; set; } = "";

        public string[] ParsedTodayTimes =>
            string.IsNullOrWhiteSpace(DingTalkTodayTimes) ? [] :
            DingTalkTodayTimes.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        public string[] ParsedNextDayTimes =>
            string.IsNullOrWhiteSpace(DingTalkNextDayTimes) ? [] :
            DingTalkNextDayTimes.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
    }

    private sealed class LegacyAuthRecord
    {
        public string Username { get; set; } = "";
        public string Salt { get; set; } = "";
        public string PasswordHash { get; set; } = "";
        public int Iterations { get; set; }
    }
}

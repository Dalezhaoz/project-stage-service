using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace ProjectStageService.Services;

public sealed class LocalAuthService
{
    public const string DefaultPassword = "11111111";
    private readonly string _authPath;

    public LocalAuthService()
    {
        var baseDir = AppDataPath.GetBaseDirectory();
        Directory.CreateDirectory(baseDir);
        _authPath = Path.Combine(baseDir, "auth.dat");
    }

    public async Task<(bool HasAccount, string? Username, bool IsAdmin, bool ForcePasswordChange)> GetStatusAsync(string? currentUsername, CancellationToken cancellationToken)
    {
        var store = await LoadStoreAsync(cancellationToken);
        var user = store.Users.FirstOrDefault(item => string.Equals(item.Username, currentUsername?.Trim(), StringComparison.Ordinal));
        return (store.Users.Count > 0, currentUsername, user?.IsAdmin ?? false, user?.ForcePasswordChange ?? false);
    }

    public async Task SetupAsync(string username, string password, CancellationToken cancellationToken)
    {
        ValidateUsernameAndPassword(username, password);

        var store = await LoadStoreAsync(cancellationToken);
        if (store.Users.Count > 0)
        {
            throw new InvalidOperationException("登录账号已存在。");
        }

        store.Users.Add(BuildUser(username.Trim(), password, isAdmin: true, forcePasswordChange: false));
        await SaveStoreAsync(store, cancellationToken);
    }

    public async Task CreateUserAsync(string username, CancellationToken cancellationToken)
    {
        ValidateUsernameAndPassword(username, DefaultPassword);

        var store = await LoadStoreAsync(cancellationToken);
        if (store.Users.Any(item => string.Equals(item.Username, username.Trim(), StringComparison.OrdinalIgnoreCase)))
        {
            throw new InvalidOperationException("用户名已存在。");
        }

        store.Users.Add(BuildUser(username.Trim(), DefaultPassword, isAdmin: false, forcePasswordChange: true));
        await SaveStoreAsync(store, cancellationToken);
    }

    public async Task<List<UserInfo>> GetUsersAsync(CancellationToken cancellationToken)
    {
        var store = await LoadStoreAsync(cancellationToken);
        return store.Users
            .OrderByDescending(item => item.IsAdmin)
            .ThenBy(item => item.Username, StringComparer.OrdinalIgnoreCase)
            .Select(item => new UserInfo(item.Username, item.IsAdmin))
            .ToList();
    }

    public async Task<AuthValidationResult> ValidateAsync(string username, string password, CancellationToken cancellationToken)
    {
        var store = await LoadStoreAsync(cancellationToken);
        var user = store.Users.FirstOrDefault(item => string.Equals(item.Username, username?.Trim(), StringComparison.Ordinal));
        if (user is null)
        {
            return new AuthValidationResult(false, false, false);
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
            user.IsAdmin,
            user.ForcePasswordChange);
    }

    public async Task<bool> IsAdminAsync(string username, CancellationToken cancellationToken)
    {
        var store = await LoadStoreAsync(cancellationToken);
        return store.Users.Any(item =>
            string.Equals(item.Username, username?.Trim(), StringComparison.Ordinal) &&
            item.IsAdmin);
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

    private async Task<AuthStore> LoadStoreAsync(CancellationToken cancellationToken)
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
                    IsAdmin = true
                }
            ]
        };
    }

    private async Task SaveStoreAsync(AuthStore store, CancellationToken cancellationToken)
    {
        var json = JsonSerializer.Serialize(store);
        var plain = Encoding.UTF8.GetBytes(json);
        var encrypted = Protect(plain);
        await File.WriteAllBytesAsync(_authPath, encrypted, cancellationToken);
    }

    private static AuthUserRecord BuildUser(string username, string password, bool isAdmin, bool forcePasswordChange)
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
            IsAdmin = isAdmin,
            ForcePasswordChange = forcePasswordChange
        };
    }

    private static void ReplacePassword(AuthUserRecord user, string password)
    {
        var updated = BuildUser(user.Username, password, user.IsAdmin, user.ForcePasswordChange);
        user.Salt = updated.Salt;
        user.PasswordHash = updated.PasswordHash;
        user.Iterations = updated.Iterations;
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

    public sealed record AuthValidationResult(bool Success, bool IsAdmin, bool ForcePasswordChange);
    public sealed record UserInfo(string Username, bool IsAdmin);

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
        public bool ForcePasswordChange { get; set; }
    }

    private sealed class LegacyAuthRecord
    {
        public string Username { get; set; } = "";
        public string Salt { get; set; } = "";
        public string PasswordHash { get; set; } = "";
        public int Iterations { get; set; }
    }
}

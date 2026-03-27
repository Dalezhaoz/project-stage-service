using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace ProjectStageService.Services;

public static class AgentPayloadProtector
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    public static AgentEncryptedEnvelope Encrypt<T>(T payload, string secret)
    {
        if (string.IsNullOrWhiteSpace(secret))
        {
            throw new InvalidOperationException("Agent 密钥不能为空。");
        }

        var plain = JsonSerializer.SerializeToUtf8Bytes(payload, JsonOptions);
        var nonce = RandomNumberGenerator.GetBytes(12);
        var cipher = new byte[plain.Length];
        var tag = new byte[16];
        var key = DeriveKey(secret);

        using var aes = new AesGcm(key, 16);
        aes.Encrypt(nonce, plain, cipher, tag);

        return new AgentEncryptedEnvelope
        {
            Version = 1,
            Nonce = Convert.ToBase64String(nonce),
            Ciphertext = Convert.ToBase64String(cipher),
            Tag = Convert.ToBase64String(tag)
        };
    }

    private static byte[] DeriveKey(string secret)
    {
        return SHA256.HashData(Encoding.UTF8.GetBytes(secret.Trim()));
    }
}

public sealed class AgentEncryptedEnvelope
{
    public int Version { get; set; } = 1;
    public string Nonce { get; set; } = "";
    public string Ciphertext { get; set; } = "";
    public string Tag { get; set; } = "";
}

public sealed class AgentSourceConfig
{
    public string DatabaseType { get; set; } = "SQL Server";
    public string Host { get; set; } = "localhost";
    public int Port { get; set; } = 1433;
    public string Username { get; set; } = "";
    public string Password { get; set; } = "";
}

public sealed class AgentTargetConfig
{
    public string Host { get; set; } = "";
    public int Port { get; set; } = 1433;
    public string DatabaseName { get; set; } = "";
    public string Username { get; set; } = "";
    public string Password { get; set; } = "";
}

public sealed class AgentSyncPayload
{
    public string ServerName { get; set; } = "";
    public AgentSourceConfig Source { get; set; } = new();
    public AgentTargetConfig Target { get; set; } = new();
}

public sealed class AgentQueryDefinition
{
    public List<string> RequiredTables { get; set; } = [];
    public string StageQuerySql { get; set; } = "";
    public string ExistingTablesSql { get; set; } = "";
    public string RegistrationTablePattern { get; set; } = "";
    public string AdmissionTicketTablePattern { get; set; } = "";
}

public sealed class AgentQueryPayload
{
    public string ServerName { get; set; } = "";
    public AgentSourceConfig Source { get; set; } = new();
    public AgentQueryDefinition Definition { get; set; } = new();
}

public sealed class AgentTestPayload
{
    public string ServerName { get; set; } = "";
    public AgentSourceConfig Source { get; set; } = new();
    public AgentQueryDefinition Definition { get; set; } = new();
}

public sealed class AgentQueryResponse
{
    public int VisitedDatabases { get; set; }
    public int MatchedDatabases { get; set; }
    public List<AgentQueryRecord> Records { get; set; } = [];
}

public sealed class AgentQueryRecord
{
    public string DatabaseName { get; set; } = "";
    public Dictionary<string, string> Values { get; set; } = [];
    public Dictionary<string, int> Metrics { get; set; } = [];
}

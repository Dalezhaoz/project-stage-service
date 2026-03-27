using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace StageAgentService;

public sealed class AgentPayloadProtector
{
    private readonly string _secret;
    private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };

    public AgentPayloadProtector(IConfiguration configuration)
    {
        _secret = configuration["AgentSecret"]?.Trim() ?? "";
    }

    public T Decrypt<T>(AgentEncryptedEnvelope envelope)
    {
        if (string.IsNullOrWhiteSpace(_secret))
        {
            throw new InvalidOperationException("AgentSecret 未配置。");
        }

        if (envelope.Version != 1)
        {
            throw new InvalidOperationException("不支持的加密版本。");
        }

        var nonce = Convert.FromBase64String(envelope.Nonce);
        var cipher = Convert.FromBase64String(envelope.Ciphertext);
        var tag = Convert.FromBase64String(envelope.Tag);
        var plain = new byte[cipher.Length];
        var key = SHA256.HashData(Encoding.UTF8.GetBytes(_secret));

        using var aes = new AesGcm(key, 16);
        aes.Decrypt(nonce, cipher, tag, plain);

        return JsonSerializer.Deserialize<T>(plain, JsonOptions)
            ?? throw new InvalidOperationException("解密后的请求体无效。");
    }
}

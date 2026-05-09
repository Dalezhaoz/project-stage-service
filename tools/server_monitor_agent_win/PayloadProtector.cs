using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Configuration;

namespace ServerMonitorAgent;

public sealed class PayloadProtector
{
    private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };
    private readonly string _secret;

    public PayloadProtector(IConfiguration configuration)
    {
        _secret = configuration["Secret"]?.Trim() ?? "";
    }

    public T Decrypt<T>(AgentEncryptedEnvelope envelope)
    {
        if (string.IsNullOrWhiteSpace(_secret))
        {
            throw new InvalidOperationException("Secret is not configured.");
        }

        if (envelope.Version != 1)
        {
            throw new InvalidOperationException("Unsupported envelope version.");
        }

        var nonce = Convert.FromBase64String(envelope.Nonce);
        var cipher = Convert.FromBase64String(envelope.Ciphertext);
        var tag = Convert.FromBase64String(envelope.Tag);
        var plain = new byte[cipher.Length];
        var key = SHA256.HashData(Encoding.UTF8.GetBytes(_secret));

        using var aes = new AesGcm(key, 16);
        aes.Decrypt(nonce, cipher, tag, plain);

        return JsonSerializer.Deserialize<T>(plain, JsonOptions)
            ?? throw new InvalidOperationException("Invalid decrypted payload.");
    }
}

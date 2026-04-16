using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;

namespace DingTalkProxy;

public sealed class HeartbeatService(IConfiguration config, ILogger<HeartbeatService> logger) : BackgroundService
{
    private const int HeartbeatInterval = 30; // seconds

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var port = config.GetValue<int>("Port", 9100);
        var mainServer = (config["MainServer"] ?? "").TrimEnd('/');
        var fixedIp = config["ProxyIp"] ?? "";

        if (string.IsNullOrWhiteSpace(mainServer))
        {
            logger.LogWarning("[心跳] 未配置 MainServer，跳过自动注册。需手动在主服务配置 ProxyUrl。");
            return;
        }

        var registerUrl = $"{mainServer}/api/dingtalk/register-proxy";
        string? lastIp = null;
        var failCount = 0;

        using var client = new HttpClient { Timeout = TimeSpan.FromSeconds(10) };

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var localIp = string.IsNullOrWhiteSpace(fixedIp)
                    ? GetLocalIp(mainServer)
                    : fixedIp;

                if (localIp is null)
                {
                    logger.LogWarning("[心跳] 无法获取本机 IP，等待重试...");
                    await Task.Delay(TimeSpan.FromSeconds(HeartbeatInterval), stoppingToken);
                    continue;
                }

                var proxyUrl = $"http://{localIp}:{port}";
                var payload = JsonSerializer.Serialize(new { proxyUrl, token = "" });
                var content = new StringContent(payload, Encoding.UTF8, "application/json");

                var response = await client.PostAsync(registerUrl, content, stoppingToken);
                response.EnsureSuccessStatusCode();

                if (localIp != lastIp)
                {
                    if (lastIp is not null)
                        logger.LogInformation("[心跳] IP 已变更 {Old} → {New}，已更新注册", lastIp, localIp);
                    else
                        logger.LogInformation("[心跳] 已注册到主服务 {ProxyUrl}", proxyUrl);
                    lastIp = localIp;
                }

                failCount = 0;
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                failCount++;
                if (failCount <= 3 || failCount % 10 == 0)
                    logger.LogWarning("[心跳] 注册失败 #{Count} {Message}", failCount, ex.Message);
            }

            await Task.Delay(TimeSpan.FromSeconds(HeartbeatInterval), stoppingToken);
        }
    }

    /// <summary>
    /// 通过向主服务建立 UDP socket 探测本机出口 IP（VPN 路由变化后会自动跟随）。
    /// </summary>
    private static string? GetLocalIp(string serverUrl)
    {
        try
        {
            var uri = new Uri(serverUrl);
            var host = uri.Host;
            var port = uri.Port > 0 ? uri.Port : 80;
            using var socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
            socket.Connect(host, port);
            return (socket.LocalEndPoint as IPEndPoint)?.Address.ToString();
        }
        catch
        {
            return null;
        }
    }
}

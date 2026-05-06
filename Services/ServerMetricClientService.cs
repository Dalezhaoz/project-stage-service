using System.Text;
using System.Text.Json;
using ProjectStageService.Models;

namespace ProjectStageService.Services;

public sealed class ServerMetricClientService
{
    private static readonly HttpClient HttpClient = new() { Timeout = TimeSpan.FromSeconds(20) };
    private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };
    private readonly ILogger<ServerMetricClientService> _logger;

    public ServerMetricClientService(ILogger<ServerMetricClientService> logger)
    {
        _logger = logger;
    }

    public async Task<AgentMetricResponse> QueryAsync(MonitorServerConfig server, CancellationToken cancellationToken)
    {
        if (!server.Enabled)
        {
            throw new InvalidOperationException($"Server {server.Name} monitor is disabled.");
        }

        if (server.MonitorPort <= 0)
        {
            throw new InvalidOperationException($"Server {server.Name} monitor port is invalid.");
        }

        if (string.IsNullOrWhiteSpace(server.MonitorSecret))
        {
            throw new InvalidOperationException($"Server {server.Name} monitor secret is missing.");
        }

        var payload = new AgentMetricPayload { ServerName = server.Name };
        var envelope = AgentPayloadProtector.Encrypt(payload, server.MonitorSecret);
        var content = new StringContent(
            JsonSerializer.Serialize(envelope, new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase }),
            Encoding.UTF8,
            "application/json");

        var url = $"http://{server.Host}:{server.MonitorPort}/metrics";
        _logger.LogInformation("Calling monitor endpoint {Url} for monitor server {ServerName}.", url, server.Name);
        using var response = await HttpClient.PostAsync(url, content, cancellationToken);
        var body = await response.Content.ReadAsStringAsync(cancellationToken);
        if (!response.IsSuccessStatusCode)
        {
            throw new InvalidOperationException($"HTTP {(int)response.StatusCode}: {body}");
        }

        var metric = JsonSerializer.Deserialize<AgentMetricResponse>(body, JsonOptions);
        if (metric is null)
        {
            throw new InvalidOperationException("Monitor agent returned invalid data.");
        }

        return metric;
    }
}

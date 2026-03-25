namespace StageAgentService;

public class SyncRequest
{
    public string ServerName { get; set; } = "";
    public SourceConfig Source { get; set; } = new();
    public TargetConfig Target { get; set; } = new();
}

public class SourceConfig
{
    public string DatabaseType { get; set; } = "SQL Server";
    public string Host { get; set; } = "localhost";
    public int Port { get; set; } = 1433;
    public string Username { get; set; } = "";
    public string Password { get; set; } = "";
}

public class TargetConfig
{
    public string Host { get; set; } = "";
    public int Port { get; set; } = 1433;
    public string DatabaseName { get; set; } = "";
    public string Username { get; set; } = "";
    public string Password { get; set; } = "";
}

public class StageRecord
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
    public int RegistrationCount { get; set; }
    public int AdmissionTicketCount { get; set; }
}

namespace ProjectStageService.Models;

public sealed class StageServerConfig
{
    public string Name { get; set; } = "";
    public string DatabaseType { get; set; } = "SQL Server";
    public string Host { get; set; } = "";
    public int Port { get; set; } = 1433;
    public string DatabaseName { get; set; } = "";
    public string Username { get; set; } = "";
    public string Password { get; set; } = "";
    public bool Enabled { get; set; } = true;
}

public sealed class TestConnectionRequest
{
    public StageServerConfig Server { get; set; } = new();
}

public sealed class ProjectStageQueryRequest
{
    public List<StageServerConfig> Servers { get; set; } = [];
    public List<string> StatusFilters { get; set; } = ["正在进行", "即将开始"];
    public string StageKeyword { get; set; } = "";
    public List<string> StageNames { get; set; } = [];
    public string ProjectKeyword { get; set; } = "";
    public DateTime? RangeStart { get; set; }
    public DateTime? RangeEnd { get; set; }
}

public sealed class ProjectStageRecord
{
    public string ServerName { get; set; } = "";
    public string DatabaseName { get; set; } = "";
    public string ProjectName { get; set; } = "";
    public string StageName { get; set; } = "";
    public DateTime StartTime { get; set; }
    public DateTime EndTime { get; set; }
    public string Status { get; set; } = "";
}

public sealed class ProjectStageSummary
{
    public List<ProjectStageRecord> Records { get; set; } = [];
    public int EnabledServers { get; set; }
    public int VisitedDatabases { get; set; }
    public int MatchedDatabases { get; set; }
    public int EndedCount { get; set; }
    public int OngoingCount { get; set; }
    public int UpcomingCount { get; set; }
}

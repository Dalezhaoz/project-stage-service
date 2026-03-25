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
    public int AgentPort { get; set; }
}

public sealed class TestConnectionRequest
{
    public StageServerConfig Server { get; set; } = new();
}

public sealed class LoginRequest
{
    public string Username { get; set; } = "";
    public string Password { get; set; } = "";
}

public sealed class SetupAuthRequest
{
    public string Username { get; set; } = "";
    public string Password { get; set; } = "";
    public string ConfirmPassword { get; set; } = "";
}

public sealed class CreateUserRequest
{
    public string Username { get; set; } = "";
}

public sealed class ChangePasswordRequest
{
    public string CurrentPassword { get; set; } = "";
    public string NewPassword { get; set; } = "";
    public string ConfirmPassword { get; set; } = "";
}

public sealed class ResetPasswordRequest
{
    public string Username { get; set; } = "";
}

public sealed class SummaryStoreConfig
{
    public string Host { get; set; } = "";
    public int Port { get; set; } = 1433;
    public string DatabaseName { get; set; } = "";
    public string Username { get; set; } = "";
    public string Password { get; set; } = "";
    public bool Enabled { get; set; }
}

public sealed class SummaryStoreTestRequest
{
    public SummaryStoreConfig Config { get; set; } = new();
}

public sealed class ProjectStageQueryRequest
{
    public List<StageServerConfig> Servers { get; set; } = [];
    public List<string> StatusFilters { get; set; } = ["正在进行", "即将开始"];
    public string TimeMatchMode { get; set; } = "overlap";
    public string StageKeyword { get; set; } = "";
    public List<string> StageNames { get; set; } = [];
    public List<string> ServerNames { get; set; } = [];
    public string ProjectKeyword { get; set; } = "";
    public string ServerKeyword { get; set; } = "";
    public string DatabaseKeyword { get; set; } = "";
    public string ExamCodeKeyword { get; set; } = "";
    public DateTime? RangeStart { get; set; }
    public DateTime? RangeEnd { get; set; }
    public List<int> DayOffsets { get; set; } = [];
}

public sealed class ProjectStageRefreshRequest
{
    public List<StageServerConfig> Servers { get; set; } = [];
}

public sealed class BoardCountRequest
{
    public ProjectStageQueryRequest Query { get; set; } = new();
    public bool IncludeRegistrationCount { get; set; }
    public bool IncludeAdmissionTicketCount { get; set; }
    public List<BoardCountTarget> Targets { get; set; } = [];
}

public sealed class BoardCountTarget
{
    public string ServerName { get; set; } = "";
    public string DatabaseName { get; set; } = "";
    public string ExamCode { get; set; } = "";
}

public sealed class ServerCountUpdate
{
    public string ServerName { get; set; } = "";
    public string DatabaseName { get; set; } = "";
    public string ExamCode { get; set; } = "";
    public int? RegistrationCount { get; set; }
    public int? AdmissionTicketCount { get; set; }
}

public sealed class ProjectStageRecord
{
    public string ServerName { get; set; } = "";
    public string DatabaseName { get; set; } = "";
    public string ExamCode { get; set; } = "";
    public string ProjectName { get; set; } = "";
    public string StageName { get; set; } = "";
    public DateTime StartTime { get; set; }
    public DateTime EndTime { get; set; }
    public string Status { get; set; } = "";
    public int RegistrationCount { get; set; }
    public int AdmissionTicketCount { get; set; }
}

public sealed class ProjectStageGroup
{
    public string ServerName { get; set; } = "";
    public string DatabaseName { get; set; } = "";
    public string ExamCode { get; set; } = "";
    public string ProjectName { get; set; } = "";
    public DateTime StartTime { get; set; }
    public DateTime EndTime { get; set; }
    public int RegistrationCount { get; set; }
    public int AdmissionTicketCount { get; set; }
    public List<string> Statuses { get; set; } = [];
    public List<ProjectStageRecord> Stages { get; set; } = [];
}

public sealed class ProjectStageSummary
{
    public List<ProjectStageRecord> Records { get; set; } = [];
    public List<ProjectStageGroup> Groups { get; set; } = [];
    public int EnabledServers { get; set; }
    public int VisitedDatabases { get; set; }
    public int MatchedDatabases { get; set; }
    public int EndedCount { get; set; }
    public int OngoingCount { get; set; }
    public int UpcomingCount { get; set; }
}

public sealed class ProjectStageCacheInfo
{
    public bool HasData { get; set; }
    public DateTime? RefreshedAt { get; set; }
    public int EnabledServers { get; set; }
    public int VisitedDatabases { get; set; }
    public int MatchedDatabases { get; set; }
    public int RecordCount { get; set; }
}

public sealed class ProjectStageRefreshResult
{
    public DateTime RefreshedAt { get; set; }
    public int EnabledServers { get; set; }
    public int VisitedDatabases { get; set; }
    public int MatchedDatabases { get; set; }
    public int RecordCount { get; set; }
    public List<AgentRefreshResult> AgentResults { get; set; } = [];
}

public sealed class AgentRefreshResult
{
    public string ServerName { get; set; } = "";
    public bool Success { get; set; }
    public int Databases { get; set; }
    public int Records { get; set; }
    public string? Error { get; set; }
}

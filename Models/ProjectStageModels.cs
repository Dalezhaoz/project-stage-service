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
    public string AgentSecret { get; set; } = "";
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
    public string Role { get; set; } = "external";
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

public sealed class AllowUserRefreshRequest
{
    public bool Allow { get; set; }
}

public sealed class UpdateUserDingTalkRequest
{
    public string Username { get; set; } = "";
    public string WebhookUrl { get; set; } = "";
    public string Secret { get; set; } = "";
    public string[]? TodayNotifyTimes { get; set; }
    public string[]? NextDayNotifyTimes { get; set; }
}

public sealed class TestUserDingTalkRequest
{
    public string Username { get; set; } = "";
}

public sealed class DingTalkProxyRegistrationRequest
{
    public string ProxyUrl { get; set; } = "";
    public string Token { get; set; } = "";
}

public sealed class UpdateUserCanAssignRequest
{
    public string Username { get; set; } = "";
    public bool CanAssign { get; set; }
}

public sealed class RenameAppServerRequest
{
    public string OldName { get; set; } = "";
    public string NewName { get; set; } = "";
}

public sealed class AppServerOptionRequest
{
    public List<string> Options { get; set; } = [];
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

public sealed class ScheduleConfig
{
    public bool StageRefreshEnabled { get; set; }
    public List<string> StageRefreshTimes { get; set; } = ["04:00"];
    public bool RefreshIntervalEnabled { get; set; }
    public int RefreshIntervalMinutes { get; set; } = 60;
    public bool CountRefreshEnabled { get; set; }
    public List<string> CountRefreshTimes { get; set; } = ["06:00"];
    public bool DingTalkEnabled { get; set; }
    public List<string> DingTalkNotifyTimes { get; set; } = ["08:00"];
    public bool DingTalkNextDayEnabled { get; set; }
    public List<string> DingTalkNextDayNotifyTimes { get; set; } = ["20:00"];
    public bool UnassignedNotifyEnabled { get; set; }
    public List<string> UnassignedNotifyTimes { get; set; } = ["09:00"];
    public bool EndingReminderEnabled { get; set; }
    public DingTalkConfig? DingTalkConfig { get; set; }
}

public sealed class DingTalkConfig
{
    public string WebhookUrl { get; set; } = "";
    public string Secret { get; set; } = "";
    public string ProxyUrl { get; set; } = "";
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

public sealed class StageWorkloadConfigRecord
{
    public string StageName { get; set; } = "";
    public decimal Hours { get; set; }
    public DateTime? UpdatedAt { get; set; }
}

public sealed class SaveStageWorkloadConfigsRequest
{
    public List<StageWorkloadConfigRecord> Items { get; set; } = [];
}

public sealed class WorkloadStatsRequest
{
    public string Granularity { get; set; } = "month";
    public string Maintainer { get; set; } = "";
    public DateTime? RangeStart { get; set; }
    public DateTime? RangeEnd { get; set; }
}

public sealed class WorkloadStatsResponse
{
    public decimal TotalHours { get; set; }
    public int TotalStages { get; set; }
    public List<WorkloadPersonSummary> People { get; set; } = [];
    public List<WorkloadPeriodSummary> Periods { get; set; } = [];
    public List<WorkloadStageSummary> Stages { get; set; } = [];
}

public sealed class WorkloadPersonSummary
{
    public string Maintainer { get; set; } = "";
    public decimal Hours { get; set; }
    public int StageCount { get; set; }
    public decimal Percent { get; set; }
}

public sealed class WorkloadPeriodSummary
{
    public string PeriodKey { get; set; } = "";
    public string PeriodLabel { get; set; } = "";
    public decimal Hours { get; set; }
    public int StageCount { get; set; }
}

public sealed class WorkloadStageSummary
{
    public string StageName { get; set; } = "";
    public decimal Hours { get; set; }
    public int StageCount { get; set; }
    public decimal ConfiguredHours { get; set; }
}

public sealed class AgentRefreshResult
{
    public string ServerName { get; set; } = "";
    public bool Success { get; set; }
    public int Databases { get; set; }
    public int Records { get; set; }
    public string? Error { get; set; }
}

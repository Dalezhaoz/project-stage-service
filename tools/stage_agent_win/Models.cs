namespace StageAgentService;

public class AgentEncryptedEnvelope
{
    public int Version { get; set; } = 1;
    public string Nonce { get; set; } = "";
    public string Ciphertext { get; set; } = "";
    public string Tag { get; set; } = "";
}

public class SyncRequest
{
    public string ServerName { get; set; } = "";
    public SourceConfig Source { get; set; } = new();
    public TargetConfig Target { get; set; } = new();
}

public class TestRequest
{
    public string ServerName { get; set; } = "";
    public SourceConfig Source { get; set; } = new();
    public QueryDefinition Definition { get; set; } = new();
}

public class QueryRequest
{
    public string ServerName { get; set; } = "";
    public SourceConfig Source { get; set; } = new();
    public QueryDefinition Definition { get; set; } = new();
}

public class QueryDefinition
{
    public List<string> RequiredTables { get; set; } = [];
    public string StageQuerySql { get; set; } = "";
    public string ExistingTablesSql { get; set; } = "";
    public string RegistrationTablePattern { get; set; } = "";
    public string AdmissionTicketTablePattern { get; set; } = "";
}

public class QueryResponse
{
    public int VisitedDatabases { get; set; }
    public int MatchedDatabases { get; set; }
    public List<QueryRecord> Records { get; set; } = [];
}

public class QueryRecord
{
    public string DatabaseName { get; set; } = "";
    public Dictionary<string, string> Values { get; set; } = [];
    public Dictionary<string, int> Metrics { get; set; } = [];
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

using System.Drawing;
using System.Text.RegularExpressions;
using System.Windows.Forms;

namespace DingTalkProxy;

public sealed partial class TrayApp : ApplicationContext
{
    private static readonly string LogDirectory = Path.Combine(AppContext.BaseDirectory, "logs");

    private readonly NotifyIcon _tray;
    private readonly HeartbeatService _heartbeat;
    private readonly ProxyListenerService _proxy;
    private readonly AppConfig _config;
    private readonly List<string> _logs = [];
    private readonly System.Windows.Forms.Timer _statusTimer;
    private DateTimeOffset? _lastHeartbeatWarningAt;

    private ToolStripMenuItem _statusItem = null!;
    private ToolStripMenuItem _ipItem = null!;

    public TrayApp(AppConfig config)
    {
        _config = config;

        void Log(string msg)
        {
            var line = $"[{DateTime.Now:HH:mm:ss}] {msg}";
            AddLogLine(line);
        }

        _heartbeat = new HeartbeatService(config, Log);
        _proxy = new ProxyListenerService(config, Log);

        _tray = new NotifyIcon
        {
            Icon = MakeIcon(Color.Gray),
            Visible = true,
            Text = "DingTalk Proxy"
        };

        BuildContextMenu();

        _heartbeat.Start();
        _proxy.Start();

        _statusTimer = new System.Windows.Forms.Timer { Interval = Math.Max(2, _config.WatchdogIntervalSeconds) * 1000 };
        _statusTimer.Tick += (_, _) =>
        {
            EnsureServicesHealthy();
            UpdateStatus();
        };
        _statusTimer.Start();

        AddLogLine($"[{DateTime.Now:HH:mm:ss}] Proxy tray app started.");
        UpdateStatus();
    }

    private void BuildContextMenu()
    {
        _statusItem = new ToolStripMenuItem("Starting...") { Enabled = false };
        _ipItem = new ToolStripMenuItem("IP: detecting...") { Enabled = false };

        var logsItem = new ToolStripMenuItem("View Logs", null, (_, _) => ShowLogs());
        var exitItem = new ToolStripMenuItem("Exit", null, (_, _) => Exit());

        var menu = new ContextMenuStrip();
        menu.Items.Add(_statusItem);
        menu.Items.Add(_ipItem);
        menu.Items.Add(new ToolStripSeparator());
        menu.Items.Add(logsItem);
        menu.Items.Add(new ToolStripSeparator());
        menu.Items.Add(exitItem);

        _tray.ContextMenuStrip = menu;
        _tray.DoubleClick += (_, _) => ShowLogs();
    }

    private void UpdateStatus()
    {
        var ip = _heartbeat.CurrentIp;
        var listenerOk = _proxy.IsListening;
        var registered = ip is not null && !_heartbeat.IsStale();
        var connected = listenerOk && registered;

        _tray.Icon = MakeIcon(connected ? Color.FromArgb(34, 197, 94) : Color.FromArgb(239, 68, 68));
        _tray.Text = connected
            ? $"DingTalk Proxy OK {ip}:{_config.Port}"
            : listenerOk
                ? "DingTalk Proxy listening, heartbeat retrying"
                : "DingTalk Proxy offline";

        _statusItem.Text = connected
            ? $"Online (port {_config.Port})"
            : listenerOk
                ? $"Listening on {_config.Port}, heartbeat retrying"
                : "Listener offline, watchdog recovering";
        _statusItem.ForeColor = connected ? Color.Green : Color.DarkOrange;
        _ipItem.Text = ip is not null ? $"IP: {ip}" : "IP: detecting...";
    }

    private void ShowLogs()
    {
        string[] lines;
        lock (_logs)
        {
            lines = [.. _logs];
        }

        var form = new Form
        {
            Text = "DingTalk Proxy Logs",
            Size = new Size(760, 420),
            StartPosition = FormStartPosition.CenterScreen,
            BackColor = Color.FromArgb(18, 18, 18),
            ForeColor = Color.White,
            FormBorderStyle = FormBorderStyle.SizableToolWindow
        };

        var box = new RichTextBox
        {
            Dock = DockStyle.Fill,
            ReadOnly = true,
            BackColor = Color.FromArgb(18, 18, 18),
            ForeColor = Color.FromArgb(220, 220, 220),
            Font = new Font("Consolas", 9.5f),
            ScrollBars = RichTextBoxScrollBars.Vertical,
            BorderStyle = BorderStyle.None,
            Text = lines.Length == 0 ? "(no logs yet)" : string.Join(Environment.NewLine, lines)
        };

        box.SelectAll();
        box.SelectionColor = Color.FromArgb(220, 220, 220);

        foreach (Match match in LogColorRegex().Matches(box.Text))
        {
            box.Select(match.Index, match.Length);
            box.SelectionColor = GetLogColor(match.Value);
        }

        box.Select(box.TextLength, 0);
        box.ScrollToCaret();

        form.Controls.Add(box);
        form.Show();
    }

    private void EnsureServicesHealthy()
    {
        if (!_proxy.IsListening)
        {
            AddLogLine($"[{DateTime.Now:HH:mm:ss}] Watchdog detected listener offline, restarting listener.");
            _proxy.Start();
        }

        if (!_heartbeat.IsRunning)
        {
            AddLogLine($"[{DateTime.Now:HH:mm:ss}] Watchdog detected heartbeat stopped, restarting heartbeat.");
            _heartbeat.Start();
        }
        else if (_heartbeat.IsStale() && _heartbeat.ConsecutiveFailures > 0)
        {
            var now = DateTimeOffset.Now;
            if (_lastHeartbeatWarningAt is null || now - _lastHeartbeatWarningAt > TimeSpan.FromMinutes(1))
            {
                _lastHeartbeatWarningAt = now;
                AddLogLine($"[{DateTime.Now:HH:mm:ss}] Heartbeat is stale, still retrying. Failures={_heartbeat.ConsecutiveFailures}.");
            }
        }
    }

    private void AddLogLine(string line)
    {
        lock (_logs)
        {
            _logs.Add(line);
            if (_logs.Count > 200)
                _logs.RemoveAt(0);
        }

        TryAppendLogFile(line);
    }

    private static void TryAppendLogFile(string line)
    {
        try
        {
            Directory.CreateDirectory(LogDirectory);
            var logPath = Path.Combine(LogDirectory, $"dingtalk_proxy_{DateTime.Now:yyyyMMdd}.log");
            File.AppendAllText(logPath, line + Environment.NewLine);
        }
        catch
        {
        }
    }

    private static Color GetLogColor(string line)
    {
        if (line.Contains("started", StringComparison.OrdinalIgnoreCase) ||
            line.Contains("registered", StringComparison.OrdinalIgnoreCase) ||
            line.Contains("success", StringComparison.OrdinalIgnoreCase) ||
            line.Contains("updated", StringComparison.OrdinalIgnoreCase))
        {
            return Color.FromArgb(34, 197, 94);
        }

        if (line.Contains("failed", StringComparison.OrdinalIgnoreCase) ||
            line.Contains("offline", StringComparison.OrdinalIgnoreCase) ||
            line.Contains("denied", StringComparison.OrdinalIgnoreCase))
        {
            return Color.FromArgb(239, 68, 68);
        }

        if (line.Contains("retry", StringComparison.OrdinalIgnoreCase) ||
            line.Contains("watchdog", StringComparison.OrdinalIgnoreCase) ||
            line.Contains("stale", StringComparison.OrdinalIgnoreCase))
        {
            return Color.FromArgb(234, 179, 8);
        }

        return Color.FromArgb(96, 165, 250);
    }

    [GeneratedRegex(@"[^\r\n]+")]
    private static partial Regex LogColorRegex();

    private void Exit()
    {
        _statusTimer.Stop();
        _heartbeat.Stop();
        _proxy.Stop();
        _tray.Visible = false;
        Application.Exit();
    }

    private static Icon MakeIcon(Color color)
    {
        var bmp = new Bitmap(16, 16);
        using var g = Graphics.FromImage(bmp);
        g.Clear(Color.Transparent);
        using var brush = new SolidBrush(color);
        g.FillEllipse(brush, 1, 1, 13, 13);
        return Icon.FromHandle(bmp.GetHicon());
    }
}

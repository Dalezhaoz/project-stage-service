using System.Drawing;
using System.Text.RegularExpressions;
using System.Windows.Forms;

namespace DingTalkProxy;

public sealed partial class TrayApp : ApplicationContext
{
    private readonly NotifyIcon _tray;
    private readonly HeartbeatService _heartbeat;
    private readonly ProxyListenerService _proxy;
    private readonly AppConfig _config;
    private readonly List<string> _logs = [];
    private readonly System.Windows.Forms.Timer _statusTimer;

    private ToolStripMenuItem _statusItem = null!;
    private ToolStripMenuItem _ipItem = null!;

    public TrayApp(AppConfig config)
    {
        _config = config;

        void Log(string msg)
        {
            var line = $"[{DateTime.Now:HH:mm:ss}] {msg}";
            lock (_logs) { _logs.Add(line); if (_logs.Count > 200) _logs.RemoveAt(0); }
        }

        _heartbeat = new HeartbeatService(config, Log);
        _proxy = new ProxyListenerService(config, Log);

        _tray = new NotifyIcon
        {
            Icon = MakeIcon(Color.Gray),
            Visible = true,
            Text = "DingTalk 转发代理"
        };

        BuildContextMenu();

        _heartbeat.Start();
        _proxy.Start();

        _statusTimer = new System.Windows.Forms.Timer { Interval = 5000 };
        _statusTimer.Tick += (_, _) => UpdateStatus();
        _statusTimer.Start();

        UpdateStatus();
    }

    private void BuildContextMenu()
    {
        _statusItem = new ToolStripMenuItem("正在启动...") { Enabled = false };
        _ipItem = new ToolStripMenuItem("IP: 探测中...") { Enabled = false };

        var logsItem = new ToolStripMenuItem("查看日志", null, (_, _) => ShowLogs());
        var exitItem = new ToolStripMenuItem("退出", null, (_, _) => Exit());

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
        var connected = ip is not null;

        _tray.Icon = MakeIcon(connected ? Color.FromArgb(34, 197, 94) : Color.FromArgb(239, 68, 68));
        _tray.Text = connected
            ? $"DingTalk 代理 ✓  {ip}:{_config.Port}"
            : "DingTalk 代理  未连接";

        _statusItem.Text = connected ? $"● 运行中（端口 {_config.Port}）" : "● 未连接主服务";
        _statusItem.ForeColor = connected ? Color.Green : Color.Red;
        _ipItem.Text = connected ? $"本机 IP：{ip}" : "IP：探测中...";
    }

    private void ShowLogs()
    {
        string[] lines;
        lock (_logs) { lines = [.. _logs]; }

        var form = new Form
        {
            Text = "DingTalk 转发代理  日志",
            Size = new Size(640, 400),
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
            Text = lines.Length == 0 ? "（暂无日志）" : string.Join(Environment.NewLine, lines)
        };

        // 着色
        box.SelectAll();
        box.SelectionColor = Color.FromArgb(220, 220, 220);
        foreach (Match m in LogColorRegex().Matches(box.Text))
        {
            box.Select(m.Index, m.Length);
            box.SelectionColor = m.Value.StartsWith('✅') ? Color.FromArgb(34, 197, 94)
                               : m.Value.StartsWith('❌') ? Color.FromArgb(239, 68, 68)
                               : m.Value.StartsWith('⚠') ? Color.FromArgb(234, 179, 8)
                               : Color.FromArgb(96, 165, 250);
        }
        box.Select(box.TextLength, 0);
        box.ScrollToCaret();

        form.Controls.Add(box);
        form.Show();
    }

    [GeneratedRegex(@"[✅❌⚠️🔄][^\r\n]*")]
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

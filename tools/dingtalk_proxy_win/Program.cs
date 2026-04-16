using System.Windows.Forms;
using DingTalkProxy;

Application.EnableVisualStyles();
Application.SetCompatibleTextRenderingDefault(false);

var config = new AppConfig();
Application.Run(new TrayApp(config));

App Server Site Reporter (IIS)
================================

1) 在主程序 appsettings.json 增加:
   "AppServerSiteReportToken": "一个强随机字符串"

2) 在应用服务器编辑 site_reporter_config.json:
   - MainServer: 主程序地址 (例如 http://172.31.78.175:5187)
   - Token: 与主程序 AppServerSiteReportToken 一致
   - ServerName: 可留空(默认机器名)
   - IntervalSeconds: 采集上报周期(建议 300)

3) 管理员运行:
   install_app_server_site_reporter_service.bat

4) 查看日志:
   logs\site_reporter.log

5) 主程序接口:
   POST /api/app-server-sites/report-agent
   GET  /api/app-server-sites
   POST /api/app-servers/auto-assign


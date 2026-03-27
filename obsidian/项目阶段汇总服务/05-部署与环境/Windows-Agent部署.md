# Windows Agent 部署

## 代码位置

- `tools/stage_agent_win/`

## 当前运行方式

- .NET Worker
- 建议作为 Windows Service 安装

## 当前配置文件

- `appsettings.json`

关键配置：

- `Port`
- `AgentSecret`

## 当前接口

- `GET /health`
- `POST /test`
- `POST /query`
- `POST /sync`

## 当前依赖

- .NET 8
- SQL Server 驱动
- MySQL 驱动

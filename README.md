# 项目阶段汇总服务

这是一个独立的 ASP.NET Core Web 服务，只负责：

- 配置多台 SQL Server 服务器
- 遍历服务器上的在线数据库
- 自动忽略不包含业务表的数据库
- 汇总项目阶段状态
- 导出 Excel

## 技术选型

- ASP.NET Core Minimal API
- Microsoft.Data.SqlClient
- ClosedXML

不用 `pyodbc`，更适合 Windows 服务器环境。

## 启动

```bash
dotnet restore
dotnet run
```

默认地址：

- `http://localhost:5000`
- `https://localhost:5001`

## 当前支持

- 状态筛选：
  - 正在进行 + 即将开始
  - 全部
  - 只看正在进行
  - 只看即将开始
- 阶段关键字筛选
- 项目关键字筛选
- Excel 导出

## 业务表要求

只有数据库中同时存在以下三张表时，才会参与统计：

- `EI_ExamTreeDesc`
- `web_SR_CodeItem`
- `WEB_SR_SetTime`

## 返回字段

- 服务器
- 数据库
- 项目名称
- 阶段名称
- 开始时间
- 结束时间
- 当前状态

# 项目阶段汇总服务

这是一个独立的 ASP.NET Core Web 服务，只负责：

- 配置多台 SQL Server / MySQL 服务器
- 遍历服务器上的数据库
- 自动忽略不包含业务表的数据库
- 汇总项目阶段状态
- 导出 Excel

## 技术选型

- ASP.NET Core Minimal API
- Microsoft.Data.SqlClient
- MySqlConnector
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

- 多服务器配置，本地加密保存
- 状态复选筛选：
  - 已结束
  - 正在进行
  - 即将开始
- 时间范围筛选
  - 查询与时间范围有交集的项目阶段
- 阶段关键字筛选
- 阶段勾选筛选
  - 先从服务器加载可用阶段，再按阶段名称勾选
- 项目关键字筛选
- Excel 导出

## SQL Server 业务表要求

只有数据库中同时存在以下三张表时，才会参与统计：

- `EI_ExamTreeDesc`
- `web_SR_CodeItem`
- `WEB_SR_SetTime`

## MySQL 业务表要求

只有数据库中同时存在以下两张表时，才会参与统计：

- `mgt_exam_organize`
- `mgt_exam_step`

MySQL 当前使用的查询语句等价于：

```sql
SELECT a.name, b.name, b.start_date, b.end_date
FROM mgt_exam_organize a, mgt_exam_step b
```

服务会在每个符合条件的库中执行该逻辑，并统一汇总结果。

## 返回字段

- 服务器
- 数据库
- 项目名称
- 阶段名称
- 开始时间
- 结束时间
- 当前状态

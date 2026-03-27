# Agent 协议

## 当前健康检查

- `GET /health`

返回：

- `{"status":"ok"}`

## 当前测试接口

- `POST /test`

用途：

- 解密请求
- 读取 `QueryDefinition.RequiredTables`
- 返回可匹配数据库数量

## 当前查询接口

- `POST /query`

用途：

- 解密请求
- 本地执行查询定义
- 返回标准化记录与人数指标

## 当前兼容接口

- `POST /sync`

用途：

- 兼容旧同步路径

建议：

- 后续逐步以 `/query` 为主

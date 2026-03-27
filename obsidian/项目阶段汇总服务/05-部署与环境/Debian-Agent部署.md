# Debian Agent 部署

## 代码位置

- `tools/stage_agent/stage_agent.py`

## 当前依赖

```bash
pip install pymysql pymssql cryptography
```

## 当前运行要求

- 本机能访问源数据库
- 本机能访问中心 SQL Server
- 对主服务开放 Agent HTTP 端口

## 当前必须配置

```bash
export STAGE_AGENT_SECRET='你的密钥'
```

## 当前接口

- `GET /health`
- `POST /test`
- `POST /query`
- `POST /sync`

## 推荐部署方式

- systemd 常驻

## systemd 建议项

- WorkingDirectory 指向 agent 目录
- ExecStart 使用固定 python3
- Environment 设置 `STAGE_AGENT_SECRET`
- Restart=always

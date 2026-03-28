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

## systemd 部署步骤

### 1. 创建服务文件

```bash
cat > /etc/systemd/system/stage-agent.service << 'EOF'
[Unit]
Description=Stage Agent
After=network.target

[Service]
Type=simple
WorkingDirectory=/data/tools/stage_agent
ExecStart=/usr/bin/python3 /data/tools/stage_agent/stage_agent.py 5200
Environment=STAGE_AGENT_SECRET=你的密钥
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF
```

### 2. 启用并启动

```bash
systemctl daemon-reload
systemctl enable stage-agent
systemctl start stage-agent
systemctl status stage-agent
```

### 3. 查看日志

```bash
journalctl -u stage-agent -n 50 --no-pager
```

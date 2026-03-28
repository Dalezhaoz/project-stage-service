# Agent 连不上

## 排查步骤

### 1. 网络连通性

```powershell
# Windows 客户端测试
Test-NetConnection <IP> -Port <端口>
```

- TCP 和 Ping 都失败 → 安全组/防火墙/网络不通
- TCP 失败但 Ping 成功 → 端口未监听或被防火墙拦截
- TCP 连接被"积极拒绝" → 网络通但目标端口无进程监听

### 2. 确认 Agent 是否在监听

```bash
# Linux
ss -tlnp | grep <端口>

# Windows
netstat -ano | findstr <端口>
```

### 3. 端口被占用

```bash
# 查找并杀掉占用进程
lsof -i :<端口> -t | xargs kill -9
```

### 4. 防火墙规则

```powershell
# Windows 添加入站规则
netsh advfirewall firewall add rule name="StageAgent" dir=in action=allow protocol=TCP localport=5100
```

云服务器还需在安全组中开放对应端口。

### 5. 常见 HTTP 500 错误

| 错误信息 | 原因 | 解决 |
|---------|------|------|
| `STAGE_AGENT_SECRET 未配置` | 环境变量未设置 | 设置 `STAGE_AGENT_SECRET` |
| `不支持的加密版本` | 主服务与 Agent 版本不匹配 | 确保两端代码都是最新版 |
| `Microsoft.Data.SqlClient is not supported on this platform` | 发布时未指定 RID | 使用 `dotnet publish -r win-x64 --self-contained` |

### 6. 其他检查项

- 密钥是否与主服务端配置一致
- 健康检查 `GET /health` 是否返回 200
- systemd / Windows Service 状态是否正常

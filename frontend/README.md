# 新前端说明

这个目录是并行迁移用的新前端，目标是：

- 保持旧版 `wwwroot` 继续可用
- 用现代前端方案逐步替换旧页面
- 不一次性重写所有功能

## 技术栈

- Vue 3
- Vite
- TypeScript
- Vue Router
- Pinia

## 开发

先启动 ASP.NET Core 后端，再启动前端开发服务器：

```powershell
dotnet run
```

```powershell
cd frontend
cmd /c npm.cmd install
cmd /c npm.cmd run dev
```

默认开发地址：

- 新前端开发服务器：`http://localhost:5173/`
- 旧版页面：`http://localhost:5000/`

Vite 已经代理：

- `/api`
- `/health`

默认代理目标是 `http://localhost:5000`

## 构建

```powershell
cd frontend
cmd /c npm.cmd install
cmd /c npm.cmd run build
```

构建产物输出到 `frontend/dist`。

如果后端运行时检测到这个目录存在，会自动把新前端挂到：

- `http://localhost:5000/new`

## 迁移建议

建议按下面顺序逐步迁移：

1. 登录态与当前用户信息
2. 顶部筛选栏
3. 统计摘要卡片
4. 主表格与分组卡片
5. 配置弹窗与管理操作

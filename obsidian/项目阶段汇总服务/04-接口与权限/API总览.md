# API 总览

## 认证

- `GET /api/auth/status`
- `POST /api/auth/setup`
- `POST /api/auth/login`
- `POST /api/auth/logout`
- `POST /api/auth/change-password`
- `POST /api/auth/reset-password`
- `GET /api/auth/users`
- `POST /api/auth/users`

## 配置

- `GET /api/servers`
- `POST /api/servers`
- `POST /api/test`
- `GET /api/summary-store`
- `POST /api/summary-store`
- `POST /api/summary-store/test`
- `GET /api/schedule`
- `POST /api/schedule`

## 查询与刷新

- `GET /api/cache-info`
- `POST /api/refresh`
- `POST /api/query`
- `POST /api/stages`
- `POST /api/export`
- `POST /api/board-counts`

## 项目元数据

- `GET /api/project-metadata`
- `POST /api/project-metadata`
- `GET /api/app-server-options`
- `POST /api/app-server-options`

## 钉钉

- `POST /api/dingtalk/test`
- `POST /api/dingtalk/test-user`
- `POST /api/dingtalk/test-personal`
- `POST /api/dingtalk/register-proxy`
- `GET /api/dingtalk/proxy-status`

# project_stage_summary

## 用途

主看板、阶段筛选、Excel 导出统一读取该表。

## 当前字段

- `source_server_name`
- `source_database_name`
- `source_database_type`
- `exam_code`
- `project_name`
- `stage_name`
- `stage_start_time`
- `stage_end_time`
- `stage_status`
- `registration_count`
- `admission_ticket_count`
- `synced_at`

## 当前页面使用关系

- `stage_status`：顶部状态统计、看板列归属
- `registration_count`：报名人数
- `admission_ticket_count`：准考证人数
- `admission_ticket_count`：当前也被用于成绩查询人数预估展示

## 当前索引目标

- 状态查询
- 时间筛选
- 阶段名筛选
- 项目名筛选
- 服务器 / 数据库 / 考试代码筛选

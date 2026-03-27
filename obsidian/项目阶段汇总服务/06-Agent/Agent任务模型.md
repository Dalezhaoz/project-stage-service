# Agent 任务模型

## 当前核心对象

- `QueryDefinition`

## 当前字段

- `RequiredTables`
- `StageQuerySql`
- `ExistingTablesSql`
- `RegistrationTablePattern`
- `AdmissionTicketTablePattern`

## 当前思路

- 业务规则放主服务
- Agent 只执行

## 当前收益

- 后续增加字段时，尽量只改主服务
- 后续增加阶段时，尽量只改主服务
- Agent 保持稳定，不频繁升级

# 自动化规则

自动化规则由一个笔记库范围、一个公共时区、多个事件分支和多个执行目标组成。事件分支可以选择 AND 或 OR 关系。每次进入规则引擎的事件只有一种类型，因此 AND 只用于组合同一种事件类型的多个条件；不同事件类型请使用 OR。跨类型 AND 需要额外的事件关联或时间窗口，目前不支持。

Cron 动作会等待所有目标完成后才记录成功；失败的尝试单独记录，下一次计划时间会再次尝试，不会把排队成功误记为执行成功。

## 事件分支

- `cron`：按 Cron 表达式触发，使用规则公共时区。
- `note_content`：匹配笔记正文内容；填写 `contentContains` 时只在内容字段发生变化时匹配，留空则兼容匹配笔记变更事件。
- `file_behavior`：匹配路径前缀、路径 Glob 以及文件/目录行为。
- `todo_reminder`：使用专用 Markdown 待办解析器，仅连接通知渠道；规则只提供公共时区。
- `manual`：通过 WebGUI 的手动触发按钮或 API 运行。

每条规则必须选择一个笔记库。备份和 Git 配置不保存笔记库，而是接收触发器传入的执行上下文。同一个目标可以被多个笔记库复用，保存时会提示潜在的存储路径、Git 分支或远端冲突。

## API

- `GET /api/automations`：列出当前用户的规则。
- `POST /api/automations`、`PUT /api/automations`：创建或更新规则。
- `DELETE /api/automations?id=<id>`：删除规则。
- `POST /api/automations/trigger`，请求体 `{ "id": 1 }`：运行包含 `manual` 分支的规则。
- `GET /api/automations/executions`：查看当前用户的执行记录，可用 `triggerId` 过滤。
- `POST /api/automations/executions/retry`，请求体 `{ "id": 1 }`：只重试该执行记录中尚未成功的目标。

示例：

```json
{
  "name": "项目变化同步",
  "enabled": true,
  "vaultId": 3,
  "timezone": "Asia/Shanghai",
  "matchMode": "any",
  "events": [
    {"type": "note_content", "contentContains": "发布"},
    {"type": "file_behavior", "pathGlob": "Projects/*.md", "eventActions": ["create", "modify"]}
  ],
  "actions": [{"type": "git", "configId": 4}]
}
```

备份任务保存自己的存储选择和备份策略；存储配置只保存连接与路径参数。Git 和通知渠道也只保存各自的执行参数与凭据，它们不会自行订阅事件。自动化规则是事件到目标的唯一连接层。

每个事件与触发器组合会生成一个执行记录，并由数据库唯一约束 `(uid, trigger_id, event_id)` 去重重复投递；同一个事件命中多个触发器仍会分别执行。失败重试先用数据库条件更新把记录从失败/取消抢占为运行中，同时提交的重试请求只有一个能执行目标；第一次重试结束且仍失败后，下一次请求才是新的重试。执行记录会保存整体状态和每个目标的结果；服务启动时及每天清理一次，删除 30 天前的记录，并将每个用户的已结束记录限制为最近 1000 条。它用于审计与幂等，不代表事件已经具备进程崩溃后的 outbox 重放能力。

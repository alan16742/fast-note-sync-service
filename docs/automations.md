# 自动化触发器

自动化由两部分组成：事件和触发器。事件只描述“发生了什么”，触发器保存匹配条件以及要执行的目标。Git 仓库账号、备份存储和通知渠道等敏感配置仍保存在各自的配置中，触发器只保存目标类型和配置 ID。

## 事件类型

- `time`：按五段 Cron 表达式和 IANA 时区触发。
- `content`：笔记创建、修改、删除、重命名、恢复或彻底删除事件，可按正文、库、路径和行为筛选。
- `file`：附件文件的创建、修改、删除、重命名、恢复或彻底删除事件，可按库、路径和行为筛选。
- `manual`：通过 WebGUI 的“手动运行”按钮或 API 触发。

事件发布不会让原始的笔记/文件写入等待目标执行。目标执行进入共享 Worker Pool；一个触发器可以绑定多个目标，也可以让多个触发器复用同一个 Git、备份或通知配置。

## API

以下接口位于 WebGUI 认证路由下：

- `GET /api/automations`：列出当前用户的触发器。
- `POST /api/automations`、`PUT /api/automations`：创建或更新触发器。
- `DELETE /api/automations?id=<id>`：删除触发器。
- `POST /api/automations/trigger`，请求体 `{ "id": 1, "vaultId": 0 }`：运行一个 `manual` 触发器。

保存请求的 `actions` 示例：

```json
[
  {"type": "git", "configId": 3},
  {"type": "webhook", "configId": 8}
]
```

现有 Git 的延迟同步和备份的既有定时/变更行为仍由它们自己的配置负责；自动化规则提供额外的、可组合的事件到目标连接能力。

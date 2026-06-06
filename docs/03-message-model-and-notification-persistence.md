# 消息类型与通知持久化

Flare IM Core 使用强类型消息体和领域事件表达稳定协议语义。业务扩展可以使用 `CustomContent`、`AppCardContent`、`attributes` 和 `extensions`，但 Core 稳定语义必须使用命名字段、enum 或明确 proto 合同。

## Message 核心字段

| 字段 | 含义 |
|------|------|
| `server_id` | 服务端消息 ID，全局唯一。 |
| `conversation_id` | 会话 ID，作为路由、存储、sync 的核心键。 |
| `client_msg_id` | 客户端/业务方消息幂等 ID。 |
| `sender_id` | 发送者 ID，HTTP/gateway 场景应来自认证上下文。 |
| `source` | `USER`、`SYSTEM`、`BOT`、`ADMIN`。 |
| `conversation_seq` | 会话内 replay/sync 水位。 |
| `created_at` | 发送方或服务端创建时间，毫秒。 |
| `conversation_type` | 单聊、群聊、频道等。 |
| `message_type` | 与 `MessageContent` 对齐的类型。 |
| `channel_id` | 单聊=对方 user_id，群聊=群 ID，频道/话题=对应 ID。 |
| `content` | 强类型消息体。 |
| `status` | 创建、已接受、已持久化、失败、撤回、删除等 author/persistence 生命周期。 |
| `retention_policy` | TTL、阅后即焚、手动清理等策略。 |
| `offline_push_info` | 离线推送展示信息。 |
| `attributes` | 字符串业务扩展，不能放 Core 稳定语义。 |
| `extensions` | bytes 业务扩展，key 建议命名空间化。 |

## MessageType

| 类型 | 用途 | 是否默认持久化 |
|------|------|----------------|
| `TEXT` | 文本与 @ 提及 | 是 |
| `IMAGE` | 图片 | 是 |
| `VIDEO` | 视频 | 是 |
| `AUDIO` | 语音或音频 | 是 |
| `FILE` | 文件附件 | 是 |
| `LOCATION` | 位置 | 是 |
| `CARD` | 名片 | 是 |
| `STICKER` | 表情贴纸 | 是 |
| `EMOJI` | 表情符号消息 | 是 |
| `LINK_CARD` | 链接预览 | 是 |
| `FORWARD` | 单条或合并转发 | 是 |
| `THREAD` | 话题/子线程根消息 | 是 |
| `QUOTE` | 引用回复 | 是 |
| `APP_CARD` | 通用应用卡片 | 是 |
| `RICH_TEXT` | 富文本 | 是 |
| `IMAGE_GROUP` | 多图/图组 | 是 |
| `SYSTEM` | 会话内系统消息 | 是 |
| `NOTIFICATION` | 会话内通知，是否持久化由 `persistent` 控制 | 取决于 `persistent` |
| `CUSTOM` | 业务自定义载荷 | 是 |
| `PLACEHOLDER` | 加密占位、导入、迁移等 | 是 |
| `UNSPECIFIED` + label | typing、system_event、operation 等兼容标签 | 取决于 label |

当前 `MessageProfile` 会根据 `MessageContent` 和 `extensions["message_type"]` 推断类型标签。稳定业务不应依赖随意字符串，应该优先使用 proto 的 `message_type` 和 `MessageContent`。

## MessageContent

`MessageContent` 是 oneof：

| oneof | 说明 |
|-------|------|
| `text` | 文本。 |
| `image` / `video` / `audio` / `file` | 媒体与附件，文件对象由 `flare-media` 管理。 |
| `location` | 地理位置。 |
| `card` | 联系人/群卡片。 |
| `sticker` / `emoji` | 表情。 |
| `quote` | 引用回复。 |
| `link_card` | 链接卡片。 |
| `forward` | 转发。 |
| `thread` | 话题/子线程。 |
| `app_card` | 应用卡片，适合业务结构化交互。 |
| `rich_text` | 富文本。 |
| `image_group` | 图组。 |
| `system` | 系统消息。 |
| `notification` | 通知。 |
| `custom` | 自定义业务消息。 |
| `placeholder` | 占位消息。 |

## SystemContent

`SystemContent` 表示会话时间线内的系统消息，例如：

- `group.member_joined`
- `group.member_removed`
- `group.dissolved`
- `friend.request_accepted`
- `conversation.title_updated`

设计规则：

- 系统消息默认持久化，参与 `conversation_seq`。
- `event_kind` 使用稳定键，建议 `domain.entity.action`。
- 展示文案可以放 `body`，结构化字段放 `attributes` 或 `payload`。
- 系统消息是时间线内容，不等于服务间领域事件。

## NotificationContent

`NotificationContent` 表示触达型通知，可以展示、角标、声音、定向用户或角色。

核心字段：

| 字段 | 含义 |
|------|------|
| `notification_type` | 业务路由键，例如 `business.friend_request`、`payment.received`。 |
| `target_user_ids` | 定向用户。 |
| `target_role_id` | 定向角色。 |
| `notify_all` | 是否全员触达。 |
| `persistent` | 是否落库和参与离线恢复。 |
| `show_in_list` | 是否展示在列表。 |
| `show_badge` | 是否影响角标。 |
| `play_sound` | 是否播放声音。 |

通知持久化规则：

| `persistent` | 路径 | 适用 |
|--------------|------|------|
| `true` | WAL + MQ main + storage + push | 重要通知、系统公告、需要历史可查的业务通知。 |
| `false` | push-only | 在线提示、弱提醒、可丢弃通知、已有业务系统兜底的提示。 |

如果 `NotificationContent.persistent` 缺省或无法判断，Core 当前按持久化路径处理，除非上层显式 `ForcePushOnly`。

## 临时消息

临时消息包括：

- typing
- presence
- system_event
- 业务明确 `ForcePushOnly` 的提示
- `persistent=false` 通知

临时消息规则：

- 不写 WAL。
- 不持久化。
- 不作为离线消息。
- 离线用户可以丢弃。
- 不应作为客户端历史状态的唯一来源。

## 操作事件

消息操作使用 `Event` 而不是伪造普通消息。

| EventType | 用途 | 是否持久化 |
|-----------|------|------------|
| `EVENT_MESSAGE` | 新消息事件流记录 | 是 |
| `EVENT_MESSAGE_RECALL` | 撤回 | 是 |
| `EVENT_MESSAGE_EDIT` | 编辑 | 是 |
| `EVENT_MESSAGE_DELETE` | 删除 | 是 |
| `EVENT_READ_RECEIPT` | 已读回执 | 是 |
| `EVENT_CONVERSATION_UPDATE` | 会话信息更新 | 是 |
| `EVENT_CONVERSATION_DELETE` | 会话删除 | 是 |
| `EVENT_REACTION` | 表情反应 | 是 |
| `EVENT_PIN` / `EVENT_UNPIN` | 置顶 / 取消置顶 | 是 |
| `EVENT_MARK` / `EVENT_UNMARK` | 标记 / 取消标记 | 是 |
| retention events | 阅后即焚/TTL 生命周期 | 是 |
| `EVENT_CUSTOM` | 自定义领域事件 | 由业务语义决定，默认持久化路径更安全 |

操作事件进入事件流，客户端通过 sync 按 seq 收敛状态。不要把撤回、删除、已读等稳定协议语义塞进普通消息 `metadata`。

## CustomContent

业务自定义消息使用：

```text
MessageType = CUSTOM
MessageContent.custom.type = "red_packet" | "transfer" | "check_in" | ...
MessageContent.custom.payload = JSON/protobuf bytes
MessageContent.custom.description = 会话列表摘要
```

建议：

- `type` 使用业务命名空间，例如 `payment.red_packet.v1`。
- `payload` 自带版本字段。
- 需要 Core 稳定处理的能力不要放在 custom payload 中。
- 需要历史可查的业务交互默认走持久化消息。
- 只在线提示才使用通知 `persistent=false` 或 push-only。

## AppCardContent

应用卡片适合可点击的业务交互，例如审批卡片、任务卡片、订单卡片。

建议业务系统负责：

- 卡片详情数据。
- 点击后的业务权限。
- 卡片状态更新事件。

Core 负责：

- 卡片消息进入时间线。
- 推送与同步。
- 后续 edit/custom event 更新卡片展示状态。

## Retention

`MessageRetentionPolicy` 支持：

- `TIME_TO_LIVE`
- `AFTER_READ`
- `ON_DEMAND`
- absolute expire time

阅后即焚当前通过 `ReadBurnMessageCommand` 和 retention 事件安排生命周期：

```text
read -> retention scheduled -> retention expired -> retention purged
```

客户端必须以 retention event 更新可见性，不能只依赖本地定时器。

## 字段使用规范

| 字段 | 可以放什么 | 不能放什么 |
|------|------------|------------|
| typed fields | Core 稳定协议、排序、状态、保留策略、通知持久化 | 临时业务私有字段 |
| `attributes` | 小型字符串业务扩展、展示辅助字段 | Core 稳定语义、权限决策唯一来源 |
| `extensions` | bytes 扩展、业务私有结构、向后兼容载荷 | 已有 typed field 的重复语义 |
| `metadata` | transport/MQ/header 上下文 | 消息协议语义 |

## 设计建议

- 需要历史可查：普通消息、系统消息、持久通知、操作事件。
- 只在线提示：临时通知、typing、presence。
- 需要业务交互：优先 `APP_CARD` 或 `CUSTOM`，不要扩展 Core enum。
- 需要权限判断：走认证上下文、业务系统 gRPC Hook、Capability，不依赖客户端自报字段。
- 需要幂等：稳定 `client_msg_id`，不要每次重试生成新 ID。

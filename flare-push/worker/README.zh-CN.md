# flare-push-worker

[English](README.md) · 中文

离线推送投递。用户不在线时，消息经此 worker 送到厂商推送通道，最终落到设备通知栏。

**这是移动端产品的底线能力** —— 离线收不到消息，IM 就不能上生产。

## 选择投递后端

生产环境设置 `FLARE_ENV=production`。此时 worker 在连 MQ 之前就检查后端。这个变量必须配置：
漏配会落回缺省的 `disabled`，每条离线任务都进死信而运行时没有任何报错，所以直接拒绝启动。
`outbox` 只落库不投递，同样拒绝。显式写 `disabled` 可以启动——没有移动端的部署仍然需要 worker
做在线推送——启动时打一条告警说明离线推送是关着的。其余后端启动时各打一行
`offline push <后端> backend enabled`（带厂商清单），部署冒烟据此核对。这项检查不能代替沙箱真机验收。

```bash
PUSH_WORKER_OFFLINE_DELIVERY_BACKEND=mobile   # outbox | getui | fcm | apns | mobile | disabled
PUSH_WORKER_OFFLINE_PROVIDERS=apns,getui,fcm  # 只用于 mobile：这台 worker 经哪些厂商发
```

| 后端 | 覆盖 | 何时用 |
|---|---|---|
| `apns` | iOS 原生 | 需要 VoIP 来电、Live Activity、Critical Alert —— 这些**只有原生 APNs 支持** |
| `fcm` | Android + 经 FCM 转发的 iOS | 海外为主的产品，一套接入覆盖两端 |
| `getui` | 国内 Android 全家桶 | 国内产品：小米/华为/OPPO/vivo 等厂商通道由个推聚合 |
| `mobile` | `PUSH_WORKER_OFFLINE_PROVIDERS` 列出的全部厂商 | 多通道的产品：一个 worker 每条任务只经设备的一条绑定发（见下文） |
| `outbox` | —— | 只落库不投递，由外部系统消费；`FLARE_ENV=production` 时拒绝启动 |
| `disabled` | —— | 关闭离线推送。非生产环境的缺省值；`FLARE_ENV=production` 时必须显式写出 |

> 要覆盖多个通道，用 `mobile` 并在 `PUSH_WORKER_OFFLINE_PROVIDERS` 里列出：worker 每条任务只选设备的一条绑定。
> `apns`、`fcm`、`getui` 是同一套投递，只限一家厂商。

## 配置

厂商凭据按应用配置，放在格式为 `flare.mobile-push-applications/v2` 的凭据文档里（每个应用一条，按任务的租户查找），
来源二选一：

| 来源 | 变量 | 说明 |
|---|---|---|
| 静态文件 | `PUSH_WORKER_MOBILE_APPLICATIONS_FILE` | 普通文件、不能是符号链接、只有一个硬链接、权限不含组与其他用户位（如 `0600`），不超过 4 MiB、1024 个应用。写错是启动错误 |
| 平台快照 | `PUSH_WORKER_MOBILE_SNAPSHOT_FILE` | 平台每 5 秒写一次，`valid_until_ms` 最多比现在晚 30 秒；租约过期或读不出时投递重试，不会退回别的来源 |

两者同时设置拒绝启动。全局凭据变量不论用哪个后端也一律拒绝启动：`PUSH_WORKER_FCM_SERVICE_ACCOUNT_JSON`、
`PUSH_WORKER_APNS_TEAM_ID`、`PUSH_WORKER_APNS_KEY_ID`、`PUSH_WORKER_APNS_PRIVATE_KEY`、`PUSH_WORKER_APNS_TOPIC`、
`PUSH_WORKER_APNS_SANDBOX`、`PUSH_WORKER_GETUI_APP_ID`、`PUSH_WORKER_GETUI_APP_KEY`、`PUSH_WORKER_GETUI_MASTER_SECRET`、
`PUSH_WORKER_GETUI_BASE_URL`。其中四个个推变量只有被忽略的沙箱测试 `getui_sandbox_push` 读取。

### APNs

```json
"apns": {
  "team_id": "XXXXXXXXXX",
  "key_id": "YYYYYYYYYY",
  "private_key_pem": "-----BEGIN PRIVATE KEY-----\n...",
  "topics": ["com.example.app", "com.example.app.beta"]
}
```

`apns-topic` 取设备绑定登记的应用标识（`appBundle`），必须列在 `topics` 里，否则任务进死信并计入
`push_worker_apns_binding_rejected_total`。沙盒还是生产也按绑定登记的 `environment` 选：开发构建注册的
device token 只在沙箱有效，发到生产会一律返回 `BadDeviceToken`。凭据里不再有全局的 topic 与沙盒开关。

### FCM

```json
"fcm": {
  "project_id": "example-project",
  "client_email": "push@example-project.iam.gserviceaccount.com",
  "private_key": "-----BEGIN PRIVATE KEY-----\n..."
}
```

### 个推

```json
"getui": {
  "app_id": "ExampleGetuiAppId",
  "app_key": "...",
  "master_secret": "...",
  "android_vendor": {
    "huawei_category": "IM",
    "honor_importance": "NORMAL",
    "xiaomi_channel_id": "100001",
    "xiaomi_template_id": "xm-direct",
    "xiaomi_group_channel_id": "100002",
    "xiaomi_group_template_id": "xm-group",
    "oppo_private_template_id": "op-im",
    "oppo_notify_level": 2,
    "vivo_category": "IM"
  }
}
```

`PUSH_WORKER_GETUI_DEFAULT_TTL_MS`（缺省 7200000，不超过个推的 3 天）与 `PUSH_WORKER_GETUI_REQUEST_TIMEOUT_MS`
（缺省 5000）调个推调用。

`android_vendor` 可省略；每一项都是厂商为本应用批准的取值，未填的不发，单项最多 64 个可见 ASCII 字符。
华为（`huawei_category`）、荣耀（`honor_importance`）、vivo（`vivo_category`）每条推送都带。小米与 OPPO 把聊天当私信，
经在厂商登记的私信通道与模板下发，只用于单聊与群聊（`kind` 为 `direct` 或 `group`）；通知类推送不带小米与 OPPO 参数：

- 小米：`xiaomi_channel_id` 与 `xiaomi_template_id`（成对填写）给单聊；`xiaomi_group_channel_id` 与
  `xiaomi_group_template_id` 给群聊，各自不填时退回单聊那一项（填它们之前要先有单聊的一对）。
- OPPO：填了 `oppo_private_template_id`，聊天按新消息分类以 `IM` 类下发，提醒级别 `oppo_notify_level`（1、2、16，
  缺省 2，只能与模板一起填）；`oppo_channel_id` 是旧私信通道。
- 模板有两个变量 `title` 与 `content`，用渲染好的标题与正文填，小米截到 50 与 128 个字符，OPPO 截到 50 与 200 个字符；
  小米的取值去掉 emoji 与其他 `\p{So}` 符号。各厂商共用的通知（`ups.notification`）截到 40 与 100 个字符，即个推列出的
  最小上限。按 UTF-16 码元计数，截在字素簇之间，截断时末尾是「…」，算在上限里。
- 通知既不露发送者也不露内容时（预览设为隐藏、内容不可见），小米拿到会话类别与条数，而不是小米列为违规的通用文案：
  「私聊消息」/「群聊消息」与「N 条新消息，打开应用查看」（英文 `Direct message` / `Group message`、
  `N new messages, open the app to read them`）。

> **私钥与 secret 一律经密钥管理服务注入凭据来源，不要提交到版本控制。**

## 失效 token 清理

厂商明确告知「此 token 永久失效」时，worker 会把它从注册表删除，避免为一个
永不可达的设备无限重试。

判定刻意**保守**：

| 厂商 | 判定为失效 | 不判定（会重试） |
|---|---|---|
| APNs | `410 Unregistered` 且时间戳不早于绑定登记时间 | 限流、5xx、`400 BadDeviceToken`、`400 DeviceTokenNotForTopic`、`ExpiredProviderToken`、`PayloadTooLarge` |
| FCM | `404` 且 FCM 错误码为 `UNREGISTERED` | 限流、5xx、`INVALID_ARGUMENT`、`PERMISSION_DENIED` |
| 个推 | `20001` `target user is invalid` | 其余一律不判 |

**把临时故障误判为失效，会删掉正常设备的 token，用户从此再也收不到推送，
且不重装 App 无法自愈。** 宁可多重试几次，也不能误删 —— 单测专门钉住了这条边界。

## 一条任务一台设备

离线任务只对应一台设备（`device_id`），只投给这台设备的推送绑定；同一用户的其他设备各有各的任务。
没有目标设备或没有处置的任务不可执行，直接进死信，**绝不退回「发给该用户的全部设备」**。
所以一台设备失败只重试它自己，不会给其他设备造成重复通知；厂商已经受理的绑定在任务重投时跳过
（任务身份取路由给的 `idempotency_key`）。

## 一台设备一次推送，按厂商治理

一台设备经多家厂商绑定时，任务只经**一条**绑定发出：设备的绑定只读一次，按 `[services.push_worker] binding_order`
（缺省 `apns`、`getui`、`fcm`）依次尝试。只有当前绑定接不了这条任务才换下一条：厂商熔断、厂商或本地限流（APNs 只限这一个
设备令牌）、可重试的失败、令牌失效（顺带删除），或这家厂商对这条任务无事可做。终态失败进死信。

每家厂商有并发上限与滚动窗口熔断（只有服务端错误与无应答计入）；每家厂商的每个应用凭据有自己的令牌桶。
参数在 `[services.push_worker.vendors.<apns|fcm|getui>]`（`max_concurrency`、`rate_per_sec`、`burst`、
`failure_ratio`、`window`、`min_calls`、`open_ms`、`half_open_probes`）。

限流降级为摘要：厂商回 429 或配额耗尽（照办 Retry-After；个推的调用频率码 10003、10005 与当日总量 30009 也算）、
或本地令牌桶空了，又没有别的绑定能接时，worker 在内存里
按设备攒下任务（每个合并键留最新一条），确认消息；厂商恢复后每台设备只发一条：待发的会话多于一个时发摘要，
否则发最新那条并带上条数。攒的量有上限，满了就让 broker 重投。worker 停机只丢它攒着的提示，消息本身打开即同步。

APNs 的 429 说的是某一个设备令牌，只有这条绑定等待、被攒下；FCM 与个推的限流让这个应用在该厂商的设备都等。

指标：`flare_push_vendor_sent_total{vendor, app_version, receipt_capable}`（`app_version` 每个进程只列前 64 个取值，
其余记为 `other`，与 proxy 的回执计数同一规则）、`flare_push_vendor_latency_seconds{vendor}`、
`flare_push_vendor_failures_total{vendor, reason}`、`flare_push_vendor_throttled_total{vendor}`、
`flare_push_vendor_breaker_state{vendor}`、`flare_push_vendor_switch_total{from, to}`、
`flare_push_digest_coalesced_total{vendor}`、`flare_push_digest_flush_total{result}`。每次尝试也写进推送决策链（`vendor` 环节）。

## 优先级通道

路由把离线任务发到 `FLARE_PUSH` 流里的三个主题上，worker 每个主题一个消费者，各有自己的消费组与并发：
批量任务积压时单聊照常推进。在线任务另有第四个消费者。

| 主题（设置） | 内容 | 消费组 | 并发（设置，缺省） |
|---|---|---|---|
| `flare.im.push.online`（`push_online_topic`） | 在线投递 | `{组}` | `PUSH_WORKER_ONLINE_CONCURRENCY`，128 |
| `flare.im.push.offline`（`push_offline_topic`） | 单聊、紧急、点名、修正 | `{组}-offline` | `PUSH_WORKER_OFFLINE_CONCURRENCY`，64 |
| `flare.im.push.offline_group`（`PUSH_WORKER_PUSH_OFFLINE_GROUP_TOPIC`） | 群里的普通提示、尾部刷新 | `{组}-offline-group` | `PUSH_WORKER_OFFLINE_GROUP_CONCURRENCY`，32 |
| `flare.im.push.offline_bulk`（`PUSH_WORKER_PUSH_OFFLINE_BULK_TOPIC`） | 大群波次、@所有人 波次、摘要、补提示 | `{组}-offline-bulk` | `PUSH_WORKER_OFFLINE_BULK_CONCURRENCY`，16 |

`{组}` 是 `PUSH_WORKER_CONSUMER_GROUP`（缺省 `push-worker`）。三个离线消费者共用一个处理器；主题要与 push server 的
`PUSH_SERVER_PUSH_OFFLINE_*_TOPIC` 一致。每个消费者只清理自己消费组的陈旧 JetStream durable。在线消费者的 durable
第一次建出来时从此刻开始投递，不会把过时的在线任务再推一遍。

## 处置

每条任务在设备上产生什么效果由路由决定，厂商适配器只做翻译。

| 处置 | 效果 |
|---|---|
| `loud` / `digest` | 展示并可响铃：高优先级，消息自己要求响铃时带声音 |
| `quiet` | 覆盖同一 `collapse_key` 的那条通知，不再响铃 |
| `recall_replace` | 同 `quiet`，正文换成「撤回了一条消息」 |
| `badge_only` | 只改角标，不弹横幅、不响铃 |
| `clear` | Android（FCM）是数据消息，由端上代码执行；APNs 上带了角标就更新角标，没带则直接确认、不调用厂商 |

同一会话的通知共用一个 `collapse_key`（`apns-collapse-id`、个推的 `notify_id`，Android 的 FCM 则由端上平台模块按载荷里的
`collapse_id` 覆盖自己的通知），刷屏的会话在通知栏始终只有一条。发给 Android 的 FCM 是纯数据消息，不带
`android.collapse_key`：可折叠消息受 FCM 每台设备突发 20 条、每 3 分钟补 1 条的限制。三家下发给端上代码的
键相同（蛇形）：`uid`、`tenant_id`、`conversation_id`、`message_id`、`seq`、`collapse_id`、`thread_id`、
`disposition`、`alert_class`、`count`、`badge`（知道时）、`title`/`body`（FCM 数据与个推透传）、`sound`，
以及 `sender_id`、`sender_name`（正文里原样出现的发送者名字，没有时为空）与 `kind`（`direct`、`group`、`notice`），
供端上换成本地显示名。发给声明了 `serviceExtension` 的绑定的 APNs 提示（`loud`、`digest`、`recall_replace`）
带 `mutable-content: 1`。
合并键叫 `collapse_id` 是因为 `collapse_key` 是 FCM 的保留数据键。
已过期的任务直接确认，不投递。60 天（`PUSH_WORKER_BINDING_INACTIVE_DAYS`）没活跃的绑定投递时跳过并删除。

## 通知文案

每条要展示的通知，服务端都渲染好标题与正文。它们是兜底：到达时端上代码能运行的通道（Android 数据消息、
iOS 通知扩展）可以用本地的会话名与设备语言重新渲染。
文案与语言规则取自 `flare-notify-rules`（`flare-protocol/flare-notify-rules`）的 `text` 模块，客户端引擎自己出的通知也用它。

- **语言**：取目标绑定登记的 `locale`。主语言子标签是 `zh` 的（`zh`、`zh-CN`、`zh-Hant-TW`、`zh_CN`）用中文，
  写了别的语言的用英文，没写的用中文。发送端写的文字不翻译，只有模板随语言变。
- **清理**：发送端的 `offline_push_info` 与从消息里取的文字（文本、富文本的纯文本、系统消息的展示文案、通知的标题正文）
  去掉控制字符、双向文字的覆盖与隔离字符、看不见的格式字符（零宽空格、BOM、韩文填充符），连续空白收成一个空格。
  标题截到 64 个、正文截到 180 个字素簇，同时不超过 256 与 1024 字节（UTF-8），超出时末尾是「…」，省略号算在上限里；
  单个字素簇超过 64 字节时只留第一个字符。清理后为空当作没有。上限对最终文案成立，条数前缀与点名标记都算在内。
- **兜底**：没有正文时按内容类型用占位（`[图片]` / `[Image]`，以及视频、语音、文件、位置、名片、链接、贴纸、表情、富文本，
  其余类型是 `[消息]` / `[Message]`）；没有标题时用「新消息」/ `New message`；什么内容都没有时用「你收到一条新消息」/
  `You have a new message`（通知类内容是「你收到一条通知」/ `You have a new notification`）。
- **点名**：提示等级为 `mention` 时正文前加「[有人@你]」/ `[Mentioned you]`，文案自己已经写了就不重复。
- **预览设置**（任务的 `preview`）：完整（未设置也按完整）显示标题与正文；只显示发送者时保留标题，正文换成「发来一条消息」/
  `sent you a message`，点名标记保留；隐藏时标题正文都用通用文案。不认识的取值按隐藏处理。内容被隐藏、打码、清除，
  或是 E2EE 占位时，不论设置一律用通用文案。
- **条数与摘要**：一条通知代表多条未读时标题前带「[N条] 」/ `[N messages] `（上限 `99+`）。摘要通知的正文由 `count` 与
  `digest_conversations` 生成：「N 个会话有 M 条新消息」/ `M new messages in N chats`，用路由给的合并键覆盖。

## 加一个新通道

1. 在 `infrastructure/` 下实现 `BindingDelivery`（经一条绑定尝试一次；换绑定、重试与限流归绑定选择）
2. 「信封 → 标题/正文」用 `push_display::push_plan` 定计划，再用 `PushAlert::text(Lang::from_locale(&binding.locale))`
   按绑定的语言渲染，**不要自己解码** ——
   各通道对「消息被撤回 / 内容不可见 / 接收者隐藏预览时显示什么」的判断必须一致，那是最容易泄露内容的地方
3. 加 `Vendor`、`OfflineDeliveryBackend` 的枚举分支、`mobile_credentials.rs` 里的凭据与 `wire.rs` 的接线
4. 失效判定单独写成纯函数并覆盖测试，尤其是**不该判定为失效**的那些情况

`fcm_push.rs` 与 `apns_push.rs` 都是可直接照抄的模板。

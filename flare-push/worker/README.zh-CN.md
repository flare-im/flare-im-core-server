# flare-push-worker

[English](README.md) · 中文

离线推送投递。用户不在线时，消息经此 worker 送到厂商推送通道，最终落到设备通知栏。

**这是移动端产品的底线能力** —— 离线收不到消息，IM 就不能上生产。

## 选择投递后端

```bash
PUSH_WORKER_OFFLINE_DELIVERY_BACKEND=fcm   # outbox | getui | fcm | apns | disabled
```

| 后端 | 覆盖 | 何时用 |
|---|---|---|
| `apns` | iOS 原生 | 需要 VoIP 来电、Live Activity、Critical Alert —— 这些**只有原生 APNs 支持** |
| `fcm` | Android + 经 FCM 转发的 iOS | 海外为主的产品，一套接入覆盖两端 |
| `getui` | 国内 Android 全家桶 | 国内产品：小米/华为/OPPO/vivo 等厂商通道由个推聚合 |
| `outbox` | —— | 只落库不投递，由外部系统消费 |
| `disabled` | —— | 关闭离线推送 |

> 一个 worker 实例只跑一个后端。要同时覆盖多个通道，就起多个实例、各配各的后端 ——
> 设备 token 注册表里带 `provider` 字段，每个实例只处理属于自己的那批 token，互不干扰。

## 配置

### APNs

```bash
PUSH_WORKER_APNS_TEAM_ID=XXXXXXXXXX          # Apple Developer Team ID
PUSH_WORKER_APNS_KEY_ID=YYYYYYYYYY           # p8 私钥的 Key ID
PUSH_WORKER_APNS_PRIVATE_KEY="$(cat AuthKey_YYYYYYYYYY.p8)"
PUSH_WORKER_APNS_TOPIC=com.example.app       # bundle id
PUSH_WORKER_APNS_SANDBOX=false               # 开发构建的设备 token 必须设 true
```

> 注意：**`SANDBOX` 设错是最常见的坑**：开发构建注册的 device token 只在沙箱有效，
> 拿到生产环境用会一律返回 `BadDeviceToken`，看起来像 token 全失效了。

### FCM

```bash
PUSH_WORKER_FCM_SERVICE_ACCOUNT_JSON="$(cat service-account.json)"
```

### 个推

```bash
PUSH_WORKER_GETUI_APP_ID=...
PUSH_WORKER_GETUI_APP_KEY=...
PUSH_WORKER_GETUI_MASTER_SECRET=...
```

> **私钥与 secret 一律走环境变量或密钥管理服务注入，不要写进配置文件提交到版本控制。**

## 失效 token 清理

厂商明确告知「此 token 永久失效」时，worker 会把它从注册表删除，避免为一个
永不可达的设备无限重试。

判定刻意**保守**：

| 厂商 | 判定为失效 | 不判定（会重试） |
|---|---|---|
| APNs | `410 Gone`、`400 BadDeviceToken`、`400 DeviceTokenNotForTopic` | 限流、5xx、`ExpiredProviderToken`、`PayloadTooLarge` |
| FCM | `404`、`UNREGISTERED`、`INVALID_ARGUMENT` | 限流、5xx、`PERMISSION_DENIED` |

**把临时故障误判为失效，会删掉正常设备的 token，用户从此再也收不到推送，
且不重装 App 无法自愈。** 宁可多重试几次，也不能误删 —— 单测专门钉住了这条边界。

## 多设备语义

一个用户可能有多台设备。**只要有一台投递成功就算整体成功** —— 否则某台旧设备
持续失败会触发整批重试，给其余正常设备造成重复通知。

## 加一个新通道

1. 在 `infrastructure/` 下实现 `OfflinePushExecutor`
2. 「信封 → 标题/正文」直接用 `push_display::notification_display`，**不要自己解码** ——
   各通道对「消息被撤回 / 内容不可见时显示什么」的判断必须一致，那是最容易泄露内容的地方
3. 在 `OfflineDeliveryBackend` 加枚举分支与 `wire.rs` 的接线
4. 失效判定单独写成纯函数并覆盖测试，尤其是**不该判定为失效**的那些情况

`fcm_push.rs` 与 `apns_push.rs` 都是可直接照抄的模板。

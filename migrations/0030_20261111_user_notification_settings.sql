-- 用户级通知设置与大群「全部消息通知」。
--
-- 1. user_notification_settings：每个（租户，用户）一行，勿扰时段、通知预览、多端提示策略，
--    带乐观并发版本。没有行的用户按默认值处理（没有勿扰时段、完整预览、延迟）。
-- 2. conversation_participants.notify_all：成员在大群里显式开启「全部消息通知」。默认 FALSE，
--    与升级前的行为一致。加列带常量默认值，只改元数据，不重写表。
-- 3. 部分索引只收 notify_all 为真的成员行：推送按会话枚举订阅者时只扫这些行。开启的人很少，
--    索引很小；建索引要扫一遍成员表并在批次提交前挡住它的写入，与 0006 一样放在维护窗口执行。

CREATE TABLE IF NOT EXISTS user_notification_settings (
    tenant_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    quiet_start_minute SMALLINT,
    quiet_end_minute SMALLINT,
    quiet_weekdays SMALLINT NOT NULL DEFAULT 0,
    quiet_allow_mentions BOOLEAN NOT NULL DEFAULT FALSE,
    preview SMALLINT NOT NULL DEFAULT 0,
    multi_device SMALLINT NOT NULL DEFAULT 0,
    settings_version BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, user_id),
    CONSTRAINT user_notification_settings_quiet_pair
        CHECK ((quiet_start_minute IS NULL) = (quiet_end_minute IS NULL)),
    CONSTRAINT user_notification_settings_quiet_range
        CHECK (quiet_start_minute BETWEEN 0 AND 1439 AND quiet_end_minute BETWEEN 0 AND 1439),
    CONSTRAINT user_notification_settings_weekdays
        CHECK (quiet_weekdays BETWEEN 0 AND 127),
    CONSTRAINT user_notification_settings_preview
        CHECK (preview BETWEEN 0 AND 3),
    CONSTRAINT user_notification_settings_multi_device
        CHECK (multi_device BETWEEN 0 AND 3),
    CONSTRAINT user_notification_settings_version
        CHECK (settings_version >= 0)
);

COMMENT ON TABLE user_notification_settings IS
    '用户级通知设置（UserNotificationSettings）：勿扰时段、通知预览、多端提示策略；没有行即默认值';
COMMENT ON COLUMN user_notification_settings.quiet_start_minute IS
    '勿扰开始，设备当地时间一天中的分钟 0..1439；与 quiet_end_minute 同为 NULL 表示没有勿扰时段';
COMMENT ON COLUMN user_notification_settings.quiet_end_minute IS
    '勿扰结束分钟；等于开始表示全天，小于开始表示跨零点';
COMMENT ON COLUMN user_notification_settings.quiet_weekdays IS
    '勿扰生效的星期，按位 bit0 周一 … bit6 周日；0 表示每天';
COMMENT ON COLUMN user_notification_settings.quiet_allow_mentions IS
    '勿扰时段内点名到我的消息照常响铃';
COMMENT ON COLUMN user_notification_settings.preview IS
    'NotificationPreview 枚举值：0 未指定（按完整）、1 完整、2 只显示发送者、3 隐藏';
COMMENT ON COLUMN user_notification_settings.multi_device IS
    'MultiDeviceAlertPolicy 枚举值：0 未指定（按延迟）、1 延迟、2 立即、3 只更新角标';
COMMENT ON COLUMN user_notification_settings.settings_version IS
    '设置版本，每次写入加一；客户端带 base_settings_version 做乐观并发';

ALTER TABLE conversation_participants
    ADD COLUMN IF NOT EXISTS notify_all BOOLEAN NOT NULL DEFAULT FALSE;

COMMENT ON COLUMN conversation_participants.notify_all IS
    '大群里显式开启「全部消息通知」：进入该会话的通知订阅者，按会话限频的波次推送';

CREATE INDEX IF NOT EXISTS idx_conversation_participants_notify_all
    ON conversation_participants (tenant_id, conversation_id, user_id)
    WHERE notify_all;

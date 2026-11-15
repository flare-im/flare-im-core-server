-- Explicit accepted Core initial v2 -> v3 only. Caller validates the released v2
-- source digest/full descriptor/owner ACL/history and sets the target source
-- digest locally.

-- Version-level review state of plugin web bundles (also in the fresh v3 schema).
CREATE TABLE public.plugin_web_bundles (
 plugin_id varchar(128) NOT NULL,
 plugin_version varchar(64) NOT NULL,
 bundle_id varchar(64) NOT NULL CHECK(bundle_id ~ '^[a-z0-9][a-z0-9-]{0,63}$'),
 sha256 text NOT NULL CHECK(sha256 ~ '^[0-9a-f]{64}$'),
 size bigint NOT NULL CHECK(size BETWEEN 1 AND 10485760),
 approved_at bigint NOT NULL DEFAULT (extract(epoch FROM clock_timestamp())*1000)::bigint,
 withdrawn_at bigint,
 updated_at bigint NOT NULL DEFAULT (extract(epoch FROM clock_timestamp())*1000)::bigint,
 PRIMARY KEY(plugin_id,plugin_version,bundle_id),
 FOREIGN KEY(plugin_id,plugin_version) REFERENCES public.plugin_versions(plugin_id,plugin_version),
 CHECK(withdrawn_at IS NULL OR withdrawn_at>=approved_at)
);
GRANT SELECT, INSERT, UPDATE, DELETE ON public.plugin_web_bundles TO core_runtime;

-- 用户级通知设置与大群「全部消息通知」（与 schema.sql、迁移 0030 同一形状，也在全新的 v3 结构里）。
ALTER TABLE public.conversation_participants
    ADD COLUMN notify_all BOOLEAN NOT NULL DEFAULT FALSE;
COMMENT ON COLUMN public.conversation_participants.notify_all IS '大群里显式开启「全部消息通知」：进入该会话的通知订阅者，按会话限频的波次推送';
CREATE INDEX idx_conversation_participants_notify_all ON public.conversation_participants(tenant_id, conversation_id, user_id) WHERE notify_all;

CREATE TABLE public.user_notification_settings (
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
COMMENT ON TABLE public.user_notification_settings IS '用户级通知设置（UserNotificationSettings）：勿扰时段、通知预览、多端提示策略；没有行即默认值';
COMMENT ON COLUMN public.user_notification_settings.quiet_start_minute IS '勿扰开始，设备当地时间一天中的分钟 0..1439；与 quiet_end_minute 同为 NULL 表示没有勿扰时段';
COMMENT ON COLUMN public.user_notification_settings.quiet_end_minute IS '勿扰结束分钟；等于开始表示全天，小于开始表示跨零点';
COMMENT ON COLUMN public.user_notification_settings.quiet_weekdays IS '勿扰生效的星期，按位 bit0 周一 … bit6 周日；0 表示每天';
COMMENT ON COLUMN public.user_notification_settings.quiet_allow_mentions IS '勿扰时段内点名到我的消息照常响铃';
COMMENT ON COLUMN public.user_notification_settings.preview IS 'NotificationPreview 枚举值：0 未指定（按完整）、1 完整、2 只显示发送者、3 隐藏';
COMMENT ON COLUMN public.user_notification_settings.multi_device IS 'MultiDeviceAlertPolicy 枚举值：0 未指定（按延迟）、1 延迟、2 立即、3 只更新角标';
COMMENT ON COLUMN public.user_notification_settings.settings_version IS '设置版本，每次写入加一；客户端带 base_settings_version 做乐观并发';
GRANT SELECT,INSERT,UPDATE,DELETE ON public.user_notification_settings TO core_runtime;

-- The history keeps the v2 transition row and gains the v3 one.
ALTER TABLE public.core_initial_history
    DROP CONSTRAINT core_initial_history_to_version_check,
    DROP CONSTRAINT core_initial_history_from_version_check,
    DROP CONSTRAINT core_initial_history_transition_check,
    DROP CONSTRAINT core_initial_history_check;
ALTER TABLE public.core_initial_history
    ADD CONSTRAINT core_initial_history_to_version_check CHECK (to_version IN (2, 3)),
    ADD CONSTRAINT core_initial_history_from_version_check CHECK (from_version IN (0, 1, 2)),
    ADD CONSTRAINT core_initial_history_transition_check
        CHECK (transition IN ('fresh', 'application-media-v2', 'plugin-web-bundles-v3')),
    ADD CONSTRAINT core_initial_history_check
        CHECK ((from_version = 0 AND from_sha256 IS NULL AND transition = 'fresh')
            OR (from_version = 1 AND to_version = 2 AND from_sha256 IS NOT NULL AND transition = 'application-media-v2')
            OR (from_version = 2 AND to_version = 3 AND from_sha256 IS NOT NULL AND transition = 'plugin-web-bundles-v3'));

ALTER TABLE public.core_initial_schema DROP CONSTRAINT core_initial_schema_version_check;
UPDATE public.core_initial_schema
    SET version = 3, schema_sha256 = current_setting('flare.core_initial_target_sha256')
    WHERE version = 2;
ALTER TABLE public.core_initial_schema ADD CONSTRAINT core_initial_schema_version_check CHECK (version = 3);

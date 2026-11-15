-- Explicit accepted Core initial v1 -> v2 only. Caller validates the old source
-- digest/full descriptor/owner ACL and sets the target source digest locally.
ALTER TABLE public.media_assets
    ADD COLUMN owner_kind TEXT NOT NULL DEFAULT 'user' CHECK (owner_kind IN ('user', 'application')),
    ADD COLUMN purpose TEXT NOT NULL DEFAULT 'general' CHECK (purpose IN ('general', 'avatar', 'image'));
ALTER TABLE public.media_assets ADD CONSTRAINT media_assets_application_owner
    CHECK (owner_kind <> 'application' OR (owner_id IS NOT NULL AND owner_id = tenant_id));
COMMENT ON COLUMN public.media_assets.owner_kind IS 'Authenticated User owner or explicitly application-owned managed asset';
COMMENT ON COLUMN public.media_assets.purpose IS 'Authoritative media purpose, never read from client metadata';

-- 用户级通知设置与大群「全部消息通知」（与 schema.sql、迁移 0030 同一形状）。
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

CREATE TABLE public.core_initial_history (
    to_version smallint PRIMARY KEY CHECK (to_version = 2),
    from_version smallint NOT NULL CHECK (from_version IN (0, 1)),
    from_sha256 text CHECK (from_sha256 ~ '^[0-9a-f]{64}$'),
    to_sha256 text NOT NULL CHECK (to_sha256 ~ '^[0-9a-f]{64}$'),
    transition text NOT NULL CHECK (transition IN ('fresh', 'application-media-v2')),
    ddl_sha256 text NOT NULL CHECK (ddl_sha256 ~ '^[0-9a-f]{64}$'),
    applied_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK ((from_version = 0 AND from_sha256 IS NULL AND transition = 'fresh')
        OR (from_version = 1 AND from_sha256 IS NOT NULL AND transition = 'application-media-v2'))
);
REVOKE ALL ON public.core_initial_history FROM core_runtime;
GRANT SELECT ON public.core_initial_history TO core_runtime;

ALTER TABLE public.core_initial_schema DROP CONSTRAINT core_initial_schema_version_check;
UPDATE public.core_initial_schema
    SET version = 2, schema_sha256 = current_setting('flare.core_initial_target_sha256')
    WHERE version = 1;
ALTER TABLE public.core_initial_schema ADD CONSTRAINT core_initial_schema_version_check CHECK (version = 2);

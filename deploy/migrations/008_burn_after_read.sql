-- 迁移：阅后即焚消息字段与 due 扫描索引
-- 日期: 2026-05-28
-- 说明: 服务端权威倒计时；焚毁事件进入 conversation seq 事件流。

ALTER TABLE messages
    ADD COLUMN IF NOT EXISTS burn_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS burn_after_read_seconds BIGINT,
    ADD COLUMN IF NOT EXISTS burn_status SMALLINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS first_read_at BIGINT,
    ADD COLUMN IF NOT EXISTS burn_at BIGINT,
    ADD COLUMN IF NOT EXISTS burned_at BIGINT;

COMMENT ON COLUMN messages.burn_enabled IS '是否启用阅后即焚';
COMMENT ON COLUMN messages.burn_after_read_seconds IS '首次阅读后多少秒焚毁';
COMMENT ON COLUMN messages.burn_status IS '阅后即焚状态：0=NONE 1=INIT 2=READ 3=BURN_PENDING 4=BURNED 5=HARD_DELETED';
COMMENT ON COLUMN messages.first_read_at IS '首次真实阅读时间（Unix 秒，服务端写入）';
COMMENT ON COLUMN messages.burn_at IS '服务端权威焚毁时间（Unix 秒）';
COMMENT ON COLUMN messages.burned_at IS '实际焚毁时间（Unix 秒）';

CREATE INDEX IF NOT EXISTS idx_messages_burn_due
    ON messages(tenant_id, burn_status, burn_at)
    WHERE burn_status = 3 AND burn_at IS NOT NULL;

-- 预留 per-user burn state。当前先实现 message 级全局焚毁；群聊独立焚毁时启用本表。
CREATE TABLE IF NOT EXISTS message_burn_user_state (
    tenant_id TEXT NOT NULL,
    msg_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    read_at BIGINT,
    burn_at BIGINT,
    burned_at BIGINT,
    status SMALLINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, msg_id, user_id)
);

CREATE INDEX IF NOT EXISTS idx_message_burn_user_state_due
    ON message_burn_user_state(tenant_id, status, burn_at)
    WHERE status = 3 AND burn_at IS NOT NULL;

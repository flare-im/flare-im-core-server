-- 用户级会话偏好：归档 / 设置版本 / 草稿（多端同步）
ALTER TABLE conversation_participants
    ADD COLUMN IF NOT EXISTS is_archived BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE conversation_participants
    ADD COLUMN IF NOT EXISTS settings_version BIGINT NOT NULL DEFAULT 0;

ALTER TABLE conversation_participants
    ADD COLUMN IF NOT EXISTS draft TEXT;

COMMENT ON COLUMN conversation_participants.is_archived IS '用户侧归档（飞书「完成」）';
COMMENT ON COLUMN conversation_participants.settings_version IS '用户偏好 LWW 版本';
COMMENT ON COLUMN conversation_participants.draft IS '用户草稿（多端同步）';

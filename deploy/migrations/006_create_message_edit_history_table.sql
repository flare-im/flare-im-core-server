-- 迁移：创建消息编辑历史表
-- 日期: 2026-01-14
-- 说明: 创建独立的 message_edit_history 表，用于存储消息的详细编辑记录

CREATE TABLE IF NOT EXISTS message_edit_history (
    id BIGSERIAL PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL,
    message_id VARCHAR(64) NOT NULL,
    edit_version INTEGER NOT NULL,
    content BYTEA NOT NULL,
    editor_id VARCHAR(64) NOT NULL,
    reason TEXT,
    show_edited_mark BOOLEAN DEFAULT TRUE,
    edited_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- 唯一约束：同一消息的同一版本只能有一条记录
    UNIQUE(tenant_id, message_id, edit_version)
);

-- 添加索引
CREATE INDEX IF NOT EXISTS idx_message_edit_history_message_id ON message_edit_history(message_id);
CREATE INDEX IF NOT EXISTS idx_message_edit_history_tenant_id ON message_edit_history(tenant_id);

-- 添加注释
COMMENT ON TABLE message_edit_history IS '消息编辑历史表';
COMMENT ON COLUMN message_edit_history.message_id IS '消息ID（server_id）';
COMMENT ON COLUMN message_edit_history.edit_version IS '编辑版本号';
COMMENT ON COLUMN message_edit_history.content IS '编辑后的内容（Protobuf二进制）';
COMMENT ON COLUMN message_edit_history.editor_id IS '编辑者ID';
COMMENT ON COLUMN message_edit_history.reason IS '编辑原因';

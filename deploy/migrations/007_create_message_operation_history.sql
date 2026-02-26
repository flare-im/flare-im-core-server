-- 迁移：创建消息操作历史表
-- 日期: 2026-01-14
-- 说明: 补全缺失的 message_operation_history 表

CREATE TABLE IF NOT EXISTS message_operation_history (
    id BIGSERIAL PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    message_id TEXT NOT NULL,
    operation_type INTEGER NOT NULL, -- 注意：代码中使用的是 i32 枚举值，所以这里应该是 INTEGER
    operator_id TEXT NOT NULL,
    target_user_id TEXT,
    operation_data JSONB,
    show_notice BOOLEAN DEFAULT TRUE,
    notice_text TEXT,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    metadata JSONB DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_message_operation_history_tenant_message_id ON message_operation_history(tenant_id, message_id);
CREATE INDEX IF NOT EXISTS idx_message_operation_history_tenant_operation_type ON message_operation_history(tenant_id, operation_type);
CREATE INDEX IF NOT EXISTS idx_message_operation_history_tenant_timestamp ON message_operation_history(tenant_id, timestamp DESC);

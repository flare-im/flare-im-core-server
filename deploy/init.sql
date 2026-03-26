-- ============================================================================
-- Flare IM 数据库初始化脚本
-- ============================================================================
-- 版本: v2.0.0
-- 说明: 按模块组织数据库表结构（租户、媒体、消息、会话、Hook引擎）
-- 数据库: PostgreSQL + TimescaleDB
-- 更新日期: 2025-01-XX
-- 
-- 整合内容：
-- - 001_create_admin_tables.sql: 租户表、告警规则表、告警历史表
-- - 002_create_gateway_tables.sql: Hook执行记录表优化
-- - 003_message_relation_model_optimization.sql: 消息关系模型优化（seq等）
-- - 004_add_edit_history.sql: 编辑历史字段（已整合到messages表）
-- ============================================================================

-- 启用 TimescaleDB 扩展
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ============================================================================
-- 0. 租户和管理模块 (Tenant & Admin Module)
-- ============================================================================
-- 职责: 租户管理、Hook配置、告警规则

-- 租户表
-- COMMENT: 租户信息表，支持多租户隔离
DROP TABLE IF EXISTS tenants CASCADE;
CREATE TABLE tenants (
    tenant_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    config JSONB DEFAULT '{}'::jsonb,
    quota JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE tenants IS '租户表，存储租户基本信息、配置和配额';
COMMENT ON COLUMN tenants.tenant_id IS '租户ID（主键）';
COMMENT ON COLUMN tenants.name IS '租户名称';
COMMENT ON COLUMN tenants.description IS '租户描述';
COMMENT ON COLUMN tenants.status IS '租户状态（active, suspended, deleted）';
COMMENT ON COLUMN tenants.config IS '租户配置（JSON格式）';
COMMENT ON COLUMN tenants.quota IS '租户配额（JSON格式）';

CREATE INDEX IF NOT EXISTS idx_tenants_status ON tenants(status);
CREATE INDEX IF NOT EXISTS idx_tenants_created_at ON tenants(created_at);

-- 告警规则表
DROP TABLE IF EXISTS alert_rules CASCADE;
CREATE TABLE alert_rules (
    rule_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    condition TEXT NOT NULL,
    threshold TEXT NOT NULL,
    duration_seconds INTEGER NOT NULL DEFAULT 300,
    notification_channels TEXT[],
    enabled BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE alert_rules IS '告警规则表';
CREATE INDEX IF NOT EXISTS idx_alert_rules_enabled ON alert_rules(enabled);
CREATE INDEX IF NOT EXISTS idx_alert_rules_metric_name ON alert_rules(metric_name);

-- 告警历史表（TimescaleDB Hypertable）
DROP TABLE IF EXISTS alert_history CASCADE;
CREATE TABLE alert_history (
    alert_id TEXT PRIMARY KEY,
    rule_id TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    current_value DOUBLE PRECISION NOT NULL,
    threshold TEXT NOT NULL,
    severity TEXT NOT NULL,
    triggered_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    resolved_at TIMESTAMP WITH TIME ZONE
);

-- SELECT create_hypertable('alert_history', 'triggered_at', 
--     chunk_time_interval => INTERVAL '1 day',
--     if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_alert_history_rule_id ON alert_history(rule_id);
CREATE INDEX IF NOT EXISTS idx_alert_history_triggered_at ON alert_history(triggered_at);
CREATE INDEX IF NOT EXISTS idx_alert_history_resolved_at ON alert_history(resolved_at);

-- ============================================================================
-- 1. 媒体模块 (Media Module)
-- ============================================================================
-- 职责: 媒体资产元数据存储、引用管理、去重存储

-- 媒体资产元数据表
-- COMMENT: 媒体服务核心表，存储上传的媒体文件元数据
DROP TABLE IF EXISTS media_assets CASCADE;
CREATE TABLE media_assets (
    tenant_id TEXT NOT NULL,                   -- 租户ID（多租户支持，必需字段）
    file_id TEXT NOT NULL,
    file_name TEXT NOT NULL,
    mime_type TEXT NOT NULL,
    file_size BIGINT NOT NULL,
    url TEXT NOT NULL,
    cdn_url TEXT NOT NULL,
    md5 TEXT,
    sha256 TEXT,
    metadata JSONB,
    uploaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    reference_count BIGINT DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'active',
    grace_expires_at TIMESTAMP WITH TIME ZONE,
    access_type TEXT NOT NULL DEFAULT 'private',
    
    PRIMARY KEY (tenant_id, file_id)  -- 多租户主键
);

COMMENT ON TABLE media_assets IS '媒体资产元数据表（多租户支持）';
COMMENT ON COLUMN media_assets.tenant_id IS '租户ID（多租户支持，必需字段，用于数据隔离）';
COMMENT ON COLUMN media_assets.file_id IS '文件唯一标识符（租户内唯一）';
COMMENT ON COLUMN media_assets.file_name IS '文件名';
COMMENT ON COLUMN media_assets.mime_type IS 'MIME类型';
COMMENT ON COLUMN media_assets.file_size IS '文件大小（字节）';
COMMENT ON COLUMN media_assets.url IS '文件访问URL';
COMMENT ON COLUMN media_assets.cdn_url IS 'CDN访问URL';
COMMENT ON COLUMN media_assets.md5 IS 'MD5哈希值';
COMMENT ON COLUMN media_assets.sha256 IS 'SHA256哈希值';
COMMENT ON COLUMN media_assets.metadata IS '元数据（JSON格式）';
COMMENT ON COLUMN media_assets.uploaded_at IS '上传时间';
COMMENT ON COLUMN media_assets.reference_count IS '引用计数';
COMMENT ON COLUMN media_assets.status IS '文件状态（active, pending, deleted等）';
COMMENT ON COLUMN media_assets.grace_expires_at IS '宽限过期时间';
COMMENT ON COLUMN media_assets.access_type IS '文件访问类型（public, private）';

-- 媒体引用表
-- COMMENT: 媒体服务核心表，存储媒体文件的引用信息
DROP TABLE IF EXISTS media_references CASCADE;
CREATE TABLE media_references (
    tenant_id TEXT NOT NULL,                   -- 租户ID（多租户支持，必需字段）
    reference_id TEXT NOT NULL,
    file_id TEXT NOT NULL,
    namespace TEXT NOT NULL,
    owner_id TEXT NOT NULL,
    business_tag TEXT,
    metadata JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP WITH TIME ZONE,
    
    PRIMARY KEY (tenant_id, reference_id),  -- 多租户主键
    
    -- 外键约束（多租户）
    FOREIGN KEY (tenant_id, file_id) REFERENCES media_assets(tenant_id, file_id) ON DELETE CASCADE
);

COMMENT ON TABLE media_references IS '媒体引用表（多租户支持）';
COMMENT ON COLUMN media_references.tenant_id IS '租户ID（多租户支持，必需字段，用于数据隔离）';
COMMENT ON COLUMN media_references.reference_id IS '引用唯一标识符（租户内唯一）';
COMMENT ON COLUMN media_references.file_id IS '关联的文件ID';
COMMENT ON COLUMN media_references.namespace IS '命名空间';
COMMENT ON COLUMN media_references.owner_id IS '拥有者ID';
COMMENT ON COLUMN media_references.business_tag IS '业务标签';
COMMENT ON COLUMN media_references.metadata IS '引用元数据（JSON格式）';
COMMENT ON COLUMN media_references.created_at IS '创建时间';
COMMENT ON COLUMN media_references.expires_at IS '过期时间';

-- 媒体模块索引（多租户优化）
CREATE INDEX IF NOT EXISTS idx_media_assets_tenant_id ON media_assets(tenant_id); -- 租户ID索引
CREATE INDEX IF NOT EXISTS idx_media_assets_tenant_uploaded_at ON media_assets(tenant_id, uploaded_at DESC); -- 多租户：按租户和上传时间查询
CREATE INDEX IF NOT EXISTS idx_media_assets_tenant_sha256 ON media_assets(tenant_id, sha256) WHERE sha256 IS NOT NULL; -- 多租户：按租户和哈希查询
CREATE INDEX IF NOT EXISTS idx_media_assets_tenant_status ON media_assets(tenant_id, status); -- 多租户：按租户和状态查询
CREATE INDEX IF NOT EXISTS idx_media_assets_tenant_access_type ON media_assets(tenant_id, access_type); -- 多租户：按租户和访问类型查询
CREATE INDEX IF NOT EXISTS idx_media_references_tenant_file_id ON media_references(tenant_id, file_id); -- 多租户：按租户和文件ID查询
CREATE INDEX IF NOT EXISTS idx_media_references_tenant_namespace ON media_references(tenant_id, namespace); -- 多租户：按租户和命名空间查询
CREATE INDEX IF NOT EXISTS idx_media_references_tenant_owner_id ON media_references(tenant_id, owner_id); -- 多租户：按租户和拥有者ID查询
CREATE INDEX IF NOT EXISTS idx_media_references_tenant_created_at ON media_references(tenant_id, created_at DESC); -- 多租户：按租户和创建时间查询

-- ============================================================================
-- 2. 消息模块 (Message Module)
-- ============================================================================
-- 职责: 消息持久化存储、历史消息查询、消息检索
-- 设计原则: 基于 FSM 设计文档，严格区分 Message FSM、User-Message FSM、Conversation FSM、Message Attribute FSM

-- 消息表（TimescaleDB Hypertable）
-- COMMENT: 消息存储核心表，使用TimescaleDB时序数据库优化，按时间分区
-- Message FSM 状态: INIT -> SENT -> EDITED (可重入) -> RECALLED/DELETED_HARD (终态)
-- 注意：TimescaleDB要求分区列（timestamp）必须包含在主键中
DROP TABLE IF EXISTS messages CASCADE;
CREATE TABLE messages (
    server_id TEXT NOT NULL,                -- 服务端消息ID（服务端生成，全局唯一）
    conversation_id TEXT NOT NULL,          -- 会话ID
    client_msg_id TEXT,                     -- 客户端消息ID（用于去重和客户端标识）
    sender_id TEXT NOT NULL,                 -- 发送者ID
    receiver_id TEXT,                        -- 接收者ID（单聊时必需，群聊时为空）
    channel_id TEXT,                         -- 通道ID（群聊/频道，等同conversation_id）
    content BYTEA,                          -- 消息内容（二进制，protobuf编码）
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- 消息类型和内容
    message_type TEXT NOT NULL,             -- 消息类型（MESSAGE_TYPE_TEXT, MESSAGE_TYPE_IMAGE等）
    content_type TEXT,                      -- 内容子类型（CONTENT_TYPE_PLAIN_TEXT, CONTENT_TYPE_MARKDOWN等）
    business_type TEXT,                     -- 业务类型（可选，业务系统扩展）
    source TEXT DEFAULT 'user',             -- 消息来源（user, system, bot, admin）
    
    -- 引用内容（QuoteContent）
    quote JSONB,                            -- 引用内容（JSON格式，包含quoted_message_id、quoted_sender_id等）
    
    -- Message FSM 状态（核心状态机）
    -- 状态值：INIT（服务端构建中，客户端不可见）、SENT（已发送，正常态）、
    --        EDITED（已被编辑，可多次进入）、RECALLED（已撤回，终态）、DELETED_HARD（已硬删除，终态）
    status TEXT DEFAULT 'INIT' NOT NULL,
    fsm_state_changed_at TIMESTAMP WITH TIME ZONE, -- FSM状态变更时间
    current_edit_version INTEGER DEFAULT 0,  -- 当前编辑版本号（从0开始，每次编辑递增）
    last_edited_at TIMESTAMP WITH TIME ZONE, -- 最后编辑时间
    
    -- 撤回相关（Message FSM: RECALLED状态）
    recall_reason TEXT,                     -- 撤回原因
    
    -- 阅后即焚
    is_burn_after_read BOOLEAN DEFAULT FALSE, -- 是否阅后即焚
    burn_after_seconds INTEGER,             -- 阅后即焚秒数
    expire_at TIMESTAMP WITH TIME ZONE,      -- 阅后即焚过期时间
    
    -- 消息关系模型优化字段
    seq BIGINT,                             -- 会话内递增序号（用于消息顺序和未读数计算）
    conversation_type TEXT,                  -- 会话类型（single, group, channel）
    
    -- 与proto文件一致的额外字段
    tenant_id TEXT NOT NULL,                -- 租户ID（多租户支持，必需字段）
    attributes JSONB DEFAULT '{}'::jsonb,   -- 业务扩展字段（如 thread_id等）
    extra JSONB DEFAULT '{}'::jsonb,        -- 系统扩展字段
    tags TEXT[] DEFAULT '{}',               -- 标签列表
    offline_push_info JSONB,                -- 离线推送信息
    
    -- 时间线信息（冗余字段，用于快速查询）
    persisted_at TIMESTAMP WITH TIME ZONE,  -- 持久化时间
    delivered_at TIMESTAMP WITH TIME ZONE,  -- 送达时间
    
    -- 复合主键：TimescaleDB要求分区列必须包含在主键中
    -- 使用 (timestamp, server_id) 顺序以优化时序查询性能
    PRIMARY KEY (timestamp, server_id)
);

COMMENT ON TABLE messages IS '消息存储表（TimescaleDB Hypertable）- Message FSM核心表';
COMMENT ON COLUMN messages.server_id IS '服务端消息ID（服务端生成，全局唯一）';
COMMENT ON COLUMN messages.conversation_id IS '会话ID';
COMMENT ON COLUMN messages.client_msg_id IS '客户端消息ID（客户端生成，用于去重和客户端标识）';
COMMENT ON COLUMN messages.sender_id IS '发送者ID';
COMMENT ON COLUMN messages.receiver_id IS '接收者ID（单聊时必需，群聊时为空）';
COMMENT ON COLUMN messages.channel_id IS '通道ID（群聊/频道，等同conversation_id）';
COMMENT ON COLUMN messages.content IS '消息内容（二进制，protobuf编码）';
COMMENT ON COLUMN messages.timestamp IS '消息时间戳（分区键）';
COMMENT ON COLUMN messages.message_type IS '消息类型（MESSAGE_TYPE_TEXT, MESSAGE_TYPE_IMAGE等）';
COMMENT ON COLUMN messages.content_type IS '内容子类型（CONTENT_TYPE_PLAIN_TEXT, CONTENT_TYPE_MARKDOWN等）';
COMMENT ON COLUMN messages.business_type IS '业务类型（可选，业务系统扩展）';
COMMENT ON COLUMN messages.source IS '消息来源（user, system, bot, admin）';
COMMENT ON COLUMN messages.quote IS '引用内容（JSON格式，包含quoted_message_id、quoted_sender_id、quoted_text_preview等）';
COMMENT ON COLUMN messages.status IS 'Message FSM状态（INIT: 服务端构建中, SENT: 已发送, EDITED: 已编辑, RECALLED: 已撤回, DELETED_HARD: 已硬删除）';
COMMENT ON COLUMN messages.fsm_state_changed_at IS 'FSM状态变更时间';
COMMENT ON COLUMN messages.current_edit_version IS '当前编辑版本号（从0开始，每次编辑递增）';
COMMENT ON COLUMN messages.last_edited_at IS '最后编辑时间';
COMMENT ON COLUMN messages.recall_reason IS '撤回原因';
COMMENT ON COLUMN messages.is_burn_after_read IS '是否阅后即焚';
COMMENT ON COLUMN messages.burn_after_seconds IS '阅后即焚秒数';
COMMENT ON COLUMN messages.expire_at IS '阅后即焚过期时间';
COMMENT ON COLUMN messages.seq IS '会话内递增序号（用于消息顺序和未读数计算）';
COMMENT ON COLUMN messages.conversation_type IS '会话类型（single, group, channel）';
COMMENT ON COLUMN messages.tenant_id IS '租户ID（多租户支持，必需字段，用于数据隔离）';
COMMENT ON COLUMN messages.attributes IS '业务扩展字段（如 thread_id等）';
COMMENT ON COLUMN messages.extra IS '系统扩展字段';
COMMENT ON COLUMN messages.tags IS '标签列表';
COMMENT ON COLUMN messages.offline_push_info IS '离线推送信息';
COMMENT ON COLUMN messages.persisted_at IS '持久化时间';
COMMENT ON COLUMN messages.delivered_at IS '送达时间';

-- 消息表索引（多租户优化：关键查询索引包含tenant_id以实现数据隔离）
-- 注意：主键已包含 (timestamp, server_id)，无需单独创建 timestamp 和 server_id 索引
CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_server_id_unique ON messages(tenant_id, server_id); -- 唯一索引，保证server_id在租户内全局唯一
CREATE INDEX IF NOT EXISTS idx_messages_conversation_id ON messages(tenant_id, conversation_id); -- 多租户：按租户和会话查询
CREATE INDEX IF NOT EXISTS idx_messages_sender_id ON messages(tenant_id, sender_id); -- 多租户：按租户和发送者查询
CREATE INDEX IF NOT EXISTS idx_messages_conversation_timestamp ON messages(tenant_id, conversation_id, timestamp DESC); -- 多租户：会话内消息查询
CREATE INDEX IF NOT EXISTS idx_messages_client_msg_id ON messages(tenant_id, client_msg_id) WHERE client_msg_id IS NOT NULL; -- 客户端消息ID索引（用于去重查询）
CREATE INDEX IF NOT EXISTS idx_messages_sender_client_msg_id ON messages(tenant_id, sender_id, client_msg_id) WHERE client_msg_id IS NOT NULL; -- 发送者+客户端消息ID复合索引（用于幂等性检查）
CREATE INDEX IF NOT EXISTS idx_messages_business_type ON messages(tenant_id, business_type) WHERE business_type IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_messages_message_type ON messages(tenant_id, message_type);
CREATE INDEX IF NOT EXISTS idx_messages_fsm_state ON messages(tenant_id, status);
CREATE INDEX IF NOT EXISTS idx_messages_fsm_state_changed_at ON messages(tenant_id, fsm_state_changed_at) WHERE fsm_state_changed_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_messages_current_edit_version ON messages(tenant_id, current_edit_version) WHERE current_edit_version > 0;
CREATE INDEX IF NOT EXISTS idx_messages_last_edited_at ON messages(tenant_id, last_edited_at) WHERE last_edited_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_messages_conversation_seq ON messages(tenant_id, conversation_id, seq) WHERE seq IS NOT NULL; -- 多租户：会话内序号查询
CREATE INDEX IF NOT EXISTS idx_messages_seq ON messages(tenant_id, seq) WHERE seq IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_messages_expire_at ON messages(tenant_id, expire_at) WHERE expire_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_messages_source ON messages(tenant_id, source);
CREATE INDEX IF NOT EXISTS idx_messages_tenant_id ON messages(tenant_id); -- 租户ID索引（用于租户级别查询）
CREATE INDEX IF NOT EXISTS idx_messages_channel_id ON messages(tenant_id, channel_id) WHERE channel_id IS NOT NULL; -- 通道ID索引
CREATE INDEX IF NOT EXISTS idx_messages_tags ON messages USING GIN(tags) WHERE tags IS NOT NULL AND tags != '{}'; -- GIN索引不支持多列，需要应用层过滤tenant_id
CREATE INDEX IF NOT EXISTS idx_messages_attributes_thread_id ON messages USING GIN(attributes) WHERE attributes ? 'thread_id'; -- 话题ID索引
CREATE INDEX IF NOT EXISTS idx_messages_quote_quoted_message_id ON messages(tenant_id, (quote->>'quoted_message_id')) WHERE quote IS NOT NULL AND quote->>'quoted_message_id' IS NOT NULL; -- 引用消息ID索引（替代reply_to_message_id）

-- 将消息表转换为 TimescaleDB 超表（Hypertable）
-- COMMENT: 按时间分区，每个分区默认 1 天，用于高效存储和查询时序消息数据
-- 注意：由于主键包含 timestamp，TimescaleDB 会自动使用主键进行分区
SELECT create_hypertable('messages', 'timestamp', 
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- ============================================================================
-- 事件化消息流：会话事件表（Conversation Events）
-- ============================================================================
-- 设计：所有「影响会话历史」的操作作为事件写入同一有序流，保证多端/离线同步顺序一致。
-- 事件类型：MESSAGE（用户消息）、RECALL、EDIT、DELETE_HARD、SYSTEM_NOTIFICATION。
-- 不属于消息流的实时态（输入中、在线状态、typing）不写入本表。
-- 参考：doc/EVENTIZED_MESSAGE_FLOW.md
DROP TABLE IF EXISTS conversation_events CASCADE;
CREATE TABLE conversation_events (
    tenant_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    seq BIGINT NOT NULL,
    event_type TEXT NOT NULL CHECK (event_type IN ('MESSAGE', 'RECALL', 'EDIT', 'DELETE_HARD', 'SYSTEM_NOTIFICATION')),
    message_id TEXT NOT NULL,
    operator_id TEXT,
    payload JSONB,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, conversation_id, seq)
);

COMMENT ON TABLE conversation_events IS '会话事件流（事件化模型）：MESSAGE/RECALL/EDIT/DELETE_HARD/SYSTEM_NOTIFICATION，同一会话 seq 单调递增';
COMMENT ON COLUMN conversation_events.seq IS '会话内单调递增序号（与 SequenceAllocator 分配一致）';
COMMENT ON COLUMN conversation_events.event_type IS 'MESSAGE=用户消息, RECALL=撤回, EDIT=编辑, DELETE_HARD=硬删除, SYSTEM_NOTIFICATION=系统通知';
COMMENT ON COLUMN conversation_events.message_id IS 'MESSAGE 时为新消息 server_id；否则为目标消息 server_id';
COMMENT ON COLUMN conversation_events.payload IS '操作类事件的 operation_data 等（JSON）';

CREATE UNIQUE INDEX IF NOT EXISTS idx_conversation_events_stream ON conversation_events(tenant_id, conversation_id, seq);
CREATE INDEX IF NOT EXISTS idx_conversation_events_conversation_seq ON conversation_events(tenant_id, conversation_id, seq ASC);

-- ============================================================================
-- Message FSM 相关表
-- ============================================================================

-- 消息编辑历史表（Message Edit History）
-- COMMENT: 记录消息的编辑历史，支持多次编辑（Message FSM: EDITED状态）
-- 设计：每次编辑创建一条记录，edit_version从1开始递增
DROP TABLE IF EXISTS message_edit_history CASCADE;
CREATE TABLE message_edit_history (
    id BIGSERIAL PRIMARY KEY,
    tenant_id TEXT NOT NULL,                 -- 租户ID（多租户支持）
    message_id TEXT NOT NULL,                -- 消息ID
    edit_version INTEGER NOT NULL,           -- 编辑版本号（从1开始递增）
    content BYTEA NOT NULL,                  -- 编辑后的内容（二进制，protobuf编码）
    editor_id TEXT NOT NULL,                 -- 编辑者ID
    edited_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    reason TEXT,                             -- 编辑原因（可选）
    show_edited_mark BOOLEAN DEFAULT TRUE,   -- 是否显示"已编辑"标记
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- 唯一约束：同一租户内同一消息的同一版本只能有一条记录
    UNIQUE(tenant_id, message_id, edit_version)
);

COMMENT ON TABLE message_edit_history IS '消息编辑历史表（Message FSM: EDITED状态）';
COMMENT ON COLUMN message_edit_history.tenant_id IS '租户ID（多租户支持）';
COMMENT ON COLUMN message_edit_history.message_id IS '消息ID';
COMMENT ON COLUMN message_edit_history.edit_version IS '编辑版本号（从1开始递增）';
COMMENT ON COLUMN message_edit_history.content IS '编辑后的内容（二进制，protobuf编码）';
COMMENT ON COLUMN message_edit_history.editor_id IS '编辑者ID';
COMMENT ON COLUMN message_edit_history.edited_at IS '编辑时间';
COMMENT ON COLUMN message_edit_history.reason IS '编辑原因（可选）';
COMMENT ON COLUMN message_edit_history.show_edited_mark IS '是否显示"已编辑"标记';

CREATE INDEX IF NOT EXISTS idx_message_edit_history_tenant_message_id ON message_edit_history(tenant_id, message_id);
CREATE INDEX IF NOT EXISTS idx_message_edit_history_tenant_editor_id ON message_edit_history(tenant_id, editor_id);
CREATE INDEX IF NOT EXISTS idx_message_edit_history_tenant_edited_at ON message_edit_history(tenant_id, edited_at DESC);

-- ============================================================================
-- User-Message FSM 相关表
-- ============================================================================
-- 设计：用户对消息的私有行为（已读、软删除、标记等），不影响消息的客观状态

-- 消息已读记录表（Message Read Records）
-- COMMENT: 记录用户对消息的已读状态（User-Message FSM）
DROP TABLE IF EXISTS message_read_records CASCADE;
CREATE TABLE message_read_records (
    id BIGSERIAL PRIMARY KEY,
    tenant_id TEXT NOT NULL,                 -- 租户ID（多租户支持）
    message_id TEXT NOT NULL,                -- 消息ID
    user_id TEXT NOT NULL,                   -- 用户ID
    read_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    burned_at TIMESTAMP WITH TIME ZONE,      -- 销毁时间（阅后即焚）
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- 唯一约束：同一租户内同一用户对同一消息只能有一条已读记录
    UNIQUE(tenant_id, message_id, user_id)
);

COMMENT ON TABLE message_read_records IS '消息已读记录表（User-Message FSM）';
COMMENT ON COLUMN message_read_records.tenant_id IS '租户ID（多租户支持）';
COMMENT ON COLUMN message_read_records.message_id IS '消息ID';
COMMENT ON COLUMN message_read_records.user_id IS '用户ID';
COMMENT ON COLUMN message_read_records.read_at IS '已读时间';
COMMENT ON COLUMN message_read_records.burned_at IS '销毁时间（阅后即焚）';

CREATE INDEX IF NOT EXISTS idx_message_read_records_tenant_message_id ON message_read_records(tenant_id, message_id);
CREATE INDEX IF NOT EXISTS idx_message_read_records_tenant_user_id ON message_read_records(tenant_id, user_id);
CREATE INDEX IF NOT EXISTS idx_message_read_records_tenant_read_at ON message_read_records(tenant_id, read_at DESC);
CREATE INDEX IF NOT EXISTS idx_message_read_records_tenant_user_message ON message_read_records(tenant_id, user_id, message_id);

-- 消息可见性表（Message Visibility）
-- COMMENT: 记录用户对消息的可见性状态（User-Message FSM: 软删除）
-- 设计：VISIBLE（可见）、HIDDEN（隐藏/软删除）、DELETED（已删除）
DROP TABLE IF EXISTS message_visibility CASCADE;
CREATE TABLE message_visibility (
    id BIGSERIAL PRIMARY KEY,
    tenant_id TEXT NOT NULL,                 -- 租户ID（多租户支持）
    message_id TEXT NOT NULL,                -- 消息ID
    user_id TEXT NOT NULL,                   -- 用户ID
    visibility_status TEXT NOT NULL DEFAULT 'VISIBLE' CHECK (visibility_status IN ('VISIBLE', 'HIDDEN', 'DELETED')),
    changed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- 唯一约束：同一租户内同一用户对同一消息只能有一条可见性记录
    UNIQUE(tenant_id, message_id, user_id)
);

COMMENT ON TABLE message_visibility IS '消息可见性表（User-Message FSM: 软删除）';
COMMENT ON COLUMN message_visibility.tenant_id IS '租户ID（多租户支持）';
COMMENT ON COLUMN message_visibility.message_id IS '消息ID';
COMMENT ON COLUMN message_visibility.user_id IS '用户ID';
COMMENT ON COLUMN message_visibility.visibility_status IS '可见性状态（VISIBLE: 可见, HIDDEN: 隐藏/软删除, DELETED: 已删除）';
COMMENT ON COLUMN message_visibility.changed_at IS '状态变更时间';

CREATE INDEX IF NOT EXISTS idx_message_visibility_tenant_message_id ON message_visibility(tenant_id, message_id);
CREATE INDEX IF NOT EXISTS idx_message_visibility_tenant_user_id ON message_visibility(tenant_id, user_id);
CREATE INDEX IF NOT EXISTS idx_message_visibility_tenant_status ON message_visibility(tenant_id, visibility_status);
CREATE INDEX IF NOT EXISTS idx_message_visibility_tenant_user_message ON message_visibility(tenant_id, user_id, message_id);

-- 消息标记表（Marked Messages）
-- COMMENT: 记录用户对消息的标记（User-Message FSM: MARK操作）
-- 标记类型：IMPORTANT（重要）、TODO（待办）、DONE（已处理）、CUSTOM（自定义）
DROP TABLE IF EXISTS marked_messages CASCADE;
CREATE TABLE marked_messages (
    id BIGSERIAL PRIMARY KEY,
    tenant_id TEXT NOT NULL,                 -- 租户ID（多租户支持）
    message_id TEXT NOT NULL,                -- 消息ID
    user_id TEXT NOT NULL,                   -- 用户ID
    conversation_id TEXT NOT NULL,           -- 会话ID（冗余，用于快速查询）
    mark_type TEXT NOT NULL CHECK (mark_type IN ('IMPORTANT', 'TODO', 'DONE', 'CUSTOM')),
    color TEXT,                              -- 标记颜色（可选）
    marked_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- 唯一约束：同一租户内同一用户对同一消息只能有一种标记类型
    UNIQUE(tenant_id, message_id, user_id, mark_type)
);

COMMENT ON TABLE marked_messages IS '消息标记表（User-Message FSM: MARK操作）';
COMMENT ON COLUMN marked_messages.tenant_id IS '租户ID（多租户支持）';
COMMENT ON COLUMN marked_messages.message_id IS '消息ID';
COMMENT ON COLUMN marked_messages.user_id IS '用户ID';
COMMENT ON COLUMN marked_messages.conversation_id IS '会话ID（冗余，用于快速查询）';
COMMENT ON COLUMN marked_messages.mark_type IS '标记类型（IMPORTANT: 重要, TODO: 待办, DONE: 已处理, CUSTOM: 自定义）';
COMMENT ON COLUMN marked_messages.color IS '标记颜色（可选）';
COMMENT ON COLUMN marked_messages.marked_at IS '标记时间';

CREATE INDEX IF NOT EXISTS idx_marked_messages_tenant_message_id ON marked_messages(tenant_id, message_id);
CREATE INDEX IF NOT EXISTS idx_marked_messages_tenant_user_id ON marked_messages(tenant_id, user_id);
CREATE INDEX IF NOT EXISTS idx_marked_messages_tenant_conversation_id ON marked_messages(tenant_id, conversation_id);
CREATE INDEX IF NOT EXISTS idx_marked_messages_tenant_mark_type ON marked_messages(tenant_id, mark_type);
CREATE INDEX IF NOT EXISTS idx_marked_messages_tenant_user_conversation ON marked_messages(tenant_id, user_id, conversation_id);

-- ============================================================================
-- Message Attribute FSM 相关表
-- ============================================================================

-- 消息反应表（Message Reactions）
-- COMMENT: 记录消息的反应（Message Attribute FSM: REACTION_ADD/REACTION_REMOVE操作）
-- 设计：每个emoji对应一条记录，user_ids数组存储用户列表
DROP TABLE IF EXISTS message_reactions CASCADE;
CREATE TABLE message_reactions (
    id BIGSERIAL PRIMARY KEY,
    tenant_id TEXT NOT NULL,                 -- 租户ID（多租户支持）
    message_id TEXT NOT NULL,                -- 消息ID
    emoji TEXT NOT NULL,                     -- 表情符号（如 👍、❤️、😂）
    user_ids TEXT[] NOT NULL DEFAULT '{}',   -- 用户ID列表
    count INTEGER NOT NULL DEFAULT 0,        -- 反应计数（冗余字段，等于user_ids长度）
    last_updated TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- 唯一约束：同一租户内同一消息的同一emoji只能有一条记录
    UNIQUE(tenant_id, message_id, emoji)
);

COMMENT ON TABLE message_reactions IS '消息反应表（Message Attribute FSM: REACTION_ADD/REACTION_REMOVE操作）';
COMMENT ON COLUMN message_reactions.tenant_id IS '租户ID（多租户支持）';
COMMENT ON COLUMN message_reactions.message_id IS '消息ID';
COMMENT ON COLUMN message_reactions.emoji IS '表情符号（如 👍、❤️、😂）';
COMMENT ON COLUMN message_reactions.user_ids IS '用户ID列表';
COMMENT ON COLUMN message_reactions.count IS '反应计数（冗余字段，等于user_ids长度）';
COMMENT ON COLUMN message_reactions.last_updated IS '最后更新时间';
COMMENT ON COLUMN message_reactions.created_at IS '创建时间';

CREATE INDEX IF NOT EXISTS idx_message_reactions_tenant_message_id ON message_reactions(tenant_id, message_id);
CREATE INDEX IF NOT EXISTS idx_message_reactions_tenant_emoji ON message_reactions(tenant_id, emoji);
CREATE INDEX IF NOT EXISTS idx_message_reactions_user_ids ON message_reactions USING GIN(user_ids) WHERE array_length(user_ids, 1) > 0; -- GIN索引不支持多列，需要应用层过滤tenant_id
CREATE INDEX IF NOT EXISTS idx_message_reactions_tenant_last_updated ON message_reactions(tenant_id, last_updated DESC);

-- ============================================================================
-- Conversation FSM 相关表
-- ============================================================================

-- 置顶消息表（Pinned Messages）
-- COMMENT: 记录会话中的置顶消息（Conversation FSM: PIN/UNPIN操作）
-- 设计：从conversations表中分离出来，更符合FSM设计
DROP TABLE IF EXISTS pinned_messages CASCADE;
CREATE TABLE pinned_messages (
    id BIGSERIAL PRIMARY KEY,
    tenant_id TEXT NOT NULL,                 -- 租户ID（多租户支持）
    message_id TEXT NOT NULL,                -- 消息ID
    conversation_id TEXT NOT NULL,           -- 会话ID
    pinned_by TEXT NOT NULL,                  -- 置顶者ID
    pinned_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expire_at TIMESTAMP WITH TIME ZONE,      -- 置顶到期时间（可选）
    reason TEXT,                             -- 置顶原因（可选）
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- 唯一约束：同一租户内同一会话的同一消息只能有一条置顶记录
    UNIQUE(tenant_id, conversation_id, message_id)
);

COMMENT ON TABLE pinned_messages IS '置顶消息表（Conversation FSM: PIN/UNPIN操作）';
COMMENT ON COLUMN pinned_messages.tenant_id IS '租户ID（多租户支持）';
COMMENT ON COLUMN pinned_messages.message_id IS '消息ID';
COMMENT ON COLUMN pinned_messages.conversation_id IS '会话ID';
COMMENT ON COLUMN pinned_messages.pinned_by IS '置顶者ID';
COMMENT ON COLUMN pinned_messages.pinned_at IS '置顶时间';
COMMENT ON COLUMN pinned_messages.expire_at IS '置顶到期时间（可选）';
COMMENT ON COLUMN pinned_messages.reason IS '置顶原因（可选）';

CREATE INDEX IF NOT EXISTS idx_pinned_messages_tenant_message_id ON pinned_messages(tenant_id, message_id);
CREATE INDEX IF NOT EXISTS idx_pinned_messages_tenant_conversation_id ON pinned_messages(tenant_id, conversation_id);
CREATE INDEX IF NOT EXISTS idx_pinned_messages_tenant_pinned_at ON pinned_messages(tenant_id, pinned_at DESC);
CREATE INDEX IF NOT EXISTS idx_pinned_messages_tenant_expire_at ON pinned_messages(tenant_id, expire_at) WHERE expire_at IS NOT NULL;

-- 消息操作历史记录表（MessageOperationHistory）
-- COMMENT: 消息操作历史记录表，记录对消息的所有操作（审计和追踪）
-- 支持的操作类型（基于 proto/common/message_operation.proto）：
-- - Message FSM操作：OPERATION_TYPE_RECALL, OPERATION_TYPE_EDIT, OPERATION_TYPE_DELETE（硬删除）
-- - User-Message FSM操作：OPERATION_TYPE_READ, OPERATION_TYPE_DELETE（软删除）, OPERATION_TYPE_MARK, OPERATION_TYPE_UNMARK
-- - Message Attribute FSM操作：OPERATION_TYPE_REACTION_ADD, OPERATION_TYPE_REACTION_REMOVE
-- - Conversation FSM操作：OPERATION_TYPE_PIN, OPERATION_TYPE_UNPIN
DROP TABLE IF EXISTS message_operation_history CASCADE;
CREATE TABLE message_operation_history (
    id BIGSERIAL PRIMARY KEY,
    tenant_id TEXT NOT NULL,                 -- 租户ID（多租户支持）
    message_id TEXT NOT NULL,                -- 目标消息ID
    operation_type TEXT NOT NULL,           -- 操作类型（OPERATION_TYPE_RECALL, OPERATION_TYPE_EDIT等）
    operator_id TEXT NOT NULL,              -- 操作者ID
    target_user_id TEXT,                    -- 目标用户ID（可选，用于定向操作，如软删除、已读等）
    operation_data JSONB,                   -- 操作数据（根据操作类型不同而不同，对应 MessageOperation.operation_data）
    show_notice BOOLEAN DEFAULT TRUE,       -- 是否显示通知（默认true）
    notice_text TEXT,                       -- 通知文本（可选）
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    metadata JSONB DEFAULT '{}'::jsonb,     -- 元数据（扩展字段）
    
    -- 外键约束（可选，如果messages表已存在）
    -- FOREIGN KEY (tenant_id, message_id) REFERENCES messages(tenant_id, server_id) ON DELETE CASCADE
);

COMMENT ON TABLE message_operation_history IS '消息操作历史记录表（记录对消息的所有操作，用于审计和追踪）';
COMMENT ON COLUMN message_operation_history.tenant_id IS '租户ID（多租户支持）';
COMMENT ON COLUMN message_operation_history.id IS '操作记录ID（自增主键）';
COMMENT ON COLUMN message_operation_history.message_id IS '目标消息ID';
COMMENT ON COLUMN message_operation_history.operation_type IS '操作类型（OPERATION_TYPE_RECALL: 撤回, OPERATION_TYPE_EDIT: 编辑, OPERATION_TYPE_DELETE: 删除, OPERATION_TYPE_READ: 已读, OPERATION_TYPE_REACTION_ADD: 添加反应, OPERATION_TYPE_REACTION_REMOVE: 移除反应, OPERATION_TYPE_PIN: 置顶, OPERATION_TYPE_UNPIN: 取消置顶, OPERATION_TYPE_MARK: 标记, OPERATION_TYPE_UNMARK: 取消标记等）';
COMMENT ON COLUMN message_operation_history.operator_id IS '操作者ID';
COMMENT ON COLUMN message_operation_history.target_user_id IS '目标用户ID（可选，用于定向操作，如软删除、已读等）';
COMMENT ON COLUMN message_operation_history.operation_data IS '操作数据（JSON格式，对应 MessageOperation.operation_data，根据操作类型不同而不同）';
COMMENT ON COLUMN message_operation_history.show_notice IS '是否显示通知（默认true）';
COMMENT ON COLUMN message_operation_history.notice_text IS '通知文本（可选）';
COMMENT ON COLUMN message_operation_history.timestamp IS '操作时间戳';
COMMENT ON COLUMN message_operation_history.created_at IS '创建时间';
COMMENT ON COLUMN message_operation_history.metadata IS '元数据（扩展字段）';

-- 消息操作历史记录表索引（多租户优化）
CREATE INDEX IF NOT EXISTS idx_message_operation_history_tenant_message_id ON message_operation_history(tenant_id, message_id);
CREATE INDEX IF NOT EXISTS idx_message_operation_history_tenant_operation_type ON message_operation_history(tenant_id, operation_type);
CREATE INDEX IF NOT EXISTS idx_message_operation_history_tenant_operator_id ON message_operation_history(tenant_id, operator_id);
CREATE INDEX IF NOT EXISTS idx_message_operation_history_tenant_timestamp ON message_operation_history(tenant_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_message_operation_history_tenant_target_user_id ON message_operation_history(tenant_id, target_user_id) WHERE target_user_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_message_operation_history_tenant_message_type ON message_operation_history(tenant_id, message_id, operation_type);
CREATE INDEX IF NOT EXISTS idx_message_operation_history_tenant_operator_timestamp ON message_operation_history(tenant_id, operator_id, timestamp DESC);

-- 启用列式存储（Columnstore）用于压缩（TimescaleDB 2.x+）
-- COMMENT: TimescaleDB 2.x+ 需要先启用 columnstore 才能使用压缩策略
-- 注意：columnstore 可以提高压缩比（约 10:1），但查询性能可能略有下降
-- 对于历史数据（30天以上），压缩带来的存储节省远大于查询性能损失
-- 
-- 配置说明（多租户优化）：
-- - enable_columnstore: 启用列式存储
-- - segmentby: 按 tenant_id, conversation_id 分段，同一租户同一会话的消息存储在一起，提高压缩效率
-- - orderby: 按 timestamp DESC, server_id 排序，优化时序查询性能
ALTER TABLE messages SET (
    timescaledb.enable_columnstore = true,
    timescaledb.segmentby = 'tenant_id, conversation_id',
    timescaledb.orderby = 'timestamp DESC, server_id'
);

-- 配置消息表列式存储策略（30天后移动到列式存储）
-- COMMENT: 自动将历史数据移动到列式存储，节省存储空间（压缩比约 10:1）
-- 列式存储的数据仍然可以正常查询，但写入性能会略有下降
-- 注意：如果 TimescaleDB 版本 < 2.x，请注释掉此策略（使用传统的 add_compression_policy）
CALL add_columnstore_policy('messages', after => INTERVAL '30 days');

-- 配置数据保留策略（可选，保留最近 90 天的数据）
-- COMMENT: 90天后的数据可以归档到对象存储或删除
-- SELECT add_retention_policy('messages', INTERVAL '90 days');

-- ACK归档记录表（AckArchiveRecords）
-- COMMENT: ACK归档记录表，用于审计和分析的ACK日志归档
DROP TABLE IF EXISTS ack_archive_records CASCADE;
CREATE TABLE ack_archive_records (
    id BIGSERIAL PRIMARY KEY,
    message_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    ack_type TEXT NOT NULL,
    ack_status TEXT NOT NULL,
    timestamp BIGINT NOT NULL,
    importance_level SMALLINT DEFAULT 1 CHECK (importance_level BETWEEN 1 AND 3),
    metadata JSONB,
    archived_at BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW())
);

COMMENT ON TABLE ack_archive_records IS 'ACK归档记录表（用于审计和分析的ACK日志归档）';
COMMENT ON COLUMN ack_archive_records.id IS '记录ID（自增主键）';
COMMENT ON COLUMN ack_archive_records.message_id IS '消息ID';
COMMENT ON COLUMN ack_archive_records.user_id IS '用户ID';
COMMENT ON COLUMN ack_archive_records.ack_type IS 'ACK类型';
COMMENT ON COLUMN ack_archive_records.ack_status IS 'ACK状态';
COMMENT ON COLUMN ack_archive_records.timestamp IS '时间戳';
COMMENT ON COLUMN ack_archive_records.importance_level IS '重要性等级：1-低，2-中，3-高';
COMMENT ON COLUMN ack_archive_records.metadata IS '元数据';
COMMENT ON COLUMN ack_archive_records.archived_at IS '归档时间';

-- ACK归档记录表索引
CREATE INDEX IF NOT EXISTS idx_ack_archive_message_id ON ack_archive_records (message_id);
CREATE INDEX IF NOT EXISTS idx_ack_archive_user_id ON ack_archive_records (user_id);
CREATE INDEX IF NOT EXISTS idx_ack_archive_timestamp_desc ON ack_archive_records (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_ack_archive_importance_level ON ack_archive_records (importance_level);
CREATE INDEX IF NOT EXISTS idx_ack_archive_message_user_type ON ack_archive_records (message_id, user_id, ack_type);

-- 消息可靠性保障表（MessageReliability）
-- COMMENT: 消息可靠性保障表，用于跟踪消息的发送、确认和重试状态
DROP TABLE IF EXISTS message_reliability CASCADE;
CREATE TABLE message_reliability (
    id BIGSERIAL PRIMARY KEY,
    message_id TEXT NOT NULL,               -- 消息ID
    conversation_id TEXT NOT NULL,               -- 会话ID
    sender_id TEXT NOT NULL,                -- 发送者ID
    recipient_ids JSONB,                    -- 接收者ID列表
    send_attempts INTEGER DEFAULT 0,        -- 发送尝试次数
    max_send_attempts INTEGER DEFAULT 3,    -- 最大发送尝试次数
    last_send_attempt TIMESTAMP WITH TIME ZONE, -- 最后发送尝试时间
    delivery_status TEXT DEFAULT 'pending', -- 投递状态（pending, delivered, failed）
    confirmation_status TEXT DEFAULT 'pending', -- 确认状态（pending, confirmed, failed）
    retry_count INTEGER DEFAULT 0,          -- 重试次数
    max_retry_count INTEGER DEFAULT 5,      -- 最大重试次数
    next_retry_at TIMESTAMP WITH TIME ZONE, -- 下次重试时间
    error_code TEXT,                        -- 错误码
    error_message TEXT,                     -- 错误信息
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- 唯一约束：每个消息只能有一条记录
    UNIQUE(message_id)
);

-- 系统监控指标表（SystemMetrics）
-- COMMENT: 系统监控指标表，用于收集和存储系统性能指标
DROP TABLE IF EXISTS system_metrics CASCADE;
CREATE TABLE system_metrics (
    id BIGSERIAL PRIMARY KEY,
    metric_name TEXT NOT NULL,              -- 指标名称
    metric_value DOUBLE PRECISION,          -- 指标值
    metric_unit TEXT,                       -- 指标单位
    metric_type TEXT,                       -- 指标类型（counter, gauge, histogram, summary）
    service_name TEXT,                      -- 服务名称
    node_id TEXT,                           -- 节点ID
    tenant_id TEXT,                         -- 租户ID
    tags JSONB,                             -- 标签
    recorded_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP, -- 记录时间
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE message_reliability IS '消息可靠性保障表（用于跟踪消息的发送、确认和重试状态）';
COMMENT ON COLUMN message_reliability.id IS '记录ID（自增主键）';
COMMENT ON COLUMN message_reliability.message_id IS '消息ID';
COMMENT ON COLUMN message_reliability.conversation_id IS '会话ID';
COMMENT ON COLUMN message_reliability.sender_id IS '发送者ID';
COMMENT ON COLUMN message_reliability.recipient_ids IS '接收者ID列表';
COMMENT ON COLUMN message_reliability.send_attempts IS '发送尝试次数';
COMMENT ON COLUMN message_reliability.max_send_attempts IS '最大发送尝试次数';
COMMENT ON COLUMN message_reliability.last_send_attempt IS '最后发送尝试时间';
COMMENT ON COLUMN message_reliability.delivery_status IS '投递状态（pending, delivered, failed）';
COMMENT ON COLUMN message_reliability.confirmation_status IS '确认状态（pending, confirmed, failed）';
COMMENT ON COLUMN message_reliability.retry_count IS '重试次数';
COMMENT ON COLUMN message_reliability.max_retry_count IS '最大重试次数';
COMMENT ON COLUMN message_reliability.next_retry_at IS '下次重试时间';
COMMENT ON COLUMN message_reliability.error_code IS '错误码';
COMMENT ON COLUMN message_reliability.error_message IS '错误信息';
COMMENT ON COLUMN message_reliability.created_at IS '创建时间';
COMMENT ON COLUMN message_reliability.updated_at IS '更新时间';

COMMENT ON TABLE system_metrics IS '系统监控指标表（用于收集和存储系统性能指标）';
COMMENT ON COLUMN system_metrics.id IS '记录ID（自增主键）';
COMMENT ON COLUMN system_metrics.metric_name IS '指标名称';
COMMENT ON COLUMN system_metrics.metric_value IS '指标值';
COMMENT ON COLUMN system_metrics.metric_unit IS '指标单位';
COMMENT ON COLUMN system_metrics.metric_type IS '指标类型（counter, gauge, histogram, summary）';
COMMENT ON COLUMN system_metrics.service_name IS '服务名称';
COMMENT ON COLUMN system_metrics.node_id IS '节点ID';
COMMENT ON COLUMN system_metrics.tenant_id IS '租户ID';
COMMENT ON COLUMN system_metrics.tags IS '标签';
COMMENT ON COLUMN system_metrics.recorded_at IS '记录时间';
COMMENT ON COLUMN system_metrics.created_at IS '创建时间';

-- 消息可靠性保障表索引
CREATE INDEX IF NOT EXISTS idx_message_reliability_message_id ON message_reliability(message_id);
CREATE INDEX IF NOT EXISTS idx_message_reliability_conversation_id ON message_reliability(conversation_id);
CREATE INDEX IF NOT EXISTS idx_message_reliability_sender_id ON message_reliability(sender_id);
CREATE INDEX IF NOT EXISTS idx_message_reliability_delivery_status ON message_reliability(delivery_status);
CREATE INDEX IF NOT EXISTS idx_message_reliability_confirmation_status ON message_reliability(confirmation_status);
CREATE INDEX IF NOT EXISTS idx_message_reliability_next_retry_at ON message_reliability(next_retry_at) WHERE next_retry_at IS NOT NULL;

-- 系统监控指标表索引
CREATE INDEX IF NOT EXISTS idx_system_metrics_metric_name ON system_metrics(metric_name);
CREATE INDEX IF NOT EXISTS idx_system_metrics_service_name ON system_metrics(service_name);
CREATE INDEX IF NOT EXISTS idx_system_metrics_node_id ON system_metrics(node_id);
CREATE INDEX IF NOT EXISTS idx_system_metrics_tenant_id ON system_metrics(tenant_id);
CREATE INDEX IF NOT EXISTS idx_system_metrics_recorded_at ON system_metrics(recorded_at);
CREATE INDEX IF NOT EXISTS idx_system_metrics_tags ON system_metrics USING GIN(tags) WHERE tags IS NOT NULL;

-- 消息可靠性保障表更新时间戳触发器
CREATE OR REPLACE FUNCTION update_message_reliability_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_message_reliability_updated_at
    BEFORE UPDATE ON message_reliability
    FOR EACH ROW
    EXECUTE FUNCTION update_message_reliability_updated_at();

-- 系统监控指标表更新时间戳触发器
CREATE OR REPLACE FUNCTION update_system_metrics_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.created_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_system_metrics_updated_at
    BEFORE INSERT ON system_metrics
    FOR EACH ROW
    EXECUTE FUNCTION update_system_metrics_updated_at();

-- ============================================================================
-- 3. 会话模块 (Conversation Module)
-- ============================================================================
-- 职责: 会话元数据存储、参与者管理、会话状态维护

-- 会话表
-- COMMENT: 会话服务核心表，存储会话元数据和基本信息
DROP TABLE IF EXISTS conversations CASCADE;
CREATE TABLE conversations (
    conversation_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL,                   -- 租户ID（多租户支持，必需字段）
    conversation_type TEXT NOT NULL,            -- 会话类型（single, group, channel等）
    business_type TEXT NOT NULL,          -- 业务类型
    display_name TEXT,                     -- 会话显示名称
    attributes JSONB,                     -- 会话属性（JSON格式）
    visibility TEXT DEFAULT 'public',      -- 可见性（public, private, hidden）
    lifecycle_state TEXT DEFAULT 'active', -- 生命周期状态（active, archived, deleted）
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    metadata JSONB,                        -- 扩展元数据（JSON格式）
    
    PRIMARY KEY (tenant_id, conversation_id),  -- 多租户主键
    
    -- 消息关系模型优化字段（来自 003_message_relation_model_optimization.sql）
    last_message_id TEXT,                  -- 最后一条消息ID
    last_message_seq BIGINT,               -- 最后一条消息的seq（用于未读数计算）
    is_destroyed BOOLEAN DEFAULT FALSE,    -- 会话是否被解散（群聊）
    
    -- 注意：置顶消息已移至独立的 pinned_messages 表（Conversation FSM）
    
    -- 与proto文件一致的额外字段
    description TEXT,                      -- 会话描述
    avatar_url TEXT,                       -- 会话头像URL
    owner_id TEXT,                         -- 会话拥有者ID
    max_members INTEGER,                   -- 最大成员数
    is_public BOOLEAN DEFAULT FALSE,       -- 是否公开会话
    join_approval_required BOOLEAN DEFAULT FALSE, -- 加入是否需要审批
    enable_history_browsing BOOLEAN DEFAULT TRUE, -- 是否允许浏览历史消息
    enable_message_reactions BOOLEAN DEFAULT TRUE, -- 是否允许消息反应
    enable_message_edit BOOLEAN DEFAULT TRUE, -- 是否允许编辑消息
    enable_message_delete BOOLEAN DEFAULT TRUE, -- 是否允许删除消息
    message_ttl_seconds INTEGER,           -- 消息生存时间（秒）
    notification_level TEXT DEFAULT 'all', -- 通知级别（all, mention, none）
    tags TEXT[] DEFAULT '{}',              -- 标签列表
    custom_data JSONB DEFAULT '{}'::jsonb  -- 自定义数据
);

COMMENT ON TABLE conversations IS '会话表（多租户支持）';
COMMENT ON COLUMN conversations.tenant_id IS '租户ID（多租户支持，必需字段，用于数据隔离）';
COMMENT ON COLUMN conversations.conversation_id IS '会话唯一标识符（租户内唯一）';
COMMENT ON COLUMN conversations.conversation_type IS '会话类型（single: 单聊, group: 群聊, channel: 频道）';
COMMENT ON COLUMN conversations.business_type IS '业务类型';
COMMENT ON COLUMN conversations.display_name IS '会话显示名称';
COMMENT ON COLUMN conversations.attributes IS '会话属性（JSON格式）';
COMMENT ON COLUMN conversations.visibility IS '可见性（public: 公开, private: 私有, hidden: 隐藏）';
COMMENT ON COLUMN conversations.lifecycle_state IS '生命周期状态（active: 活跃, archived: 归档, deleted: 已删除）';
COMMENT ON COLUMN conversations.created_at IS '创建时间';
COMMENT ON COLUMN conversations.updated_at IS '更新时间';
COMMENT ON COLUMN conversations.metadata IS '扩展元数据（JSON格式）';
COMMENT ON COLUMN conversations.last_message_id IS '最后一条消息ID';
COMMENT ON COLUMN conversations.last_message_seq IS '最后一条消息的seq（用于未读数计算）';
COMMENT ON COLUMN conversations.is_destroyed IS '会话是否被解散（群聊）';
COMMENT ON COLUMN conversations.description IS '会话描述';
COMMENT ON COLUMN conversations.avatar_url IS '会话头像URL';
COMMENT ON COLUMN conversations.owner_id IS '会话拥有者ID';
COMMENT ON COLUMN conversations.max_members IS '最大成员数';
COMMENT ON COLUMN conversations.is_public IS '是否公开会话';
COMMENT ON COLUMN conversations.join_approval_required IS '加入是否需要审批';
COMMENT ON COLUMN conversations.enable_history_browsing IS '是否允许浏览历史消息';
COMMENT ON COLUMN conversations.enable_message_reactions IS '是否允许消息反应';
COMMENT ON COLUMN conversations.enable_message_edit IS '是否允许编辑消息';
COMMENT ON COLUMN conversations.enable_message_delete IS '是否允许删除消息';
COMMENT ON COLUMN conversations.message_ttl_seconds IS '消息生存时间（秒）';
COMMENT ON COLUMN conversations.notification_level IS '通知级别（all, mention, none）';
COMMENT ON COLUMN conversations.tags IS '标签列表';
COMMENT ON COLUMN conversations.custom_data IS '自定义数据';

-- 会话参与者表
-- COMMENT: 会话参与者关系表，存储会话成员信息
DROP TABLE IF EXISTS conversation_participants CASCADE;
CREATE TABLE conversation_participants (
    tenant_id TEXT NOT NULL,                   -- 租户ID（多租户支持）
    conversation_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    roles TEXT[],                         -- 角色列表（owner: 拥有者, admin: 管理员, member: 成员, guest: 访客, observer: 观察者）
    muted BOOLEAN DEFAULT FALSE,          -- 是否静音（向后兼容，建议使用 mute_until）
    pinned BOOLEAN DEFAULT FALSE,         -- 是否置顶
    attributes JSONB,                     -- 参与者属性（JSON格式）
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- 消息关系模型优化字段（来自 003_message_relation_model_optimization.sql）
    last_read_msg_seq BIGINT DEFAULT 0,   -- 已读消息的seq（用于未读数计算）
    last_sync_msg_seq BIGINT DEFAULT 0,   -- 多端同步游标（最后同步的seq）
    unread_count INTEGER DEFAULT 0,        -- 未读数（冗余字段，用于快速查询）
    is_deleted BOOLEAN DEFAULT FALSE,      -- 用户侧"删除会话"（软删除）
    mute_until TIMESTAMP WITH TIME ZONE,   -- 静音截止时间（NULL表示未静音）
    quit_at TIMESTAMP WITH TIME ZONE,      -- 退出时间（NULL表示仍在会话中）
    
    PRIMARY KEY (tenant_id, conversation_id, user_id),  -- 多租户主键
    FOREIGN KEY (tenant_id, conversation_id) REFERENCES conversations(tenant_id, conversation_id) ON DELETE CASCADE
);

COMMENT ON TABLE conversation_participants IS '会话参与者表（多租户支持）';
COMMENT ON COLUMN conversation_participants.tenant_id IS '租户ID（多租户支持）';
COMMENT ON COLUMN conversation_participants.conversation_id IS '会话ID';
COMMENT ON COLUMN conversation_participants.user_id IS '用户ID';
COMMENT ON COLUMN conversation_participants.roles IS '角色列表（owner: 拥有者, admin: 管理员, member: 成员, guest: 访客, observer: 观察者）';
COMMENT ON COLUMN conversation_participants.muted IS '是否静音';
COMMENT ON COLUMN conversation_participants.pinned IS '是否置顶';
COMMENT ON COLUMN conversation_participants.attributes IS '参与者属性（JSON格式）';
COMMENT ON COLUMN conversation_participants.created_at IS '加入时间';
COMMENT ON COLUMN conversation_participants.updated_at IS '更新时间';
COMMENT ON COLUMN conversation_participants.last_read_msg_seq IS '已读消息的seq（用于未读数计算）';
COMMENT ON COLUMN conversation_participants.last_sync_msg_seq IS '多端同步游标（最后同步的seq）';
COMMENT ON COLUMN conversation_participants.unread_count IS '未读数（冗余字段，用于快速查询）';
COMMENT ON COLUMN conversation_participants.is_deleted IS '用户侧"删除会话"（软删除）';
COMMENT ON COLUMN conversation_participants.mute_until IS '静音截止时间（NULL表示未静音）';
COMMENT ON COLUMN conversation_participants.quit_at IS '退出时间（NULL表示仍在会话中）';

-- 用户同步光标表
-- COMMENT: 用户同步光标表，记录用户在各会话中的同步位置（用于多端同步）
DROP TABLE IF EXISTS user_sync_cursor CASCADE;
CREATE TABLE user_sync_cursor (
    tenant_id TEXT NOT NULL,                   -- 租户ID（多租户支持）
    user_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    last_synced_ts BIGINT NOT NULL,       -- 最后同步时间戳（毫秒）
    device_id TEXT,                       -- 设备ID（可选，用于设备级光标）
    version INTEGER DEFAULT 1,            -- 版本号（用于乐观锁）
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- 消息关系模型优化字段（来自 003_message_relation_model_optimization.sql）
    last_synced_seq BIGINT DEFAULT 0,     -- 最后同步的seq（替代时间戳，更精确）
    
    PRIMARY KEY (tenant_id, user_id, conversation_id)  -- 多租户主键
);

COMMENT ON TABLE user_sync_cursor IS '用户同步光标表（多租户支持）';
COMMENT ON COLUMN user_sync_cursor.tenant_id IS '租户ID（多租户支持）';
COMMENT ON COLUMN user_sync_cursor.user_id IS '用户ID';
COMMENT ON COLUMN user_sync_cursor.conversation_id IS '会话ID';
COMMENT ON COLUMN user_sync_cursor.last_synced_ts IS '最后同步时间戳（毫秒）';
COMMENT ON COLUMN user_sync_cursor.device_id IS '设备ID（可选，用于设备级光标）';
COMMENT ON COLUMN user_sync_cursor.version IS '版本号（用于乐观锁）';
COMMENT ON COLUMN user_sync_cursor.created_at IS '创建时间';
COMMENT ON COLUMN user_sync_cursor.updated_at IS '更新时间';
COMMENT ON COLUMN user_sync_cursor.last_synced_seq IS '最后同步的seq（替代时间戳，更精确）';

-- 会话模块索引（多租户优化）
CREATE INDEX IF NOT EXISTS idx_conversations_tenant_id ON conversations(tenant_id); -- 租户ID索引
CREATE INDEX IF NOT EXISTS idx_conversations_tenant_business_type ON conversations(tenant_id, business_type, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_conversations_tenant_lifecycle_state ON conversations(tenant_id, lifecycle_state, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_conversations_tenant_conversation_type ON conversations(tenant_id, conversation_type);
CREATE INDEX IF NOT EXISTS idx_conversations_tenant_updated_at ON conversations(tenant_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_conversations_tenant_owner_id ON conversations(tenant_id, owner_id) WHERE owner_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_conversations_tenant_is_public ON conversations(tenant_id, is_public) WHERE is_public = true;
CREATE INDEX IF NOT EXISTS idx_conversations_tenant_notification_level ON conversations(tenant_id, notification_level);
CREATE INDEX IF NOT EXISTS idx_conversations_tags ON conversations USING GIN(tags) WHERE tags IS NOT NULL AND tags != '{}'; -- GIN索引不支持多列，需要应用层过滤tenant_id
CREATE INDEX IF NOT EXISTS idx_conversation_participants_tenant_user_id ON conversation_participants(tenant_id, user_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_conversation_participants_tenant_conversation_id ON conversation_participants(tenant_id, conversation_id);
CREATE INDEX IF NOT EXISTS idx_conversation_participants_tenant_last_read_seq ON conversation_participants(tenant_id, last_read_msg_seq);
CREATE INDEX IF NOT EXISTS idx_conversation_participants_tenant_last_sync_seq ON conversation_participants(tenant_id, last_sync_msg_seq);
CREATE INDEX IF NOT EXISTS idx_conversation_participants_tenant_unread_count ON conversation_participants(tenant_id, unread_count);
CREATE INDEX IF NOT EXISTS idx_conversation_participants_tenant_is_deleted ON conversation_participants(tenant_id, is_deleted) WHERE is_deleted = true;
CREATE INDEX IF NOT EXISTS idx_conversations_tenant_last_message_id ON conversations(tenant_id, last_message_id) WHERE last_message_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_conversations_tenant_last_message_seq ON conversations(tenant_id, last_message_seq) WHERE last_message_seq IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_user_sync_cursor_tenant_user_id ON user_sync_cursor(tenant_id, user_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_user_sync_cursor_tenant_conversation_id ON user_sync_cursor(tenant_id, conversation_id);
CREATE INDEX IF NOT EXISTS idx_user_sync_cursor_tenant_user_device ON user_sync_cursor(tenant_id, user_id, device_id, conversation_id) WHERE device_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_user_sync_cursor_tenant_last_synced_seq ON user_sync_cursor(tenant_id, last_synced_seq) WHERE last_synced_seq > 0;

-- 消息关系表（MessageRelations）
-- COMMENT: 消息关系表，存储消息之间的回复、转发、引用等关系
-- 注意：这些操作创建新消息，原消息FSM状态不变
-- 注意：回复/引用关系现在通过 messages.quote.quoted_message_id 存储，此表主要用于转发和话题回复
DROP TABLE IF EXISTS message_relations CASCADE;
CREATE TABLE message_relations (
    id BIGSERIAL PRIMARY KEY,
    tenant_id TEXT NOT NULL,                 -- 租户ID（多租户支持）
    source_message_id TEXT NOT NULL,        -- 源消息ID（被回复/转发/引用的消息）
    target_message_id TEXT NOT NULL,        -- 目标消息ID（新创建的消息）
    relation_type TEXT NOT NULL,            -- 关系类型（FORWARD: 转发, THREAD_REPLY: 话题回复）
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    metadata JSONB DEFAULT '{}'::jsonb,     -- 元数据（扩展字段，如转发原因、引用预览等）
    
    -- 唯一约束：同一租户内同一源消息和目标消息只能有一种关系类型
    UNIQUE(tenant_id, source_message_id, target_message_id, relation_type)
);

COMMENT ON TABLE message_relations IS '消息关系表（存储消息之间的转发、话题回复等关系，回复/引用通过messages.quote存储）';
COMMENT ON COLUMN message_relations.tenant_id IS '租户ID（多租户支持）';
COMMENT ON COLUMN message_relations.id IS '关系记录ID（自增主键）';
COMMENT ON COLUMN message_relations.source_message_id IS '源消息ID（被转发/引用的消息）';
COMMENT ON COLUMN message_relations.target_message_id IS '目标消息ID（新创建的消息）';
COMMENT ON COLUMN message_relations.relation_type IS '关系类型（FORWARD: 转发, THREAD_REPLY: 话题回复）';
COMMENT ON COLUMN message_relations.created_at IS '创建时间';
COMMENT ON COLUMN message_relations.metadata IS '元数据（扩展字段）';

-- 消息关系表索引（多租户优化）
CREATE INDEX IF NOT EXISTS idx_message_relations_tenant_source_message_id ON message_relations(tenant_id, source_message_id);
CREATE INDEX IF NOT EXISTS idx_message_relations_tenant_target_message_id ON message_relations(tenant_id, target_message_id);
CREATE INDEX IF NOT EXISTS idx_message_relations_tenant_relation_type ON message_relations(tenant_id, relation_type);
CREATE INDEX IF NOT EXISTS idx_message_relations_tenant_source_type ON message_relations(tenant_id, source_message_id, relation_type);
CREATE INDEX IF NOT EXISTS idx_message_relations_tenant_target_type ON message_relations(tenant_id, target_message_id, relation_type);

-- 消息ACK记录表（MessageAckRecords）
-- COMMENT: 消息ACK记录表，记录所有ACK相关信息
DROP TABLE IF EXISTS message_ack_records CASCADE;
CREATE TABLE message_ack_records (
    id BIGSERIAL PRIMARY KEY,
    tenant_id TEXT NOT NULL,                 -- 租户ID（多租户支持）
    message_id TEXT NOT NULL,               -- 消息ID
    user_id TEXT NOT NULL,                  -- 用户ID
    ack_type TEXT NOT NULL,                 -- ACK类型（client, push, storage）
    ack_status TEXT NOT NULL,               -- ACK状态（received, processed, failed）
    ack_timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP, -- ACK时间戳
    device_id TEXT,                         -- 设备ID（可选）
    client_msg_id TEXT,                     -- 客户端消息ID（可选）
    error_code TEXT,                        -- 错误码（可选）
    error_message TEXT,                     -- 错误信息（可选）
    metadata JSONB DEFAULT '{}'::jsonb,     -- 元数据（扩展字段）
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- 唯一约束：同一租户内同一用户对同一消息的同一类型ACK只能有一条记录
    UNIQUE(tenant_id, message_id, user_id, ack_type)
);

COMMENT ON TABLE message_ack_records IS '消息ACK记录表（记录所有ACK相关信息）';
COMMENT ON COLUMN message_ack_records.tenant_id IS '租户ID（多租户支持）';
COMMENT ON COLUMN message_ack_records.id IS 'ACK记录ID（自增主键）';
COMMENT ON COLUMN message_ack_records.message_id IS '消息ID';
COMMENT ON COLUMN message_ack_records.user_id IS '用户ID';
COMMENT ON COLUMN message_ack_records.ack_type IS 'ACK类型（client, push, storage）';
COMMENT ON COLUMN message_ack_records.ack_status IS 'ACK状态（received, processed, failed）';
COMMENT ON COLUMN message_ack_records.ack_timestamp IS 'ACK时间戳';
COMMENT ON COLUMN message_ack_records.device_id IS '设备ID（可选）';
COMMENT ON COLUMN message_ack_records.client_msg_id IS '客户端消息ID（可选）';
COMMENT ON COLUMN message_ack_records.error_code IS '错误码（可选）';
COMMENT ON COLUMN message_ack_records.error_message IS '错误信息（可选）';
COMMENT ON COLUMN message_ack_records.metadata IS '元数据（扩展字段）';
COMMENT ON COLUMN message_ack_records.created_at IS '创建时间';
COMMENT ON COLUMN message_ack_records.updated_at IS '更新时间';

-- 消息ACK记录表索引（多租户优化）
CREATE INDEX IF NOT EXISTS idx_message_ack_records_tenant_message_id ON message_ack_records(tenant_id, message_id);
CREATE INDEX IF NOT EXISTS idx_message_ack_records_tenant_user_id ON message_ack_records(tenant_id, user_id);
CREATE INDEX IF NOT EXISTS idx_message_ack_records_tenant_ack_type ON message_ack_records(tenant_id, ack_type);
CREATE INDEX IF NOT EXISTS idx_message_ack_records_tenant_ack_status ON message_ack_records(tenant_id, ack_status);
CREATE INDEX IF NOT EXISTS idx_message_ack_records_tenant_ack_timestamp ON message_ack_records(tenant_id, ack_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_message_ack_records_tenant_user_message ON message_ack_records(tenant_id, user_id, message_id);

-- 消息ACK记录表更新时间戳触发器
CREATE OR REPLACE FUNCTION update_message_ack_records_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_message_ack_records_updated_at
    BEFORE UPDATE ON message_ack_records
    FOR EACH ROW
    EXECUTE FUNCTION update_message_ack_records_updated_at();

-- ============================================================================
-- 4. Hook引擎模块 (Hook Engine Module)
-- ============================================================================
-- 职责: Hook配置管理、动态配置存储、多租户支持

-- Hook配置表
-- COMMENT: Hook引擎核心表，存储Hook配置信息（动态API配置，最高优先级）
DROP TABLE IF EXISTS hook_configs CASCADE;
CREATE TABLE hook_configs (
    id BIGSERIAL PRIMARY KEY,
    hook_id TEXT UNIQUE,                  -- Hook ID（唯一标识，兼容旧版本）
    tenant_id TEXT,                       -- 租户ID（NULL表示全局配置）
    hook_type TEXT NOT NULL,              -- Hook类型（pre_send, post_send, delivery, recall等）
    name TEXT NOT NULL,                   -- Hook名称
    version TEXT,                         -- Hook版本
    description TEXT,                     -- Hook描述
    enabled BOOLEAN NOT NULL DEFAULT true,
    priority INTEGER NOT NULL DEFAULT 100, -- 优先级（0-1000，越小越高）
    group_name TEXT,                      -- Hook分组（validation, critical, business）
    timeout_ms BIGINT NOT NULL DEFAULT 1000,
    max_retries INTEGER NOT NULL DEFAULT 0,
    error_policy TEXT NOT NULL DEFAULT 'fail_fast', -- 错误策略（fail_fast, retry, ignore）
    require_success BOOLEAN NOT NULL DEFAULT true,
    selector_config JSONB NOT NULL DEFAULT '{}',    -- 选择器配置（JSON格式，兼容 selector）
    transport_config JSONB NOT NULL,               -- 传输配置（JSON格式，兼容 transport）
    retry_policy JSONB DEFAULT '{}'::jsonb,        -- 重试策略（JSON格式，兼容旧版本）
    metadata JSONB,                                 -- 元数据（JSON格式）
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by TEXT,                                -- 创建者
    
    -- 唯一约束：同一租户下同一类型的Hook名称唯一
    UNIQUE(tenant_id, hook_type, name),
    -- 外键约束（可选）
    CONSTRAINT fk_hook_configs_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(tenant_id) ON DELETE CASCADE
);

COMMENT ON TABLE hook_configs IS 'Hook配置表（动态API配置，最高优先级）';
COMMENT ON COLUMN hook_configs.id IS '配置ID（自增主键）';
COMMENT ON COLUMN hook_configs.tenant_id IS '租户ID（NULL表示全局配置，对所有租户生效）';
COMMENT ON COLUMN hook_configs.hook_type IS 'Hook类型（pre_send, post_send, delivery, recall, conversation_create, user_login等）';
COMMENT ON COLUMN hook_configs.name IS 'Hook名称（唯一标识）';
COMMENT ON COLUMN hook_configs.version IS 'Hook版本';
COMMENT ON COLUMN hook_configs.description IS 'Hook描述';
COMMENT ON COLUMN hook_configs.enabled IS '是否启用';
COMMENT ON COLUMN hook_configs.priority IS '优先级（0-1000，数字越小优先级越高）';
COMMENT ON COLUMN hook_configs.group_name IS 'Hook分组（validation: 校验组, critical: 关键组, business: 业务组）';
COMMENT ON COLUMN hook_configs.timeout_ms IS '超时时间（毫秒）';
COMMENT ON COLUMN hook_configs.max_retries IS '最大重试次数';
COMMENT ON COLUMN hook_configs.error_policy IS '错误策略（fail_fast: 快速失败, retry: 重试, ignore: 忽略）';
COMMENT ON COLUMN hook_configs.require_success IS '是否要求成功';
COMMENT ON COLUMN hook_configs.selector_config IS '选择器配置（JSON格式，包含tenants, conversation_types, message_types等）';
COMMENT ON COLUMN hook_configs.transport_config IS '传输配置（JSON格式，包含type, endpoint等）';
COMMENT ON COLUMN hook_configs.metadata IS '元数据（JSON格式）';
COMMENT ON COLUMN hook_configs.created_at IS '创建时间';
COMMENT ON COLUMN hook_configs.updated_at IS '更新时间';
COMMENT ON COLUMN hook_configs.created_by IS '创建者';

-- Hook配置表索引
CREATE INDEX IF NOT EXISTS idx_hook_configs_tenant_type ON hook_configs(tenant_id, hook_type, enabled);
CREATE INDEX IF NOT EXISTS idx_hook_configs_priority ON hook_configs(priority DESC);
CREATE INDEX IF NOT EXISTS idx_hook_configs_hook_type ON hook_configs(hook_type);
CREATE INDEX IF NOT EXISTS idx_hook_configs_enabled ON hook_configs(enabled);
CREATE INDEX IF NOT EXISTS idx_hook_configs_updated_at ON hook_configs(updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_hook_configs_tenant ON hook_configs(tenant_id);

-- Hook执行记录表（TimescaleDB Hypertable）
-- COMMENT: Hook执行记录表，记录Hook执行历史
DROP TABLE IF EXISTS hook_executions CASCADE;
CREATE TABLE hook_executions (
    execution_id TEXT PRIMARY KEY,
    hook_id TEXT NOT NULL,
    hook_name TEXT NOT NULL,
    hook_type TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    message_id TEXT,
    success BOOLEAN NOT NULL,
    latency_ms INTEGER,
    error_code TEXT,
    error_message TEXT,
    executed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- SELECT create_hypertable('hook_executions', 'executed_at', 
--     chunk_time_interval => INTERVAL '1 day',
--     if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_hook_executions_hook_id ON hook_executions(hook_id);
CREATE INDEX IF NOT EXISTS idx_hook_executions_tenant ON hook_executions(tenant_id);
CREATE INDEX IF NOT EXISTS idx_hook_executions_message_id ON hook_executions(message_id);
CREATE INDEX IF NOT EXISTS idx_hook_executions_executed_at ON hook_executions(executed_at);
CREATE INDEX IF NOT EXISTS idx_hook_executions_success ON hook_executions(success);

COMMENT ON TABLE hook_executions IS 'Hook执行记录表，记录Hook执行历史';

-- ============================================================================
-- 7. 连续聚合视图（TimescaleDB Continuous Aggregates）
-- ============================================================================
-- 职责: 预聚合统计指标，提高查询性能

-- 消息每小时统计视图（TimescaleDB连续聚合）
-- COMMENT: 用于统计每小时的消息数量和唯一发送者数量
-- 注意：TimescaleDB连续聚合视图不支持 IF NOT EXISTS，需要先DROP再CREATE
DROP MATERIALIZED VIEW IF EXISTS messages_hourly_stats CASCADE;

CREATE MATERIALIZED VIEW messages_hourly_stats
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', timestamp) AS hour,
    conversation_id,
    COUNT(*) AS message_count,
    COUNT(DISTINCT sender_id) AS unique_senders
FROM messages
GROUP BY hour, conversation_id;

COMMENT ON MATERIALIZED VIEW messages_hourly_stats IS '消息每小时统计视图（TimescaleDB连续聚合）';
COMMENT ON COLUMN messages_hourly_stats.hour IS '小时时间戳';
COMMENT ON COLUMN messages_hourly_stats.conversation_id IS '会话ID';
COMMENT ON COLUMN messages_hourly_stats.message_count IS '消息数量';
COMMENT ON COLUMN messages_hourly_stats.unique_senders IS '唯一发送者数量';

-- 设置连续聚合的刷新策略
-- COMMENT: 每小时刷新一次连续聚合视图，延迟3小时以确保数据完整性
-- 注意：不同版本的TimescaleDB可能使用不同的调用方式
-- 使用 DO 块来处理可能的函数不存在的情况
DO $$
BEGIN
    -- 尝试使用新版本的 CALL 语法
    BEGIN
        EXECUTE 'CALL add_continuous_aggregate_policy(''messages_hourly_stats'', 
            start_offset => INTERVAL ''3 hours'',
            end_offset => INTERVAL ''1 hour'', 
            schedule_interval => INTERVAL ''1 hour'')';
    EXCEPTION WHEN undefined_function OR syntax_error THEN
        -- 如果 CALL 不支持，尝试使用 SELECT
        BEGIN
            PERFORM add_continuous_aggregate_policy('messages_hourly_stats',
                start_offset => INTERVAL '3 hours',
                end_offset => INTERVAL '1 hour',
                schedule_interval => INTERVAL '1 hour');
        EXCEPTION WHEN undefined_function THEN
            -- 如果函数都不存在，跳过策略设置
            RAISE NOTICE 'add_continuous_aggregate_policy function not available, skipping continuous aggregate policy setup';
        END;
    END;
END $$;

-- ============================================================================
-- 8. 触发器（自动更新时间戳）
-- ============================================================================

-- 会话表更新时间戳触发器
CREATE OR REPLACE FUNCTION update_conversations_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_conversations_updated_at
    BEFORE UPDATE ON conversations
    FOR EACH ROW
    EXECUTE FUNCTION update_conversations_updated_at();

-- 会话参与者表更新时间戳触发器
CREATE OR REPLACE FUNCTION update_conversation_participants_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_conversation_participants_updated_at
    BEFORE UPDATE ON conversation_participants
    FOR EACH ROW
    EXECUTE FUNCTION update_conversation_participants_updated_at();

-- 用户同步光标表更新时间戳触发器
CREATE OR REPLACE FUNCTION update_user_sync_cursor_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_user_sync_cursor_updated_at
    BEFORE UPDATE ON user_sync_cursor
    FOR EACH ROW
    EXECUTE FUNCTION update_user_sync_cursor_updated_at();

-- Hook配置表更新时间戳触发器
CREATE OR REPLACE FUNCTION update_hook_configs_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_hook_configs_updated_at
    BEFORE UPDATE ON hook_configs
    FOR EACH ROW
    EXECUTE FUNCTION update_hook_configs_updated_at();



-- ============================================================================
-- 初始化完成
-- ============================================================================

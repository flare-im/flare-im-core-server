-- ============================================================================
-- Flare IM 数据库初始化（对齐 flare-proto，单文件合并版）
-- ============================================================================
-- 设计依据: flare-proto/IM_PROTO_DESIGN.md, common/message.proto, common/event.proto
-- 数据库: PostgreSQL + TimescaleDB
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ============================================================================
-- 1. 租户与媒体模块（支撑层）
-- ============================================================================

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
COMMENT ON TABLE tenants IS '租户表（多租户隔离）';
COMMENT ON COLUMN tenants.tenant_id IS '租户ID（主键）';
COMMENT ON COLUMN tenants.name IS '租户名称';
COMMENT ON COLUMN tenants.description IS '租户描述';
COMMENT ON COLUMN tenants.status IS '租户状态（active, suspended, deleted）';
COMMENT ON COLUMN tenants.config IS '租户配置（JSON）';
COMMENT ON COLUMN tenants.quota IS '租户配额（JSON）';
COMMENT ON COLUMN tenants.created_at IS '创建时间';
COMMENT ON COLUMN tenants.updated_at IS '更新时间';
CREATE INDEX IF NOT EXISTS idx_tenants_status ON tenants(status);

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
COMMENT ON COLUMN alert_rules.rule_id IS '规则ID（主键）';
COMMENT ON COLUMN alert_rules.name IS '规则名称';
COMMENT ON COLUMN alert_rules.metric_name IS '指标名称';
COMMENT ON COLUMN alert_rules.condition IS '触发条件';
COMMENT ON COLUMN alert_rules.threshold IS '阈值';
COMMENT ON COLUMN alert_rules.duration_seconds IS '持续时长（秒）';
COMMENT ON COLUMN alert_rules.notification_channels IS '通知渠道列表';
COMMENT ON COLUMN alert_rules.enabled IS '是否启用';
CREATE INDEX IF NOT EXISTS idx_alert_rules_enabled ON alert_rules(enabled);

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
COMMENT ON TABLE alert_history IS '告警历史表';
COMMENT ON COLUMN alert_history.alert_id IS '告警ID（主键）';
COMMENT ON COLUMN alert_history.rule_id IS '关联规则ID';
COMMENT ON COLUMN alert_history.current_value IS '当前指标值';
COMMENT ON COLUMN alert_history.severity IS '严重程度';
COMMENT ON COLUMN alert_history.triggered_at IS '触发时间';
COMMENT ON COLUMN alert_history.resolved_at IS '恢复时间';
CREATE INDEX IF NOT EXISTS idx_alert_history_triggered_at ON alert_history(triggered_at);

DROP TABLE IF EXISTS media_assets CASCADE;
CREATE TABLE media_assets (
    tenant_id TEXT NOT NULL,
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
    PRIMARY KEY (tenant_id, file_id)
);
COMMENT ON TABLE media_assets IS '媒体资产元数据（common/metadata.proto MediaAttachment 等）';
COMMENT ON COLUMN media_assets.tenant_id IS '租户ID';
COMMENT ON COLUMN media_assets.file_id IS '文件唯一标识（租户内唯一）';
COMMENT ON COLUMN media_assets.file_name IS '文件名';
COMMENT ON COLUMN media_assets.mime_type IS 'MIME 类型';
COMMENT ON COLUMN media_assets.file_size IS '文件大小（字节）';
COMMENT ON COLUMN media_assets.url IS '访问 URL';
COMMENT ON COLUMN media_assets.cdn_url IS 'CDN URL';
COMMENT ON COLUMN media_assets.reference_count IS '引用计数';
COMMENT ON COLUMN media_assets.status IS '状态（active 等）';
COMMENT ON COLUMN media_assets.grace_expires_at IS '宽限过期时间';
COMMENT ON COLUMN media_assets.access_type IS '访问类型（private/public）';
CREATE INDEX IF NOT EXISTS idx_media_assets_tenant_uploaded_at ON media_assets(tenant_id, uploaded_at DESC);

DROP TABLE IF EXISTS media_references CASCADE;
CREATE TABLE media_references (
    tenant_id TEXT NOT NULL,
    reference_id TEXT NOT NULL,
    file_id TEXT NOT NULL,
    namespace TEXT NOT NULL,
    owner_id TEXT NOT NULL,
    business_tag TEXT,
    metadata JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP WITH TIME ZONE,
    PRIMARY KEY (tenant_id, reference_id),
    FOREIGN KEY (tenant_id, file_id) REFERENCES media_assets(tenant_id, file_id) ON DELETE CASCADE
);
COMMENT ON TABLE media_references IS '媒体引用表';
COMMENT ON COLUMN media_references.reference_id IS '引用ID（租户内唯一）';
COMMENT ON COLUMN media_references.namespace IS '命名空间';
COMMENT ON COLUMN media_references.owner_id IS '拥有者ID';
COMMENT ON COLUMN media_references.business_tag IS '业务标签';
COMMENT ON COLUMN media_references.expires_at IS '过期时间';
CREATE INDEX IF NOT EXISTS idx_media_references_tenant_file_id ON media_references(tenant_id, file_id);

-- ============================================================================
-- 2. Message 聚合根（Message BC）- 对齐 common/message.proto
-- ============================================================================
-- FSM: CREATED → SENT → DELIVERED → READ | RECALLED | DELETED_SOFT | DELETED_HARD

DROP TABLE IF EXISTS messages CASCADE;
CREATE TABLE messages (
    server_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    client_msg_id TEXT,
    sender_id TEXT NOT NULL,
    receiver_id TEXT,
    content BYTEA,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    message_type TEXT NOT NULL,
    content_type TEXT,
    business_type TEXT,
    source TEXT DEFAULT 'user',
    quote JSONB,
    status TEXT NOT NULL DEFAULT 'CREATED',
    fsm_state_changed_at TIMESTAMP WITH TIME ZONE,
    current_edit_version INTEGER DEFAULT 0,
    last_edited_at TIMESTAMP WITH TIME ZONE,
    recall_reason TEXT,
    recalled_at TIMESTAMP WITH TIME ZONE,
    is_burn_after_read BOOLEAN DEFAULT FALSE,
    burn_after_seconds INTEGER,
    expire_at TIMESTAMP WITH TIME ZONE,
    seq BIGINT,
    conversation_type TEXT,
    tenant_id TEXT NOT NULL,
    attributes JSONB DEFAULT '{}'::jsonb,
    extra JSONB DEFAULT '{}'::jsonb,
    tags TEXT[] DEFAULT '{}',
    offline_push_info JSONB,
    persisted_at TIMESTAMP WITH TIME ZONE,
    delivered_at TIMESTAMP WITH TIME ZONE,
    PRIMARY KEY (created_at, server_id)
);
COMMENT ON TABLE messages IS 'Message 聚合根（common/message.proto）；FSM 与 MessageStatus 对齐';
COMMENT ON COLUMN messages.server_id IS '服务端消息ID（全局唯一）';
COMMENT ON COLUMN messages.conversation_id IS '会话ID';
COMMENT ON COLUMN messages.client_msg_id IS '客户端消息ID（去重/幂等）';
COMMENT ON COLUMN messages.sender_id IS '发送者ID';
COMMENT ON COLUMN messages.receiver_id IS '接收者ID（单聊必填，群聊为空）';
COMMENT ON COLUMN messages.content IS '消息内容（protobuf 编码 BYTEA）';
COMMENT ON COLUMN messages.created_at IS '消息创建时间（Hypertable 分区键，与 proto Message.created_at 一致）';
COMMENT ON COLUMN messages.updated_at IS '行更新时间（编辑/撤回等）';
COMMENT ON COLUMN messages.message_type IS '消息类型（MESSAGE_TYPE_TEXT 等）';
COMMENT ON COLUMN messages.content_type IS '内容子类型（CONTENT_TYPE_PLAIN_TEXT 等）';
COMMENT ON COLUMN messages.business_type IS '业务类型（扩展）';
COMMENT ON COLUMN messages.source IS '消息来源（user, system, bot, admin）';
COMMENT ON COLUMN messages.quote IS '引用内容（QuoteContent JSON）';
COMMENT ON COLUMN messages.status IS 'MessageStatus: CREATED,SENT,DELIVERED,READ,RECALLED,DELETED_SOFT,DELETED_HARD';
COMMENT ON COLUMN messages.fsm_state_changed_at IS 'FSM 状态变更时间';
COMMENT ON COLUMN messages.current_edit_version IS '当前编辑版本号（0=未编辑）';
COMMENT ON COLUMN messages.last_edited_at IS '最后编辑时间';
COMMENT ON COLUMN messages.recall_reason IS '撤回原因（仅 RECALLED 时有效）';
COMMENT ON COLUMN messages.recalled_at IS '撤回时间（仅 RECALLED 时有效）';
COMMENT ON COLUMN messages.is_burn_after_read IS '是否阅后即焚';
COMMENT ON COLUMN messages.burn_after_seconds IS '阅后即焚秒数';
COMMENT ON COLUMN messages.expire_at IS '阅后即焚过期时间';
COMMENT ON COLUMN messages.seq IS '会话内递增序号（Sync 锚点 last_seq）';
COMMENT ON COLUMN messages.conversation_type IS '会话类型（single, group, channel）';
COMMENT ON COLUMN messages.tenant_id IS '租户ID';
COMMENT ON COLUMN messages.attributes IS '业务扩展（如 thread_id）';
COMMENT ON COLUMN messages.extra IS '系统扩展';
COMMENT ON COLUMN messages.tags IS '标签列表';
COMMENT ON COLUMN messages.offline_push_info IS '离线推送信息（JSON）';
COMMENT ON COLUMN messages.persisted_at IS '持久化时间';
COMMENT ON COLUMN messages.delivered_at IS '送达时间';
CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_server_id_unique ON messages(tenant_id, server_id);
CREATE INDEX IF NOT EXISTS idx_messages_conversation_seq ON messages(tenant_id, conversation_id, seq) WHERE seq IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_messages_conversation_created_at ON messages(tenant_id, conversation_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_messages_sender_client_msg_id ON messages(tenant_id, sender_id, client_msg_id) WHERE client_msg_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_messages_conversation_id ON messages(tenant_id, conversation_id);
SELECT create_hypertable('messages', 'created_at', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

-- ============================================================================
-- 3. 事件流（Event BC）- 对齐 common/event.proto EventType
-- ============================================================================

DROP TABLE IF EXISTS events CASCADE;
CREATE TABLE events (
    tenant_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    seq BIGINT NOT NULL,
    event_type TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    operator_id TEXT,
    request_id TEXT,
    event_seq BIGINT,
    payload_message BYTEA,
    payload_recall JSONB,
    payload_edit JSONB,
    payload_delete JSONB,
    payload_read JSONB,
    payload_typing JSONB,
    payload_conversation JSONB,
    payload_conversation_delete JSONB,
    payload_presence JSONB,
    payload_call_signal JSONB,
    payload_reaction JSONB,
    payload_pin JSONB,
    payload_unpin JSONB,
    payload_mark JSONB,
    payload_unmark JSONB,
    payload_custom JSONB,
    PRIMARY KEY (tenant_id, conversation_id, seq)
);
COMMENT ON TABLE events IS '领域事件流（common/event.proto）；SyncRequest 读本表返回 EventEnvelope';
COMMENT ON COLUMN events.tenant_id IS '租户ID';
COMMENT ON COLUMN events.conversation_id IS '会话ID';
COMMENT ON COLUMN events.seq IS '会话内递增序号（主序）';
COMMENT ON COLUMN events.event_type IS 'EventType: EVENT_MESSAGE,EVENT_MESSAGE_RECALL,EVENT_MESSAGE_EDIT,EVENT_MESSAGE_DELETE,EVENT_READ_RECEIPT,EVENT_TYPING,EVENT_CONVERSATION_UPDATE,EVENT_CONVERSATION_DELETE,EVENT_PRESENCE,EVENT_CALL_SIGNAL,EVENT_REACTION,EVENT_PIN,EVENT_UNPIN,EVENT_MARK,EVENT_UNMARK,EVENT_CUSTOM';
COMMENT ON COLUMN events.created_at IS '事件创建时间';
COMMENT ON COLUMN events.operator_id IS '操作者ID';
COMMENT ON COLUMN events.request_id IS '请求ID（与 OperationResponse 关联）';
COMMENT ON COLUMN events.event_seq IS '每用户全局序（可选）';
COMMENT ON COLUMN events.payload_message IS 'EVENT_MESSAGE 时 Message 序列化';
COMMENT ON COLUMN events.payload_recall IS 'MessageRecallEvent';
COMMENT ON COLUMN events.payload_edit IS 'MessageEditEvent';
COMMENT ON COLUMN events.payload_delete IS 'MessageDeleteEvent';
COMMENT ON COLUMN events.payload_read IS 'ReadReceiptEvent';
COMMENT ON COLUMN events.payload_typing IS 'TypingEvent';
COMMENT ON COLUMN events.payload_conversation IS 'ConversationUpdateEvent';
COMMENT ON COLUMN events.payload_conversation_delete IS 'ConversationDeleteEvent';
COMMENT ON COLUMN events.payload_presence IS 'PresenceEvent';
COMMENT ON COLUMN events.payload_call_signal IS 'CallSignalEvent';
COMMENT ON COLUMN events.payload_reaction IS 'ReactionEvent';
COMMENT ON COLUMN events.payload_pin IS 'PinEvent';
COMMENT ON COLUMN events.payload_unpin IS 'UnpinEvent';
COMMENT ON COLUMN events.payload_mark IS 'MarkEvent';
COMMENT ON COLUMN events.payload_unmark IS 'UnmarkEvent';
COMMENT ON COLUMN events.payload_custom IS 'CustomEvent';
CREATE UNIQUE INDEX IF NOT EXISTS idx_events_stream ON events(tenant_id, conversation_id, seq);
CREATE INDEX IF NOT EXISTS idx_events_conversation_seq ON events(tenant_id, conversation_id, seq ASC);
CREATE INDEX IF NOT EXISTS idx_events_created_at ON events(tenant_id, created_at DESC);

-- ============================================================================
-- 4. Message 旁路表（按需 Query）- 对齐 common/models.proto
-- ============================================================================

DROP TABLE IF EXISTS message_edit_history CASCADE;
CREATE TABLE message_edit_history (
    id BIGSERIAL PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    message_id TEXT NOT NULL,
    edit_version INTEGER NOT NULL,
    content BYTEA NOT NULL,
    editor_id TEXT NOT NULL,
    edited_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    reason TEXT,
    show_edited_mark BOOLEAN DEFAULT TRUE,
    UNIQUE(tenant_id, message_id, edit_version)
);
COMMENT ON TABLE message_edit_history IS '编辑历史（GetMessageEditHistory，EditHistory）';
COMMENT ON COLUMN message_edit_history.message_id IS '消息 server_id';
COMMENT ON COLUMN message_edit_history.edit_version IS '编辑版本号（从 1 递增）';
COMMENT ON COLUMN message_edit_history.content IS '该版本内容（protobuf）';
COMMENT ON COLUMN message_edit_history.editor_id IS '编辑者ID';
COMMENT ON COLUMN message_edit_history.show_edited_mark IS '是否显示「已编辑」';
CREATE INDEX IF NOT EXISTS idx_message_edit_history_tenant_message ON message_edit_history(tenant_id, message_id);

DROP TABLE IF EXISTS message_read_records CASCADE;
CREATE TABLE message_read_records (
    id BIGSERIAL PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    message_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    read_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    burned_at TIMESTAMP WITH TIME ZONE,
    UNIQUE(tenant_id, message_id, user_id)
);
COMMENT ON TABLE message_read_records IS '已读记录（GetMessageReadReceipts，MessageReadRecord）';
COMMENT ON COLUMN message_read_records.message_id IS '消息 server_id';
COMMENT ON COLUMN message_read_records.user_id IS '已读用户ID';
COMMENT ON COLUMN message_read_records.read_at IS '已读时间';
COMMENT ON COLUMN message_read_records.burned_at IS '阅后即焚销毁时间';
CREATE INDEX IF NOT EXISTS idx_message_read_records_tenant_message ON message_read_records(tenant_id, message_id);

DROP TABLE IF EXISTS message_visibility CASCADE;
CREATE TABLE message_visibility (
    id BIGSERIAL PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    message_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    visibility_status TEXT NOT NULL DEFAULT 'VISIBLE' CHECK (visibility_status IN ('VISIBLE', 'HIDDEN', 'DELETED')),
    changed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(tenant_id, message_id, user_id)
);
COMMENT ON TABLE message_visibility IS '用户维度消息可见性（软删/隐藏）';
COMMENT ON COLUMN message_visibility.visibility_status IS 'VISIBLE=可见, HIDDEN=隐藏, DELETED=已删除';
COMMENT ON COLUMN message_visibility.changed_at IS '状态变更时间';
CREATE INDEX IF NOT EXISTS idx_message_visibility_tenant_user ON message_visibility(tenant_id, user_id);

DROP TABLE IF EXISTS message_reactions CASCADE;
CREATE TABLE message_reactions (
    id BIGSERIAL PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    message_id TEXT NOT NULL,
    emoji TEXT NOT NULL,
    user_ids TEXT[] NOT NULL DEFAULT '{}',
    count INTEGER NOT NULL DEFAULT 0,
    last_updated TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(tenant_id, message_id, emoji)
);
COMMENT ON TABLE message_reactions IS '反应（GetMessageReactions，Reaction）';
COMMENT ON COLUMN message_reactions.emoji IS '表情（如 👍、❤️）';
COMMENT ON COLUMN message_reactions.user_ids IS '点赞用户ID列表';
COMMENT ON COLUMN message_reactions.count IS '计数（等于 user_ids 长度）';
CREATE INDEX IF NOT EXISTS idx_message_reactions_tenant_message ON message_reactions(tenant_id, message_id);

DROP TABLE IF EXISTS pinned_messages CASCADE;
CREATE TABLE pinned_messages (
    id BIGSERIAL PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    message_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    pinned_by TEXT NOT NULL,
    pinned_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expire_at TIMESTAMP WITH TIME ZONE,
    reason TEXT,
    UNIQUE(tenant_id, conversation_id, message_id)
);
COMMENT ON TABLE pinned_messages IS '置顶消息（PinnedMessageInfo，PinEvent/UnpinEvent）';
COMMENT ON COLUMN pinned_messages.pinned_by IS '置顶操作者ID';
COMMENT ON COLUMN pinned_messages.expire_at IS '置顶到期时间（可选）';
COMMENT ON COLUMN pinned_messages.reason IS '置顶说明';
CREATE INDEX IF NOT EXISTS idx_pinned_messages_tenant_conversation ON pinned_messages(tenant_id, conversation_id);

DROP TABLE IF EXISTS marked_messages CASCADE;
CREATE TABLE marked_messages (
    id BIGSERIAL PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    message_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    mark_type TEXT NOT NULL CHECK (mark_type IN ('IMPORTANT', 'TODO', 'DONE', 'CUSTOM')),
    color TEXT,
    marked_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(tenant_id, message_id, user_id, mark_type)
);
COMMENT ON TABLE marked_messages IS '消息标记（MarkedMessageInfo，MarkType）';
COMMENT ON COLUMN marked_messages.mark_type IS 'IMPORTANT=重要, TODO=待办, DONE=已处理, CUSTOM=自定义';
COMMENT ON COLUMN marked_messages.color IS '标记颜色（可选）';
CREATE INDEX IF NOT EXISTS idx_marked_messages_tenant_user ON marked_messages(tenant_id, user_id);

-- 操作历史（Event 序列化存储，QueryMessageOperations 读模型）
DROP TABLE IF EXISTS message_operation_history CASCADE;
CREATE TABLE message_operation_history (
    id BIGSERIAL PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    message_id TEXT NOT NULL,
    operation_type TEXT NOT NULL,
    operator_id TEXT NOT NULL DEFAULT '',
    target_user_id TEXT DEFAULT '',
    operation_data JSONB DEFAULT '{}'::jsonb,
    show_notice BOOLEAN DEFAULT FALSE,
    notice_text TEXT DEFAULT '',
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    metadata JSONB DEFAULT '{}'::jsonb
);
COMMENT ON TABLE message_operation_history IS '领域事件历史（Event 存 operation_data.event_base64）；QueryMessageOperations 读模型';
COMMENT ON COLUMN message_operation_history.operation_data IS 'JSON: { "event_base64": "..." } 或兼容旧格式';
CREATE INDEX IF NOT EXISTS idx_message_operation_history_tenant_message_id ON message_operation_history(tenant_id, message_id);
CREATE INDEX IF NOT EXISTS idx_message_operation_history_timestamp ON message_operation_history(message_id, timestamp ASC);

-- ============================================================================
-- 5. 会话写模型（Session BC）- 对齐 common/conversation.proto
-- ============================================================================

DROP TABLE IF EXISTS conversation_participants CASCADE;
DROP TABLE IF EXISTS conversations CASCADE;

CREATE TABLE conversations (
    tenant_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    conversation_type TEXT NOT NULL,
    business_type TEXT NOT NULL DEFAULT '',
    display_name TEXT,
    avatar_url TEXT,
    description TEXT,
    attributes JSONB DEFAULT '{}'::jsonb,
    visibility TEXT DEFAULT 'public',
    lifecycle_state TEXT DEFAULT 'active',
    announcement TEXT,
    announcement_updated_at TIMESTAMP WITH TIME ZONE,
    announcement_updated_by TEXT,
    owner_id TEXT,
    max_members INTEGER,
    extended_config JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    metadata JSONB DEFAULT '{}'::jsonb,
    last_message_seq BIGINT,
    PRIMARY KEY (tenant_id, conversation_id)
);
COMMENT ON TABLE conversations IS '会话元数据（ConversationDetail 写模型）';
COMMENT ON COLUMN conversations.last_message_seq IS '最后一条消息的 seq（用于未读数计算）';
COMMENT ON COLUMN conversations.conversation_type IS '会话类型（single, group, channel）';
COMMENT ON COLUMN conversations.business_type IS '业务类型';
COMMENT ON COLUMN conversations.display_name IS '展示名称';
COMMENT ON COLUMN conversations.avatar_url IS '头像 URL';
COMMENT ON COLUMN conversations.description IS '会话描述';
COMMENT ON COLUMN conversations.attributes IS '会话属性（JSON）';
COMMENT ON COLUMN conversations.visibility IS '可见性（public, private 等）';
COMMENT ON COLUMN conversations.lifecycle_state IS '生命周期（active, archived, deleted）';
COMMENT ON COLUMN conversations.announcement IS '会话公告';
COMMENT ON COLUMN conversations.announcement_updated_at IS '公告更新时间';
COMMENT ON COLUMN conversations.announcement_updated_by IS '公告更新人';
COMMENT ON COLUMN conversations.owner_id IS '拥有者ID';
COMMENT ON COLUMN conversations.max_members IS '最大成员数';
COMMENT ON COLUMN conversations.extended_config IS '扩展配置（JSON）';
COMMENT ON COLUMN conversations.metadata IS '扩展元数据';

CREATE TABLE conversation_participants (
    tenant_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    roles TEXT[] DEFAULT '{}',
    muted BOOLEAN DEFAULT FALSE,
    pinned BOOLEAN DEFAULT FALSE,
    attributes JSONB,
    joined_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    nickname TEXT,
    last_read_msg_seq BIGINT DEFAULT 0,
    last_sync_msg_seq BIGINT DEFAULT 0,
    unread_count INTEGER DEFAULT 0,
    is_deleted BOOLEAN DEFAULT FALSE,
    mute_until TIMESTAMP WITH TIME ZONE,
    quit_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, conversation_id, user_id),
    FOREIGN KEY (tenant_id, conversation_id) REFERENCES conversations(tenant_id, conversation_id) ON DELETE CASCADE
);
COMMENT ON TABLE conversation_participants IS '参与者与读模型字段（ConversationLight/Summary 未读与游标）';
COMMENT ON COLUMN conversation_participants.roles IS '角色列表（owner, admin, member 等）';
COMMENT ON COLUMN conversation_participants.muted IS '是否静音';
COMMENT ON COLUMN conversation_participants.pinned IS '是否置顶';
COMMENT ON COLUMN conversation_participants.nickname IS '群昵称';
COMMENT ON COLUMN conversation_participants.last_read_msg_seq IS '已读到的 seq（未读数计算）';
COMMENT ON COLUMN conversation_participants.last_sync_msg_seq IS '最后同步的 seq（Sync 游标）';
COMMENT ON COLUMN conversation_participants.unread_count IS '未读数（冗余）';
COMMENT ON COLUMN conversation_participants.is_deleted IS '用户侧删除会话（软删）';
COMMENT ON COLUMN conversation_participants.mute_until IS '静音截止时间';
COMMENT ON COLUMN conversation_participants.quit_at IS '退出时间（NULL=仍在会话中）';
CREATE INDEX IF NOT EXISTS idx_conversations_tenant_updated ON conversations(tenant_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_conversation_participants_tenant_user ON conversation_participants(tenant_id, user_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_conversation_participants_tenant_conv ON conversation_participants(tenant_id, conversation_id);

-- ============================================================================
-- 6. Sync 游标
-- ============================================================================

DROP TABLE IF EXISTS user_sync_cursor CASCADE;
CREATE TABLE user_sync_cursor (
    tenant_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    last_synced_seq BIGINT NOT NULL DEFAULT 0,
    last_synced_ts BIGINT NOT NULL DEFAULT 0,
    device_id TEXT,
    version INTEGER DEFAULT 1,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, user_id, conversation_id)
);
COMMENT ON TABLE user_sync_cursor IS '按会话 Sync 游标（last_seq）；可与 conversation_participants.last_sync_msg_seq 二选一';
COMMENT ON COLUMN user_sync_cursor.last_synced_seq IS '最后同步的 seq';
COMMENT ON COLUMN user_sync_cursor.last_synced_ts IS '最后同步时间戳（毫秒）';
COMMENT ON COLUMN user_sync_cursor.device_id IS '设备ID（可选，设备级游标）';
COMMENT ON COLUMN user_sync_cursor.version IS '版本号（乐观锁）';
CREATE INDEX IF NOT EXISTS idx_user_sync_cursor_tenant_user ON user_sync_cursor(tenant_id, user_id);

-- ============================================================================
-- 7. Hook 引擎（支撑层）
-- ============================================================================

DROP TABLE IF EXISTS hook_executions CASCADE;
DROP TABLE IF EXISTS hook_configs CASCADE;

CREATE TABLE hook_configs (
    id BIGSERIAL PRIMARY KEY,
    hook_id TEXT UNIQUE,
    tenant_id TEXT,
    hook_type TEXT NOT NULL,
    name TEXT NOT NULL,
    version TEXT,
    description TEXT,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    priority INTEGER NOT NULL DEFAULT 100,
    timeout_ms BIGINT NOT NULL DEFAULT 1000,
    max_retries INTEGER NOT NULL DEFAULT 0,
    error_policy TEXT NOT NULL DEFAULT 'fail_fast',
    selector_config JSONB NOT NULL DEFAULT '{}'::jsonb,
    transport_config JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(tenant_id, hook_type, name)
);
COMMENT ON TABLE hook_configs IS 'Hook 配置表';
COMMENT ON COLUMN hook_configs.hook_id IS 'Hook 唯一标识';
COMMENT ON COLUMN hook_configs.hook_type IS 'Hook 类型（pre_send, post_send, recall 等）';
COMMENT ON COLUMN hook_configs.priority IS '优先级（越小越高）';
COMMENT ON COLUMN hook_configs.timeout_ms IS '超时（毫秒）';
COMMENT ON COLUMN hook_configs.error_policy IS '错误策略（fail_fast, retry, ignore）';
COMMENT ON COLUMN hook_configs.selector_config IS '选择器配置（JSON）';
COMMENT ON COLUMN hook_configs.transport_config IS '传输配置（JSON）';
CREATE INDEX IF NOT EXISTS idx_hook_configs_tenant_type ON hook_configs(tenant_id, hook_type, enabled);

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
COMMENT ON TABLE hook_executions IS 'Hook 执行记录表';
COMMENT ON COLUMN hook_executions.hook_name IS 'Hook 名称';
COMMENT ON COLUMN hook_executions.message_id IS '关联消息ID（可选）';
COMMENT ON COLUMN hook_executions.latency_ms IS '耗时（毫秒）';
COMMENT ON COLUMN hook_executions.executed_at IS '执行时间';
CREATE INDEX IF NOT EXISTS idx_hook_executions_tenant_executed ON hook_executions(tenant_id, executed_at DESC);

-- ============================================================================
-- 8. TimescaleDB 策略（压缩、连续聚合，可选）
-- ============================================================================

ALTER TABLE messages SET (
    timescaledb.enable_columnstore = true,
    timescaledb.segmentby = 'tenant_id, conversation_id',
    timescaledb.orderby = 'created_at DESC, server_id'
);

DO $$
BEGIN
    BEGIN
        CALL add_columnstore_policy('messages', after => INTERVAL '30 days');
    EXCEPTION WHEN undefined_function OR syntax_error THEN
        RAISE NOTICE 'add_columnstore_policy not available, skip';
    END;
END $$;

DROP MATERIALIZED VIEW IF EXISTS messages_hourly_stats CASCADE;
CREATE MATERIALIZED VIEW messages_hourly_stats
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', created_at) AS hour,
    tenant_id,
    conversation_id,
    COUNT(*) AS message_count,
    COUNT(DISTINCT sender_id) AS unique_senders
FROM messages
GROUP BY hour, tenant_id, conversation_id;

DO $$
BEGIN
    BEGIN
        EXECUTE 'CALL add_continuous_aggregate_policy(''messages_hourly_stats'', start_offset => INTERVAL ''3 hours'', end_offset => INTERVAL ''1 hour'', schedule_interval => INTERVAL ''1 hour'')';
    EXCEPTION WHEN undefined_function OR syntax_error THEN
        BEGIN
            PERFORM add_continuous_aggregate_policy('messages_hourly_stats', start_offset => INTERVAL '3 hours', end_offset => INTERVAL '1 hour', schedule_interval => INTERVAL '1 hour');
        EXCEPTION WHEN undefined_function THEN
            RAISE NOTICE 'add_continuous_aggregate_policy not available, skip';
        END;
    END;
END $$;

-- ============================================================================
-- 9. 触发器（updated_at）
-- ============================================================================

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_conversations_updated_at ON conversations;
CREATE TRIGGER trigger_conversations_updated_at
    BEFORE UPDATE ON conversations FOR EACH ROW EXECUTE PROCEDURE set_updated_at();

DROP TRIGGER IF EXISTS trigger_conversation_participants_updated_at ON conversation_participants;
CREATE TRIGGER trigger_conversation_participants_updated_at
    BEFORE UPDATE ON conversation_participants FOR EACH ROW EXECUTE PROCEDURE set_updated_at();

DROP TRIGGER IF EXISTS trigger_user_sync_cursor_updated_at ON user_sync_cursor;
CREATE TRIGGER trigger_user_sync_cursor_updated_at
    BEFORE UPDATE ON user_sync_cursor FOR EACH ROW EXECUTE PROCEDURE set_updated_at();

DROP TRIGGER IF EXISTS trigger_hook_configs_updated_at ON hook_configs;
CREATE TRIGGER trigger_hook_configs_updated_at
    BEFORE UPDATE ON hook_configs FOR EACH ROW EXECUTE PROCEDURE set_updated_at();

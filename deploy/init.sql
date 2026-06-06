-- ============================================================================
-- Flare IM Core 数据库初始化（唯一入口）
-- ============================================================================
-- 设计依据:
--   - common/message.proto   (Message, MessageStatus, MessageSource, MessageType, MessageTimeline, MessageReadRecord)
--   - common/conversation.proto (ConversationDetail, ConversationParticipant, ConversationLight/Summary, DevicePresence)
--   - common/event.proto     (Event, EventType, *Event payloads)
--   - common/models.proto    (PinnedMessageInfo, MarkedMessageInfo, EditHistory, Reaction, ThreadInfo)
--   - common/enums.proto     (DeleteType, MarkType, ReactionAction)
--   - storage.proto         (StoreMessage, VisibilityStatus)
--   - flare-capability: hook_configs / hook_executions（Hook 引擎）；capability_*（CapabilityService 策略）
-- 数据库: PostgreSQL + TimescaleDB
-- 维护约定: 与本仓 IM 相关的 DDL 变更请在本文件增改，勿另建零散 .sql，便于单源对齐与评审。
-- 开发阶段: 可随时删库或清空数据卷后对目标 PostgreSQL 执行本文件全量初始化
--   例: psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f deploy/init.sql
-- 表结构: 凡 CREATE TABLE 前须有对应 DROP TABLE IF EXISTS ... CASCADE。
-- 可选标记 FLARE_EXTRACT:* 仅用于在编辑器中定位第 9 节（Hook+Capability）起止；改该节 DDL 时请保持两标记包住完整 DROP/CREATE。
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ============================================================================
-- 1. 租户与支撑层
-- ============================================================================

DROP TABLE IF EXISTS tenants CASCADE;
CREATE TABLE tenants (
    tenant_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    config JSONB DEFAULT '{}'::jsonb,
    quota JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE tenants IS '租户表（多租户隔离）';
COMMENT ON COLUMN tenants.tenant_id IS '租户 ID（主键）';
COMMENT ON COLUMN tenants.name IS '租户名称';
COMMENT ON COLUMN tenants.description IS '租户描述';
COMMENT ON COLUMN tenants.status IS '状态：active / suspended / deleted';
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
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE alert_rules IS '告警规则表';
COMMENT ON COLUMN alert_rules.rule_id IS '规则 ID（主键）';
COMMENT ON COLUMN alert_rules.name IS '规则名称';
COMMENT ON COLUMN alert_rules.metric_name IS '指标名称';
COMMENT ON COLUMN alert_rules.condition IS '触发条件';
COMMENT ON COLUMN alert_rules.threshold IS '阈值';
COMMENT ON COLUMN alert_rules.duration_seconds IS '持续时长（秒）';
COMMENT ON COLUMN alert_rules.notification_channels IS '通知渠道列表';
COMMENT ON COLUMN alert_rules.enabled IS '是否启用';
COMMENT ON COLUMN alert_rules.created_at IS '创建时间';
COMMENT ON COLUMN alert_rules.updated_at IS '更新时间';
CREATE INDEX IF NOT EXISTS idx_alert_rules_enabled ON alert_rules(enabled);

DROP TABLE IF EXISTS alert_history CASCADE;
CREATE TABLE alert_history (
    alert_id TEXT PRIMARY KEY,
    rule_id TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    current_value DOUBLE PRECISION NOT NULL,
    threshold TEXT NOT NULL,
    severity TEXT NOT NULL,
    triggered_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    resolved_at TIMESTAMPTZ
);
COMMENT ON TABLE alert_history IS '告警历史表';
COMMENT ON COLUMN alert_history.alert_id IS '告警 ID（主键）';
COMMENT ON COLUMN alert_history.rule_id IS '关联规则 ID';
COMMENT ON COLUMN alert_history.metric_name IS '指标名称';
COMMENT ON COLUMN alert_history.current_value IS '当前指标值';
COMMENT ON COLUMN alert_history.threshold IS '阈值';
COMMENT ON COLUMN alert_history.severity IS '严重程度';
COMMENT ON COLUMN alert_history.triggered_at IS '触发时间';
COMMENT ON COLUMN alert_history.resolved_at IS '恢复时间';
CREATE INDEX IF NOT EXISTS idx_alert_history_triggered_at ON alert_history(triggered_at);

-- ============================================================================
-- 2. Message 聚合根（common/message.proto Message）
-- ============================================================================
-- 与 proto 严格对齐：无 receiver_id，单聊时 channel_id=对方 user_id。
-- FSM: MESSAGE_STATUS_CREATED → SENT → DELIVERED → READ | RECALLED | DELETED_SOFT | DELETED_HARD

DROP TABLE IF EXISTS messages CASCADE;
CREATE TABLE messages (
    tenant_id TEXT NOT NULL,
    server_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    client_msg_id TEXT,
    sender_id TEXT NOT NULL,
    sender_name TEXT,
    sender_avatar TEXT,
    channel_id TEXT,
    source INT NOT NULL DEFAULT 1,  -- MessageSource: USER=1, SYSTEM=2, BOT=3, ADMIN=4
    seq BIGINT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    conversation_type INT NOT NULL DEFAULT 0,  -- ConversationType: SINGLE=1, GROUP=2, AI=3, SYSTEM=4, CUSTOMER=5, TEMP=6
    message_type INT NOT NULL DEFAULT 0,      -- MessageType 枚举值
    content BYTEA,
    status INT NOT NULL DEFAULT 1,  -- MessageStatus: CREATED=1, SENT=2, DELIVERED=3, READ=4, FAILED=5, RECALLED=6, DELETED_HARD=7, DELETED_SOFT=8
    burn_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    burn_after_read_seconds BIGINT,
    burn_status SMALLINT NOT NULL DEFAULT 0, -- BurnStatus: NONE=0 INIT=1 READ=2 BURN_PENDING=3 BURNED=4 HARD_DELETED=5
    first_read_at BIGINT,
    burn_at BIGINT,
    burned_at BIGINT,
    offline_push_info JSONB,
    extra JSONB DEFAULT '{}'::jsonb,
    extensions JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    persisted_at TIMESTAMPTZ,
    delivered_at TIMESTAMPTZ,
    PRIMARY KEY (created_at, server_id)
);
COMMENT ON TABLE messages IS 'Message 聚合根（common/message.proto）；一会话一流，seq 主序';
COMMENT ON COLUMN messages.tenant_id IS '租户 ID（多租户隔离，非 proto 字段）';
COMMENT ON COLUMN messages.server_id IS '服务端消息 ID（全局唯一）';
COMMENT ON COLUMN messages.conversation_id IS '会话 ID';
COMMENT ON COLUMN messages.client_msg_id IS '客户端消息 ID（去重/幂等）';
COMMENT ON COLUMN messages.sender_id IS '发送者 ID';
COMMENT ON COLUMN messages.sender_name IS '发送者昵称（展示用，可选）';
COMMENT ON COLUMN messages.sender_avatar IS '发送者头像 URL（展示用，可选）';
COMMENT ON COLUMN messages.channel_id IS '会话频道 ID：单聊=对方 user_id，群聊=群 ID，频道/话题=对应 ID（proto 无 receiver_id）';
COMMENT ON COLUMN messages.source IS '消息来源：1=USER 2=SYSTEM 3=BOT 4=ADMIN';
COMMENT ON COLUMN messages.seq IS '会话内序列号（读扩散主序）';
COMMENT ON COLUMN messages.timestamp IS '消息时间戳';
COMMENT ON COLUMN messages.conversation_type IS '会话类型：0=UNSPECIFIED 1=SINGLE 2=GROUP 3=AI 4=SYSTEM 5=CUSTOMER 6=TEMP（与 CID 前缀一致）';
COMMENT ON COLUMN messages.message_type IS '消息类型（MessageType 枚举值，见 message.proto）';
COMMENT ON COLUMN messages.content IS '消息体 bytes（按 message_type 解析，见 message_content.proto）';
COMMENT ON COLUMN messages.status IS '消息状态：1=CREATED 2=SENT 3=DELIVERED 4=READ 5=FAILED 6=RECALLED 7=DELETED_HARD 8=DELETED_SOFT';
COMMENT ON COLUMN messages.burn_enabled IS '是否启用阅后即焚';
COMMENT ON COLUMN messages.burn_after_read_seconds IS '首次阅读后多少秒焚毁';
COMMENT ON COLUMN messages.burn_status IS '阅后即焚状态：0=NONE 1=INIT 2=READ 3=BURN_PENDING 4=BURNED 5=HARD_DELETED';
COMMENT ON COLUMN messages.first_read_at IS '首次真实阅读时间（Unix 秒，服务端写入）';
COMMENT ON COLUMN messages.burn_at IS '服务端权威焚毁时间（Unix 秒）';
COMMENT ON COLUMN messages.burned_at IS '实际焚毁时间（Unix 秒）';
COMMENT ON COLUMN messages.offline_push_info IS '离线推送展示（OfflinePushInfo JSON）';
COMMENT ON COLUMN messages.extra IS '扩展键值（conversation_type、business_type、thread_id 等）';
COMMENT ON COLUMN messages.extensions IS '业务扩展（key 建议命名空间）';
COMMENT ON COLUMN messages.created_at IS '入库时间（Hypertable 分区键）';
COMMENT ON COLUMN messages.persisted_at IS '持久化完成时间（MessageTimeline.persisted_at）';
COMMENT ON COLUMN messages.delivered_at IS '投递时间（MessageTimeline.delivered_at）';
-- TimescaleDB: 唯一索引必须包含分区列 created_at
CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_tenant_server_id ON messages(tenant_id, server_id, created_at);
CREATE INDEX IF NOT EXISTS idx_messages_tenant_conv_seq ON messages(tenant_id, conversation_id, seq);
CREATE INDEX IF NOT EXISTS idx_messages_conversation_ts ON messages(tenant_id, conversation_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_messages_sender_client ON messages(tenant_id, sender_id, client_msg_id) WHERE client_msg_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_messages_burn_due ON messages(tenant_id, burn_status, burn_at) WHERE burn_status = 3 AND burn_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_messages_tenant_timestamp ON messages(tenant_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_messages_tenant_sender_timestamp ON messages(tenant_id, sender_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_messages_tenant_message_type_timestamp ON messages(tenant_id, message_type, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_messages_tenant_status_timestamp ON messages(tenant_id, status, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_messages_tenant_source_timestamp ON messages(tenant_id, source, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_messages_tenant_channel_timestamp ON messages(tenant_id, channel_id, timestamp DESC) WHERE channel_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_messages_tenant_persisted_at ON messages(tenant_id, persisted_at DESC) WHERE persisted_at IS NOT NULL;

SELECT create_hypertable('messages', 'created_at', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

-- 非 hypertable 的 durable write ledger：Timescale 唯一索引必须包含分区列，
-- 因此消息 ID 的最终幂等屏障放在普通表中。
DROP TABLE IF EXISTS message_write_ledger CASCADE;
CREATE TABLE message_write_ledger (
    tenant_id TEXT NOT NULL,
    server_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    seq BIGINT NOT NULL,
    write_state TEXT NOT NULL DEFAULT 'broker_accepted',
    archive_persisted_at TIMESTAMPTZ,
    storage_persisted_at TIMESTAMPTZ,
    wal_cleaned_at TIMESTAMPTZ,
    ack_published_at TIMESTAMPTZ,
    failed_at TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, server_id)
);
COMMENT ON TABLE message_write_ledger IS '消息写入幂等账本：保障 server_id 级 durable 去重';
COMMENT ON COLUMN message_write_ledger.tenant_id IS '租户 ID';
COMMENT ON COLUMN message_write_ledger.server_id IS '服务端消息 ID';
COMMENT ON COLUMN message_write_ledger.conversation_id IS '会话 ID';
COMMENT ON COLUMN message_write_ledger.seq IS '会话内序列号';
COMMENT ON COLUMN message_write_ledger.write_state IS '写入状态：broker_accepted/archive_persisted/storage_persisted/wal_cleaned/ack_published/*_failed';
COMMENT ON COLUMN message_write_ledger.last_error IS '最后一次写链路错误，用于恢复和管理端诊断';
CREATE INDEX IF NOT EXISTS idx_message_write_ledger_conversation_seq
    ON message_write_ledger(tenant_id, conversation_id, seq);
CREATE INDEX IF NOT EXISTS idx_message_write_ledger_tenant_updated
    ON message_write_ledger(tenant_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_message_write_ledger_tenant_state_updated
    ON message_write_ledger(tenant_id, write_state, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_message_write_ledger_failed_updated
    ON message_write_ledger(tenant_id, updated_at DESC)
    WHERE failed_at IS NOT NULL;

-- ============================================================================
-- 3. 事件流（common/event.proto Event, EventType）
-- ============================================================================

DROP TABLE IF EXISTS events CASCADE;
CREATE TABLE events (
    tenant_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    seq BIGINT NOT NULL,
    event_type INT NOT NULL,  -- EventType: EVENT_MESSAGE=1, EVENT_MESSAGE_RECALL=2, ...
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    operator_id TEXT,
    request_id TEXT,
    event_seq BIGINT,
    payload BYTEA,
    PRIMARY KEY (tenant_id, conversation_id, seq)
);
COMMENT ON TABLE events IS '领域事件流（common/event.proto）；Sync 读本表返回 EventEnvelope';
COMMENT ON COLUMN events.tenant_id IS '租户 ID';
COMMENT ON COLUMN events.conversation_id IS '会话 ID';
COMMENT ON COLUMN events.seq IS '会话内事件序列号（主序）';
COMMENT ON COLUMN events.event_type IS '事件类型：1=EVENT_MESSAGE 2=RECALL 3=EDIT 4=DELETE 5=READ_RECEIPT … 见 EventType';
COMMENT ON COLUMN events.created_at IS '事件产生时间';
COMMENT ON COLUMN events.operator_id IS '操作者 user_id';
COMMENT ON COLUMN events.request_id IS '上行请求 ID（与 OperationResponse 关联）';
COMMENT ON COLUMN events.event_seq IS '关联消息 seq（如反应/置顶针对的 message）';
COMMENT ON COLUMN events.payload IS 'Event.payload oneof 序列化（按 event_type 解析为 Message/MessageRecallEvent/...）';
CREATE UNIQUE INDEX IF NOT EXISTS idx_events_stream ON events(tenant_id, conversation_id, seq);
CREATE INDEX IF NOT EXISTS idx_events_tenant_conversation_event_seq_type
    ON events(tenant_id, conversation_id, event_seq, event_type, seq)
    WHERE event_seq IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_events_created_at ON events(tenant_id, created_at DESC);

DROP TABLE IF EXISTS message_export_tasks CASCADE;
CREATE TABLE message_export_tasks (
    tenant_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ NOT NULL,
    filters JSONB NOT NULL DEFAULT '[]'::jsonb,
    requested_by TEXT,
    request_id TEXT,
    trace_id TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    object_key TEXT,
    row_count BIGINT,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMPTZ,
    PRIMARY KEY (tenant_id, task_id)
);
COMMENT ON TABLE message_export_tasks IS 'Admin 消息导出任务；HTTP/gRPC 只登记任务，后续 worker 生成对象文件';
CREATE INDEX IF NOT EXISTS idx_message_export_tasks_status_created
    ON message_export_tasks(tenant_id, status, created_at);
CREATE INDEX IF NOT EXISTS idx_message_export_tasks_conversation_time
    ON message_export_tasks(tenant_id, conversation_id, start_time, end_time);

-- ============================================================================
-- 4. Message 旁路表（common/models.proto, MessageReadRecord）
-- ============================================================================

DROP TABLE IF EXISTS message_edit_history CASCADE;
CREATE TABLE message_edit_history (
    id BIGSERIAL PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    message_id TEXT NOT NULL,
    edit_version INT NOT NULL,
    content BYTEA NOT NULL,
    editor_id TEXT NOT NULL,
    edited_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    reason TEXT,
    show_edited_mark BOOLEAN DEFAULT TRUE,
    UNIQUE(tenant_id, message_id, edit_version)
);
COMMENT ON TABLE message_edit_history IS '编辑历史（EditHistory）；QueryMessageEditHistory';
COMMENT ON COLUMN message_edit_history.id IS '自增主键';
COMMENT ON COLUMN message_edit_history.tenant_id IS '租户 ID';
COMMENT ON COLUMN message_edit_history.message_id IS '消息 server_id';
COMMENT ON COLUMN message_edit_history.edit_version IS '编辑版本号（从 1 递增）';
COMMENT ON COLUMN message_edit_history.content IS '该版本内容（protobuf 编码）';
COMMENT ON COLUMN message_edit_history.editor_id IS '编辑者 user_id';
COMMENT ON COLUMN message_edit_history.edited_at IS '编辑时间';
COMMENT ON COLUMN message_edit_history.reason IS '编辑原因（可选）';
COMMENT ON COLUMN message_edit_history.show_edited_mark IS '是否展示「已编辑」';
CREATE INDEX IF NOT EXISTS idx_message_edit_history_tenant_message ON message_edit_history(tenant_id, message_id);

DROP TABLE IF EXISTS message_read_records CASCADE;
CREATE TABLE message_read_records (
    id BIGSERIAL PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    message_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    read_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    burned_at TIMESTAMPTZ,
    UNIQUE(tenant_id, message_id, user_id)
);
COMMENT ON TABLE message_read_records IS '已读记录（MessageReadRecord）；QueryMessageReadList';
COMMENT ON COLUMN message_read_records.id IS '自增主键';
COMMENT ON COLUMN message_read_records.tenant_id IS '租户 ID';
COMMENT ON COLUMN message_read_records.message_id IS '消息 server_id';
COMMENT ON COLUMN message_read_records.user_id IS '已读用户 ID';
COMMENT ON COLUMN message_read_records.read_at IS '已读时间';
COMMENT ON COLUMN message_read_records.burned_at IS '阅后即焚已烧毁时间（可选）';
CREATE INDEX IF NOT EXISTS idx_message_read_records_tenant_message ON message_read_records(tenant_id, message_id);

DROP TABLE IF EXISTS message_visibility CASCADE;
CREATE TABLE message_visibility (
    id BIGSERIAL PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    message_id TEXT NOT NULL,
    user_id TEXT NOT NULL DEFAULT '',
    scope INT NOT NULL DEFAULT 1,           -- DeleteScope: USER_PRIVATE=1, CONVERSATION_GLOBAL=2
    visibility_status INT NOT NULL DEFAULT 0,  -- VisibilityStatus: VISIBLE=0, HIDDEN=1, DELETED=2
    changed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (
        (scope = 1 AND user_id <> '')
        OR (scope = 2 AND user_id = '')
    ),
    UNIQUE(tenant_id, message_id, user_id, scope)
);
COMMENT ON TABLE message_visibility IS '用户维度消息可见性（storage.proto VisibilityStatus）';
COMMENT ON COLUMN message_visibility.id IS '自增主键';
COMMENT ON COLUMN message_visibility.tenant_id IS '租户 ID';
COMMENT ON COLUMN message_visibility.message_id IS '消息 server_id';
COMMENT ON COLUMN message_visibility.user_id IS '用户 ID（scope=2 时为空串）';
COMMENT ON COLUMN message_visibility.scope IS '删除作用域：1=仅自己 2=所有人';
COMMENT ON COLUMN message_visibility.visibility_status IS '可见性：0=VISIBLE 1=HIDDEN 2=DELETED';
COMMENT ON COLUMN message_visibility.changed_at IS '状态变更时间';
CREATE INDEX IF NOT EXISTS idx_message_visibility_tenant_user ON message_visibility(tenant_id, user_id);
CREATE INDEX IF NOT EXISTS idx_message_visibility_tenant_message_scope ON message_visibility(tenant_id, message_id, scope);

DROP TABLE IF EXISTS message_reactions CASCADE;
CREATE TABLE message_reactions (
    id BIGSERIAL PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    message_id TEXT NOT NULL,
    emoji TEXT NOT NULL,
    user_ids TEXT[] NOT NULL DEFAULT '{}',
    count INT NOT NULL DEFAULT 0,
    last_updated TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(tenant_id, message_id, emoji)
);
COMMENT ON TABLE message_reactions IS '反应（Reaction）；QueryMessageReactions';
COMMENT ON COLUMN message_reactions.id IS '自增主键';
COMMENT ON COLUMN message_reactions.tenant_id IS '租户 ID';
COMMENT ON COLUMN message_reactions.message_id IS '消息 server_id';
COMMENT ON COLUMN message_reactions.emoji IS '表情标识（如 👍）';
COMMENT ON COLUMN message_reactions.user_ids IS '点了该表情的用户 ID 列表';
COMMENT ON COLUMN message_reactions.count IS '该表情被点击次数';
COMMENT ON COLUMN message_reactions.last_updated IS '最后更新时间';
COMMENT ON COLUMN message_reactions.created_at IS '首次添加时间';
CREATE INDEX IF NOT EXISTS idx_message_reactions_tenant_message ON message_reactions(tenant_id, message_id);

DROP TABLE IF EXISTS pinned_messages CASCADE;
CREATE TABLE pinned_messages (
    id BIGSERIAL PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    message_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    pinned_by TEXT NOT NULL,
    pinned_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expire_at TIMESTAMPTZ,
    reason TEXT,
    UNIQUE(tenant_id, conversation_id, message_id)
);
COMMENT ON TABLE pinned_messages IS '置顶消息（PinnedMessageInfo）；PinEvent/UnpinEvent';
COMMENT ON COLUMN pinned_messages.id IS '自增主键';
COMMENT ON COLUMN pinned_messages.tenant_id IS '租户 ID';
COMMENT ON COLUMN pinned_messages.message_id IS '被置顶消息的 server_id';
COMMENT ON COLUMN pinned_messages.conversation_id IS '会话 ID';
COMMENT ON COLUMN pinned_messages.pinned_by IS '置顶操作者 user_id';
COMMENT ON COLUMN pinned_messages.pinned_at IS '置顶时间';
COMMENT ON COLUMN pinned_messages.expire_at IS '置顶过期时间（空=长期）';
COMMENT ON COLUMN pinned_messages.reason IS '置顶说明';
CREATE INDEX IF NOT EXISTS idx_pinned_messages_tenant_conversation ON pinned_messages(tenant_id, conversation_id);

DROP TABLE IF EXISTS marked_messages CASCADE;
CREATE TABLE marked_messages (
    id BIGSERIAL PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    message_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    mark_type INT NOT NULL,  -- MarkType: IMPORTANT=1, TODO=2, DONE=3, CUSTOM=4
    color TEXT,
    marked_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(tenant_id, message_id, user_id, mark_type)
);
COMMENT ON TABLE marked_messages IS '消息标记（MarkedMessageInfo）；MarkEvent/UnmarkEvent';
COMMENT ON COLUMN marked_messages.id IS '自增主键';
COMMENT ON COLUMN marked_messages.tenant_id IS '租户 ID';
COMMENT ON COLUMN marked_messages.message_id IS '被标记消息的 server_id';
COMMENT ON COLUMN marked_messages.user_id IS '标记归属用户';
COMMENT ON COLUMN marked_messages.conversation_id IS '会话 ID';
COMMENT ON COLUMN marked_messages.mark_type IS '标记类型：1=IMPORTANT 2=TODO 3=DONE 4=CUSTOM';
COMMENT ON COLUMN marked_messages.color IS '自定义颜色（如 #FF0000）';
COMMENT ON COLUMN marked_messages.marked_at IS '标记时间';
CREATE INDEX IF NOT EXISTS idx_marked_messages_tenant_user ON marked_messages(tenant_id, user_id);

DROP TABLE IF EXISTS message_operation_history CASCADE;
CREATE TABLE message_operation_history (
    id BIGSERIAL PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    message_id TEXT NOT NULL,
    operation_type TEXT NOT NULL,
    operator_id TEXT NOT NULL DEFAULT '',
    target_user_id TEXT DEFAULT '',
    operation_data JSONB DEFAULT '{}'::jsonb,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    metadata JSONB DEFAULT '{}'::jsonb
);
COMMENT ON TABLE message_operation_history IS '操作历史（Event 序列化/索引）；QueryMessageEvents 等';
COMMENT ON COLUMN message_operation_history.id IS '自增主键';
COMMENT ON COLUMN message_operation_history.tenant_id IS '租户 ID';
COMMENT ON COLUMN message_operation_history.message_id IS '消息 server_id';
COMMENT ON COLUMN message_operation_history.operation_type IS '操作类型（与 EventType 对应）';
COMMENT ON COLUMN message_operation_history.operator_id IS '操作者 user_id';
COMMENT ON COLUMN message_operation_history.target_user_id IS '目标用户 ID（如软删生效用户）';
COMMENT ON COLUMN message_operation_history.operation_data IS '操作数据（如 event_base64 或 JSON）';
COMMENT ON COLUMN message_operation_history.timestamp IS '操作时间';
COMMENT ON COLUMN message_operation_history.metadata IS '扩展元数据';
CREATE INDEX IF NOT EXISTS idx_message_operation_history_tenant_message ON message_operation_history(tenant_id, message_id);

-- ============================================================================
-- 5. 话题/子线程（common/models.proto ThreadInfo）
-- ============================================================================

DROP TABLE IF EXISTS thread_participants CASCADE;
DROP TABLE IF EXISTS threads CASCADE;
CREATE TABLE threads (
    id TEXT PRIMARY KEY,
    conversation_id TEXT NOT NULL,
    root_message_id TEXT NOT NULL,
    title TEXT,
    creator_id TEXT NOT NULL,
    reply_count INT NOT NULL DEFAULT 0,
    last_reply_at TIMESTAMPTZ,
    last_reply_id TEXT,
    last_reply_user_id TEXT,
    participant_count INT NOT NULL DEFAULT 0,
    is_pinned BOOLEAN NOT NULL DEFAULT FALSE,
    is_locked BOOLEAN NOT NULL DEFAULT FALSE,
    is_archived BOOLEAN NOT NULL DEFAULT FALSE,
    extra JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE threads IS '话题/子线程（ThreadInfo）；与 PostgresThreadRepository 当前查询列对齐';
COMMENT ON COLUMN threads.id IS '话题 ID，通常等于 root_message_id';
COMMENT ON COLUMN threads.conversation_id IS '所属会话 ID';
COMMENT ON COLUMN threads.root_message_id IS '根消息 server_id（话题入口）';
COMMENT ON COLUMN threads.title IS '话题标题';
COMMENT ON COLUMN threads.creator_id IS '创建者 user_id';
COMMENT ON COLUMN threads.reply_count IS '回复数';
COMMENT ON COLUMN threads.last_reply_at IS '最后回复时间';
COMMENT ON COLUMN threads.last_reply_id IS '最后回复消息 ID';
COMMENT ON COLUMN threads.last_reply_user_id IS '最后回复用户 ID';
COMMENT ON COLUMN threads.participant_count IS '参与用户数';
COMMENT ON COLUMN threads.is_pinned IS '是否置顶';
COMMENT ON COLUMN threads.is_locked IS '是否锁定';
COMMENT ON COLUMN threads.is_archived IS '是否归档';
COMMENT ON COLUMN threads.extra IS '扩展属性';
COMMENT ON COLUMN threads.created_at IS '创建时间';
COMMENT ON COLUMN threads.updated_at IS '更新时间';
CREATE INDEX IF NOT EXISTS idx_threads_conversation_id ON threads(conversation_id);
CREATE INDEX IF NOT EXISTS idx_threads_root_message_id ON threads(root_message_id);
CREATE INDEX IF NOT EXISTS idx_threads_creator_id ON threads(creator_id);
CREATE INDEX IF NOT EXISTS idx_threads_last_reply_at ON threads(last_reply_at DESC);
CREATE INDEX IF NOT EXISTS idx_threads_is_pinned ON threads(is_pinned) WHERE is_pinned = TRUE;
CREATE INDEX IF NOT EXISTS idx_threads_is_archived ON threads(is_archived) WHERE is_archived = FALSE;

CREATE TABLE thread_participants (
    thread_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    first_participated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_participated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    reply_count INT NOT NULL DEFAULT 0,
    is_muted BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (thread_id, user_id)
);
COMMENT ON TABLE thread_participants IS '话题参与者表；用于话题通知、参与者数与静音状态';
COMMENT ON COLUMN thread_participants.thread_id IS '话题 ID';
COMMENT ON COLUMN thread_participants.user_id IS '用户 ID';
COMMENT ON COLUMN thread_participants.first_participated_at IS '首次参与时间';
COMMENT ON COLUMN thread_participants.last_participated_at IS '最后参与时间';
COMMENT ON COLUMN thread_participants.reply_count IS '该用户在此话题的回复数';
COMMENT ON COLUMN thread_participants.is_muted IS '是否静音';
COMMENT ON COLUMN thread_participants.updated_at IS '更新时间';
CREATE INDEX IF NOT EXISTS idx_thread_participants_thread_id ON thread_participants(thread_id);
CREATE INDEX IF NOT EXISTS idx_thread_participants_user_id ON thread_participants(user_id);
CREATE INDEX IF NOT EXISTS idx_thread_participants_last_participated_at ON thread_participants(last_participated_at DESC);

-- ============================================================================
-- 6. 会话写模型（common/conversation.proto ConversationDetail, ConversationParticipant）
-- ============================================================================

DROP TABLE IF EXISTS conversation_participants CASCADE;
DROP TABLE IF EXISTS conversations CASCADE;

CREATE TABLE conversations (
    tenant_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    conversation_type INT NOT NULL DEFAULT 0,  -- ConversationType: SINGLE=1, GROUP=2, AI=3, SYSTEM=4, CUSTOMER=5, TEMP=6（与 CID 前缀一致）
    business_type TEXT NOT NULL DEFAULT '',
    display_name TEXT,
    avatar_url TEXT,
    description TEXT,
    announcement TEXT,
    announcement_updated_at TIMESTAMPTZ,
    announcement_updated_by TEXT,
    visibility INT NOT NULL DEFAULT 0,  -- ConversationVisibility: PRIVATE=1, TENANT=2, PUBLIC=3
    lifecycle_state TEXT NOT NULL DEFAULT 'active',  -- ConversationLifecycleState
    policy JSONB DEFAULT '{}'::jsonb,  -- ConversationPolicy
    attributes JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    last_message_seq BIGINT,
    member_count INT DEFAULT 0,
    channel_id TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (tenant_id, conversation_id)
);
COMMENT ON TABLE conversations IS '会话元数据（ConversationDetail）';
COMMENT ON COLUMN conversations.tenant_id IS '租户 ID';
COMMENT ON COLUMN conversations.conversation_id IS '会话 ID（主键）';
COMMENT ON COLUMN conversations.conversation_type IS '会话类型：0=UNSPECIFIED 1=SINGLE 2=GROUP 3=AI 4=SYSTEM 5=CUSTOMER 6=TEMP（与 CID 前缀一致）';
COMMENT ON COLUMN conversations.business_type IS '业务类型';
COMMENT ON COLUMN conversations.display_name IS '展示名称';
COMMENT ON COLUMN conversations.avatar_url IS '头像 URL';
COMMENT ON COLUMN conversations.description IS '会话描述';
COMMENT ON COLUMN conversations.announcement IS '会话公告';
COMMENT ON COLUMN conversations.announcement_updated_at IS '公告更新时间';
COMMENT ON COLUMN conversations.announcement_updated_by IS '公告更新人 user_id';
COMMENT ON COLUMN conversations.visibility IS '可见性：0=UNSPECIFIED 1=PRIVATE 2=TENANT 3=PUBLIC';
COMMENT ON COLUMN conversations.lifecycle_state IS '生命周期：active / suspended / archived / deleted';
COMMENT ON COLUMN conversations.policy IS 'ConversationPolicy（conflict_resolution, max_devices, allow_*）';
COMMENT ON COLUMN conversations.attributes IS '会话属性（JSON）';
COMMENT ON COLUMN conversations.created_at IS '创建时间';
COMMENT ON COLUMN conversations.updated_at IS '更新时间';
COMMENT ON COLUMN conversations.last_message_seq IS '最后一条消息的 seq（未读数计算）';
COMMENT ON COLUMN conversations.member_count IS '成员数';
COMMENT ON COLUMN conversations.channel_id IS '路由频道：单聊库中为空（读模型组装对端 user_id）；群/频道等为消息 channel_id（如群业务 ID）';
CREATE INDEX IF NOT EXISTS idx_conversations_tenant_updated ON conversations(tenant_id, updated_at DESC);

CREATE TABLE conversation_participants (
    tenant_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    roles TEXT[] DEFAULT '{}',
    muted BOOLEAN DEFAULT FALSE,
    pinned BOOLEAN DEFAULT FALSE,
    attributes JSONB DEFAULT '{}'::jsonb,
    joined_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    nickname TEXT,
    last_read_seq BIGINT DEFAULT 0,
    last_sync_seq BIGINT DEFAULT 0,
    unread_count INT DEFAULT 0,
    is_deleted BOOLEAN DEFAULT FALSE,
    mute_until TIMESTAMPTZ,
    is_archived BOOLEAN NOT NULL DEFAULT FALSE,
    settings_version BIGINT NOT NULL DEFAULT 0,
    draft TEXT,
    quit_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, conversation_id, user_id),
    FOREIGN KEY (tenant_id, conversation_id) REFERENCES conversations(tenant_id, conversation_id) ON DELETE CASCADE
);
COMMENT ON TABLE conversation_participants IS '参与者（ConversationParticipant）+ 读模型未读/游标（ConversationLight/Summary）';
COMMENT ON COLUMN conversation_participants.tenant_id IS '租户 ID';
COMMENT ON COLUMN conversation_participants.conversation_id IS '会话 ID';
COMMENT ON COLUMN conversation_participants.user_id IS '用户 ID';
COMMENT ON COLUMN conversation_participants.roles IS '角色列表（owner, admin, member 等）';
COMMENT ON COLUMN conversation_participants.muted IS '是否静音';
COMMENT ON COLUMN conversation_participants.pinned IS '是否置顶';
COMMENT ON COLUMN conversation_participants.attributes IS '参与者属性（JSON）';
COMMENT ON COLUMN conversation_participants.joined_at IS '加入时间';
COMMENT ON COLUMN conversation_participants.nickname IS '群昵称';
COMMENT ON COLUMN conversation_participants.last_read_seq IS '已读到的 seq（未读数 = max_seq - last_read_seq）';
COMMENT ON COLUMN conversation_participants.last_sync_seq IS 'Sync 游标 last_seq';
COMMENT ON COLUMN conversation_participants.unread_count IS '未读数（冗余）';
COMMENT ON COLUMN conversation_participants.is_deleted IS '用户侧删除会话（软删）';
COMMENT ON COLUMN conversation_participants.mute_until IS '静音截止时间（空=长期免打扰）';
COMMENT ON COLUMN conversation_participants.quit_at IS '退出时间（NULL=仍在会话中）';
COMMENT ON COLUMN conversation_participants.created_at IS '创建时间';
COMMENT ON COLUMN conversation_participants.updated_at IS '更新时间';
CREATE INDEX IF NOT EXISTS idx_conversation_participants_tenant_user ON conversation_participants(tenant_id, user_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_conversation_participants_tenant_conv ON conversation_participants(tenant_id, conversation_id);

-- ============================================================================
-- 7. Sync 游标（common/sync.proto）
-- ============================================================================

DROP TABLE IF EXISTS user_sync_cursor CASCADE;
CREATE TABLE user_sync_cursor (
    tenant_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    last_synced_seq BIGINT NOT NULL DEFAULT 0,
    last_synced_ts BIGINT NOT NULL DEFAULT 0,
    device_id TEXT,
    version INT DEFAULT 1,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, user_id, conversation_id)
);
COMMENT ON TABLE user_sync_cursor IS '按会话 Sync 游标（last_seq）';
COMMENT ON COLUMN user_sync_cursor.tenant_id IS '租户 ID';
COMMENT ON COLUMN user_sync_cursor.user_id IS '用户 ID';
COMMENT ON COLUMN user_sync_cursor.conversation_id IS '会话 ID';
COMMENT ON COLUMN user_sync_cursor.last_synced_seq IS '最后同步的 seq';
COMMENT ON COLUMN user_sync_cursor.last_synced_ts IS '最后同步时间戳（毫秒）';
COMMENT ON COLUMN user_sync_cursor.device_id IS '设备 ID（可选，设备级游标）';
COMMENT ON COLUMN user_sync_cursor.version IS '版本号（乐观锁）';
COMMENT ON COLUMN user_sync_cursor.created_at IS '创建时间';
COMMENT ON COLUMN user_sync_cursor.updated_at IS '更新时间';
CREATE INDEX IF NOT EXISTS idx_user_sync_cursor_tenant_user ON user_sync_cursor(tenant_id, user_id);

-- ============================================================================
-- 8. 媒体资产（common/metadata.proto MediaAttachment 等）
-- ============================================================================

DROP TABLE IF EXISTS media_references CASCADE;
DROP TABLE IF EXISTS media_assets CASCADE;

CREATE TABLE media_assets (
    tenant_id TEXT NOT NULL,
    file_id TEXT NOT NULL,
    file_name TEXT NOT NULL,
    mime_type TEXT NOT NULL,
    file_size BIGINT NOT NULL,
    url TEXT NOT NULL,
    cdn_url TEXT DEFAULT '',
    md5 TEXT,
    sha256 TEXT,
    metadata JSONB,
    uploaded_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    reference_count BIGINT DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'active',
    grace_expires_at TIMESTAMPTZ,
    access_type TEXT NOT NULL DEFAULT 'private',
    PRIMARY KEY (tenant_id, file_id)
);
COMMENT ON TABLE media_assets IS '媒体资产元数据';
COMMENT ON COLUMN media_assets.tenant_id IS '租户 ID';
COMMENT ON COLUMN media_assets.file_id IS '文件唯一标识（租户内唯一）';
COMMENT ON COLUMN media_assets.file_name IS '文件名';
COMMENT ON COLUMN media_assets.mime_type IS 'MIME 类型';
COMMENT ON COLUMN media_assets.file_size IS '文件大小（字节）';
COMMENT ON COLUMN media_assets.url IS '访问 URL';
COMMENT ON COLUMN media_assets.cdn_url IS 'CDN URL';
COMMENT ON COLUMN media_assets.md5 IS 'MD5 校验';
COMMENT ON COLUMN media_assets.sha256 IS 'SHA256 校验';
COMMENT ON COLUMN media_assets.metadata IS '扩展元数据（JSON）';
COMMENT ON COLUMN media_assets.uploaded_at IS '上传时间';
COMMENT ON COLUMN media_assets.reference_count IS '引用计数';
COMMENT ON COLUMN media_assets.status IS '状态（active 等）';
COMMENT ON COLUMN media_assets.grace_expires_at IS '宽限过期时间';
COMMENT ON COLUMN media_assets.access_type IS '访问类型（private/public）';
CREATE INDEX IF NOT EXISTS idx_media_assets_tenant_uploaded ON media_assets(tenant_id, uploaded_at DESC);
CREATE INDEX IF NOT EXISTS idx_media_assets_sha256_active
    ON media_assets(sha256, uploaded_at DESC)
    WHERE sha256 IS NOT NULL
      AND status <> 'soft_deleted';
CREATE INDEX IF NOT EXISTS idx_media_assets_message_orphan_due
    ON media_assets(grace_expires_at)
    WHERE reference_count = 0
      AND status = 'pending'
      AND grace_expires_at IS NOT NULL
      AND (
            LOWER(COALESCE(metadata->>'media_lifecycle_scope', '')) IN ('message', 'messages', 'im_message', 'im-message')
         OR LOWER(COALESCE(metadata->>'lifecycle_scope', '')) IN ('message', 'messages', 'im_message', 'im-message')
         OR LOWER(COALESCE(metadata->>'media_scope', '')) IN ('message', 'messages', 'im_message', 'im-message')
         OR LOWER(COALESCE(metadata->>'media_usage', '')) IN ('message', 'messages', 'im_message', 'im-message')
         OR LOWER(COALESCE(metadata->>'usage', '')) IN ('message', 'messages', 'im_message', 'im-message')
         OR LOWER(COALESCE(metadata->>'namespace', '')) IN ('message', 'messages', 'im_message', 'im-message')
         OR LOWER(COALESCE(metadata->>'business_tag', '')) IN ('message', 'messages', 'im_message', 'im-message')
      );

CREATE TABLE media_references (
    tenant_id TEXT NOT NULL,
    reference_id TEXT NOT NULL,
    file_id TEXT NOT NULL,
    namespace TEXT NOT NULL,
    owner_id TEXT NOT NULL,
    business_tag TEXT,
    metadata JSONB,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMPTZ,
    PRIMARY KEY (tenant_id, reference_id),
    FOREIGN KEY (tenant_id, file_id) REFERENCES media_assets(tenant_id, file_id) ON DELETE CASCADE
);
COMMENT ON TABLE media_references IS '媒体引用表';
COMMENT ON COLUMN media_references.tenant_id IS '租户 ID';
COMMENT ON COLUMN media_references.reference_id IS '引用 ID（租户内唯一）';
COMMENT ON COLUMN media_references.file_id IS '关联文件 file_id';
COMMENT ON COLUMN media_references.namespace IS '命名空间';
COMMENT ON COLUMN media_references.owner_id IS '拥有者 ID';
COMMENT ON COLUMN media_references.business_tag IS '业务标签';
COMMENT ON COLUMN media_references.metadata IS '扩展元数据（JSON）';
COMMENT ON COLUMN media_references.created_at IS '创建时间';
COMMENT ON COLUMN media_references.expires_at IS '过期时间';
CREATE INDEX IF NOT EXISTS idx_media_references_tenant_file ON media_references(tenant_id, file_id);
CREATE INDEX IF NOT EXISTS idx_media_references_tenant_scope_lookup
    ON media_references(tenant_id, file_id, namespace, owner_id, business_tag);

-- ============================================================================
-- 9. ACK 审计归档
-- ============================================================================

DROP TABLE IF EXISTS ack_archive_records CASCADE;
CREATE TABLE ack_archive_records (
    id BIGSERIAL PRIMARY KEY,
    message_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    ack_type TEXT NOT NULL,
    ack_status TEXT NOT NULL,
    timestamp BIGINT NOT NULL,
    importance_level SMALLINT NOT NULL DEFAULT 1 CHECK (importance_level BETWEEN 1 AND 3),
    metadata JSONB,
    archived_at BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM NOW())::BIGINT)
);
COMMENT ON TABLE ack_archive_records IS 'ACK 归档记录表，用于审计和分析 ACK 日志';
COMMENT ON COLUMN ack_archive_records.message_id IS '消息 ID';
COMMENT ON COLUMN ack_archive_records.user_id IS '用户 ID';
COMMENT ON COLUMN ack_archive_records.ack_type IS 'ACK 类型';
COMMENT ON COLUMN ack_archive_records.ack_status IS 'ACK 状态';
COMMENT ON COLUMN ack_archive_records.timestamp IS 'ACK 时间戳';
COMMENT ON COLUMN ack_archive_records.importance_level IS '重要性等级：1=低 2=中 3=高';
COMMENT ON COLUMN ack_archive_records.metadata IS '扩展元数据';
COMMENT ON COLUMN ack_archive_records.archived_at IS '归档时间戳';
CREATE INDEX IF NOT EXISTS idx_ack_archive_message_id ON ack_archive_records(message_id);
CREATE INDEX IF NOT EXISTS idx_ack_archive_user_id ON ack_archive_records(user_id);
CREATE INDEX IF NOT EXISTS idx_ack_archive_timestamp_desc ON ack_archive_records(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_ack_archive_importance_level ON ack_archive_records(importance_level);
CREATE INDEX IF NOT EXISTS idx_ack_archive_message_user_type ON ack_archive_records(message_id, user_id, ack_type);

-- ============================================================================
-- 10. Hook 引擎 + Capability 策略（flare-capability）
-- ============================================================================
-- hook_configs 列与 PostgresHookConfigRepository（Rust sqlx::FromRow）一致。
-- capability_* 与 PostgresCapabilityPolicy、CapabilityService gRPC 一致。
-- FLARE_EXTRACT:BEGIN_HOOK_CAPABILITY（第 9 节边界标记，勿删改此行）

DROP TABLE IF EXISTS hook_executions CASCADE;
DROP TABLE IF EXISTS hook_configs CASCADE;
DROP TABLE IF EXISTS capability_audit_log CASCADE;
DROP TABLE IF EXISTS capability_user_grants CASCADE;
DROP TABLE IF EXISTS capability_tenant_switches CASCADE;
DROP TABLE IF EXISTS capability_service_settings CASCADE;

CREATE TABLE hook_configs (
    id BIGSERIAL PRIMARY KEY,
    hook_id TEXT UNIQUE,
    tenant_id TEXT,
    hook_type TEXT NOT NULL,
    name TEXT NOT NULL,
    version TEXT,
    description TEXT,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    priority INT NOT NULL DEFAULT 100,
    group_name TEXT,
    timeout_ms BIGINT NOT NULL DEFAULT 1000,
    max_retries INT NOT NULL DEFAULT 0,
    error_policy TEXT NOT NULL DEFAULT 'fail_fast',
    require_success BOOLEAN NOT NULL DEFAULT TRUE,
    selector_config JSONB NOT NULL DEFAULT '{}'::jsonb,
    transport_config JSONB NOT NULL DEFAULT '{}'::jsonb,
    metadata JSONB,
    created_by TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(tenant_id, hook_type, name)
);
COMMENT ON TABLE hook_configs IS 'Hook 配置表（动态加载，与 flare-capability postgres_config 对齐）';
COMMENT ON COLUMN hook_configs.id IS '自增主键';
COMMENT ON COLUMN hook_configs.hook_id IS '可选外部稳定 ID（UUID 等），与 id 二选一使用';
COMMENT ON COLUMN hook_configs.tenant_id IS '租户 ID（空=全局）';
COMMENT ON COLUMN hook_configs.hook_type IS 'Hook 类型（pre_send, post_send, recall 等）';
COMMENT ON COLUMN hook_configs.name IS 'Hook 名称';
COMMENT ON COLUMN hook_configs.version IS '版本';
COMMENT ON COLUMN hook_configs.description IS '描述';
COMMENT ON COLUMN hook_configs.enabled IS '是否启用';
COMMENT ON COLUMN hook_configs.priority IS '优先级（越小越高）';
COMMENT ON COLUMN hook_configs.group_name IS '分组（validation/critical/business）';
COMMENT ON COLUMN hook_configs.timeout_ms IS '超时（毫秒）';
COMMENT ON COLUMN hook_configs.max_retries IS '最大重试次数';
COMMENT ON COLUMN hook_configs.error_policy IS '错误策略（fail_fast, retry, ignore）';
COMMENT ON COLUMN hook_configs.require_success IS '是否要求成功';
COMMENT ON COLUMN hook_configs.selector_config IS '选择器配置（JSON）';
COMMENT ON COLUMN hook_configs.transport_config IS '传输配置（JSON）';
COMMENT ON COLUMN hook_configs.metadata IS '元数据（JSON）';
COMMENT ON COLUMN hook_configs.created_by IS '创建者';
COMMENT ON COLUMN hook_configs.created_at IS '创建时间';
COMMENT ON COLUMN hook_configs.updated_at IS '更新时间';
CREATE INDEX IF NOT EXISTS idx_hook_configs_tenant_type ON hook_configs(tenant_id, hook_type, enabled);

CREATE TABLE hook_executions (
    execution_id TEXT PRIMARY KEY,
    hook_id TEXT NOT NULL,
    hook_name TEXT NOT NULL,
    hook_type TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    message_id TEXT,
    request_id TEXT,
    trace_id TEXT,
    success BOOLEAN NOT NULL,
    latency_ms INT,
    error_code TEXT,
    error_message TEXT,
    executed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE hook_executions IS 'Hook 执行记录表（审计/排障）';
COMMENT ON COLUMN hook_executions.execution_id IS '执行 ID（主键）';
COMMENT ON COLUMN hook_executions.hook_id IS '关联 hook_configs.id 或 hook_id 文本';
COMMENT ON COLUMN hook_executions.hook_name IS 'Hook 名称';
COMMENT ON COLUMN hook_executions.hook_type IS 'Hook 类型';
COMMENT ON COLUMN hook_executions.tenant_id IS '租户 ID';
COMMENT ON COLUMN hook_executions.message_id IS '关联消息 ID（可选）';
COMMENT ON COLUMN hook_executions.request_id IS '上游请求 ID（可选）';
COMMENT ON COLUMN hook_executions.trace_id IS '追踪 ID（可选）';
COMMENT ON COLUMN hook_executions.success IS '是否成功';
COMMENT ON COLUMN hook_executions.latency_ms IS '耗时（毫秒）';
COMMENT ON COLUMN hook_executions.error_code IS '错误码（失败时）';
COMMENT ON COLUMN hook_executions.error_message IS '错误信息（失败时）';
COMMENT ON COLUMN hook_executions.executed_at IS '执行时间';
CREATE INDEX IF NOT EXISTS idx_hook_executions_tenant_executed ON hook_executions(tenant_id, executed_at DESC);

-- 全局总开关（单行）；无行时服务端按「启用」处理
CREATE TABLE capability_service_settings (
    id SMALLINT PRIMARY KEY CHECK (id = 1),
    global_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE capability_service_settings IS '能力服务全局开关（与 InMemoryCapabilityGrants.global_enabled 对应）';

INSERT INTO capability_service_settings (id, global_enabled) VALUES (1, TRUE)
ON CONFLICT (id) DO NOTHING;

-- 租户维度关闭某能力；无行表示不额外禁用（与内存策略一致）
CREATE TABLE capability_tenant_switches (
    tenant_id TEXT NOT NULL,
    capability_id TEXT NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, capability_id)
);
COMMENT ON TABLE capability_tenant_switches IS '租户级能力开关（显式 false 时拒绝 Dispatch）';
CREATE INDEX IF NOT EXISTS idx_capability_tenant_switches_tenant ON capability_tenant_switches(tenant_id);

CREATE TABLE capability_user_grants (
    tenant_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    capability_id TEXT NOT NULL,
    granted_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMPTZ,
    plan_code TEXT,
    source TEXT,
    PRIMARY KEY (tenant_id, user_id, capability_id)
);
COMMENT ON TABLE capability_user_grants IS '用户能力授权（付费/运营开通；支持 namespace.* 通配）';
COMMENT ON COLUMN capability_user_grants.expires_at IS '过期时间，NULL 表示长期有效';
COMMENT ON COLUMN capability_user_grants.user_id IS '用户 ID；特殊值 * 表示该 tenant_id 下任意用户（仅建议开发/内网；生产按用户灌库后应删除通配行）';
CREATE INDEX IF NOT EXISTS idx_capability_user_grants_tenant_user ON capability_user_grants(tenant_id, user_id);

-- 默认租户 0 + 租户级 RTC 通配（与编排器 ctx.tenant_id().unwrap_or("0")、Capability Dispatch 对齐）
-- 默认租户统一使用 tenant_id `0`。
INSERT INTO capability_user_grants (tenant_id, user_id, capability_id, plan_code, source)
VALUES ('0', '*', 'rtc.*', 'dev', 'init_bootstrap')
ON CONFLICT (tenant_id, user_id, capability_id) DO NOTHING;

-- 策略变更审计（Grant / Revoke / SetTenantSwitch；Dispatch 高频路径默认不落库）
CREATE TABLE capability_audit_log (
    id BIGSERIAL PRIMARY KEY,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    action TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    actor_id TEXT,
    target_user_id TEXT,
    capability_id TEXT,
    detail JSONB,
    trace_id TEXT
);
COMMENT ON TABLE capability_audit_log IS '能力策略变更审计（合规/排障）；由 flare-capability 写入';
COMMENT ON COLUMN capability_audit_log.action IS 'grant | revoke | tenant_switch 等';
COMMENT ON COLUMN capability_audit_log.actor_id IS '操作者（metadata x-actor-id / x-user-id）';
COMMENT ON COLUMN capability_audit_log.detail IS '扩展字段 JSON';
CREATE INDEX IF NOT EXISTS idx_capability_audit_tenant_time ON capability_audit_log(tenant_id, occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_capability_audit_action_time ON capability_audit_log(action, occurred_at DESC);

-- 可按用户追加（示例，与 tenant 0 一致时取消注释）
-- INSERT INTO capability_user_grants (tenant_id, user_id, capability_id, plan_code, source)
-- VALUES ('0', '具体用户ID', 'rtc.*', 'dev', 'bootstrap')
-- ON CONFLICT (tenant_id, user_id, capability_id) DO NOTHING;

-- FLARE_EXTRACT:END_HOOK_CAPABILITY（第 9 节边界标记，勿删改此行）
-- ============================================================================
-- 11. TimescaleDB 策略与触发器
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

DROP TRIGGER IF EXISTS trigger_capability_tenant_switches_updated_at ON capability_tenant_switches;
CREATE TRIGGER trigger_capability_tenant_switches_updated_at
    BEFORE UPDATE ON capability_tenant_switches FOR EACH ROW EXECUTE PROCEDURE set_updated_at();

DROP TRIGGER IF EXISTS trigger_capability_service_settings_updated_at ON capability_service_settings;
CREATE TRIGGER trigger_capability_service_settings_updated_at
    BEFORE UPDATE ON capability_service_settings FOR EACH ROW EXECUTE PROCEDURE set_updated_at();

DROP TRIGGER IF EXISTS trigger_threads_updated_at ON threads;
CREATE TRIGGER trigger_threads_updated_at
    BEFORE UPDATE ON threads FOR EACH ROW EXECUTE PROCEDURE set_updated_at();

DROP TRIGGER IF EXISTS trigger_thread_participants_updated_at ON thread_participants;
CREATE TRIGGER trigger_thread_participants_updated_at
    BEFORE UPDATE ON thread_participants FOR EACH ROW EXECUTE PROCEDURE set_updated_at();

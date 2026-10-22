-- Plugin entity shared state (R6 A). Additive only: two new tables, no existing row or object changes.
--
-- plugin_entity_bindings: which stored plugin cards show a plugin entity (one poll, one solitaire).
-- Storage Writer registers a row when it stores a plugin card (APP_CARD `flare.plugin.message.v1`,
-- app_id = plugin id) of a plugin the tenant has enabled whose message-content contribution declares
-- `entityPath`; entity is the payload value at that path. Capability publishes shared state only for
-- bound entities, and raises published_version by compare-and-set before fanning a version out, so a
-- duplicate or older version is never sent to a message twice. Writer sets recalled_at when the card
-- is recalled: a recalled card takes no more state and Capability refuses invocations naming its
-- entity. One entity may be bound to several messages (reserved for forwarding).
--
-- message_plugin_states: the latest shared state of each plugin card (EVENT_MESSAGE_PLUGIN_STATE),
-- one row per message. Writer stores an event only for a bound message and only when it is newer
-- (the same version is idempotent, an older one is dropped); the event itself is always appended to
-- `events`. Storage Reader exposes it as extra `pluginSharedStateJson` / `pluginSharedStateVersion`.
-- values_json keeps the event's exact text.
CREATE TABLE IF NOT EXISTS plugin_entity_bindings (
    tenant_id TEXT NOT NULL,
    plugin_id TEXT NOT NULL,
    entity TEXT NOT NULL,
    server_msg_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    contribution_id TEXT NOT NULL,
    published_version BIGINT NOT NULL DEFAULT 0,
    recalled_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT plugin_entity_bindings_pkey PRIMARY KEY (tenant_id, plugin_id, entity, server_msg_id),
    CONSTRAINT plugin_entity_bindings_tenant_check
        CHECK (char_length(tenant_id) BETWEEN 1 AND 256 AND tenant_id <> '*'),
    CONSTRAINT plugin_entity_bindings_plugin_check
        CHECK (char_length(plugin_id) BETWEEN 1 AND 128 AND char_length(contribution_id) BETWEEN 1 AND 160),
    CONSTRAINT plugin_entity_bindings_entity_check
        CHECK (entity ~ '^[A-Za-z0-9._:-]{1,128}$'),
    CONSTRAINT plugin_entity_bindings_message_check
        CHECK (char_length(server_msg_id) BETWEEN 1 AND 256 AND char_length(conversation_id) BETWEEN 1 AND 256),
    CONSTRAINT plugin_entity_bindings_version_check
        CHECK (published_version >= 0)
);
CREATE INDEX IF NOT EXISTS idx_plugin_entity_bindings_message
    ON plugin_entity_bindings(tenant_id, server_msg_id);
CREATE TABLE IF NOT EXISTS message_plugin_states (
    tenant_id TEXT NOT NULL,
    server_msg_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    plugin_id TEXT NOT NULL,
    entity TEXT NOT NULL,
    version BIGINT NOT NULL,
    values_json TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT message_plugin_states_pkey PRIMARY KEY (tenant_id, server_msg_id),
    CONSTRAINT message_plugin_states_version_check CHECK (version > 0),
    CONSTRAINT message_plugin_states_values_check CHECK (octet_length(values_json) <= 8192)
);

-- Plugin entity lifecycle outbox (plugin platform v2 §3). Additive only: one new table.
--
-- A row asks Core to tell a plugin that the card of one of its entities went away, so the plugin
-- cancels a poll or refunds a red packet instead of leaving it open:
-- - `recalled`: Storage Writer adds it in the statement that marks a bound card recalled
--   (plugin_entity_bindings.recalled_at), so a recall and its notification commit together;
-- - `post_failed`: Capability adds it when it could not post the card a plugin operation returned.
--   When Ingest refused the card it is confirmed at once; when the outcome was unknown (Ingest
--   unavailable, a first send still in flight) it is `unconfirmed`, due after a verification delay,
--   and delivered only if the entity still has no binding (Storage Writer binds a stored card).
-- Capability's worker claims due rows (FOR UPDATE SKIP LOCKED, leased by next_attempt_at) and calls
-- the operation the contribution declares as `entityLifecycle.unbound` with a platform system
-- ticket, input {entity, reason, serverMsgId?}; delivery is at least once and plugins handle it
-- idempotently per entity. A row ends `delivered` (the plugin answered), `skipped` (the
-- contribution declares no unbound operation, or a post_failed whose card was stored after all) or
-- `abandoned` (the plugin stayed disabled or
-- unreachable past the retention window). One row per (tenant, plugin, entity, reason).
CREATE TABLE IF NOT EXISTS plugin_entity_lifecycle_outbox (
    id BIGSERIAL PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    plugin_id TEXT NOT NULL,
    contribution_id TEXT NOT NULL,
    entity TEXT NOT NULL,
    reason TEXT NOT NULL,
    server_msg_id TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    -- post_failed added while the post's outcome was unknown (Ingest unavailable or a first send
    -- still in flight): due after a verification delay, delivered only if the entity still has no
    -- binding, and not counted as given up until the worker ends it.
    unconfirmed BOOLEAN NOT NULL DEFAULT FALSE
        CONSTRAINT plugin_entity_lifecycle_outbox_unconfirmed_check
        CHECK (NOT unconfirmed OR reason = 'post_failed'),
    CONSTRAINT plugin_entity_lifecycle_outbox_entity_key UNIQUE (tenant_id, plugin_id, entity, reason),
    CONSTRAINT plugin_entity_lifecycle_outbox_tenant_check
        CHECK (char_length(tenant_id) BETWEEN 1 AND 256 AND tenant_id <> '*'),
    CONSTRAINT plugin_entity_lifecycle_outbox_plugin_check
        CHECK (char_length(plugin_id) BETWEEN 1 AND 128 AND char_length(contribution_id) BETWEEN 1 AND 160),
    CONSTRAINT plugin_entity_lifecycle_outbox_entity_check
        CHECK (entity ~ '^[A-Za-z0-9._:-]{1,128}$'),
    CONSTRAINT plugin_entity_lifecycle_outbox_reason_check
        CHECK (reason IN ('recalled', 'post_failed')),
    CONSTRAINT plugin_entity_lifecycle_outbox_message_check
        CHECK (server_msg_id IS NULL OR char_length(server_msg_id) BETWEEN 1 AND 256),
    CONSTRAINT plugin_entity_lifecycle_outbox_status_check
        CHECK (status IN ('pending', 'delivered', 'skipped', 'abandoned')),
    CONSTRAINT plugin_entity_lifecycle_outbox_error_check
        CHECK (last_error IS NULL OR char_length(last_error) <= 512)
);
CREATE INDEX IF NOT EXISTS idx_plugin_entity_lifecycle_outbox_due
    ON plugin_entity_lifecycle_outbox(next_attempt_at) WHERE status = 'pending';
-- A database that ran this migration before `unconfirmed` existed gets the column here; on a new
-- database the statement does nothing.
ALTER TABLE plugin_entity_lifecycle_outbox
    ADD COLUMN IF NOT EXISTS unconfirmed BOOLEAN NOT NULL DEFAULT FALSE
        CONSTRAINT plugin_entity_lifecycle_outbox_unconfirmed_check
        CHECK (NOT unconfirmed OR reason = 'post_failed');

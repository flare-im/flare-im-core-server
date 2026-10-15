-- Final-development initialization input; applied with the owning Core business schema.
CREATE TABLE core_audit_outbox (
    event_id UUID PRIMARY KEY,
    app_id TEXT NOT NULL CHECK (app_id <> '' AND app_id <> '*'),
    payload JSONB NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    delivered_at TIMESTAMPTZ
);
CREATE INDEX core_audit_outbox_pending ON core_audit_outbox(next_at,event_id) WHERE delivered_at IS NULL;
CREATE INDEX core_audit_outbox_delivered ON core_audit_outbox(delivered_at) WHERE delivered_at IS NOT NULL;

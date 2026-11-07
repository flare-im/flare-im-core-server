-- Client policy projection and Core's own tenant notice outbox. Additive only: two new tables.
--
-- tenant_client_policy: one row per application, replaced by strictly increasing version
-- (TenantProjection.UpsertClientPolicy). access_serial and release_generation are watermarks that
-- never move back; gateways read the row through the tenant runtime cache and answer CONNECT_ACK
-- with access_serial. build_policies maps a platform to
-- {min_build, min_build_effective_from_ms, blocked: [{from, to}]}.
--
-- tenant_notice_outbox: a projection that moves a watermark adds its flare.access / flare.release
-- notice here in the same transaction, so a client never hears about a version Core does not hold
-- and a crash between commit and publish loses nothing. Capability's relay claims due rows
-- (FOR UPDATE SKIP LOCKED, leased by next_attempt_at), publishes them on the notice channel and
-- deletes them. One row per (tenant, topic): a newer version takes over a pending one.
CREATE TABLE tenant_client_policy (
    tenant_id TEXT PRIMARY KEY CHECK (tenant_id ~ '^[0-9A-Za-z_-]{1,32}$'),
    version BIGINT NOT NULL CHECK (version > 0),
    access_serial BIGINT NOT NULL CHECK (access_serial >= 0),
    release_generation BIGINT NOT NULL CHECK (release_generation >= 0),
    build_policies JSONB NOT NULL
        CHECK (jsonb_typeof(build_policies) = 'object' AND octet_length(build_policies::text) <= 262144),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE tenant_notice_outbox (
    tenant_id TEXT NOT NULL CHECK (tenant_id ~ '^[0-9A-Za-z_-]{1,32}$'),
    topic TEXT NOT NULL CHECK (topic IN ('flare.access', 'flare.release')),
    version BIGINT NOT NULL CHECK (version > 0),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_error TEXT CHECK (last_error IS NULL OR char_length(last_error) <= 512),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, topic)
);
CREATE INDEX idx_tenant_notice_outbox_due ON tenant_notice_outbox(next_attempt_at);

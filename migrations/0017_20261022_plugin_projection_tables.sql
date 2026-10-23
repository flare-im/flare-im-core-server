-- Plugin authorization projection and resource callback quota (plugin platform v2 §8). Until now
-- these two tables existed only in deploy/init.sql, so a database built from migrations could not
-- take a plugin projection (TenantProjection.UpsertPlugins) or serve plugin resource callbacks.
-- Same definitions as deploy/init.sql; additive only.
--
-- plugin_authorization_snapshots: the authenticated control-plane plugin snapshot of one
-- application, replaced by strictly increasing version. Empty snapshots keep the watermark;
-- invocation never queries the Marketplace. Capability authorizes plugin calls from it and Storage
-- Writer reads it to bind plugin cards to entities.
CREATE TABLE IF NOT EXISTS plugin_authorization_snapshots (
    app_id text PRIMARY KEY CHECK (app_id ~ '^[0-9A-Za-z_-]{1,32}$'),
    version bigint NOT NULL CHECK (version > 0),
    snapshot jsonb NOT NULL
        CHECK (jsonb_typeof(snapshot) = 'object' AND octet_length(snapshot::text) <= 4194304),
    valid_until_ms bigint NOT NULL CHECK (valid_until_ms > 0),
    CHECK (snapshot->>'tenantId' = app_id),
    CHECK ((snapshot->>'version')::bigint = version),
    CHECK ((snapshot->>'validUntilMs')::bigint = valid_until_ms)
);

-- capability_plugin_resource_calls: shared, leased callback quota per signed invocation (at most 8
-- host callbacks per call); no message, configuration, credential or payload is stored.
CREATE TABLE IF NOT EXISTS capability_plugin_resource_calls (
    tenant_id text NOT NULL CHECK (tenant_id ~ '^[0-9A-Za-z_-]{1,32}$'),
    plugin_id text NOT NULL CHECK (char_length(plugin_id) BETWEEN 1 AND 128),
    actor_id text NOT NULL CHECK (char_length(actor_id) BETWEEN 1 AND 192),
    request_id text NOT NULL CHECK (char_length(request_id) BETWEEN 1 AND 192),
    calls integer NOT NULL CHECK (calls BETWEEN 1 AND 8),
    expires_at_ms bigint NOT NULL CHECK (expires_at_ms > 0),
    PRIMARY KEY (tenant_id, plugin_id, actor_id, request_id)
);
CREATE INDEX IF NOT EXISTS capability_plugin_resource_calls_expiry
    ON capability_plugin_resource_calls(tenant_id, expires_at_ms);
CREATE INDEX IF NOT EXISTS capability_plugin_resource_calls_global_expiry
    ON capability_plugin_resource_calls(expires_at_ms);

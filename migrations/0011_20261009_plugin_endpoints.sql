-- Plugin endpoint registrations (CapabilityService.RegisterPluginEndpoint), shared by every
-- Capability instance: each loads the table at startup, writes through on register/deregister,
-- re-reads it periodically and deletes rows neither re-registered nor healthy within its TTL.
-- Endpoints from configuration (plugin_discovery_endpoints) are process-local and never stored.
CREATE TABLE IF NOT EXISTS capability_plugin_endpoints (
    tenant_id TEXT NOT NULL,
    plugin_id TEXT NOT NULL,
    capability_id TEXT NOT NULL,
    grpc_authority TEXT NOT NULL,
    labels JSONB NOT NULL DEFAULT '{}'::jsonb,
    plugin_version TEXT NOT NULL DEFAULT '',
    api_version TEXT NOT NULL DEFAULT '',
    manifest_sha256 TEXT NOT NULL DEFAULT '',
    declared_operations TEXT[] NOT NULL DEFAULT '{}',
    unverified BOOLEAN NOT NULL,
    seat_model TEXT NOT NULL DEFAULT '',
    registered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_healthy_at TIMESTAMPTZ,
    CONSTRAINT capability_plugin_endpoints_pkey PRIMARY KEY (tenant_id, plugin_id),
    CONSTRAINT capability_plugin_endpoints_tenant_check
        CHECK (char_length(tenant_id) BETWEEN 1 AND 256 AND tenant_id <> '*'),
    CONSTRAINT capability_plugin_endpoints_plugin_check
        CHECK (char_length(plugin_id) BETWEEN 1 AND 128),
    CONSTRAINT capability_plugin_endpoints_capability_check
        CHECK (char_length(capability_id) BETWEEN 1 AND 128),
    CONSTRAINT capability_plugin_endpoints_authority_check
        CHECK (char_length(grpc_authority) BETWEEN 1 AND 512),
    CONSTRAINT capability_plugin_endpoints_labels_check
        CHECK (jsonb_typeof(labels) = 'object' AND octet_length(labels::text) <= 16384),
    CONSTRAINT capability_plugin_endpoints_manifest_check
        CHECK (char_length(plugin_version) <= 64 AND char_length(api_version) <= 64
               AND char_length(manifest_sha256) <= 128 AND char_length(seat_model) <= 32),
    CONSTRAINT capability_plugin_endpoints_operations_check
        CHECK (cardinality(declared_operations) <= 256
               AND array_position(declared_operations, NULL) IS NULL),
    CONSTRAINT capability_plugin_endpoints_unverified_check
        CHECK (unverified = (cardinality(declared_operations) = 0))
);

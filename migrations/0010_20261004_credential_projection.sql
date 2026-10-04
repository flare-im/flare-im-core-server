-- Data-plane projection only. Control owns lifecycle; no token/secret/pepper.
-- No tenant FK: credentials can arrive before a tenant snapshot. Business APIs
-- must independently require the existing active tenant gate before execution.
CREATE TABLE IF NOT EXISTS credential_projection_versions (
    app_id VARCHAR(32) PRIMARY KEY CHECK (app_id ~ '^[A-Za-z0-9_-]{1,32}$'),
    version BIGINT NOT NULL CHECK (version > 0),
    projected_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS credential_keys (
    key_id VARCHAR(12) PRIMARY KEY CHECK (key_id ~ '^[A-Z2-7]{12}$'),
    app_id VARCHAR(32) NOT NULL REFERENCES credential_projection_versions(app_id),
    payload JSONB NOT NULL CHECK (jsonb_typeof(payload) = 'object' AND octet_length(payload::text) <= 16384)
);
CREATE INDEX IF NOT EXISTS credential_keys_app_idx ON credential_keys(app_id);

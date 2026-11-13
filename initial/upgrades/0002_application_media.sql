-- Explicit accepted Core initial v1 -> v2 only. Caller validates the old source
-- digest/full descriptor/owner ACL and sets the target source digest locally.
ALTER TABLE public.media_assets
    ADD COLUMN owner_kind TEXT NOT NULL DEFAULT 'user' CHECK (owner_kind IN ('user', 'application')),
    ADD COLUMN purpose TEXT NOT NULL DEFAULT 'general' CHECK (purpose IN ('general', 'avatar', 'image'));
ALTER TABLE public.media_assets ADD CONSTRAINT media_assets_application_owner
    CHECK (owner_kind <> 'application' OR (owner_id IS NOT NULL AND owner_id = tenant_id));
COMMENT ON COLUMN public.media_assets.owner_kind IS 'Authenticated User owner or explicitly application-owned managed asset';
COMMENT ON COLUMN public.media_assets.purpose IS 'Authoritative media purpose, never read from client metadata';

CREATE TABLE public.core_initial_history (
    to_version smallint PRIMARY KEY CHECK (to_version = 2),
    from_version smallint NOT NULL CHECK (from_version IN (0, 1)),
    from_sha256 text CHECK (from_sha256 ~ '^[0-9a-f]{64}$'),
    to_sha256 text NOT NULL CHECK (to_sha256 ~ '^[0-9a-f]{64}$'),
    transition text NOT NULL CHECK (transition IN ('fresh', 'application-media-v2')),
    ddl_sha256 text NOT NULL CHECK (ddl_sha256 ~ '^[0-9a-f]{64}$'),
    applied_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK ((from_version = 0 AND from_sha256 IS NULL AND transition = 'fresh')
        OR (from_version = 1 AND from_sha256 IS NOT NULL AND transition = 'application-media-v2'))
);
REVOKE ALL ON public.core_initial_history FROM core_runtime;
GRANT SELECT ON public.core_initial_history TO core_runtime;

ALTER TABLE public.core_initial_schema DROP CONSTRAINT core_initial_schema_version_check;
UPDATE public.core_initial_schema
    SET version = 2, schema_sha256 = current_setting('flare.core_initial_target_sha256')
    WHERE version = 1;
ALTER TABLE public.core_initial_schema ADD CONSTRAINT core_initial_schema_version_check CHECK (version = 2);

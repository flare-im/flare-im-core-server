-- Explicit accepted Core initial v2 -> v3 only. Caller validates the released v2
-- source digest/full descriptor/owner ACL/history and sets the target source
-- digest locally.

-- Version-level review state of plugin web bundles (also in the fresh v3 schema).
CREATE TABLE public.plugin_web_bundles (
 plugin_id varchar(128) NOT NULL,
 plugin_version varchar(64) NOT NULL,
 bundle_id varchar(64) NOT NULL CHECK(bundle_id ~ '^[a-z0-9][a-z0-9-]{0,63}$'),
 sha256 text NOT NULL CHECK(sha256 ~ '^[0-9a-f]{64}$'),
 size bigint NOT NULL CHECK(size BETWEEN 1 AND 10485760),
 approved_at bigint NOT NULL DEFAULT (extract(epoch FROM clock_timestamp())*1000)::bigint,
 withdrawn_at bigint,
 updated_at bigint NOT NULL DEFAULT (extract(epoch FROM clock_timestamp())*1000)::bigint,
 PRIMARY KEY(plugin_id,plugin_version,bundle_id),
 FOREIGN KEY(plugin_id,plugin_version) REFERENCES public.plugin_versions(plugin_id,plugin_version),
 CHECK(withdrawn_at IS NULL OR withdrawn_at>=approved_at)
);
GRANT SELECT, INSERT, UPDATE, DELETE ON public.plugin_web_bundles TO core_runtime;

-- The history keeps the v2 transition row and gains the v3 one.
ALTER TABLE public.core_initial_history
    DROP CONSTRAINT core_initial_history_to_version_check,
    DROP CONSTRAINT core_initial_history_from_version_check,
    DROP CONSTRAINT core_initial_history_transition_check,
    DROP CONSTRAINT core_initial_history_check;
ALTER TABLE public.core_initial_history
    ADD CONSTRAINT core_initial_history_to_version_check CHECK (to_version IN (2, 3)),
    ADD CONSTRAINT core_initial_history_from_version_check CHECK (from_version IN (0, 1, 2)),
    ADD CONSTRAINT core_initial_history_transition_check
        CHECK (transition IN ('fresh', 'application-media-v2', 'plugin-web-bundles-v3')),
    ADD CONSTRAINT core_initial_history_check
        CHECK ((from_version = 0 AND from_sha256 IS NULL AND transition = 'fresh')
            OR (from_version = 1 AND to_version = 2 AND from_sha256 IS NOT NULL AND transition = 'application-media-v2')
            OR (from_version = 2 AND to_version = 3 AND from_sha256 IS NOT NULL AND transition = 'plugin-web-bundles-v3'));

ALTER TABLE public.core_initial_schema DROP CONSTRAINT core_initial_schema_version_check;
UPDATE public.core_initial_schema
    SET version = 3, schema_sha256 = current_setting('flare.core_initial_target_sha256')
    WHERE version = 2;
ALTER TABLE public.core_initial_schema ADD CONSTRAINT core_initial_schema_version_check CHECK (version = 3);

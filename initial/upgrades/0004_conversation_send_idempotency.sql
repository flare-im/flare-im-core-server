-- Explicit accepted Core initial v3 -> v4 only. Caller validates the released v3
-- source digest/full descriptor/owner ACL/history and sets the target source
-- digest locally.

-- Each conversation owns its send receipt, including client IDs reused elsewhere
-- (the shape of the fresh v4 schema and of migration 0032).
ALTER TABLE public.message_idempotency_ledger
    DROP CONSTRAINT message_idempotency_ledger_pkey;
ALTER TABLE public.message_idempotency_ledger
    ADD PRIMARY KEY (tenant_id, sender_id, conversation_id, client_msg_id);

-- Earlier writers stored reused sends without a receipt. Recover the first persisted result.
INSERT INTO public.message_idempotency_ledger (
    tenant_id, sender_id, conversation_id, client_msg_id, server_id,
    conversation_seq, durability, created_at
)
SELECT DISTINCT ON (tenant_id, sender_id, conversation_id, client_msg_id)
    tenant_id, sender_id, conversation_id, client_msg_id, server_id, seq, 4, created_at
FROM public.messages
WHERE created_at >= CURRENT_TIMESTAMP - INTERVAL '7 days'
  AND NULLIF(BTRIM(client_msg_id), '') IS NOT NULL
ORDER BY tenant_id, sender_id, conversation_id, client_msg_id, created_at, seq, server_id
ON CONFLICT (tenant_id, sender_id, conversation_id, client_msg_id) DO NOTHING;

-- The history keeps its earlier rows and gains the v4 one.
ALTER TABLE public.core_initial_history
    DROP CONSTRAINT core_initial_history_to_version_check,
    DROP CONSTRAINT core_initial_history_from_version_check,
    DROP CONSTRAINT core_initial_history_transition_check,
    DROP CONSTRAINT core_initial_history_check;
ALTER TABLE public.core_initial_history
    ADD CONSTRAINT core_initial_history_to_version_check CHECK (to_version IN (2, 3, 4)),
    ADD CONSTRAINT core_initial_history_from_version_check CHECK (from_version IN (0, 1, 2, 3)),
    ADD CONSTRAINT core_initial_history_transition_check
        CHECK (transition IN ('fresh', 'application-media-v2', 'plugin-web-bundles-v3',
                              'conversation-send-idempotency-v4')),
    ADD CONSTRAINT core_initial_history_check
        CHECK ((from_version = 0 AND from_sha256 IS NULL AND transition = 'fresh')
            OR (from_version = 1 AND to_version = 2 AND from_sha256 IS NOT NULL AND transition = 'application-media-v2')
            OR (from_version = 2 AND to_version = 3 AND from_sha256 IS NOT NULL AND transition = 'plugin-web-bundles-v3')
            OR (from_version = 3 AND to_version = 4 AND from_sha256 IS NOT NULL
                AND transition = 'conversation-send-idempotency-v4'));

ALTER TABLE public.core_initial_schema DROP CONSTRAINT core_initial_schema_version_check;
UPDATE public.core_initial_schema
    SET version = 4, schema_sha256 = current_setting('flare.core_initial_target_sha256')
    WHERE version = 3;
ALTER TABLE public.core_initial_schema ADD CONSTRAINT core_initial_schema_version_check CHECK (version = 4);

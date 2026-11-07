-- Older Social bridges projected a committed group dissolution as a suspension.
-- The historical marker distinguishes it from a temporary suspension.
-- Apply after the Social bridges projecting dissolution as archived are live.
WITH archived AS (
    UPDATE conversations
    SET lifecycle_state = 'archived',
        updated_at = GREATEST(clock_timestamp(), updated_at + INTERVAL '1 millisecond')
    WHERE conversation_type = 2
      AND lifecycle_state = 'suspended'
      AND attributes ->> 'social_state' = 'group_dissolved'
    RETURNING tenant_id, conversation_id
)
UPDATE conversation_participants sp
SET settings_version = sp.settings_version + 1,
    updated_at = GREATEST(clock_timestamp(), sp.updated_at + INTERVAL '1 millisecond',
                          sp.joined_at + INTERVAL '1 millisecond')
FROM archived
WHERE sp.tenant_id = archived.tenant_id
  AND sp.conversation_id = archived.conversation_id
  AND NOT COALESCE(sp.is_deleted, false);

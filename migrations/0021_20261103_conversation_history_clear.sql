-- Sequence cutoffs hide history from participants and preserve storage for audit/retention.
ALTER TABLE conversations
    ADD COLUMN IF NOT EXISTS history_cleared_through_seq BIGINT NOT NULL DEFAULT 0;

ALTER TABLE conversation_participants
    ADD COLUMN IF NOT EXISTS history_cleared_through_seq BIGINT NOT NULL DEFAULT 0;

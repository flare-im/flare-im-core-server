-- Each conversation owns its send receipt, including client IDs reused elsewhere.
ALTER TABLE message_idempotency_ledger
    DROP CONSTRAINT message_idempotency_ledger_pkey;
ALTER TABLE message_idempotency_ledger
    ADD PRIMARY KEY (tenant_id, sender_id, conversation_id, client_msg_id);
COMMENT ON TABLE message_idempotency_ledger IS '发送幂等账本：会话内发送身份唯一，覆盖 Redis 窗口之外的重发；保留 7 天';

-- Earlier writers stored reused sends without a receipt. Recover the first persisted result.
INSERT INTO message_idempotency_ledger (
    tenant_id, sender_id, conversation_id, client_msg_id, server_id,
    conversation_seq, durability, created_at
)
SELECT DISTINCT ON (tenant_id, sender_id, conversation_id, client_msg_id)
    tenant_id, sender_id, conversation_id, client_msg_id, server_id, seq, 4, created_at
FROM messages
WHERE created_at >= CURRENT_TIMESTAMP - INTERVAL '7 days'
  AND NULLIF(BTRIM(client_msg_id), '') IS NOT NULL
ORDER BY tenant_id, sender_id, conversation_id, client_msg_id, created_at, seq, server_id
ON CONFLICT (tenant_id, sender_id, conversation_id, client_msg_id) DO NOTHING;

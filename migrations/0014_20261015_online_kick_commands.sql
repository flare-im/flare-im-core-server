-- Durable physical-target command journal. Redis session indexes remain projections.
CREATE TABLE online_kick_commands (
 id UUID PRIMARY KEY,
 app_id TEXT NOT NULL,
 request_id TEXT NOT NULL,
 physical_id TEXT NOT NULL,
 target JSONB NOT NULL,
 evidence JSONB NOT NULL,
 next_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 attempts INTEGER NOT NULL DEFAULT 0,
 completed_at TIMESTAMPTZ,
 UNIQUE(app_id,request_id,physical_id)
);
CREATE INDEX online_kick_due ON online_kick_commands(next_at) WHERE completed_at IS NULL;

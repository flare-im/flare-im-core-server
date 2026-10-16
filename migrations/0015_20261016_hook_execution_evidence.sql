-- Hook execution evidence (Capability): every HookPlugin call is recorded through a bounded
-- buffer written in batches. Rejections, failures and timeouts are kept in full; successes and
-- circuit-open short circuits are kept 1 in N. sample_weight is how many executions a row stands
-- for (1 when kept in full, N when sampled), so weighted statistics stay unbiased; hook_version is
-- the configured version of the hook when it ran. Statistics and execution queries always name a
-- tenant and a hook (tenant, hook_type, hook_name index); the retention sweep deletes the oldest
-- rows in batches (executed_at index). Additive only: existing rows count once.
ALTER TABLE hook_executions
    ADD COLUMN IF NOT EXISTS hook_version TEXT;
ALTER TABLE hook_executions
    ADD COLUMN IF NOT EXISTS sample_weight INTEGER NOT NULL DEFAULT 1
        CONSTRAINT hook_executions_sample_weight_check CHECK (sample_weight >= 1);
CREATE INDEX IF NOT EXISTS idx_hook_executions_tenant_hook_executed
    ON hook_executions(tenant_id, hook_type, hook_name, executed_at DESC);
CREATE INDEX IF NOT EXISTS idx_hook_executions_executed_at
    ON hook_executions(executed_at);

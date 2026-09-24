-- 给已有库的 tenants 投影表补齐控制面版本字段。
-- 可重复执行；旧数据以 version=0 表示尚未接收过版本化投影。

ALTER TABLE tenants
    ADD COLUMN IF NOT EXISTS version BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS projected_at TIMESTAMPTZ;

COMMENT ON COLUMN tenants.version IS
    '控制面全局单调版本；收到的 version 不大于本值即丢弃';

COMMENT ON COLUMN tenants.projected_at IS
    '最近一次接受投影的时间';

-- 单租户/开发环境在接入控制面前仍需要默认租户可用；与 init.sql 保持一致。
INSERT INTO tenants (tenant_id, name, status)
VALUES ('0', 'default', 'active')
ON CONFLICT (tenant_id) DO NOTHING;

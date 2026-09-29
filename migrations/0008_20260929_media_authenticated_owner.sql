-- SEC-05. Apply before the new Media binary. Do not infer historical ownership
-- from upload metadata: it was client supplied. NULL private objects fail closed.
ALTER TABLE media_assets ADD COLUMN IF NOT EXISTS owner_id TEXT;
COMMENT ON COLUMN media_assets.owner_id IS
    'Authenticated uploader; NULL legacy private assets require ownership review';
CREATE INDEX IF NOT EXISTS idx_media_assets_tenant_orphan_due
    ON media_assets(tenant_id, grace_expires_at, file_id)
    WHERE reference_count = 0 AND status = 'pending' AND grace_expires_at IS NOT NULL;


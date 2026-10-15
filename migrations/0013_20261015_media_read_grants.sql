-- Final initialization input. Application read grants, never bucket/IAM ACLs.
CREATE TABLE media_read_grants (
    tenant_id TEXT NOT NULL,
    file_id TEXT NOT NULL,
    user_id TEXT NOT NULL CHECK (length(user_id) BETWEEN 1 AND 256),
    PRIMARY KEY (tenant_id,file_id,user_id),
    FOREIGN KEY (tenant_id,file_id) REFERENCES media_assets(tenant_id,file_id) ON DELETE CASCADE
);

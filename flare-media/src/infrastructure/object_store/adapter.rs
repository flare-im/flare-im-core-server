use crate::error::Result;

use super::s3::S3ObjectStore;
use crate::domain::repository::ObjectRepositoryRef;
use flare_im_core::config::ObjectStoreConfig;

pub async fn build_object_store(
    profile: Option<&ObjectStoreConfig>,
) -> Result<Option<ObjectRepositoryRef>> {
    if let Some(profile) = profile {
        // 统一以 S3 兼容协议落地（MinIO、OSS、COS、GCS、七牛等均通过 endpoint/ak/sk 适配）
        // profile_type 可为: "s3" | "minio" | "oss" | "cos" | "gcs" | "qiniu"
        let store = S3ObjectStore::from_config(profile).await?;
        return Ok(Some(std::sync::Arc::new(store) as ObjectRepositoryRef));
    }
    Ok(None)
}

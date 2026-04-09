use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::fs;

use crate::domain::model::UploadContext;
use crate::domain::repository::MediaLocalStore;
use crate::error::{ErrorCode, Result, map_infra_error};

#[derive(Clone)]
pub struct FilesystemMediaStore {
    root: PathBuf,
    base_url: Option<String>,
}

impl FilesystemMediaStore {
    pub fn new(root: impl AsRef<Path>, base_url: Option<String>) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        std::fs::create_dir_all(&root)?;
        Ok(Self { root, base_url })
    }

    fn file_path(&self, file_id: &str) -> PathBuf {
        self.root.join(file_id)
    }
}

#[async_trait::async_trait]
impl MediaLocalStore for FilesystemMediaStore {
    async fn write(&self, context: &UploadContext<'_>) -> Result<String> {
        let path = self.file_path(context.file_id);
        fs::write(&path, context.payload)
            .await
            .map_err(|e| {
                map_infra_error(e, ErrorCode::InternalError, format!("write file to {:?}", path))
            })?;
        Ok(context.file_id.to_string())
    }

    async fn read(&self, file_id: &str) -> Result<Vec<u8>> {
        let path = self.file_path(file_id);
        fs::read(&path).await.map_err(|e| {
            map_infra_error(e, ErrorCode::InternalError, format!("read file from {:?}", path))
        })
    }

    async fn delete(&self, file_id: &str) -> Result<()> {
        let path = self.file_path(file_id);
        if path.exists() {
            fs::remove_file(&path)
                .await
                .map_err(|e| {
                    map_infra_error(e, ErrorCode::InternalError, format!("remove file {:?}", path))
                })?;
        }
        Ok(())
    }

    fn base_url(&self) -> Option<String> {
        self.base_url.clone()
    }
}

pub type FilesystemMediaStoreRef = Arc<FilesystemMediaStore>;

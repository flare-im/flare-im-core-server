//! Storage + Sync gRPC 客户端池

use flare_grpc_proto::storage::storage_reader_service_client::StorageReaderServiceClient;
use flare_grpc_proto::sync::sync_service_client::SyncServiceClient;
use flare_im_core::ServiceClient;
use flare_im_core::service_names::{STORAGE_READER, SYNC_ORCHESTRATOR, get_service_name};
use flare_server_core::error::{ErrorBuilder, ErrorCode, Result};
use tokio::sync::Mutex;
use tonic::transport::Channel;

pub struct StorageSyncGrpcPool {
    storage_service_name: String,
    sync_service_name: String,
    storage_service_client: Mutex<Option<ServiceClient>>,
    sync_service_client: Mutex<Option<ServiceClient>>,
    storage_reader_grpc: Mutex<Option<StorageReaderServiceClient<Channel>>>,
    sync_grpc: Mutex<Option<SyncServiceClient<Channel>>>,
}

impl StorageSyncGrpcPool {
    pub fn new() -> Self {
        Self {
            storage_service_name: get_service_name(STORAGE_READER),
            sync_service_name: get_service_name(SYNC_ORCHESTRATOR),
            storage_service_client: Mutex::new(None),
            sync_service_client: Mutex::new(None),
            storage_reader_grpc: Mutex::new(None),
            sync_grpc: Mutex::new(None),
        }
    }

    async fn ensure_storage_service_client(&self) -> Result<()> {
        let mut sc_guard = self.storage_service_client.lock().await;
        if sc_guard.is_some() {
            return Ok(());
        }

        let discover = flare_im_core::discovery::create_discover(&self.storage_service_name)
            .await
            .map_err(|e| {
                ErrorBuilder::new(ErrorCode::ServiceUnavailable, "storage reader unavailable")
                    .details(format!(
                        "Failed to create service discover for {}: {}",
                        self.storage_service_name, e
                    ))
                    .build_error()
            })?;

        if let Some(discover) = discover {
            *sc_guard = Some(ServiceClient::new(discover));
            return Ok(());
        }

        Err(
            ErrorBuilder::new(ErrorCode::ServiceUnavailable, "storage reader unavailable")
                .details("Service discovery not configured for storage reader")
                .build_error(),
        )
    }

    async fn ensure_sync_service_client(&self) -> Result<()> {
        let mut sc_guard = self.sync_service_client.lock().await;
        if sc_guard.is_some() {
            return Ok(());
        }

        let discover = flare_im_core::discovery::create_discover(&self.sync_service_name)
            .await
            .map_err(|e| {
                ErrorBuilder::new(
                    ErrorCode::ServiceUnavailable,
                    "sync orchestrator unavailable",
                )
                .details(format!(
                    "Failed to create service discover for {}: {}",
                    self.sync_service_name, e
                ))
                .build_error()
            })?;

        if let Some(discover) = discover {
            *sc_guard = Some(ServiceClient::new(discover));
            return Ok(());
        }

        Err(ErrorBuilder::new(
            ErrorCode::ServiceUnavailable,
            "sync orchestrator unavailable",
        )
        .details("Service discovery not configured for sync orchestrator")
        .build_error())
    }

    pub async fn ensure_storage_reader_client(
        &self,
    ) -> Result<StorageReaderServiceClient<Channel>> {
        let mut guard = self.storage_reader_grpc.lock().await;
        if let Some(c) = guard.as_ref() {
            return Ok(c.clone());
        }

        self.ensure_storage_service_client().await?;
        let mut sc_guard = self.storage_service_client.lock().await;
        let service_client = sc_guard.as_mut().ok_or_else(|| {
            ErrorBuilder::new(
                ErrorCode::InternalError,
                "storage reader service client not initialized",
            )
            .build_error()
        })?;
        let channel = service_client.get_channel().await.map_err(|e| {
            ErrorBuilder::new(ErrorCode::ServiceUnavailable, "storage reader unavailable")
                .details(format!("Failed to get channel: {}", e))
                .build_error()
        })?;

        let client = StorageReaderServiceClient::new(channel);
        *guard = Some(client.clone());
        Ok(client)
    }

    pub async fn ensure_sync_client(&self) -> Result<SyncServiceClient<Channel>> {
        let mut guard = self.sync_grpc.lock().await;
        if let Some(c) = guard.as_ref() {
            return Ok(c.clone());
        }

        self.ensure_sync_service_client().await?;
        let mut sc_guard = self.sync_service_client.lock().await;
        let service_client = sc_guard.as_mut().ok_or_else(|| {
            ErrorBuilder::new(
                ErrorCode::InternalError,
                "sync orchestrator service client not initialized",
            )
            .build_error()
        })?;
        let channel = service_client.get_channel().await.map_err(|e| {
            ErrorBuilder::new(
                ErrorCode::ServiceUnavailable,
                "sync orchestrator unavailable",
            )
            .details(format!("Failed to get channel: {}", e))
            .build_error()
        })?;

        let client = SyncServiceClient::new(channel);
        *guard = Some(client.clone());
        Ok(client)
    }
}

impl Default for StorageSyncGrpcPool {
    fn default() -> Self {
        Self::new()
    }
}

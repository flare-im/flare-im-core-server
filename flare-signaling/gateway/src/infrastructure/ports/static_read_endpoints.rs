//! Same-Cell static targets for the Gateway's internal gRPC pools.

use flare_im_contracts::service_names::{
    CONVERSATION, SIGNALING_ROUTE, STORAGE_READER, SYNC_ORCHESTRATOR,
};
use flare_im_service_kit::config::FlareAppConfig;
use flare_server_core::error::Result;

#[derive(Debug, Default)]
pub(crate) struct StaticReadEndpoints {
    pub conversation: Option<String>,
    pub route: Option<String>,
    pub storage_reader: Option<String>,
    pub sync: Option<String>,
}

impl StaticReadEndpoints {
    pub(crate) fn resolve(app: &FlareAppConfig, static_registry_mode: bool) -> Result<Self> {
        if !static_registry_mode {
            return Ok(Self::default());
        }
        let endpoint = |dependency| {
            flare_im_service_kit::discovery::static_grpc_endpoint_from_app_config(app, dependency)
                .map(Some)
        };
        Ok(Self {
            conversation: endpoint(CONVERSATION)?,
            route: endpoint(SIGNALING_ROUTE)?,
            storage_reader: endpoint(STORAGE_READER)?,
            sync: endpoint(SYNC_ORCHESTRATOR)?,
        })
    }
}

#[cfg(test)]
mod tests;

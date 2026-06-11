//! ExtensionPlugin operation contract.

use std::sync::Arc;

use async_trait::async_trait;
use flare_core_base::context::Ctx;
use prost_types::Any;
use tonic::Status;

#[async_trait]
pub trait ExtensionOperationHandler: Send + Sync {
    fn id(&self) -> &str;

    fn operation_prefixes(&self) -> &[&'static str];

    async fn call(&self, ctx: &Ctx, operation: &str, payload: Option<Any>) -> Result<Any, Status>;
}

pub type DynExtensionOperationHandler = Arc<dyn ExtensionOperationHandler>;

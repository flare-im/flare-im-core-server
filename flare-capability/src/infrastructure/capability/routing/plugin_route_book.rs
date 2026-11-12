//! What calling a remote plugin needs: the authorized snapshot of an application, the ticket
//! signer, and card and shared state publishing. Where a plugin runs comes from the snapshot alone;
//! a deployment gets there through the registry and a grant, never by announcing itself.

use std::sync::Arc;

use flare_grpc_proto::capability::RegisteredPluginInstance;
use flare_im_capability_core::plugin::manifest::PLUGIN_API_VERSION;
use flare_server_core::error::Result;

#[derive(Clone, Default)]
pub struct PluginRouteBook {
    pub(crate) authorization: Option<Arc<dyn crate::domain::repository::PluginAuthorizationReader>>,
    pub(crate) invocation_signer:
        Option<Arc<flare_im_capability_core::plugin::invocation::PluginCallSigner>>,
    /// Plugin entity shared state publishing (call results and the bridge write); `None` without
    /// a database or an Orchestrator authority: shared state is then never published.
    pub(crate) state_publisher:
        Option<Arc<crate::application::handler::plugin_state::PluginStatePublisher>>,
    /// Plugin entity bindings (the database): card actions are checked against them and a posted
    /// card is bound at once. `None` without a database: calls naming an entity are refused.
    pub(crate) entity_bindings:
        Option<Arc<dyn crate::domain::repository::plugin_state::PluginEntityBindingStore>>,
    /// Platform posting of the card a call result carries (`data.message`).
    pub(crate) card_poster: Option<Arc<crate::application::handler::plugin_card::PluginCardPoster>>,
    /// The entity lifecycle outbox (`post_failed` and `recalled` notifications to plugins).
    pub(crate) lifecycle:
        Option<Arc<dyn crate::domain::repository::entity_lifecycle::EntityLifecycleOutbox>>,
    /// The deployment's web bundle switch and the client catalog's variant selection cache.
    pub(crate) catalog_selection:
        Arc<crate::application::handler::catalog_selection::CatalogSelection>,
}

impl PluginRouteBook {
    /// Endpoint discovery remains diagnostics. Only the durable authenticated
    /// authorization projection may supply a business dispatch target.
    pub fn with_runtime_authorization(
        mut self,
        reader: Arc<dyn crate::domain::repository::PluginAuthorizationReader>,
        signer: Option<Arc<flare_im_capability_core::plugin::invocation::PluginCallSigner>>,
    ) -> Self {
        self.authorization = Some(reader);
        self.invocation_signer = signer;
        self
    }
    pub fn with_state_publisher(
        mut self,
        publisher: Option<Arc<crate::application::handler::plugin_state::PluginStatePublisher>>,
    ) -> Self {
        self.state_publisher = publisher;
        self
    }
    pub fn with_entity_bindings(
        mut self,
        bindings: Option<
            Arc<dyn crate::domain::repository::plugin_state::PluginEntityBindingStore>,
        >,
    ) -> Self {
        self.entity_bindings = bindings;
        self
    }
    pub fn with_card_posting(
        mut self,
        poster: Option<Arc<crate::application::handler::plugin_card::PluginCardPoster>>,
        lifecycle: Option<
            Arc<dyn crate::domain::repository::entity_lifecycle::EntityLifecycleOutbox>,
        >,
    ) -> Self {
        self.card_poster = poster;
        self.lifecycle = lifecycle;
        self
    }
    /// `plugin.webBundles.enabled` of the deployment: when off, catalogs select declarative
    /// variants only (the client's bundle runtime is ignored).
    pub fn with_web_bundles(mut self, enabled: bool) -> Self {
        self.catalog_selection = Arc::new(
            crate::application::handler::catalog_selection::CatalogSelection::new(
                enabled,
                crate::application::handler::catalog_selection::SELECTION_CACHE_CAPACITY,
            ),
        );
        self
    }
    pub fn new() -> Self {
        Self::default()
    }

    /// The plugins of `tenant_id` that declare `capability_id` (all of them when it is empty), for
    /// diagnostics. Dispatch reads the snapshot itself.
    pub async fn instances(
        &self,
        tenant_id: &str,
        capability_id: &str,
    ) -> Result<Vec<RegisteredPluginInstance>> {
        let Some(reader) = &self.authorization else {
            return Ok(Vec::new());
        };
        let Some(snapshot) = reader.plugin_snapshot(tenant_id).await? else {
            return Ok(Vec::new());
        };
        let mut instances: Vec<RegisteredPluginInstance> = snapshot
            .plugins
            .iter()
            .map(|plugin| RegisteredPluginInstance {
                plugin_id: plugin.deployment.plugin_id.clone(),
                capability_id: capability_id.to_owned(),
                grpc_authority: plugin.deployment.grpc_authority.clone(),
                plugin_version: plugin.deployment.plugin_version.clone(),
                api_version: PLUGIN_API_VERSION.to_owned(),
                manifest_sha256: plugin.deployment.manifest_digest.clone(),
                declared_operations: plugin.manifest().operation_ids().into_iter().collect(),
                ..Default::default()
            })
            .filter(|instance| {
                capability_id.is_empty()
                    || instance
                        .declared_operations
                        .iter()
                        .any(|operation| operation == capability_id)
            })
            .collect();
        instances.sort_by(|a, b| a.plugin_id.cmp(&b.plugin_id));
        Ok(instances)
    }
}

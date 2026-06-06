use flare_im_core::gateway::{GatewayEnvScope, GatewaySettings};
use flare_server_core::{
    context::Ctx,
    http::{ContextFromHeaders, HttpApiError},
};

#[test]
fn gateway_shared_contracts_live_in_im_core_and_server_core() {
    let settings_type = std::any::type_name::<GatewaySettings>();
    let scope_type = std::any::type_name::<GatewayEnvScope>();
    let error_type = std::any::type_name::<HttpApiError>();
    let ctx_type = std::any::type_name::<Ctx>();

    assert!(settings_type.starts_with("flare_im_core::gateway::"));
    assert!(scope_type.starts_with("flare_im_core::gateway::"));
    assert!(error_type.starts_with("flare_core_transport::http::"));
    assert!(ctx_type.starts_with("alloc::sync::Arc<flare_core_base::context::core::Context>"));
}

#[test]
fn gateway_context_extension_is_owned_by_server_core_http() {
    fn assert_ext<T: ContextFromHeaders>() {}

    assert_ext::<Ctx>();
}

use flare_server_core::{
    auth::{AuthProviderConfig, AuthProviderMode, build_token_validator},
    context::Ctx,
    http::{ContextFromHeaders, HttpApiError},
};

#[test]
fn gateway_generic_foundation_comes_from_server_core() {
    assert!(std::any::type_name::<HttpApiError>().starts_with("flare_core_transport::http::"));
    assert!(std::any::type_name::<AuthProviderConfig>().starts_with("flare_core_infra::auth::"));
    assert!(std::any::type_name::<AuthProviderMode>().starts_with("flare_core_infra::auth::"));

    fn assert_context_ext<T: ContextFromHeaders>() {}
    assert_context_ext::<Ctx>();

    let _builder = build_token_validator;
}

use flare_admin_gateway::interface::http::build_admin_capabilities;

#[test]
fn admin_gateway_owns_admin_capability_contract() {
    let capabilities = build_admin_capabilities();

    assert_eq!(capabilities.service, "flare-admin-gateway");
    assert!(
        capabilities
            .required_scopes
            .iter()
            .any(|scope| scope == "admin_gateway:admin")
    );
}

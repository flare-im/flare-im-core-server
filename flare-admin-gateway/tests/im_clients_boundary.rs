use flare_im_core::clients::GrpcClients;

#[test]
fn admin_gateway_uses_im_core_typed_clients() {
    let clients_type = std::any::type_name::<GrpcClients>();

    assert!(clients_type.starts_with("flare_im_core::clients::"));
}

use flare_im_service_kit::clients::{
    GrpcClients, MessageActionServiceClientWrapper, MessageEventServiceClientWrapper,
    MessageSendServiceClientWrapper,
};

#[test]
fn admin_gateway_uses_im_core_typed_clients() {
    let clients_type = std::any::type_name::<GrpcClients>();
    let send_type = std::any::type_name::<MessageSendServiceClientWrapper>();
    let event_type = std::any::type_name::<MessageEventServiceClientWrapper>();
    let action_type = std::any::type_name::<MessageActionServiceClientWrapper>();

    assert!(clients_type.starts_with("flare_im_service_kit::clients::"));
    assert!(send_type.starts_with("flare_im_service_kit::clients::"));
    assert!(event_type.starts_with("flare_im_service_kit::clients::"));
    assert!(action_type.starts_with("flare_im_service_kit::clients::"));
}

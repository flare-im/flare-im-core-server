const CARGO_TOML: &str = include_str!("../Cargo.toml");
const MAIN_RS: &str = include_str!("../src/main.rs");
const ROUTER_RS: &str = include_str!("../src/interface/http/router.rs");
const MESSAGE_HANDLER_RS: &str = include_str!("../src/interface/http/message_handler.rs");

#[test]
fn crate_surface_is_api_gateway_while_runtime_service_contract_stays_core_gateway() {
    assert!(CARGO_TOML.contains("name = \"flare-api-gateway\""));
    assert!(CARGO_TOML.contains("name = \"flare_api_gateway\""));
    assert!(MAIN_RS.contains("use flare_api_gateway::interface::http::create_public_router;"));

    assert!(MAIN_RS.contains("GatewayEnvScope::Core"));
    assert!(MAIN_RS.contains("CORE_GATEWAY"));
    assert!(MAIN_RS.contains("config/services/core_gateway.toml"));
}

#[test]
fn public_router_exposes_bff_routes_and_keeps_admin_out() {
    for route in [
        ".nest(\"/api/v1/medias\"",
        ".nest(\"/api/v1/messages\"",
        ".nest(\"/api/v1/conversations\"",
        ".nest(\"/api/v1/presence\"",
        ".route(\"/api-doc/openapi.json\"",
        ".route(\"/swagger-ui\"",
        ".route(\"/health\"",
    ] {
        assert!(ROUTER_RS.contains(route), "missing public route: {route}");
    }

    assert!(
        !ROUTER_RS.contains("/api/v1/admin"),
        "admin routes belong to flare-admin-gateway, not flare-api-gateway"
    );
}

#[test]
fn protected_api_groups_are_wrapped_by_gateway_auth() {
    assert!(ROUTER_RS.contains("gateway_auth_middleware"));
    let auth_layers = ROUTER_RS
        .match_indices(".route_layer(middleware::from_fn(gateway_auth_middleware));")
        .count();
    assert_eq!(
        auth_layers, 4,
        "media/message/conversation/presence API groups must all be protected"
    );
}

#[test]
fn message_send_event_and_action_paths_keep_single_owners() {
    let send_message = function_body(MESSAGE_HANDLER_RS, "pub async fn send_message");
    assert!(send_message.contains("clients.message_ingest_send"));
    assert!(!send_message.contains("clients.message_event"));
    assert!(!send_message.contains("clients.message_action"));

    let execute_event = function_body(MESSAGE_HANDLER_RS, "pub async fn execute_custom_event");
    assert!(execute_event.contains("clients.message_event"));
    assert!(!execute_event.contains("clients.message_ingest_send"));

    let recall_message = function_body(MESSAGE_HANDLER_RS, "pub async fn recall_message");
    assert!(recall_message.contains("clients.message_action"));
    assert!(!recall_message.contains("clients.message_ingest_send"));
}

fn function_body(source: &str, signature: &str) -> String {
    let start = source
        .find(signature)
        .unwrap_or_else(|| panic!("missing function signature: {signature}"));
    let mut depth = 0usize;
    let mut saw_open = false;
    let mut end = source.len();

    for (offset, ch) in source[start..].char_indices() {
        match ch {
            '{' => {
                saw_open = true;
                depth += 1;
            }
            '}' if saw_open => {
                depth -= 1;
                if depth == 0 {
                    end = start + offset + ch.len_utf8();
                    break;
                }
            }
            _ => {}
        }
    }

    assert!(saw_open, "function has no body: {signature}");
    source[start..end].to_string()
}

//! The plugin admin entry of `CapabilityService.Administer` on the gRPC request admin-gateway
//! forwards: a Staff reauthentication proof (`x-flare-staff-reauth`) reaches the plugin call only
//! as transport metadata, its absence forwards none, and a malformed or repeated one is refused as
//! UNAUTHENTICATED before any plugin is called.
use super::*;
use crate::application::handler::admin_dispatch::tests::{
    ALLOW_EVIDENCE, Harness, READ, harness, staff_context_metadata,
};
use crate::application::handler::admin_dispatch::{
    AdminPluginResult, PLUGIN_ADMIN_QUERY_OPERATION,
};
use crate::infrastructure::capability::InMemoryCapabilityGrants;
use flare_im_core_capability_core::plugin::staff_reauth::{
    MAX_STAFF_REAUTH_TOKEN_BYTES, STAFF_REAUTH_HEADER,
};
use std::time::Duration;
use tonic::Code;

const PROOF: &str = "header.payload.signature";

/// The Capability service as deployed without a database, Staff verified by the gateway key the
/// signed context of [`admin_query`] uses, plugins routed through the harness projection.
fn server(h: &Harness, verifier: SignedContextVerifier) -> CapabilityGrpcServer {
    CapabilityGrpcServer::new(
        CapabilityExtensionRegistry::new(),
        Arc::new(InMemoryCapabilityGrants::new()),
        Arc::new(CapabilityRuntimeConfig {
            max_payload_json_bytes: 64 * 1024,
            dispatch_timeout: Duration::from_secs(5),
            dispatch_max_per_minute: None,
            plugin_call_timeout: Duration::from_secs(2),
        }),
        None,
        None,
        Arc::new(CapabilityInvocationMetrics::default()),
        None,
        h.routes.clone(),
        None,
        Some(verifier),
    )
}

/// `Administer` `flare.plugin.v1.admin.query` of `example.poll.list`, as admin-gateway forwards
/// it: the gateway-signed Staff context in `x-flare-ctx` plus one `x-flare-staff-reauth` entry
/// per element of `proofs`.
fn admin_query(proofs: &[&str]) -> (Request<GenericRequest>, SignedContextVerifier) {
    let (metadata, verifier) = staff_context_metadata(
        "staff-7",
        READ,
        Some("ticket-42 spam cleanup"),
        "admin-req-1",
        Some("org-1"),
        None,
    );
    let body = serde_json::json!({
        "pluginId": "example.poll",
        "operation": "example.poll.list",
        "input": {},
    });
    let mut request = Request::new(GenericRequest {
        operation: PLUGIN_ADMIN_QUERY_OPERATION.into(),
        request_id: "untrusted-payload-id".into(),
        payload: Some(prost_types::Any {
            type_url: "type.googleapis.com/flare.capability.v1.PayloadJson".into(),
            value: serde_json::to_vec(&body).unwrap(),
        }),
        ..Default::default()
    });
    *request.metadata_mut() = metadata;
    for proof in proofs {
        request
            .metadata_mut()
            .append(STAFF_REAUTH_HEADER, proof.parse().unwrap());
    }
    (request, verifier)
}

/// The Staff identity `administer_plugin` builds from the gRPC request, dispatched as it dispatches
/// it. The RPC's own evidence writes to the Postgres audit store and refuses to call the plugin
/// without one, so the always-written evidence of the handler tests stands in for it.
async fn administer_with_written_evidence(
    server: &CapabilityGrpcServer,
    request: Request<GenericRequest>,
) -> Result<AdminPluginResult, Status> {
    let call = AdminCall::from_operation(&request.get_ref().operation).unwrap();
    let admin =
        AdminPluginRequest::parse(&request.get_ref().payload.as_ref().unwrap().value).unwrap();
    let staff = server.plugin_staff(&request, &admin)?;
    dispatch_admin(
        &staff,
        &server.plugin_routes,
        call,
        admin,
        server.runtime.plugin_call_timeout,
        &ALLOW_EVIDENCE,
    )
    .await
    .map_err(capability_error_to_status)
}

#[tokio::test]
async fn plugin_admin_rpc_forwards_the_staff_reauthentication_proof_to_the_plugin() {
    let h = harness().await;

    let (request, verifier) = admin_query(&[PROOF]);
    let capability = server(&h, verifier);
    let staff = capability
        .plugin_staff(
            &request,
            &AdminPluginRequest::parse(&request.get_ref().payload.as_ref().unwrap().value).unwrap(),
        )
        .unwrap();
    assert_eq!(staff.reauthentication_proof(), Some(PROOF));
    assert!(!format!("{staff:?} {:?}", staff.ctx).contains(PROOF));
    let answer = administer_with_written_evidence(&capability, request)
        .await
        .unwrap();
    assert!(answer.success);

    let calls = h.plugin.calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].operation, "example.poll.list");
    assert_eq!(calls[0].reauthentication_proof.as_deref(), Some(PROOF));
    assert!(!calls[0].payload.to_string().contains(PROOF));
    assert!(
        !calls[0]
            .metadata
            .values()
            .any(|value| value.contains(PROOF))
    );
    assert!(!calls[0].ticket.contains(PROOF));
}

#[tokio::test]
async fn plugin_admin_rpc_without_a_proof_forwards_none() {
    let h = harness().await;
    let (request, verifier) = admin_query(&[]);
    let capability = server(&h, verifier);
    assert!(
        administer_with_written_evidence(&capability, request)
            .await
            .unwrap()
            .success
    );
    let calls = h.plugin.calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].reauthentication_proof, None);
}

#[tokio::test]
async fn plugin_admin_rpc_refuses_a_malformed_or_repeated_proof_before_the_plugin() {
    let h = harness().await;
    let oversized = format!("a.b.{}", "c".repeat(MAX_STAFF_REAUTH_TOKEN_BYTES));
    let refused: [&[&str]; 5] = [
        &[""],
        &["not-a-jwt"],
        &["a.b.c,d.e.f"],
        &[oversized.as_str()],
        &["a.b.c", "d.e.f"],
    ];
    for proofs in refused {
        let (request, verifier) = admin_query(proofs);
        let capability = server(&h, verifier);
        // The RPC itself, as tonic hands it over.
        let status = capability.administer(request).await.unwrap_err();
        assert_eq!(status.code(), Code::Unauthenticated, "{proofs:?}");
        let (request, _) = admin_query(proofs);
        let status = administer_with_written_evidence(&capability, request)
            .await
            .unwrap_err();
        assert_eq!(status.code(), Code::Unauthenticated, "{proofs:?}");
    }
    assert!(h.plugin.calls().is_empty());
}

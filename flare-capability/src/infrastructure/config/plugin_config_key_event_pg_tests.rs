use super::*;
use crate::domain::{
    plugin::{DesiredAppState, RuntimePatch, StateLifecycle},
    repository::plugin_registry::PluginRegistryRepository,
};
use crate::infrastructure::persistence::postgres_plugin_registry::PostgresPluginRegistry;
use flare_im_core_capability_core::plugin::{
    PluginManifest,
    runtime::{configuration_digest, digest_bytes},
};
use sqlx::Row;
use std::sync::Arc;

#[tokio::test]
#[ignore = "requires FLARE_PLUGIN_EVENT_TEST_DATABASE_URL; uses an isolated disposable schema"]
async fn event_activation_secret_record_delivery_keeps_the_accepted_backlog() {
    let url =
        std::env::var("FLARE_PLUGIN_EVENT_TEST_DATABASE_URL").expect("explicit event fixture URL");
    let admin = sqlx::PgPool::connect(&url).await.unwrap();
    let schema = format!("plugin_event_secret_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&admin)
        .await
        .unwrap();
    let search = format!("SET search_path={schema},pg_temp");
    let pool = Arc::new(
        sqlx::postgres::PgPoolOptions::new()
            .max_connections(3)
            .after_connect(move |connection, _| {
                let search = search.clone();
                Box::pin(async move {
                    sqlx::query(&search).execute(connection).await?;
                    Ok(())
                })
            })
            .connect(&url)
            .await
            .unwrap(),
    );
    for ddl in [
        include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../migrations/0025_20261107_plugin_registry.sql"
        )),
        include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../migrations/0026_20261107_plugin_development.sql"
        )),
        include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../migrations/0028_20261108_plugin_event_activation.sql"
        )),
    ] {
        sqlx::raw_sql(ddl).execute(pool.as_ref()).await.unwrap();
    }
    let raw=serde_json::to_string(&json!({"manifestVersion":1,"pluginId":"observer","version":"1.0.0","apiVersion":"1.0","displayName":"Observer",
        "publisher":{"publisherId":"test","displayName":"Test"},"runtime":{"kind":"grpc","service":"test.Plugin","protocol":"flare.capability.v1"},
        "capabilities":[{"id":"observer","operations":[{"id":"run","inputSchema":{"type":"object"},"outputSchema":{"type":"object"},"permissionIds":["observe"],"idempotent":true,"timeoutMs":1000}]}],
        "permissions":[{"id":"observe","description":"Observe","riskLevel":"high","dataScopes":["messages.body.read","messages.events.read"]}],"eventSubscriptions":[{"eventType":"im.message.created","schemaVersion":1,"operation":"observer.run","permissionIds":["observe"],"includeBody":true}],"features":[{"id":"run","label":{"en-US":"Run"},"gates":{"operations":["observer.run"]}}],
        "configurationSchema":{"type":"object","properties":{"apiKey":{"type":"string","writeOnly":true}}},
        "artifact":{"digest":format!("sha256:{}","a".repeat(64)),"signatureRef":"test"}})).unwrap();
    let manifest = PluginManifest::from_json(raw.as_bytes()).unwrap();
    assert_eq!(
        manifest.secret_configuration_paths(),
        vec![vec!["apiKey".to_string()]]
    );
    let digest = digest_bytes(raw.as_bytes());
    sqlx::query("INSERT INTO plugin_versions(plugin_id,plugin_version,display_name,publisher_id,manifest_json,manifest_digest,key_id,signature,kind) VALUES('observer','1.0.0','Observer','test',$1,$2,'test','test','business')")
        .bind(&raw).bind(&digest).execute(pool.as_ref()).await.unwrap();
    let cipher = Arc::new(
        PluginConfigCipher::parse(&format!("test:{}", STANDARD.encode([81u8; 32]))).unwrap(),
    );
    let registry = PostgresPluginRegistry::new(pool.clone()).with_cipher(cipher.clone());
    let desired = DesiredAppState {
        app_id: "app".into(),
        binding_id: "grant".into(),
        plugin_id: "observer".into(),
        plugin_version: "1.0.0".into(),
        manifest_digest: digest.clone(),
        revision: 1,
        enabled: true,
        configured: true,
        configuration: json!({"apiKey":"dummy-acceptance-secret"}),
        secrets_from: None,
    };
    let now = chrono::Utc::now().timestamp_millis();
    sqlx::query("INSERT INTO plugin_deployments(deployment_id,plugin_id,plugin_version,manifest_digest,artifact_digest,grpc_authority,healthy_until_ms) VALUES('dep','observer','1.0.0',$1,$2,'http://127.0.0.1:1',$3)").bind(&digest).bind(format!("sha256:{}","a".repeat(64))).bind(now+60000).execute(pool.as_ref()).await.unwrap();
    let initial = registry.put_config(&desired, None).await.unwrap();
    let config_digest = configuration_digest(&desired.configuration).unwrap();
    registry
        .record_delivery(
            &initial,
            RuntimePatch {
                deployment_id: Some(Some("dep".into())),
                runtime_revision: Some(1),
                runtime_enabled: Some(true),
                runtime_healthy_until_ms: Some(now + 60000),
                runtime_configuration_digest: Some(Some(config_digest)),
                runtime_configuration: Some(Some(desired.configuration.clone())),
                lifecycle: Some(StateLifecycle::Installed),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    sqlx::query("UPDATE plugin_app_states SET approved=$1,features=$2,organization_id='org'").bind(json!({"operations":["observer.run"],"permissions":["observe"],"dataScopes":["messages.body.read","messages.events.read"]})).bind(json!({"run":true})).execute(pool.as_ref()).await.unwrap();
    let seen = registry
        .app_states(&["app".into()], None)
        .await
        .unwrap()
        .remove(0);
    let row = sqlx::query("SELECT configuration,event_active_since_ms FROM plugin_app_states")
        .fetch_one(pool.as_ref())
        .await
        .unwrap();
    let before_secret: Value = row.get("configuration");
    let before: i64 = row.get("event_active_since_ms");
    assert!(before_secret["apiKey"].is_object());
    assert!(
        registry
            .record_delivery(
                &seen,
                RuntimePatch {
                    runtime_healthy_until_ms: Some(now + 120000),
                    runtime_configuration: Some(Some(desired.configuration.clone())),
                    ..Default::default()
                }
            )
            .await
            .unwrap()
    );
    let row = sqlx::query("SELECT configuration,event_active_since_ms FROM plugin_app_states")
        .fetch_one(pool.as_ref())
        .await
        .unwrap();
    let after_secret: Value = row.get("configuration");
    let after: i64 = row.get("event_active_since_ms");
    use crate::domain::repository::PluginAuthorizationReader;
    use crate::infrastructure::persistence::postgres_plugin_authorization::PostgresPluginAuthorization;
    use flare_im_core_capability_core::plugin::{
        authorization::TenantPluginGrant,
        runtime::{PluginProjection, ProjectedLifecycle, ProjectedPlugin},
    };
    sqlx::raw_sql(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../migrations/0017_20261022_plugin_projection_tables.sql"
    )))
    .execute(pool.as_ref())
    .await
    .unwrap();
    let projection = PluginProjection {
        tenant_id: "app".into(),
        organization_id: Some("org".into()),
        version: 1,
        issued_at_ms: now,
        valid_until_ms: now + 60000,
        bindings: vec![],
        plugins: vec![ProjectedPlugin {
            grant: TenantPluginGrant {
                grant_id: "grant".into(),
                revision: 1,
                tenant_id: "app".into(),
                plugin_id: "observer".into(),
                plugin_version: "1.0.0".into(),
                manifest_digest: digest,
                approved_operations: ["observer.run".into()].into(),
                approved_permissions: ["observe".into()].into(),
                approved_data_scopes: ["messages.events.read".into(), "messages.body.read".into()]
                    .into(),
                enabled: true,
                revoked: false,
                suspended: false,
                valid_from_ms: now,
                valid_until_ms: now + 60000,
            },
            lifecycle: ProjectedLifecycle::Installed,
            export_until_ms: None,
            features: [("run".into(), true)].into(),
            web_bundles: Vec::new(),
        }],
    };
    let value = serde_json::to_value(&projection).unwrap();
    sqlx::query("INSERT INTO plugin_authorization_snapshots(app_id,version,snapshot,valid_until_ms) VALUES('app',1,$1,$2)").bind(&value).bind(now+60000).execute(pool.as_ref()).await.unwrap();
    let authorization = PostgresPluginAuthorization::new(pool.clone()).with_cipher(cipher);
    let healthy = authorization.event_directory("app").await.unwrap();
    assert_eq!(healthy.subscriptions.len(), 1);
    assert_eq!(healthy.routes.unwrap().plugins.len(), 1);
    // A local registry entry can lag the desired control projection. Keep the
    // independent healthy subscriber deliverable while retaining the unknown binding.
    let mut pending_projection = projection.clone();
    let mut pending = projection.plugins[0].clone();
    pending.grant.grant_id = "pending-grant".into();
    pending.grant.plugin_id = "pending".into();
    pending.grant.manifest_digest = digest_bytes(b"temporarily missing manifest");
    pending_projection.plugins.push(pending.clone());
    sqlx::query("UPDATE plugin_authorization_snapshots SET snapshot=$1")
        .bind(serde_json::to_value(&pending_projection).unwrap())
        .execute(pool.as_ref())
        .await
        .unwrap();
    let missing_version_directory = authorization.event_directory("app").await;
    sqlx::query("INSERT INTO plugin_versions(plugin_id,plugin_version,display_name,publisher_id,manifest_json,manifest_digest,key_id,signature,kind) VALUES('pending','1.0.0','Pending','test','{}',$1,'test','test','business')")
        .bind(&pending.grant.manifest_digest).execute(pool.as_ref()).await.unwrap();
    let invalid_manifest_directory = authorization.event_directory("app").await;
    pending_projection.plugins[1].grant.enabled = false;
    sqlx::query("UPDATE plugin_authorization_snapshots SET snapshot=$1")
        .bind(serde_json::to_value(&pending_projection).unwrap())
        .execute(pool.as_ref())
        .await
        .unwrap();
    let disabled_pending_directory = authorization.event_directory("app").await;
    sqlx::query("UPDATE plugin_authorization_snapshots SET snapshot=$1")
        .bind(&value)
        .execute(pool.as_ref())
        .await
        .unwrap();
    sqlx::query("UPDATE plugin_deployments SET healthy_until_ms=0")
        .execute(pool.as_ref())
        .await
        .unwrap();
    sqlx::query("UPDATE plugin_registry_revisions SET revision=revision+1")
        .execute(pool.as_ref())
        .await
        .unwrap();
    let unhealthy = authorization.event_directory("app").await.unwrap();
    assert_eq!(unhealthy.subscriptions.len(), 1);
    assert!(
        unhealthy.routes.unwrap().plugins.is_empty(),
        "health expiry retains the desired subscription"
    );
    let mut expired = projection.clone();
    expired.issued_at_ms = now - 1000;
    expired.valid_until_ms = now - 1;
    expired.plugins[0].grant.valid_from_ms = now - 1000;
    expired.plugins[0].grant.valid_until_ms = now - 1;
    sqlx::query("UPDATE plugin_authorization_snapshots SET snapshot=$1,valid_until_ms=$2")
        .bind(serde_json::to_value(expired).unwrap())
        .bind(now - 1)
        .execute(pool.as_ref())
        .await
        .unwrap();
    let stale = authorization.event_directory("app").await.unwrap();
    assert_eq!(stale.subscriptions.len(), 1);
    assert!(
        stale.routes.is_none(),
        "projection expiry retains the desired subscription"
    );
    sqlx::query("UPDATE plugin_deployments SET healthy_until_ms=$1")
        .bind(now + 60000)
        .execute(pool.as_ref())
        .await
        .unwrap();
    sqlx::query("UPDATE plugin_authorization_snapshots SET snapshot=$1,valid_until_ms=$2")
        .bind(value)
        .bind(now + 60000)
        .execute(pool.as_ref())
        .await
        .unwrap();
    assert_eq!(
        authorization
            .event_directory("app")
            .await
            .unwrap()
            .routes
            .unwrap()
            .plugins
            .len(),
        1
    );
    sqlx::query("UPDATE plugin_app_states SET enabled=false")
        .execute(pool.as_ref())
        .await
        .unwrap();
    let mut closed = projection.clone();
    closed.plugins[0].grant.enabled = false;
    sqlx::query("UPDATE plugin_authorization_snapshots SET snapshot=$1")
        .bind(serde_json::to_value(closed).unwrap())
        .execute(pool.as_ref())
        .await
        .unwrap();
    assert!(
        authorization
            .event_directory("app")
            .await
            .unwrap()
            .subscriptions
            .is_empty(),
        "an explicit stop removes the subscription"
    );
    pool.close().await;
    sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
    for (cause, directory) in [
        ("missing version", missing_version_directory),
        ("invalid manifest", invalid_manifest_directory),
    ] {
        let directory = directory.unwrap_or_else(|_| {
            panic!("{cause} of one binding must not block the healthy event subscriber")
        });
        assert_eq!(directory.subscriptions.len(), 1);
        assert_eq!(directory.routes.unwrap().plugins.len(), 1);
        assert_eq!(
            directory.unavailable_grants,
            ["pending-grant".into()].into()
        );
    }
    let disabled_pending_directory = disabled_pending_directory.unwrap();
    assert_eq!(disabled_pending_directory.subscriptions.len(), 1);
    assert!(disabled_pending_directory.unavailable_grants.is_empty());
    assert_eq!(
        before, after,
        "real record_delivery with a secret must preserve the accepted event boundary"
    );
    assert_eq!(
        before_secret, after_secret,
        "health renewal must preserve the desired ciphertext"
    );
}

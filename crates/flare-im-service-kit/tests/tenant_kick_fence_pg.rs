//! Focused receiver-side business fencing; owns a new nonce DB, no migration/compatibility gate.
use flare_im_service_kit::tenant_kick_fence::TenantKickFence;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

#[tokio::test]
#[ignore = "requires owned local PostgreSQL at 127.0.0.1:19886"]
async fn late_command_and_projection_are_conditionally_fenced() {
    let options: PgConnectOptions = std::env::var("FLARE_FUNCTIONAL_PG_ADMIN_URL")
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(options.get_host(), "127.0.0.1");
    assert_eq!(options.get_port(), 19886);
    assert_eq!(options.get_username(), "r4route_admin");
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(options.clone())
        .await
        .unwrap();
    let name = format!("flare_kick_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE DATABASE {name}"))
        .execute(&admin)
        .await
        .unwrap();
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect_with(options.database(&name))
        .await
        .unwrap();
    let business_pool = pool.clone();
    let result=tokio::spawn(async move {
        let pool=business_pool;
        sqlx::raw_sql("CREATE TABLE tenants(tenant_id text PRIMARY KEY,status text NOT NULL,version bigint NOT NULL);
            INSERT INTO tenants VALUES('a','suspended',1),('b','suspended',1)").execute(&pool).await.unwrap();
        let fence=TenantKickFence::new(pool.clone());
        assert_eq!(fence.acquire("a",0).await.err().unwrap().code(),tonic::Code::InvalidArgument);
        assert_eq!(fence.acquire("absent",1).await.err().unwrap().code(),tonic::Code::FailedPrecondition);
        assert_eq!(fence.acquire("a",2).await.err().unwrap().code(),tonic::Code::FailedPrecondition,"projection behind is retryable, not superseded");
        // Emulate a command already accepted by transport but still waiting at its receiver.
        let mut renewal=pool.begin().await.unwrap();
        sqlx::query("SELECT tenant_id FROM tenants WHERE tenant_id='a' FOR UPDATE").execute(&mut *renewal).await.unwrap();
        let effects=Arc::new(AtomicUsize::new(0));
        let receiver=fence.clone(); let observed=effects.clone();
        let mut late=tokio::spawn(async move {
            if let Some(lease)=receiver.acquire("a",1).await.unwrap() {
                observed.fetch_add(1,Ordering::SeqCst); lease.finish().await.unwrap(); false
            } else { true }
        });
        assert!(tokio::time::timeout(Duration::from_millis(30),&mut late).await.is_err(),"caller deadline expired; receiver NOT cancelled");
        sqlx::query("UPDATE tenants SET status='active',version=2 WHERE tenant_id='a'").execute(&mut *renewal).await.unwrap(); renewal.commit().await.unwrap();
        assert!(late.await.unwrap(),"late command sees newer generation and is superseded");
        assert_eq!(effects.load(Ordering::SeqCst),0,"no stale session side effect");
        assert!(fence.acquire("a",1).await.unwrap().is_none());
        assert!(fence.acquire("a",2).await.unwrap().is_none(),"active exact generation cannot auto kick");
        // A matching in-progress kick owns a shared fence until its bounded work ends.
        let lease=fence.acquire("b",1).await.unwrap().unwrap();
        assert_eq!(fence.acquire("a",2).await.err().unwrap().code(),tonic::Code::ResourceExhausted,"zero command queue");
        let replacement=pool.clone();
        let mut change=tokio::spawn(async move { sqlx::query("UPDATE tenants SET status='active',version=2 WHERE tenant_id='b'").execute(&replacement).await.unwrap(); });
        assert!(tokio::time::timeout(Duration::from_millis(30),&mut change).await.is_err());
        lease.finish().await.unwrap(); change.await.unwrap();
        assert!(fence.acquire("b",1).await.unwrap().is_none());
    }).await;
    pool.close().await;
    // Only this fresh nonce database belongs to the test. PostgreSQL may still be
    // processing a just-closed client; FORCE here cannot affect another scope.
    sqlx::query(&format!("DROP DATABASE {name} WITH (FORCE)"))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
    if let Err(error) = result {
        std::panic::resume_unwind(error.into_panic());
    }
}

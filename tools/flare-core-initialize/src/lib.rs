//! Generic Core communication database initialization. No product directories/policy.
use flare_server_core::error::{FlareError, Result};
use sha2::{Digest, Sha256};
use sqlx::{Connection, PgConnection};
mod model;
mod upgrade;
pub use upgrade::upgrade;
pub const SQL: &str = include_str!("../../../initial/schema.sql");
pub const SEEDS: &str = include_str!("../../../initial/seeds.sql");
pub fn digest() -> String {
    let mut value = Sha256::new();
    value.update(SQL);
    value.update(b"\0");
    value.update(SEEDS);
    format!("{:x}", value.finalize())
}

/// Refuses with `message` unless `condition` holds.
pub fn ensure(condition: bool, message: &str) -> Result<()> {
    if condition {
        Ok(())
    } else {
        Err(FlareError::system(message))
    }
}

pub async fn initialize(connection: &mut PgConnection) -> Result<bool> {
    initialize_sql(connection, SQL).await
}
async fn initialize_sql(connection: &mut PgConnection, sql: &str) -> Result<bool> {
    let mut tx = connection
        .begin()
        .await
        .map_err(|_| FlareError::system("Core initializer transaction unavailable"))?;
    let work=async {
        model::begin_checks(&mut tx).await?;
        let exists:bool=sqlx::query_scalar("SELECT to_regclass('public.core_initial_schema') IS NOT NULL")
            .fetch_one(&mut *tx).await.map_err(|_|FlareError::system("Core initialization state unavailable"))?;
        if exists {
            model::lock_tables(&mut tx).await?;
            model::verify_current(&mut tx).await?;
            return Ok(false);
        }
        let empty:bool=sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM pg_namespace n WHERE nspname NOT IN('public','information_schema') AND nspname NOT LIKE 'pg\\_%' ESCAPE '\\' AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE d.classid='pg_namespace'::regclass AND d.objid=n.oid AND d.deptype='e')) AND NOT EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relkind IN('r','p','v','m','S','f') AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE d.classid='pg_class'::regclass AND d.objid=c.oid AND d.deptype='e')) AND NOT EXISTS(SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='public' AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE d.classid='pg_proc'::regclass AND d.objid=p.oid AND d.deptype='e'))")
            .fetch_one(&mut *tx).await.map_err(|_|FlareError::system("Core empty-database check unavailable"))?;
        ensure(empty,"Core initializer refuses existing data; use a new empty owned database")?;
        // The accepted fresh source creates unqualified public objects. Existing
        // databases never enter this branch; metadata admission keeps pg_catalog
        // first so application functions cannot shadow the catalog verifier.
        sqlx::raw_sql("SET LOCAL search_path=public,pg_catalog,pg_temp;").execute(&mut *tx).await.map_err(|_|FlareError::system("Core initial public schema unavailable"))?;
        sqlx::raw_sql(sql).execute(&mut *tx).await.map_err(|_|FlareError::system("Core initial schema failed; transaction rolled back"))?;
        sqlx::raw_sql(SEEDS).execute(&mut *tx).await.map_err(|_|FlareError::system("Core initial system seed failed; transaction rolled back"))?;
        sqlx::raw_sql("SET LOCAL search_path=pg_catalog,public,pg_temp;").execute(&mut *tx).await.map_err(|_|FlareError::system("Core initial catalog schema unavailable"))?;
        sqlx::query("INSERT INTO public.core_initial_schema(version,schema_sha256) VALUES($1,$2)")
            .bind(model::CURRENT_VERSION).bind(digest()).execute(&mut *tx).await.map_err(|_|FlareError::system("Core initial version failed; transaction rolled back"))?;
        sqlx::query("INSERT INTO public.core_initial_history(to_version,from_version,to_sha256,transition,ddl_sha256) VALUES($1,0,$2,'fresh',$2)")
            .bind(model::CURRENT_VERSION).bind(digest()).execute(&mut *tx).await.map_err(|_|FlareError::system("Core initial provenance failed; transaction rolled back"))?;
        model::verify_current(&mut tx).await?;
        Ok(true)
    }.await;
    match work {
        Ok(created) => {
            tx.commit().await.map_err(|_| {
                FlareError::system(
                    "Core initialization commit unconfirmed; retry explicit initializer",
                )
            })?;
            Ok(created)
        }
        Err(error) => {
            let _ = tx.rollback().await;
            Err(error)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn target_is_generic_and_has_no_implicit_tenant_or_grant() {
        assert!(!SQL.contains("INSERT INTO tenants"));
        assert!(!SQL.contains("INSERT INTO capability_tenant_switches"));
        assert!(!SQL.contains("platform_iam"));
        assert!(!SQL.contains("flare_friendships"));
        assert!(!SQL.contains("_sqlx_migrations"));
        assert!(!SQL.contains("DEFAULT '0'"));
        assert!(SQL.contains("CREATE TABLE core_audit_outbox"));
        assert!(SQL.contains("CREATE TABLE online_kick_commands"));
        assert!(SQL.contains("sample_weight integer NOT NULL DEFAULT 1"));
        assert!(SQL.contains("CREATE TABLE IF NOT EXISTS plugin_web_bundles"));
        assert!(SQL.contains("CHECK(version=4)"));
        assert_eq!(digest().len(), 64);
    }
    #[tokio::test]
    #[ignore = "requires a fresh owned actual TimescaleDB Core initial fixture"]
    async fn actual_timescale_initial_rollback_and_runtime() {
        use std::str::FromStr;
        let url = std::env::var("FLARE_CORE_INITIAL_TEST_URL")
            .expect("owned TimescaleDB fixture required");
        let mut admin = PgConnection::connect(&url).await.unwrap();
        let database:String=sqlx::query_scalar("SELECT current_database() WHERE current_database() LIKE 'flare_core_initial_test_%' AND shobj_description((SELECT oid FROM pg_database WHERE datname=current_database()),'pg_database')='flare-core-initial-owned/v1'")
            .fetch_one(&mut admin).await.unwrap();
        assert!(database.starts_with("flare_core_initial_test_"));
        assert!(initialize(&mut admin).await.is_err());
        let options = sqlx::postgres::PgConnectOptions::from_str(&url).unwrap();
        let mut owner = PgConnection::connect_with(&options.clone().username("core_initialize"))
            .await
            .unwrap();
        sqlx::query("CREATE TABLE public.keep_me(value int)")
            .execute(&mut owner)
            .await
            .unwrap();
        assert!(initialize(&mut owner).await.is_err());
        assert!(
            sqlx::query_scalar::<_, bool>("SELECT to_regclass('public.keep_me') IS NOT NULL")
                .fetch_one(&mut owner)
                .await
                .unwrap()
        );
        sqlx::query("DROP TABLE public.keep_me")
            .execute(&mut owner)
            .await
            .unwrap();
        assert!(
            initialize_sql(&mut owner, &format!("{SQL}\nSELECT 1/0;"))
                .await
                .is_err()
        );
        assert!(
            sqlx::query_scalar::<_, bool>("SELECT to_regclass('public.messages') IS NULL")
                .fetch_one(&mut owner)
                .await
                .unwrap()
        );
        assert!(initialize(&mut owner).await.unwrap());
        assert!(!initialize(&mut owner).await.unwrap());
        let mut runtime = PgConnection::connect_with(&options.username("core_runtime"))
            .await
            .unwrap();
        for table in [
            "tenants",
            "messages",
            "capability_tenant_switches",
            "core_audit_outbox",
            "online_kick_commands",
            "plugin_entity_lifecycle_outbox",
        ] {
            assert_eq!(
                sqlx::query_scalar::<_, i64>(&format!("SELECT count(*) FROM public.{table}"))
                    .fetch_one(&mut runtime)
                    .await
                    .unwrap(),
                0
            );
        }
        assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM timescaledb_information.hypertables WHERE hypertable_name='messages'").fetch_one(&mut runtime).await.unwrap(),1);
        // Actual hypertable chunk creation/DML privileges; NOT a message/ACK,
        // tenant authorization or durability proof through the communication API.
        sqlx::query("INSERT INTO public.messages(tenant_id,server_id,client_msg_id,conversation_id,sender_id,seq) VALUES('fixture','schema-only-message','schema-only-client','schema-only-conversation','schema-only-user',1)")
            .execute(&mut runtime).await.unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM public.messages")
                .fetch_one(&mut runtime)
                .await
                .unwrap(),
            1
        );
        assert!(
            sqlx::query("CREATE TABLE public.not_allowed(value int)")
                .execute(&mut runtime)
                .await
                .is_err()
        );
        assert!(
            sqlx::query("UPDATE public.core_initial_schema SET initialized_at=clock_timestamp()")
                .execute(&mut runtime)
                .await
                .is_err()
        );
        assert!(initialize(&mut runtime).await.is_err());
        runtime.close().await.unwrap();
        owner.close().await.unwrap();
        admin.close().await.unwrap();
    }
}

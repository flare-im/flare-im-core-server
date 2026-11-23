//! Explicit accepted initial v1 -> v2 -> v3 -> v4 transitions; no SQLx history adoption.
use crate::{digest, ensure, model};
use flare_server_core::error::{FlareError, Result};
use sqlx::{Connection, PgConnection};
/// Returns true only when this transaction commits the accepted forward DDL.
pub async fn upgrade(connection: &mut PgConnection) -> Result<bool> {
    upgrade_with(
        connection,
        model::V2_UPGRADE_SQL,
        model::V3_UPGRADE_SQL,
        model::V4_UPGRADE_SQL,
    )
    .await
}
/// Applies one accepted step: the reviewed DDL, the marker it advances and the
/// history row recording the old source, target source and exact DDL hashes.
async fn step(
    tx: &mut PgConnection,
    ddl: &str,
    reviewed: &str,
    (from, to): (i16, i16),
    (old, target): (&str, &str),
    transition: &str,
) -> Result<()> {
    sqlx::query("SELECT set_config('flare.core_initial_target_sha256',$1,true)")
        .bind(target)
        .execute(&mut *tx)
        .await
        .map_err(|_| FlareError::system("Core upgrade target unavailable"))?;
    sqlx::raw_sql(ddl).execute(&mut *tx).await.map_err(|_| {
        FlareError::system("Core initial upgrade DDL failed; transaction rolled back")
    })?;
    sqlx::query("INSERT INTO public.core_initial_history(to_version,from_version,from_sha256,to_sha256,transition,ddl_sha256) VALUES($1,$2,$3,$4,$5,$6)")
        .bind(to)
        .bind(from)
        .bind(old)
        .bind(target)
        .bind(transition)
        .bind(model::sha(reviewed))
        .execute(&mut *tx)
        .await
        .map_err(|_| FlareError::system("Core upgrade provenance failed; transaction rolled back"))?;
    Ok(())
}
async fn upgrade_with(
    connection: &mut PgConnection,
    v2_ddl: &str,
    v3_ddl: &str,
    v4_ddl: &str,
) -> Result<bool> {
    let mut tx = connection
        .begin()
        .await
        .map_err(|_| FlareError::system("Core upgrade transaction unavailable"))?;
    let work = async {
        model::begin_checks(&mut tx).await?;
        model::lock_tables(&mut tx).await?;
        let (version, marker) = model::marker(&mut tx).await?;
        if version == model::CURRENT_VERSION {
            model::verify_current(&mut tx).await?;
            return Ok(false);
        }
        if version == 1 {
            ensure(
                marker == model::V1_DIGEST,
                "Core initial upgrade baseline is not accepted",
            )?;
            model::shape(&mut tx, 1).await?;
            step(
                &mut tx,
                v2_ddl,
                model::V2_UPGRADE_SQL,
                (1, 2),
                (model::V1_DIGEST, model::V2_DIGEST),
                model::V2_TRANSITION,
            )
            .await?;
        } else {
            ensure(
                version == 2 || version == 3,
                "Core initial upgrade baseline is not accepted",
            )?;
        }
        if version < 3 {
            // Every path passes through the exact released v2 model before v3.
            model::verify_v2(&mut tx).await?;
            step(
                &mut tx,
                v3_ddl,
                model::V3_UPGRADE_SQL,
                (2, 3),
                (model::V2_DIGEST, model::V3_DIGEST),
                model::V3_TRANSITION,
            )
            .await?;
        }
        // And through the exact released v3 model before v4.
        model::verify_v3(&mut tx).await?;
        step(
            &mut tx,
            v4_ddl,
            model::V4_UPGRADE_SQL,
            (3, 4),
            (model::V3_DIGEST, &digest()),
            model::V4_TRANSITION,
        )
        .await?;
        model::verify_current(&mut tx).await?;
        Ok(true)
    }
    .await;
    match work {
        Ok(changed) => {
            tx.commit().await.map_err(|_|FlareError::system("Core upgrade commit unconfirmed; inspect and repeat the identical explicit command"))?;
            Ok(changed)
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
    use std::str::FromStr;
    #[test]
    fn archived_source_and_upgrade_are_explicit() {
        assert_eq!(model::v1_digest(), model::V1_DIGEST);
        assert_eq!(model::v2_digest(), model::V2_DIGEST);
        assert_eq!(model::v3_digest(), model::V3_DIGEST);
        assert_ne!(digest(), model::V1_DIGEST);
        assert_ne!(digest(), model::V2_DIGEST);
        assert_ne!(digest(), model::V3_DIGEST);
        assert!(model::V2_UPGRADE_SQL.contains("WHERE version = 1"));
        assert!(model::V3_UPGRADE_SQL.contains("WHERE version = 2"));
        assert!(model::V3_UPGRADE_SQL.contains("CREATE TABLE public.plugin_web_bundles"));
        assert!(!model::V2_SQL.contains("plugin_web_bundles"));
        assert!(model::V4_UPGRADE_SQL.contains("WHERE version = 3"));
        // The writer claims its send receipt per conversation (ON CONFLICT on these columns).
        let key = "PRIMARY KEY (tenant_id, sender_id, conversation_id, client_msg_id)";
        assert!(model::V4_UPGRADE_SQL.contains(key) && crate::SQL.contains(key));
        assert!(!model::V3_SQL.contains(key));
        for ddl in [
            model::V2_UPGRADE_SQL,
            model::V3_UPGRADE_SQL,
            model::V4_UPGRADE_SQL,
        ] {
            assert!(!ddl.contains("_sqlx_migrations"));
        }
    }
    #[tokio::test]
    #[ignore = "requires an owned actual TimescaleDB Core initial upgrade fixture"]
    async fn initial_upgrade_validates_source_authority_and_commits_atomically() {
        let url = std::env::var("FLARE_CORE_INITIAL_UPGRADE_TEST_URL")
            .expect("owned TimescaleDB fixture required");
        let mut admin = PgConnection::connect(&url).await.unwrap();
        let database:String=sqlx::query_scalar("SELECT current_database() WHERE current_database() LIKE 'flare_core_initial_test_%' AND shobj_description((SELECT oid FROM pg_database WHERE datname=current_database()),'pg_database')='flare-core-initial-owned/v1'").fetch_one(&mut admin).await.unwrap();
        assert!(database.starts_with("flare_core_initial_test_"));
        assert!(upgrade(&mut admin).await.is_err());
        let options = sqlx::postgres::PgConnectOptions::from_str(&url).unwrap();
        let mut owner = PgConnection::connect_with(&options.clone().username("core_initialize"))
            .await
            .unwrap();
        sqlx::raw_sql(model::V1_SQL)
            .execute(&mut owner)
            .await
            .unwrap();
        sqlx::raw_sql(model::V1_SEEDS)
            .execute(&mut owner)
            .await
            .unwrap();
        sqlx::query("INSERT INTO public.core_initial_schema(version,schema_sha256) VALUES(1,$1)")
            .bind(model::V1_DIGEST)
            .execute(&mut owner)
            .await
            .unwrap();
        sqlx::query("INSERT INTO public.media_assets(tenant_id,file_id,owner_id,file_name,mime_type,file_size,url,metadata) VALUES('fixture','preserved','actual-user','fixture','text/plain',1,'','{\"owner_kind\":\"application\",\"purpose\":\"avatar\"}')").execute(&mut owner).await.unwrap();
        sqlx::query(
            "UPDATE public.capability_service_settings SET global_enabled=false WHERE id=1",
        )
        .execute(&mut owner)
        .await
        .unwrap();
        let original: serde_json::Value = sqlx::query_scalar(
            "SELECT to_jsonb(a) FROM public.media_assets a WHERE file_id='preserved'",
        )
        .fetch_one(&mut owner)
        .await
        .unwrap();
        assert!(crate::initialize(&mut owner).await.is_err());
        let db = database.replace('"', "\"\"");
        sqlx::raw_sql(&format!(
            "GRANT CREATE ON DATABASE \"{db}\" TO core_runtime"
        ))
        .execute(&mut admin)
        .await
        .unwrap();
        assert!(upgrade(&mut owner).await.is_err());
        sqlx::raw_sql(&format!(
            "REVOKE CREATE ON DATABASE \"{db}\" FROM core_runtime"
        ))
        .execute(&mut admin)
        .await
        .unwrap();
        assert!(
            !sqlx::query_scalar::<_, bool>(
                "SELECT has_database_privilege('core_runtime',current_database(),'CREATE')"
            )
            .fetch_one(&mut owner)
            .await
            .unwrap()
        );
        // Only the cluster administrator owns the accepted extensions. The
        // initializer cannot hide application relations as extension members.
        for extension in ["timescaledb", "plpgsql"] {
            let denied = sqlx::raw_sql(&format!(
                "ALTER EXTENSION {extension} ADD TABLE public.media_assets"
            ))
            .execute(&mut owner)
            .await
            .unwrap_err();
            assert_eq!(
                denied.as_database_error().and_then(|e| e.code()).as_deref(),
                Some("42501")
            );
        }
        // A newly installed trusted extension is itself outside the reviewed
        // inventory, even when its owner adds a table that member filters omit.
        sqlx::raw_sql("CREATE EXTENSION pg_trgm; CREATE TABLE public.hidden_upgrade(value int); ALTER EXTENSION pg_trgm ADD TABLE public.hidden_upgrade;").execute(&mut owner).await.unwrap();
        assert!(upgrade(&mut owner).await.is_err());
        sqlx::raw_sql("DROP EXTENSION pg_trgm CASCADE;")
            .execute(&mut owner)
            .await
            .unwrap();
        sqlx::query("UPDATE public.core_initial_schema SET schema_sha256=repeat('0',64)")
            .execute(&mut owner)
            .await
            .unwrap();
        assert!(upgrade(&mut owner).await.is_err());
        sqlx::query("UPDATE public.core_initial_schema SET schema_sha256=$1")
            .bind(model::V1_DIGEST)
            .execute(&mut owner)
            .await
            .unwrap();
        sqlx::query("CREATE INDEX unreviewed ON public.media_assets(file_name)")
            .execute(&mut owner)
            .await
            .unwrap();
        assert!(upgrade(&mut owner).await.is_err());
        sqlx::query("DROP INDEX public.unreviewed")
            .execute(&mut owner)
            .await
            .unwrap();
        let mut drift = owner.begin().await.unwrap();
        sqlx::query("GRANT UPDATE(schema_sha256) ON public.core_initial_schema TO core_runtime")
            .execute(&mut *drift)
            .await
            .unwrap();
        assert!(upgrade(&mut drift).await.is_err());
        drift.rollback().await.unwrap();
        sqlx::query("GRANT USAGE ON SCHEMA _timescaledb_catalog TO core_runtime")
            .execute(&mut admin)
            .await
            .unwrap();
        assert!(upgrade(&mut owner).await.is_err());
        sqlx::query("REVOKE USAGE ON SCHEMA _timescaledb_catalog FROM core_runtime")
            .execute(&mut admin)
            .await
            .unwrap();
        sqlx::query("ALTER TABLE public.media_assets OWNER TO core_runtime")
            .execute(&mut admin)
            .await
            .unwrap();
        assert!(upgrade(&mut owner).await.is_err());
        sqlx::query("ALTER TABLE public.media_assets OWNER TO core_initialize")
            .execute(&mut admin)
            .await
            .unwrap();
        // OWNER changes rewrite ACL grantors; restore the canonical effective ACL.
        sqlx::raw_sql("REVOKE ALL ON public.media_assets FROM core_runtime; GRANT SELECT,INSERT,UPDATE,DELETE ON public.media_assets TO core_runtime;").execute(&mut owner).await.unwrap();
        assert!(
            upgrade_with(
                &mut owner,
                &format!("{}\nSELECT 1/0;", model::V2_UPGRADE_SQL),
                model::V3_UPGRADE_SQL,
                model::V4_UPGRADE_SQL
            )
            .await
            .is_err()
        );
        assert_eq!(
            model::marker(&mut owner).await.unwrap(),
            (1, model::V1_DIGEST.to_owned())
        );
        assert!(sqlx::query_scalar::<_,bool>("SELECT to_regclass('public.core_initial_history') IS NULL AND NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='media_assets' AND column_name='owner_kind')").fetch_one(&mut owner).await.unwrap());
        let mut second = PgConnection::connect_with(&options.clone().username("core_initialize"))
            .await
            .unwrap();
        let (a, b) = tokio::join!(upgrade(&mut owner), upgrade(&mut second));
        assert_eq!(usize::from(a.unwrap()) + usize::from(b.unwrap()), 1);
        let history: serde_json::Value = sqlx::query_scalar(
            "SELECT jsonb_agg(to_jsonb(h) ORDER BY to_version) FROM public.core_initial_history h",
        )
        .fetch_one(&mut owner)
        .await
        .unwrap();
        assert!(!upgrade(&mut owner).await.unwrap());
        assert!(!crate::initialize(&mut owner).await.unwrap());
        let replay: serde_json::Value = sqlx::query_scalar(
            "SELECT jsonb_agg(to_jsonb(h) ORDER BY to_version) FROM public.core_initial_history h",
        )
        .fetch_one(&mut owner)
        .await
        .unwrap();
        assert_eq!(history, replay);
        let retained:serde_json::Value=sqlx::query_scalar("SELECT to_jsonb(a)-'owner_kind'-'purpose' FROM public.media_assets a WHERE file_id='preserved'").fetch_one(&mut owner).await.unwrap();
        assert_eq!(retained, original);
        assert_eq!(sqlx::query_scalar::<_,String>("SELECT owner_kind||'|'||purpose||'|'||owner_id FROM public.media_assets WHERE file_id='preserved'").fetch_one(&mut owner).await.unwrap(),"user|general|actual-user");
        assert!(
            !sqlx::query_scalar::<_, bool>(
                "SELECT global_enabled FROM public.capability_service_settings WHERE id=1"
            )
            .fetch_one(&mut owner)
            .await
            .unwrap()
        );
        let mut runtime = PgConnection::connect_with(&options.username("core_runtime"))
            .await
            .unwrap();
        assert!(upgrade(&mut runtime).await.is_err());
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM public.core_initial_history")
                .fetch_one(&mut runtime)
                .await
                .unwrap(),
            3
        );
        assert_eq!(
            model::marker(&mut owner).await.unwrap(),
            (model::CURRENT_VERSION, digest())
        );
        assert!(
            sqlx::query("UPDATE public.core_initial_history SET applied_at=clock_timestamp()")
                .execute(&mut runtime)
                .await
                .is_err()
        );
        assert!(sqlx::query("INSERT INTO public.media_assets(tenant_id,file_id,owner_id,file_name,mime_type,file_size,url,owner_kind,purpose) VALUES('fixture','bad','other','f','image/png',1,'','application','avatar')").execute(&mut runtime).await.is_err());
        sqlx::query("UPDATE public.core_initial_history SET ddl_sha256=repeat('0',64)")
            .execute(&mut owner)
            .await
            .unwrap();
        assert!(upgrade(&mut owner).await.is_err());
        assert!(crate::initialize(&mut owner).await.is_err());
        runtime.close().await.unwrap();
        second.close().await.unwrap();
        owner.close().await.unwrap();
        admin.close().await.unwrap();
    }
    /// A released v3 database upgrades to v4 in one step: the ledger keys a send receipt per
    /// conversation (the writer's `ON CONFLICT` target) and keeps the receipts it recovers.
    #[tokio::test]
    #[ignore = "requires an owned actual TimescaleDB Core initial upgrade fixture"]
    async fn released_v3_upgrades_to_v4_with_conversation_scoped_receipts() {
        let url = std::env::var("FLARE_CORE_INITIAL_V3_UPGRADE_TEST_URL")
            .expect("owned TimescaleDB fixture required");
        let mut admin = PgConnection::connect(&url).await.unwrap();
        let database:String=sqlx::query_scalar("SELECT current_database() WHERE current_database() LIKE 'flare_core_initial_test_%' AND shobj_description((SELECT oid FROM pg_database WHERE datname=current_database()),'pg_database')='flare-core-initial-owned/v1'").fetch_one(&mut admin).await.unwrap();
        assert!(database.starts_with("flare_core_initial_test_"));
        let options = sqlx::postgres::PgConnectOptions::from_str(&url).unwrap();
        let mut owner = PgConnection::connect_with(&options.clone().username("core_initialize"))
            .await
            .unwrap();
        // The released v3 fresh initialization, exactly as the v3 binary ran it.
        sqlx::raw_sql(&format!(
            "BEGIN; SET LOCAL search_path=public,pg_catalog,pg_temp;\n{}\n{}\nSET LOCAL search_path=pg_catalog,public,pg_temp; INSERT INTO public.core_initial_schema(version,schema_sha256) VALUES(3,'{d}'); INSERT INTO public.core_initial_history(to_version,from_version,to_sha256,transition,ddl_sha256) VALUES(3,0,'{d}','fresh','{d}'); COMMIT;",
            model::V3_SQL,
            model::V3_SEEDS,
            d = model::V3_DIGEST
        ))
        .execute(&mut owner)
        .await
        .unwrap();
        sqlx::query("INSERT INTO public.messages(tenant_id,server_id,client_msg_id,conversation_id,sender_id,seq) VALUES('fixture','kept','cli-reused','conversation-a','sender',1)")
            .execute(&mut owner).await.unwrap();
        assert!(crate::initialize(&mut owner).await.is_err());
        assert!(upgrade(&mut owner).await.unwrap());
        assert!(!upgrade(&mut owner).await.unwrap());
        assert!(!crate::initialize(&mut owner).await.unwrap());
        assert_eq!(
            model::marker(&mut owner).await.unwrap(),
            (model::CURRENT_VERSION, digest())
        );
        let history: Vec<(i16, i16, String)> = sqlx::query_as(
            "SELECT to_version,from_version,transition FROM public.core_initial_history ORDER BY to_version",
        )
        .fetch_all(&mut owner)
        .await
        .unwrap();
        assert_eq!(
            history,
            vec![
                (3, 0, "fresh".to_owned()),
                (4, 3, model::V4_TRANSITION.to_owned())
            ]
        );
        // The writer's claim: the same client id is a new receipt in another conversation and a
        // conflict in the one that already holds it (here the recovered receipt).
        let mut runtime = PgConnection::connect_with(&options.username("core_runtime"))
            .await
            .unwrap();
        let claim = |conversation: &'static str, server: &'static str| {
            sqlx::query_scalar::<sqlx::Postgres, String>("INSERT INTO message_idempotency_ledger(tenant_id,sender_id,client_msg_id,conversation_id,server_id,conversation_seq,durability,created_at) VALUES('fixture','sender','cli-reused',$1,$2,1,4,CURRENT_TIMESTAMP) ON CONFLICT (tenant_id, sender_id, conversation_id, client_msg_id) DO NOTHING RETURNING server_id")
                .bind(conversation)
                .bind(server)
        };
        assert_eq!(
            claim("conversation-b", "new")
                .fetch_all(&mut runtime)
                .await
                .unwrap(),
            ["new"]
        );
        assert!(
            claim("conversation-a", "again")
                .fetch_all(&mut runtime)
                .await
                .unwrap()
                .is_empty()
        );
        runtime.close().await.unwrap();
        owner.close().await.unwrap();
        admin.close().await.unwrap();
    }
    /// A database initialized fresh with the released v2 model upgrades to v3
    /// in one step; a failing v3 DDL leaves the v2 marker, history and shape.
    #[tokio::test]
    #[ignore = "requires an owned actual TimescaleDB Core initial upgrade fixture"]
    async fn released_v2_upgrades_to_v3_atomically() {
        let url = std::env::var("FLARE_CORE_INITIAL_V2_UPGRADE_TEST_URL")
            .expect("owned TimescaleDB fixture required");
        let mut admin = PgConnection::connect(&url).await.unwrap();
        let database:String=sqlx::query_scalar("SELECT current_database() WHERE current_database() LIKE 'flare_core_initial_test_%' AND shobj_description((SELECT oid FROM pg_database WHERE datname=current_database()),'pg_database')='flare-core-initial-owned/v1'").fetch_one(&mut admin).await.unwrap();
        assert!(database.starts_with("flare_core_initial_test_"));
        let options = sqlx::postgres::PgConnectOptions::from_str(&url).unwrap();
        let mut owner = PgConnection::connect_with(&options.clone().username("core_initialize"))
            .await
            .unwrap();
        // The released v2 fresh initialization, exactly as the v2 binary ran it.
        sqlx::raw_sql(&format!(
            "BEGIN; SET LOCAL search_path=public,pg_catalog,pg_temp;\n{}\n{}\nSET LOCAL search_path=pg_catalog,public,pg_temp; INSERT INTO public.core_initial_schema(version,schema_sha256) VALUES(2,'{d}'); INSERT INTO public.core_initial_history(to_version,from_version,to_sha256,transition,ddl_sha256) VALUES(2,0,'{d}','fresh','{d}'); COMMIT;",
            model::V2_SQL,
            model::V2_SEEDS,
            d = model::V2_DIGEST
        ))
        .execute(&mut owner)
        .await
        .unwrap();
        sqlx::query("INSERT INTO public.media_assets(tenant_id,file_id,owner_id,file_name,mime_type,file_size,url) VALUES('fixture','kept','actual-user','fixture','text/plain',1,'')").execute(&mut owner).await.unwrap();
        // Runtime and a fresh initialize never advance a released v2 database.
        let mut runtime = PgConnection::connect_with(&options.clone().username("core_runtime"))
            .await
            .unwrap();
        assert!(upgrade(&mut runtime).await.is_err());
        assert!(crate::initialize(&mut owner).await.is_err());
        // Extra objects outside the released v2 descriptor are refused.
        sqlx::query("CREATE INDEX unreviewed ON public.media_assets(file_name)")
            .execute(&mut owner)
            .await
            .unwrap();
        assert!(upgrade(&mut owner).await.is_err());
        sqlx::query("DROP INDEX public.unreviewed")
            .execute(&mut owner)
            .await
            .unwrap();
        assert!(
            upgrade_with(
                &mut owner,
                model::V2_UPGRADE_SQL,
                &format!("{}\nSELECT 1/0;", model::V3_UPGRADE_SQL),
                model::V4_UPGRADE_SQL
            )
            .await
            .is_err()
        );
        assert_eq!(
            model::marker(&mut owner).await.unwrap(),
            (2, model::V2_DIGEST.to_owned())
        );
        assert!(
            sqlx::query_scalar::<_, bool>(
                "SELECT to_regclass('public.plugin_web_bundles') IS NULL"
            )
            .fetch_one(&mut owner)
            .await
            .unwrap()
        );
        assert!(upgrade(&mut owner).await.unwrap());
        assert!(!upgrade(&mut owner).await.unwrap());
        assert!(!crate::initialize(&mut owner).await.unwrap());
        assert_eq!(
            model::marker(&mut owner).await.unwrap(),
            (model::CURRENT_VERSION, digest())
        );
        let history: Vec<(i16, i16, String)> = sqlx::query_as(
            "SELECT to_version,from_version,transition FROM public.core_initial_history ORDER BY to_version",
        )
        .fetch_all(&mut owner)
        .await
        .unwrap();
        assert_eq!(
            history,
            vec![
                (2, 0, "fresh".to_owned()),
                (3, 2, model::V3_TRANSITION.to_owned()),
                (4, 3, model::V4_TRANSITION.to_owned())
            ]
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM public.media_assets WHERE file_id='kept'"
            )
            .fetch_one(&mut runtime)
            .await
            .unwrap(),
            1
        );
        // Runtime can use the new table and still cannot touch the provenance.
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM public.plugin_web_bundles")
                .fetch_one(&mut runtime)
                .await
                .unwrap(),
            0
        );
        assert!(
            sqlx::query("DELETE FROM public.core_initial_history")
                .execute(&mut runtime)
                .await
                .is_err()
        );
        runtime.close().await.unwrap();
        owner.close().await.unwrap();
        admin.close().await.unwrap();
    }
}

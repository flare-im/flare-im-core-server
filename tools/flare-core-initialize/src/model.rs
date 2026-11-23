//! Source-bound initial schema admission and authority checks.
use crate::{SEEDS, SQL, digest, ensure};
use flare_server_core::error::{FlareError, Result};
use sha2::{Digest, Sha256};
use sqlx::PgConnection;
/// The version a fresh initialization creates and every upgrade ends at.
pub const CURRENT_VERSION: i16 = 4;
pub const V1_SQL: &str = include_str!("../../../initial/versions/v1/schema.sql");
pub const V1_SEEDS: &str = include_str!("../../../initial/versions/v1/seeds.sql");
pub const V1_DIGEST: &str = "0ef150419f08a05d966226025d426e7cb2cf6013d976565f0002e9b733275093";
pub const V2_SQL: &str = include_str!("../../../initial/versions/v2/schema.sql");
pub const V2_SEEDS: &str = include_str!("../../../initial/versions/v2/seeds.sql");
pub const V2_DIGEST: &str = "3e882365f055b204d38c8667bed194ee8393d7cf25dfe670fc7401fdba808f4f";
pub const V3_SQL: &str = include_str!("../../../initial/versions/v3/schema.sql");
pub const V3_SEEDS: &str = include_str!("../../../initial/versions/v3/seeds.sql");
pub const V3_DIGEST: &str = "90acd60d209939d9973e484dae2d132fba692ee51f5e92bd44fa09e2e7c06afd";
/// Accepted v1 -> v2 forward DDL (released with v2; never edited).
pub const V2_UPGRADE_SQL: &str =
    include_str!("../../../initial/upgrades/0002_application_media.sql");
/// Accepted v2 -> v3 forward DDL (released with v3; never edited).
pub const V3_UPGRADE_SQL: &str =
    include_str!("../../../initial/upgrades/0003_plugin_web_bundles.sql");
/// Accepted v3 -> v4 forward DDL.
pub const V4_UPGRADE_SQL: &str =
    include_str!("../../../initial/upgrades/0004_conversation_send_idempotency.sql");
pub const V2_TRANSITION: &str = "application-media-v2";
pub const V3_TRANSITION: &str = "plugin-web-bundles-v3";
pub const V4_TRANSITION: &str = "conversation-send-idempotency-v4";
const CATALOG: &str = include_str!("../../../initial/upgrades/catalog.sql");
const ROLES: &str = include_str!("../../../initial/roles.sql");
const DATABASE: &str = include_str!("../../../initial/database.sql");
pub fn sha(value: &str) -> String {
    format!("{:x}", Sha256::digest(value.as_bytes()))
}
fn bundle_digest(schema: &str, seeds: &str) -> String {
    let mut h = Sha256::new();
    h.update(schema);
    h.update(b"\0");
    h.update(seeds);
    format!("{:x}", h.finalize())
}
pub fn v1_digest() -> String {
    bundle_digest(V1_SQL, V1_SEEDS)
}
pub fn v2_digest() -> String {
    bundle_digest(V2_SQL, V2_SEEDS)
}
pub fn v3_digest() -> String {
    bundle_digest(V3_SQL, V3_SEEDS)
}
/// Sources each reviewed descriptor was generated from. Each released descriptor
/// names its own tree as `initial/schema.sql`, archived since under
/// `versions/v<N>`; the v4 descriptors name the current tree.
fn descriptor_sources(version: i16) -> Vec<(&'static str, &'static str)> {
    let mut sources = vec![
        ("initial/versions/v1/schema.sql", V1_SQL),
        ("initial/versions/v1/seeds.sql", V1_SEEDS),
        ("initial/roles.sql", ROLES),
        ("initial/database.sql", DATABASE),
        (
            "initial/upgrades/0002_application_media.sql",
            V2_UPGRADE_SQL,
        ),
        ("initial/upgrades/catalog.sql", CATALOG),
    ];
    if version < 3 {
        sources.extend([
            ("initial/schema.sql", V2_SQL),
            ("initial/seeds.sql", V2_SEEDS),
        ]);
    } else {
        sources.extend([
            ("initial/versions/v2/schema.sql", V2_SQL),
            ("initial/versions/v2/seeds.sql", V2_SEEDS),
            (
                "initial/upgrades/0003_plugin_web_bundles.sql",
                V3_UPGRADE_SQL,
            ),
        ]);
        if version < 4 {
            sources.extend([
                ("initial/schema.sql", V3_SQL),
                ("initial/seeds.sql", V3_SEEDS),
            ]);
        } else {
            sources.extend([
                ("initial/versions/v3/schema.sql", V3_SQL),
                ("initial/versions/v3/seeds.sql", V3_SEEDS),
                ("initial/schema.sql", SQL),
                ("initial/seeds.sql", SEEDS),
                (
                    "initial/upgrades/0004_conversation_send_idempotency.sql",
                    V4_UPGRADE_SQL,
                ),
            ]);
        }
    }
    sources
}
pub async fn begin_checks(connection: &mut PgConnection) -> Result<()> {
    ensure(
        v1_digest() == V1_DIGEST,
        "Core accepted v1 source digest differs",
    )?;
    ensure(
        v2_digest() == V2_DIGEST,
        "Core accepted v2 source digest differs",
    )?;
    ensure(
        v3_digest() == V3_DIGEST,
        "Core accepted v3 source digest differs",
    )?;
    sqlx::raw_sql("SET LOCAL search_path=pg_catalog,public,pg_temp; SET LOCAL lock_timeout='2s'; SET LOCAL statement_timeout='30s'; SELECT pg_advisory_xact_lock(5065494824365673545);")
        .execute(&mut *connection).await.map_err(|_|FlareError::system("Core initializer is busy"))?;
    let allowed:bool=sqlx::query_scalar("SELECT current_user=session_user AND current_user='core_initialize' AND d.datdba=r.oid AND NOT EXISTS(SELECT 1 FROM pg_roles p WHERE p.rolname IN ('core_initialize','core_runtime') AND (p.rolsuper OR p.rolbypassrls OR p.rolcreaterole OR p.rolcreatedb OR p.rolreplication OR NOT p.rolcanlogin OR (p.rolname='core_runtime' AND has_database_privilege(p.oid,current_database(),'CREATE')) OR EXISTS(SELECT 1 FROM pg_auth_members m WHERE m.member=p.oid))) AND EXISTS(SELECT 1 FROM pg_roles WHERE rolname='core_runtime') FROM pg_roles r CROSS JOIN pg_database d WHERE r.rolname=current_user AND d.datname=current_database()")
        .fetch_one(&mut *connection).await.map_err(|_|FlareError::system("Core initializer role check unavailable"))?;
    ensure(
        allowed,
        "Core initializer requires its isolated database-owner login and runtime role",
    )?;
    let extension:bool=sqlx::query_scalar("SELECT current_setting('server_version_num')::int/10000=16 AND EXISTS(SELECT 1 FROM pg_extension e JOIN pg_roles r ON r.oid=e.extowner WHERE e.extname='timescaledb' AND e.extversion IN ('2.29.1','2.30.0') AND r.rolsuper) AND EXISTS(SELECT 1 FROM pg_extension e JOIN pg_roles r ON r.oid=e.extowner JOIN pg_namespace n ON n.oid=e.extnamespace WHERE e.extname='plpgsql' AND n.nspname='pg_catalog' AND r.rolsuper) AND NOT EXISTS(SELECT 1 FROM pg_extension WHERE extname NOT IN ('timescaledb','plpgsql')) AND to_regclass('public._sqlx_migrations') IS NULL AND to_regclass('public.flare_schema_migrations') IS NULL")
        .fetch_one(&mut *connection).await.map_err(|_|FlareError::system("Core initial model check unavailable"))?;
    ensure(
        extension,
        "Core initial model requires reviewed PostgreSQL 16/TimescaleDB and no SQLx migration ledger",
    )
}
pub async fn lock_tables(connection: &mut PgConnection) -> Result<()> {
    let tables:Vec<String>=sqlx::query_scalar("SELECT quote_ident(n.nspname)||'.'||quote_ident(c.relname) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relkind IN ('r','p') AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE d.classid='pg_class'::regclass AND d.objid=c.oid AND d.deptype='e') ORDER BY c.relname")
        .fetch_all(&mut *connection).await.map_err(|_|FlareError::system("Core initial table inventory unavailable"))?;
    for table in tables {
        sqlx::raw_sql(&format!("LOCK TABLE {table} IN SHARE ROW EXCLUSIVE MODE"))
            .execute(&mut *connection)
            .await
            .map_err(|_| FlareError::system("Core initial tables are busy"))?;
    }
    Ok(())
}
pub async fn shape(connection: &mut PgConnection, version: i16) -> Result<()> {
    let ts: String =
        sqlx::query_scalar("SELECT extversion FROM pg_extension WHERE extname='timescaledb'")
            .fetch_one(&mut *connection)
            .await
            .map_err(|_| FlareError::system("Core TimescaleDB version unavailable"))?;
    let text = match (version, ts.as_str()) {
        (1, "2.29.1") => include_str!("../../../initial/upgrades/v1-pg16-ts2.29.1.json"),
        (1, "2.30.0") => include_str!("../../../initial/upgrades/v1-pg16-ts2.30.0.json"),
        (2, "2.29.1") => include_str!("../../../initial/upgrades/v2-pg16-ts2.29.1.json"),
        (2, "2.30.0") => include_str!("../../../initial/upgrades/v2-pg16-ts2.30.0.json"),
        (3, "2.29.1") => include_str!("../../../initial/upgrades/v3-pg16-ts2.29.1.json"),
        (3, "2.30.0") => include_str!("../../../initial/upgrades/v3-pg16-ts2.30.0.json"),
        (4, "2.29.1") => include_str!("../../../initial/upgrades/v4-pg16-ts2.29.1.json"),
        (4, "2.30.0") => include_str!("../../../initial/upgrades/v4-pg16-ts2.30.0.json"),
        _ => {
            return Err(FlareError::system(
                "Core initial descriptor environment is not reviewed",
            ));
        }
    };
    let expected: serde_json::Value = serde_json::from_str(text)
        .map_err(|_| FlareError::system("Core initial descriptor is invalid"))?;
    ensure(
        expected["version"].as_i64() == Some(i64::from(version)),
        "Core initial descriptor version drift",
    )?;
    for (name, value) in descriptor_sources(version) {
        ensure(
            expected["sourceSha256"][name].as_str() == Some(sha(value).as_str()),
            "Core initial descriptor source drift",
        )?;
    }
    let bundle = match version {
        1 => V1_DIGEST.to_owned(),
        2 => V2_DIGEST.to_owned(),
        3 => V3_DIGEST.to_owned(),
        _ => digest(),
    };
    ensure(
        expected["bundleSha256"].as_str() == Some(bundle.as_str()),
        "Core initial descriptor bundle drift",
    )?;
    let actual: serde_json::Value = sqlx::query_scalar(CATALOG)
        .fetch_one(connection)
        .await
        .map_err(|_| FlareError::system("Core initial catalog unavailable"))?;
    ensure(
        actual == expected["objects"],
        "Core initial schema shape, owner, ACL or Timescale settings differ from the accepted source model",
    )
}
pub async fn marker(connection: &mut PgConnection) -> Result<(i16, String)> {
    let rows: Vec<(i16, String)> =
        sqlx::query_as("SELECT version,schema_sha256 FROM public.core_initial_schema FOR UPDATE")
            .fetch_all(connection)
            .await
            .map_err(|_| FlareError::system("Core initial marker unavailable"))?;
    ensure(
        rows.len() == 1,
        "Core initial marker must contain exactly one version",
    )?;
    Ok(rows.into_iter().next().expect("checked singleton"))
}
type HistoryRow = (i16, i16, Option<String>, String, String, String);
async fn history(connection: &mut PgConnection) -> Result<Vec<HistoryRow>> {
    sqlx::query_as("SELECT to_version,from_version,from_sha256,to_sha256,transition,ddl_sha256 FROM public.core_initial_history ORDER BY to_version")
        .fetch_all(connection)
        .await
        .map_err(|_| FlareError::system("Core initial history unavailable"))
}
/// The genuine v2 row: a fresh v2 initialization or the accepted v1 -> v2 upgrade.
fn genuine_v2(row: &HistoryRow) -> bool {
    let (to, from, old, target, transition, ddl) = row;
    *to == 2
        && *target == V2_DIGEST
        && ((*from == 0 && old.is_none() && transition == "fresh" && *ddl == V2_DIGEST)
            || (*from == 1
                && old.as_deref() == Some(V1_DIGEST)
                && transition == V2_TRANSITION
                && *ddl == sha(V2_UPGRADE_SQL)))
}
/// A released v2 database: marker, full descriptor and its single history row.
pub async fn verify_v2(connection: &mut PgConnection) -> Result<()> {
    ensure(
        marker(connection).await? == (2, V2_DIGEST.to_owned()),
        "Core initial upgrade baseline is not accepted",
    )?;
    shape(connection, 2).await?;
    let rows = history(connection).await?;
    ensure(
        rows.len() == 1 && genuine_v2(&rows[0]),
        "Core initial v2 history provenance differs",
    )
}
/// The genuine v3 rows: a fresh v3 initialization, or a genuine v2 and the accepted
/// v2 -> v3 upgrade.
fn genuine_v3(rows: &[HistoryRow]) -> bool {
    let fresh = |row: &HistoryRow| {
        let (to, from, old, target, transition, ddl) = row;
        *to == 3
            && *from == 0
            && old.is_none()
            && *target == V3_DIGEST
            && transition == "fresh"
            && *ddl == V3_DIGEST
    };
    let upgraded = |row: &HistoryRow| {
        let (to, from, old, target, transition, ddl) = row;
        *to == 3
            && *from == 2
            && old.as_deref() == Some(V2_DIGEST)
            && *target == V3_DIGEST
            && transition == V3_TRANSITION
            && *ddl == sha(V3_UPGRADE_SQL)
    };
    match rows {
        [only] => fresh(only),
        [v2, v3] => genuine_v2(v2) && upgraded(v3),
        _ => false,
    }
}
/// A released v3 database: marker, full descriptor and its genuine history.
pub async fn verify_v3(connection: &mut PgConnection) -> Result<()> {
    ensure(
        marker(connection).await? == (3, V3_DIGEST.to_owned()),
        "Core initial upgrade baseline is not accepted",
    )?;
    shape(connection, 3).await?;
    ensure(
        genuine_v3(&history(connection).await?),
        "Core initial v3 history provenance differs",
    )
}
pub async fn verify_current(connection: &mut PgConnection) -> Result<()> {
    ensure(
        marker(connection).await? == (CURRENT_VERSION, digest()),
        "Core initial version differs; accepted v1, v2 or v3 requires the explicit upgrade command",
    )?;
    shape(connection, CURRENT_VERSION).await?;
    let rows = history(connection).await?;
    let fresh = |row: &HistoryRow| {
        let (to, from, old, target, transition, ddl) = row;
        *to == 4
            && *from == 0
            && old.is_none()
            && *target == digest()
            && transition == "fresh"
            && *ddl == digest()
    };
    let upgraded = |row: &HistoryRow| {
        let (to, from, old, target, transition, ddl) = row;
        *to == 4
            && *from == 3
            && old.as_deref() == Some(V3_DIGEST)
            && *target == digest()
            && transition == V4_TRANSITION
            && *ddl == sha(V4_UPGRADE_SQL)
    };
    ensure(
        match rows.as_slice() {
            [only] => fresh(only),
            [earlier @ .., v4] => genuine_v3(earlier) && upgraded(v4),
            _ => false,
        },
        "Core initial history provenance differs",
    )
}

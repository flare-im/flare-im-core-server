//! Opt-in real PostgreSQL concurrency regression: orphan cleanup vs. reference
//! creation. Needs several connections, so it uses a fresh throwaway schema (not
//! TEMP tables) in the disposable database and drops that schema afterwards.
use super::*;
use crate::domain::model::{
    MEDIA_LIFECYCLE_SCOPE_METADATA_KEY, MESSAGE_MEDIA_LIFECYCLE_SCOPE, MediaDomainConfig,
    MediaReferenceScope, UploadContext,
};
use crate::domain::repository::{
    MediaObjectRepository, MetadataStoreRef, ObjectRepositoryRef, ReferenceStoreRef,
};
use crate::domain::service::MediaService;
use flare_server_core::context::Context;
use futures_util::FutureExt;
use std::collections::HashSet;
use std::sync::Mutex;
use std::time::Duration;
use tokio::sync::Notify;

const TENANT: &str = "race-tenant";
const OWNER: &str = "race-owner";

fn ctx() -> Context {
    crate::domain::service::isolation_tests::ctx(TENANT, OWNER)
}

fn scope() -> MediaReferenceScope {
    MediaReferenceScope {
        namespace: "message".to_string(),
        owner_id: "conversation-1".to_string(),
        business_tag: None,
    }
}

/// A pause point: the code under test announces arrival and waits to be released.
#[derive(Default)]
struct Gate {
    entered: Notify,
    resume: Notify,
}

impl Gate {
    async fn pass(&self) {
        self.entered.notify_one();
        self.resume.notified().await;
    }
}

/// In-memory object store; `delete_object` can pause at a gate or fail on demand.
#[derive(Default)]
struct Objects {
    keys: Mutex<HashSet<String>>,
    delete_gate: Option<Arc<Gate>>,
    fail_delete: Mutex<bool>,
}

#[async_trait::async_trait]
impl MediaObjectRepository for Objects {
    async fn put_object(&self, context: &UploadContext<'_>) -> Result<String> {
        Ok(context.file_id.to_string())
    }
    async fn delete_object(&self, object_path: &str) -> Result<()> {
        if let Some(gate) = &self.delete_gate {
            gate.pass().await;
        }
        if *self.fail_delete.lock().unwrap() {
            return Err(flare_server_core::flare_err!(
                ErrorCode::ServiceUnavailable,
                "object store unavailable"
            ));
        }
        self.keys.lock().unwrap().remove(object_path);
        Ok(())
    }
    async fn presign_object(&self, object_path: &str, _expires_in: i64) -> Result<String> {
        Ok(object_path.to_string())
    }
    fn base_url(&self) -> Option<String> {
        None
    }
    fn cdn_base_url(&self) -> Option<String> {
        None
    }
    fn use_presigned_urls(&self) -> bool {
        false
    }
}

/// Real repository whose candidate listing pauses at a gate after reading its snapshot.
struct GatedListing {
    inner: PostgresMetadataStore,
    gate: Arc<Gate>,
}

#[async_trait::async_trait]
impl MediaMetadataStore for GatedListing {
    async fn save_metadata(&self, ctx: &Context, metadata: &MediaFileMetadata) -> Result<()> {
        self.inner.save_metadata(ctx, metadata).await
    }
    async fn load_metadata(&self, ctx: &Context, id: &str) -> Result<Option<MediaFileMetadata>> {
        self.inner.load_metadata(ctx, id).await
    }
    async fn load_by_hash(&self, ctx: &Context, hash: &str) -> Result<Option<MediaFileMetadata>> {
        self.inner.load_by_hash(ctx, hash).await
    }
    async fn delete_metadata(&self, ctx: &Context, id: &str) -> Result<()> {
        self.inner.delete_metadata(ctx, id).await
    }
    async fn list_orphaned_assets(
        &self,
        ctx: &Context,
        before: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<MediaFileMetadata>> {
        let snapshot = self.inner.list_orphaned_assets(ctx, before, limit).await?;
        self.gate.pass().await;
        Ok(snapshot)
    }
    async fn claim_orphaned_asset(
        &self,
        ctx: &Context,
        id: &str,
        before: DateTime<Utc>,
    ) -> Result<Option<Box<dyn OrphanAssetClaim>>> {
        self.inner.claim_orphaned_asset(ctx, id, before).await
    }
    async fn update_status(
        &self,
        ctx: &Context,
        id: &str,
        status: MediaAssetStatus,
        grace: Option<DateTime<Utc>>,
    ) -> Result<()> {
        self.inner.update_status(ctx, id, status, grace).await
    }
}

struct RaceDb {
    url: String,
    schema: String,
    store: PostgresMetadataStore,
    chunks: std::path::PathBuf,
}

impl RaceDb {
    async fn new() -> Self {
        let url = std::env::var("FLARE_MEDIA_TEST_DATABASE_URL")
            .expect("set FLARE_MEDIA_TEST_DATABASE_URL to a disposable PostgreSQL instance");
        let schema = format!("media_race_{}", uuid::Uuid::new_v4().simple());
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .unwrap();
        sqlx::raw_sql(&format!("CREATE SCHEMA {schema}"))
            .execute(&admin)
            .await
            .unwrap();
        admin.close().await;
        let options: sqlx::postgres::PgConnectOptions = url.parse().unwrap();
        let pool = PgPoolOptions::new()
            .max_connections(12)
            .connect_with(options.options([("search_path", schema.as_str())]))
            .await
            .unwrap();
        let source = include_str!("../../../../deploy/init.sql");
        let tables = source
            .split("CREATE TABLE media_assets (")
            .nth(1)
            .unwrap()
            .split("DROP TABLE IF EXISTS ack_archive_records")
            .next()
            .unwrap();
        sqlx::raw_sql(&format!("CREATE TABLE media_assets ({tables}"))
            .execute(&pool)
            .await
            .unwrap();
        // Never touch durable media tables: both must resolve inside the fresh schema.
        let owners: Vec<String> = sqlx::query_scalar(
            "SELECT n.nspname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE c.oid IN ('media_assets'::regclass, 'media_references'::regclass)",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(owners, vec![schema.clone(), schema.clone()]);
        let chunks = std::env::temp_dir().join(format!("flare-media-race-{schema}"));
        Self {
            url,
            schema,
            store: PostgresMetadataStore {
                pool: Arc::new(pool),
            },
            chunks,
        }
    }

    fn service(&self, metadata: MetadataStoreRef, objects: Arc<Objects>) -> Arc<MediaService> {
        Arc::new(MediaService::new(
            Some(objects as ObjectRepositoryRef),
            Some(metadata),
            Some(Arc::new(self.store.clone()) as ReferenceStoreRef),
            None,
            None,
            None,
            MediaDomainConfig::new(3600, None, 60, self.chunks.clone(), 3600, 1024 * 1024),
        ))
    }

    async fn seed(&self, objects: &Objects, file_id: &str) {
        let storage_path = format!("{TENANT}/{file_id}");
        self.store
            .save_metadata(
                &ctx(),
                &MediaFileMetadata {
                    owner_id: Some(OWNER.to_string()),
                    file_id: file_id.to_string(),
                    file_name: "attachment.bin".to_string(),
                    mime_type: "application/octet-stream".to_string(),
                    file_size: 1,
                    status: MediaAssetStatus::Pending,
                    reference_count: 0,
                    grace_expires_at: Some(Utc::now() - chrono::Duration::seconds(30)),
                    storage_path: Some(storage_path.clone()),
                    metadata: HashMap::from([
                        (
                            MEDIA_LIFECYCLE_SCOPE_METADATA_KEY.to_string(),
                            MESSAGE_MEDIA_LIFECYCLE_SCOPE.to_string(),
                        ),
                        (
                            crate::domain::model::STORAGE_PATH_METADATA_KEY.to_string(),
                            storage_path.clone(),
                        ),
                    ]),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        objects.keys.lock().unwrap().insert(storage_path);
    }

    /// (metadata row present, reference rows) for one asset.
    async fn state(&self, file_id: &str) -> (bool, i64) {
        let asset: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM media_assets WHERE tenant_id = $1 AND file_id = $2",
        )
        .bind(TENANT)
        .bind(file_id)
        .fetch_one(self.store.pool())
        .await
        .unwrap();
        let references: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM media_references WHERE tenant_id = $1 AND file_id = $2",
        )
        .bind(TENANT)
        .bind(file_id)
        .fetch_one(self.store.pool())
        .await
        .unwrap();
        (asset == 1, references)
    }

    async fn drop_schema(self) {
        // A task left parked at a gate by a failed assertion must not hang teardown.
        let _ = tokio::time::timeout(Duration::from_secs(5), self.store.pool.close()).await;
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&self.url)
            .await
            .unwrap();
        let dropped = sqlx::raw_sql(&format!(
            "SET lock_timeout = '5s'; DROP SCHEMA {} CASCADE",
            self.schema
        ))
        .execute(&admin)
        .await;
        if dropped.is_err() {
            eprintln!(
                "left throwaway schema {} in the disposable database",
                self.schema
            );
        }
        admin.close().await;
        let _ = std::fs::remove_dir_all(&self.chunks);
    }
}

/// Runs `body`, then always drops the throwaway schema before re-raising a failure.
async fn with_race_db<F, Fut>(body: F)
where
    F: FnOnce(Arc<RaceDb>) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let db = Arc::new(RaceDb::new().await);
    let outcome = std::panic::AssertUnwindSafe(body(db.clone()))
        .catch_unwind()
        .await;
    let db = Arc::try_unwrap(db).unwrap_or_else(|_| panic!("race test leaked its database"));
    db.drop_schema().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

/// Invariant for one asset after cleanup and reference creation have both finished:
/// a successful reference means the asset, its reference and its object all survived
/// and cleanup did not report it; a failed reference means cleanup removed everything.
async fn assert_consistent(
    db: &RaceDb,
    objects: &Objects,
    file_id: &str,
    referenced: bool,
    reported_cleaned: usize,
) {
    let (asset, references) = db.state(file_id).await;
    let object = objects
        .keys
        .lock()
        .unwrap()
        .contains(&format!("{TENANT}/{file_id}"));
    if referenced {
        assert!(
            asset && references == 1 && object && reported_cleaned == 0,
            "{file_id}: reference succeeded but asset={asset} refs={references} \
             object={object} cleaned_reports={reported_cleaned}"
        );
    } else {
        assert!(
            !asset && references == 0 && !object && reported_cleaned == 1,
            "{file_id}: reference failed but asset={asset} refs={references} \
             object={object} cleaned_reports={reported_cleaned}"
        );
    }
}

#[tokio::test]
#[ignore = "requires FLARE_MEDIA_TEST_DATABASE_URL pointing at disposable PostgreSQL"]
async fn reference_created_after_candidate_listing_survives_cleanup() {
    with_race_db(|db| async move {
        let gate = Arc::new(Gate::default());
        let objects = Arc::new(Objects::default());
        let listing = Arc::new(GatedListing {
            inner: db.store.clone(),
            gate: gate.clone(),
        });
        let service = db.service(listing, objects.clone());
        db.seed(&objects, "late-reference").await;

        let cleaner = tokio::spawn({
            let service = service.clone();
            async move { service.cleanup_orphaned_assets(&ctx(), 10).await }
        });
        // Cleanup has taken its snapshot and still believes the asset is an orphan.
        gate.entered.notified().await;
        let referenced = service
            .add_reference(&ctx(), "late-reference", scope(), HashMap::new())
            .await;
        assert!(referenced.is_ok(), "{referenced:?}");
        gate.resume.notify_one();
        let cleaned = cleaner.await.unwrap().unwrap();

        let reports = cleaned.iter().filter(|id| *id == "late-reference").count();
        assert_consistent(&db, &objects, "late-reference", true, reports).await;
    })
    .await;
}

#[tokio::test]
#[ignore = "requires FLARE_MEDIA_TEST_DATABASE_URL pointing at disposable PostgreSQL"]
async fn reference_racing_an_in_progress_claim_waits_then_fails_cleanly() {
    with_race_db(|db| async move {
        let gate = Arc::new(Gate::default());
        let objects = Arc::new(Objects {
            delete_gate: Some(gate.clone()),
            ..Default::default()
        });
        let service = db.service(Arc::new(db.store.clone()), objects.clone());
        db.seed(&objects, "claimed").await;

        let cleaner = tokio::spawn({
            let service = service.clone();
            async move { service.cleanup_orphaned_assets(&ctx(), 10).await }
        });
        // Cleanup decided to delete and is removing the stored object.
        gate.entered.notified().await;
        let reference = tokio::spawn({
            let service = service.clone();
            async move {
                service
                    .add_reference(&ctx(), "claimed", scope(), HashMap::new())
                    .await
            }
        });
        tokio::time::sleep(Duration::from_millis(300)).await;
        let blocked = !reference.is_finished();
        gate.resume.notify_one();
        let cleaned = cleaner.await.unwrap().unwrap();
        let referenced = reference.await.unwrap();

        let reports = cleaned.iter().filter(|id| *id == "claimed").count();
        assert_consistent(&db, &objects, "claimed", referenced.is_ok(), reports).await;
        assert!(
            referenced.is_err(),
            "reference to a claimed asset must fail"
        );
        assert!(
            blocked,
            "reference creation must wait for the uncommitted claim"
        );
    })
    .await;
}

#[tokio::test]
#[ignore = "requires FLARE_MEDIA_TEST_DATABASE_URL pointing at disposable PostgreSQL"]
async fn storage_failure_releases_the_claim_for_a_later_retry() {
    with_race_db(|db| async move {
        let objects = Arc::new(Objects::default());
        let service = db.service(Arc::new(db.store.clone()), objects.clone());
        db.seed(&objects, "retry").await;

        *objects.fail_delete.lock().unwrap() = true;
        assert!(service.cleanup_orphaned_assets(&ctx(), 10).await.is_err());
        assert_eq!(db.state("retry").await, (true, 0));
        assert!(objects.keys.lock().unwrap().contains("race-tenant/retry"));

        *objects.fail_delete.lock().unwrap() = false;
        assert_eq!(
            service.cleanup_orphaned_assets(&ctx(), 10).await.unwrap(),
            vec!["retry".to_string()]
        );
        assert_eq!(db.state("retry").await, (false, 0));
        assert!(objects.keys.lock().unwrap().is_empty());
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires FLARE_MEDIA_TEST_DATABASE_URL pointing at disposable PostgreSQL"]
async fn concurrent_cleaners_and_reference_creation_never_leave_dangling_references() {
    with_race_db(|db| async move {
        const ROUNDS: usize = 8;
        const ASSETS: usize = 24;
        let objects = Arc::new(Objects::default());
        let service = db.service(Arc::new(db.store.clone()), objects.clone());
        let (mut kept, mut removed) = (0, 0);
        for round in 0..ROUNDS {
            let ids: Vec<String> = (0..ASSETS).map(|i| format!("r{round}-a{i}")).collect();
            for id in &ids {
                db.seed(&objects, id).await;
            }
            let spawn_cleaners = || -> Vec<_> {
                (0..2)
                    .map(|_| {
                        let service = service.clone();
                        tokio::spawn(
                            async move { service.cleanup_orphaned_assets(&ctx(), 1000).await },
                        )
                    })
                    .collect()
            };
            // Alternate who starts first so both interleavings get exercised.
            let early_cleaners = (round % 2 == 0).then(spawn_cleaners);
            let references: Vec<_> = ids
                .iter()
                .cloned()
                .map(|id| {
                    let service = service.clone();
                    tokio::spawn(async move {
                        let outcome = service
                            .add_reference(&ctx(), &id, scope(), HashMap::new())
                            .await;
                        (id, outcome.is_ok())
                    })
                })
                .collect();
            let cleaners = early_cleaners.unwrap_or_else(spawn_cleaners);
            let mut reported = Vec::new();
            for cleaner in cleaners {
                reported.extend(cleaner.await.unwrap().unwrap());
            }
            for reference in references {
                let (id, referenced) = reference.await.unwrap();
                let reports = reported.iter().filter(|cleaned| **cleaned == id).count();
                assert_consistent(&db, &objects, &id, referenced, reports).await;
                if referenced {
                    kept += 1;
                } else {
                    removed += 1;
                }
            }
        }
        // The split varies per run; the deterministic tests above pin each interleaving.
        println!("race outcome: referenced-and-kept={kept} cleaned={removed}");
        assert_eq!(kept + removed, ROUNDS * ASSETS);
    })
    .await;
}

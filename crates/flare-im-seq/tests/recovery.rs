use std::future::Future;
use std::pin::Pin;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use flare_im_seq::{SeqFloorProvider, SequenceAllocator};
use flare_server_core::context::{Context, Ctx};
use flare_server_core::error::{ErrorCode, FlareError, Result};
use tokio::sync::{Barrier, mpsc};

struct RedisServer {
    child: Child,
    dir: std::path::PathBuf,
    client: Arc<redis::Client>,
}

impl RedisServer {
    async fn start() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let dir = std::path::PathBuf::from(format!(
            "/tmp/flare-seq-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let socket = dir.join("redis.sock");
        let child = Command::new("redis-server")
            .args([
                "--port",
                "0",
                "--save",
                "",
                "--appendonly",
                "no",
                "--unixsocket",
            ])
            .arg(&socket)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("install redis-server to run the sequence recovery tests");
        let server = Self {
            child,
            dir,
            client: Arc::new(
                redis::Client::open(format!("redis+unix://{}", socket.display())).unwrap(),
            ),
        };
        tokio::time::timeout(Duration::from_secs(5), async {
            while server
                .client
                .get_multiplexed_async_connection()
                .await
                .is_err()
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("isolated Redis starts");
        server
    }
}

impl Drop for RedisServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

struct GatedFloor {
    entered: mpsc::Sender<()>,
    release: Arc<Barrier>,
}

impl SeqFloorProvider for GatedFloor {
    fn persisted_max_seq<'a>(
        &'a self,
        _ctx: &'a Ctx,
        _tenant: &'a str,
        _conversation: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<u64>> + Send + 'a>> {
        Box::pin(async move {
            self.entered.send(()).await.unwrap();
            self.release.wait().await;
            Ok(100)
        })
    }
}

#[tokio::test]
#[ignore = "requires redis-server executable"]
async fn concurrent_writers_and_batches_wait_for_the_durable_floor_after_counter_loss() {
    let server = RedisServer::start().await;
    let ctx = Arc::new(Context::root());
    let (entered, mut reads) = mpsc::channel(3);
    let release = Arc::new(Barrier::new(4));
    let provider = Arc::new(GatedFloor {
        entered,
        release: release.clone(),
    });
    let mut writers = Vec::new();
    for _ in 0..3 {
        writers.push(
            SequenceAllocator::new(server.client.clone(), 10)
                .await
                .unwrap()
                .with_floor_provider(provider.clone()),
        );
    }
    // All instances have used this counter before it disappears.
    writers[0]
        .allocate_seq_with_floor("conv", "tenant", 99)
        .await
        .unwrap();
    let mut conn = server
        .client
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let _: () = redis::cmd("DEL")
        .arg("seq:tenant:conv")
        .query_async(&mut conn)
        .await
        .unwrap();
    let mut tasks = Vec::new();
    for (i, writer) in writers.into_iter().enumerate() {
        let ctx = ctx.clone();
        tasks.push(tokio::spawn(async move {
            if i == 2 {
                writer.allocate_batch(&ctx, "conv", "tenant").await.unwrap()
            } else {
                vec![writer.allocate_seq(&ctx, "conv", "tenant").await.unwrap()]
            }
        }));
    }
    tokio::time::timeout(Duration::from_secs(1), async {
        for _ in 0..3 {
            reads.recv().await.unwrap();
        }
    })
    .await
    .expect("every writer consults the floor before creating the key");
    let exists: bool = redis::cmd("EXISTS")
        .arg("seq:tenant:conv")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert!(!exists, "no concurrent writer may expose a low counter");
    release.wait().await;
    let mut seqs = Vec::new();
    for task in tasks {
        seqs.extend(task.await.unwrap());
    }
    seqs.sort_unstable();
    assert_eq!(seqs, (101..=112).collect::<Vec<_>>());
    let ttl: i64 = redis::cmd("TTL")
        .arg("seq:tenant:conv")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(ttl, -1, "a recovered counter must not expire again");
}

struct UnavailableFloor;
impl SeqFloorProvider for UnavailableFloor {
    fn persisted_max_seq<'a>(
        &'a self,
        _ctx: &'a Ctx,
        _tenant: &'a str,
        _conversation: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<u64>> + Send + 'a>> {
        Box::pin(async {
            Err(FlareError::localized(
                ErrorCode::ServiceUnavailable,
                "storage down",
            ))
        })
    }
}

#[tokio::test]
#[ignore = "requires redis-server executable"]
async fn an_unavailable_floor_never_initializes_a_low_counter() {
    let server = RedisServer::start().await;
    let writer = SequenceAllocator::new(server.client.clone(), 10)
        .await
        .unwrap()
        .with_floor_provider(Arc::new(UnavailableFloor));
    let ctx = Arc::new(Context::root());
    let error = writer
        .allocate_seq(&ctx, "conv", "tenant")
        .await
        .unwrap_err();
    assert_eq!(error.code(), Some(ErrorCode::ServiceUnavailable));
    let mut conn = server
        .client
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let exists: bool = redis::cmd("EXISTS")
        .arg("seq:tenant:conv")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert!(!exists);
    writer
        .allocate_seq_with_floor("conv", "tenant", 100)
        .await
        .unwrap();
    assert_eq!(
        writer.allocate_seq(&ctx, "conv", "tenant").await.unwrap(),
        102
    );
    assert_eq!(
        writer
            .allocate_seq(&ctx, "conv", "other-tenant")
            .await
            .unwrap_err()
            .code(),
        Some(ErrorCode::ServiceUnavailable)
    );
}

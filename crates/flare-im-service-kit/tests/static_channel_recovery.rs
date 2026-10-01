//! 静态 gRPC 通道在下游重启、换地址后能在有界时间内恢复（W6-S2 F2）。
//!
//! 下游是本机回环上的假 HookPlugin 服务。「名字解析」由测试连接器控制：先指向服务 A；
//! A 停掉后指向一个永远连不上的旧地址（建连一直挂起，相当于 SYN 发往已经不存在的 IP）；
//! 随后在新端口上起服务 B 并把名字指过去。没有建连上限时，通道停在那次挂起的建连上，
//! 之后每次调用都只能等满调用方的超时；有上限时，上限一到就重新解析，调用随即恢复。

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use flare_grpc_proto::capability::hook_plugin_client::HookPluginClient;
use flare_grpc_proto::capability::hook_plugin_server::{HookPlugin, HookPluginServer};
use flare_grpc_proto::{GenericRequest, GenericResponse};
use flare_im_service_kit::discovery::{
    StaticChannelPolicy, connect_static_channel_lazy_with, invalidate_static_channel,
    invalidate_static_channel_on_transport_failure, is_transport_failure, resolve_grpc_channel,
};
use hyper_util::rt::TokioIo;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tonic::codegen::http::Uri;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};

/// 与 Core 上 PreSend Hook 的超时一致（`hooks.social.static.toml` 的 `timeout_ms = 300`）。
const CALL_TIMEOUT: Duration = Duration::from_millis(300);

struct FakeHook {
    name: &'static str,
}

#[tonic::async_trait]
impl HookPlugin for FakeHook {
    async fn call(
        &self,
        request: Request<GenericRequest>,
    ) -> Result<Response<GenericResponse>, Status> {
        Ok(Response::new(GenericResponse {
            ok: true,
            request_id: request.into_inner().request_id,
            payload: None,
            error_code: String::new(),
            error_message: self.name.to_string(),
        }))
    }
}

struct RunningServer {
    addr: SocketAddr,
    stop: Option<oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<()>,
}

impl RunningServer {
    async fn start(name: &'static str) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (stop, stop_rx) = oneshot::channel::<()>();
        let task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(HookPluginServer::new(FakeHook { name }))
                .serve_with_incoming_shutdown(
                    tonic::transport::server::TcpIncoming::from(listener),
                    async {
                        let _ = stop_rx.await;
                    },
                )
                .await
                .unwrap();
        });
        Self {
            addr,
            stop: Some(stop),
            task,
        }
    }

    async fn stop(mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        let _ = tokio::time::timeout(Duration::from_secs(5), &mut self.task).await;
        self.task.abort();
    }
}

/// 一次调用（带调用方超时）；成功时返回应答方的名字。
async fn call(channel: &Channel) -> Result<String, String> {
    let mut client = HookPluginClient::new(channel.clone());
    match tokio::time::timeout(
        CALL_TIMEOUT,
        client.call(GenericRequest {
            operation: "probe".into(),
            ..Default::default()
        }),
    )
    .await
    {
        Ok(Ok(response)) => Ok(response.into_inner().error_message),
        Ok(Err(status)) => Err(format!("status {:?}: {}", status.code(), status.message())),
        Err(_) => Err("caller timeout".into()),
    }
}

/// 反复调用直到成功，返回所用时间；超过 `limit` 返回 None。
async fn recover_within(channel: &Channel, expect: &str, limit: Duration) -> Option<Duration> {
    let started = Instant::now();
    while started.elapsed() < limit {
        if call(channel).await.as_deref() == Ok(expect) {
            return Some(started.elapsed());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    None
}

#[tokio::test]
async fn static_channel_recovers_after_a_hung_connect_to_a_vanished_address() {
    // None：名字仍解析到已经不存在的旧地址，建连挂起。
    let target: Arc<Mutex<Option<SocketAddr>>> = Arc::new(Mutex::new(None));
    let dial = Arc::clone(&target);
    let connector = tower::service_fn(move |_uri: Uri| {
        let current = *dial.lock().unwrap();
        async move {
            match current {
                Some(addr) => TcpStream::connect(addr).await.map(TokioIo::new),
                None => std::future::pending().await,
            }
        }
    });
    let policy = StaticChannelPolicy::DEFAULT;
    let channel =
        connect_static_channel_lazy_with("http://social.test:50210", policy, connector).unwrap();

    let first = RunningServer::start("a").await;
    *target.lock().unwrap() = Some(first.addr);
    assert_eq!(call(&channel).await.as_deref(), Ok("a"));

    // 下游停止，名字暂时指向旧地址：调用失败或等满调用方超时。
    *target.lock().unwrap() = None;
    first.stop().await;
    for _ in 0..3 {
        assert!(call(&channel).await.is_err(), "stopped downstream answered");
    }

    // 下游在新端口上起来，名字指过去。
    let second = RunningServer::start("b").await;
    *target.lock().unwrap() = Some(second.addr);
    let bound = policy.connect_timeout + Duration::from_secs(1);
    let recovered = recover_within(&channel, "b", Duration::from_secs(8)).await;
    println!("hung connect: recovered after {recovered:?} (bound {bound:?})");
    second.stop().await;
    let elapsed = recovered.expect("channel never left the hung connect to the vanished address");
    assert!(
        elapsed <= bound,
        "recovered after {elapsed:?}, bound {bound:?}"
    );
}

/// 本机 TCP 转发：模拟名字不变而背后的实例换了。`freeze` 让已有连接停止转发但保持打开
/// （对端消失却没有发 FIN/RST），新连接转发到 `retarget` 设的新地址。
struct Forwarder {
    addr: SocketAddr,
    state: Arc<Mutex<ForwardState>>,
    task: tokio::task::JoinHandle<()>,
}

struct ForwardState {
    target: SocketAddr,
    generation: u64,
    frozen_before: u64,
}

impl Forwarder {
    async fn start(target: SocketAddr) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let state = Arc::new(Mutex::new(ForwardState {
            target,
            generation: 0,
            frozen_before: 0,
        }));
        let accept_state = Arc::clone(&state);
        let task = tokio::spawn(async move {
            while let Ok((inbound, _)) = listener.accept().await {
                let (target, generation) = {
                    let mut state = accept_state.lock().unwrap();
                    state.generation += 1;
                    (state.target, state.generation)
                };
                let Ok(outbound) = TcpStream::connect(target).await else {
                    continue;
                };
                let (in_read, in_write) = inbound.into_split();
                let (out_read, out_write) = outbound.into_split();
                tokio::spawn(pump(
                    in_read,
                    out_write,
                    Arc::clone(&accept_state),
                    generation,
                ));
                tokio::spawn(pump(
                    out_read,
                    in_write,
                    Arc::clone(&accept_state),
                    generation,
                ));
            }
        });
        Self { addr, state, task }
    }

    /// 冻结已有连接，新连接转发到 `target`。
    fn freeze_and_retarget(&self, target: SocketAddr) {
        let mut state = self.state.lock().unwrap();
        state.frozen_before = state.generation + 1;
        state.target = target;
    }
}

impl Drop for Forwarder {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn pump(
    mut from: tokio::net::tcp::OwnedReadHalf,
    mut to: tokio::net::tcp::OwnedWriteHalf,
    state: Arc<Mutex<ForwardState>>,
    generation: u64,
) {
    let mut buffer = [0u8; 16 * 1024];
    loop {
        let read = match from.read(&mut buffer).await {
            Ok(0) | Err(_) => return,
            Ok(read) => read,
        };
        if generation < state.lock().unwrap().frozen_before {
            // 冻结：吞掉数据，连接保持打开，直到测试结束（任务被丢弃）。
            loop {
                if from.read(&mut buffer).await.unwrap_or(0) == 0 {
                    std::future::pending::<()>().await;
                }
            }
        }
        if to.write_all(&buffer[..read]).await.is_err() {
            return;
        }
    }
}

#[tokio::test]
async fn static_channel_drops_a_silent_connection_through_http2_keepalive() {
    let first = RunningServer::start("a").await;
    let second = RunningServer::start("b").await;
    let forwarder = Forwarder::start(first.addr).await;
    let policy = StaticChannelPolicy {
        keepalive_interval: Duration::from_millis(200),
        keepalive_timeout: Duration::from_millis(300),
        ..StaticChannelPolicy::DEFAULT
    };
    let forward_addr = forwarder.addr;
    let connector = tower::service_fn(move |_uri: Uri| async move {
        TcpStream::connect(forward_addr).await.map(TokioIo::new)
    });
    let channel =
        connect_static_channel_lazy_with("http://social.test:50210", policy, connector).unwrap();
    assert_eq!(call(&channel).await.as_deref(), Ok("a"));

    // 已建立的连接从此石沉大海（不 FIN、不 RST），名字指向新实例。
    forwarder.freeze_and_retarget(second.addr);
    let bound = policy.keepalive_interval + policy.keepalive_timeout + Duration::from_secs(1);
    let recovered = recover_within(&channel, "b", Duration::from_secs(6)).await;
    println!("silent connection: recovered after {recovered:?} (bound {bound:?})");
    drop(forwarder);
    first.stop().await;
    second.stop().await;
    let elapsed = recovered.expect("channel kept using the silent connection");
    assert!(
        elapsed <= bound,
        "recovered after {elapsed:?}, bound {bound:?}"
    );
}

#[tokio::test]
async fn transport_failures_drop_the_cached_static_channel_but_server_statuses_do_not() {
    // 一个刚关掉的回环端口：连上会被拒绝。
    let closed = {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        listener.local_addr().unwrap()
    };
    let authority = format!("http://{closed}");
    let channel = resolve_grpc_channel(&authority).await.unwrap();
    let status = HookPluginClient::new(channel)
        .call(GenericRequest::default())
        .await
        .expect_err("nothing listens on a closed port");
    assert!(is_transport_failure(&status), "{status:?}");
    assert!(!is_transport_failure(&Status::unavailable(
        "returned by the server"
    )));
    assert!(!invalidate_static_channel_on_transport_failure(
        &authority,
        &Status::unavailable("returned by the server")
    ));
    assert!(invalidate_static_channel_on_transport_failure(
        &authority, &status
    ));
    assert!(!invalidate_static_channel_on_transport_failure(
        "discovery://flare-social-hook",
        &status
    ));

    // 丢弃之后再解析，得到的是新通道：对端起来后第一次调用就成功。
    let server = RunningServer::start("fresh").await;
    let authority = format!("http://{}", server.addr);
    let stale = resolve_grpc_channel(&authority).await.unwrap();
    assert_eq!(call(&stale).await.as_deref(), Ok("fresh"));
    invalidate_static_channel(&authority);
    let fresh = resolve_grpc_channel(&authority).await.unwrap();
    assert_eq!(call(&fresh).await.as_deref(), Ok("fresh"));
    server.stop().await;
}

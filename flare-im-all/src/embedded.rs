use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use flare_core_runtime::signal::{ChannelSignal, ShutdownSignal};
use flare_server_core::error::{FlareError, Result};
use tokio::sync::{mpsc, oneshot};
use tokio::task::{JoinHandle, LocalSet};
use tracing::{error, info, warn};

use crate::{
    ALL_RUNTIME_SERVICES, DeploymentProfile, ProcessShape, RuntimeUnit, ServiceSpec, StandardGroup,
    profile_units, standard_group_unit,
};

type LocalServiceFuture = Pin<Box<dyn Future<Output = Result<()>> + 'static>>;
type EmbeddedRunFn = fn(flare_im_service_kit::RuntimeShutdownSignals) -> LocalServiceFuture;

#[derive(Clone, Copy)]
pub struct EmbeddedServiceRunner {
    pub service: ServiceSpec,
    run: EmbeddedRunFn,
}

impl EmbeddedServiceRunner {
    const fn new(service: ServiceSpec, run: EmbeddedRunFn) -> Self {
        Self { service, run }
    }

    fn start(self, shutdown_rx: oneshot::Receiver<()>) -> LocalServiceFuture {
        let signal: Box<dyn ShutdownSignal> = Box::new(ChannelSignal::new(
            format!("{}-embedded-shutdown", self.service.service_name),
            shutdown_rx,
        ));
        (self.run)(vec![signal])
    }
}

pub const EMBEDDED_SERVICE_RUNNERS: [EmbeddedServiceRunner; 16] = [
    EmbeddedServiceRunner::new(ALL_RUNTIME_SERVICES[0], run_api_gateway),
    EmbeddedServiceRunner::new(ALL_RUNTIME_SERVICES[1], run_admin_gateway),
    EmbeddedServiceRunner::new(ALL_RUNTIME_SERVICES[2], run_access_gateway),
    EmbeddedServiceRunner::new(ALL_RUNTIME_SERVICES[3], run_signaling_route),
    EmbeddedServiceRunner::new(ALL_RUNTIME_SERVICES[4], run_message_ingest),
    EmbeddedServiceRunner::new(ALL_RUNTIME_SERVICES[5], run_orchestrator),
    EmbeddedServiceRunner::new(ALL_RUNTIME_SERVICES[6], run_conversation),
    EmbeddedServiceRunner::new(ALL_RUNTIME_SERVICES[7], run_sync_orchestrator),
    EmbeddedServiceRunner::new(ALL_RUNTIME_SERVICES[8], run_push_proxy),
    EmbeddedServiceRunner::new(ALL_RUNTIME_SERVICES[9], run_push_server),
    EmbeddedServiceRunner::new(ALL_RUNTIME_SERVICES[10], run_push_worker),
    EmbeddedServiceRunner::new(ALL_RUNTIME_SERVICES[11], run_capability),
    EmbeddedServiceRunner::new(ALL_RUNTIME_SERVICES[12], run_media),
    EmbeddedServiceRunner::new(ALL_RUNTIME_SERVICES[13], run_storage_writer),
    EmbeddedServiceRunner::new(ALL_RUNTIME_SERVICES[14], run_storage_reader),
    EmbeddedServiceRunner::new(ALL_RUNTIME_SERVICES[15], run_signaling_online),
];

pub async fn run_embedded_dev() -> Result<()> {
    let unit = profile_units(DeploymentProfile::Dev)
        .into_iter()
        .next()
        .ok_or_else(|| FlareError::system("dev deployment profile is empty"))?;
    run_embedded_unit(unit).await
}

pub async fn run_embedded_standard_group(group: StandardGroup) -> Result<()> {
    run_embedded_unit(standard_group_unit(group)).await
}

pub async fn run_embedded_unit(unit: RuntimeUnit) -> Result<()> {
    if unit.shape == ProcessShape::IndependentServiceProcess {
        return Err(FlareError::system(
            "full profile uses independent service processes; use the service binaries directly",
        ));
    }

    let app_config = flare_im_service_kit::load_app_config_from_env();
    flare_im_service_kit::tracing::init_tracing_from_config(Some(app_config.logging()));

    let local = LocalSet::new();
    local.run_until(run_embedded_unit_local(unit)).await
}

async fn run_embedded_unit_local(unit: RuntimeUnit) -> Result<()> {
    let runners = runners_for_unit(&unit)?;
    let (result_tx, mut result_rx) = mpsc::unbounded_channel();
    let mut shutdown_txs = Vec::with_capacity(runners.len());
    let mut handles = Vec::with_capacity(runners.len());

    info!(
        process = %unit.name,
        profile = %unit.profile.as_str(),
        shape = %unit.shape.as_str(),
        service_count = runners.len(),
        "Starting embedded Flare IM runtime unit"
    );

    for runner in runners {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        shutdown_txs.push(shutdown_tx);

        let service_name = runner.service.service_name;
        let result_tx = result_tx.clone();
        let future = runner.start(shutdown_rx);
        let handle = tokio::task::spawn_local(async move {
            let result = future.await;
            let _ = result_tx.send((service_name, result));
        });
        handles.push(handle);
    }
    drop(result_tx);

    tokio::select! {
        _ = wait_for_process_shutdown() => {
            info!(process = %unit.name, "Embedded runtime shutdown requested");
            stop_embedded_services(shutdown_txs, handles).await;
            Ok(())
        }
        completed = result_rx.recv() => {
            let Some((service_name, result)) = completed else {
                return Ok(());
            };

            match result {
                Ok(()) => {
                    warn!(service = service_name, "Embedded service exited before process shutdown");
                    stop_embedded_services(shutdown_txs, handles).await;
                    Err(FlareError::system(format!(
                        "embedded service exited before profile shutdown: {service_name}"
                    )))
                }
                Err(error) => {
                    error!(service = service_name, error = %error, "Embedded service failed");
                    stop_embedded_services(shutdown_txs, handles).await;
                    Err(error)
                }
            }
        }
    }
}

fn runners_for_unit(unit: &RuntimeUnit) -> Result<Vec<EmbeddedServiceRunner>> {
    unit.services
        .iter()
        .map(|service| {
            EMBEDDED_SERVICE_RUNNERS
                .iter()
                .copied()
                .find(|runner| runner.service.service_name == service.service_name)
                .ok_or_else(|| {
                    FlareError::system(format!(
                        "no embedded runner registered for {}",
                        service.service_name
                    ))
                })
        })
        .collect()
}

async fn stop_embedded_services(
    shutdown_txs: Vec<oneshot::Sender<()>>,
    mut handles: Vec<JoinHandle<()>>,
) {
    for shutdown_tx in shutdown_txs {
        let _ = shutdown_tx.send(());
    }

    let deadline = tokio::time::sleep(Duration::from_secs(30));
    tokio::pin!(deadline);

    while let Some(mut handle) = handles.pop() {
        tokio::select! {
            result = &mut handle => {
                if let Err(error) = result {
                    warn!(error = %error, "Embedded service task join failed");
                }
            }
            _ = &mut deadline => {
                warn!("Timed out while waiting for embedded services to stop; aborting remaining tasks");
                handle.abort();
                for handle in handles {
                    handle.abort();
                }
                return;
            }
        }
    }
}

async fn wait_for_process_shutdown() {
    #[cfg(target_family = "unix")]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
    }

    #[cfg(not(target_family = "unix"))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

fn run_api_gateway(signals: flare_im_service_kit::RuntimeShutdownSignals) -> LocalServiceFuture {
    Box::pin(flare_api_gateway::ApplicationBootstrap::run_with_shutdown_signals(signals))
}

fn run_admin_gateway(signals: flare_im_service_kit::RuntimeShutdownSignals) -> LocalServiceFuture {
    Box::pin(flare_admin_gateway::ApplicationBootstrap::run_with_shutdown_signals(signals))
}

fn run_access_gateway(signals: flare_im_service_kit::RuntimeShutdownSignals) -> LocalServiceFuture {
    Box::pin(flare_signaling_gateway::ApplicationBootstrap::run_with_shutdown_signals(signals))
}

fn run_signaling_route(
    signals: flare_im_service_kit::RuntimeShutdownSignals,
) -> LocalServiceFuture {
    Box::pin(flare_signaling_route::ApplicationBootstrap::run_with_shutdown_signals(signals))
}

fn run_message_ingest(signals: flare_im_service_kit::RuntimeShutdownSignals) -> LocalServiceFuture {
    Box::pin(flare_message_ingest::ApplicationBootstrap::run_with_shutdown_signals(signals))
}

fn run_orchestrator(signals: flare_im_service_kit::RuntimeShutdownSignals) -> LocalServiceFuture {
    Box::pin(flare_orchestrator::ApplicationBootstrap::run_with_shutdown_signals(signals))
}

fn run_conversation(signals: flare_im_service_kit::RuntimeShutdownSignals) -> LocalServiceFuture {
    Box::pin(flare_conversation::ApplicationBootstrap::run_with_shutdown_signals(signals))
}

fn run_sync_orchestrator(
    signals: flare_im_service_kit::RuntimeShutdownSignals,
) -> LocalServiceFuture {
    Box::pin(flare_sync_orchestrator::ApplicationBootstrap::run_with_shutdown_signals(signals))
}

fn run_push_proxy(signals: flare_im_service_kit::RuntimeShutdownSignals) -> LocalServiceFuture {
    Box::pin(flare_push_proxy::ApplicationBootstrap::run_with_shutdown_signals(signals))
}

fn run_push_server(signals: flare_im_service_kit::RuntimeShutdownSignals) -> LocalServiceFuture {
    Box::pin(flare_push_server::ApplicationBootstrap::run_with_shutdown_signals(signals))
}

fn run_push_worker(signals: flare_im_service_kit::RuntimeShutdownSignals) -> LocalServiceFuture {
    Box::pin(flare_push_worker::ApplicationBootstrap::run_with_shutdown_signals(signals))
}

fn run_capability(signals: flare_im_service_kit::RuntimeShutdownSignals) -> LocalServiceFuture {
    Box::pin(
        flare_capability::composition::ApplicationBootstrap::run_from_env_with_shutdown_signals(
            signals,
        ),
    )
}

fn run_media(signals: flare_im_service_kit::RuntimeShutdownSignals) -> LocalServiceFuture {
    Box::pin(flare_media::ApplicationBootstrap::run_with_shutdown_signals(signals))
}

fn run_storage_writer(signals: flare_im_service_kit::RuntimeShutdownSignals) -> LocalServiceFuture {
    Box::pin(flare_storage_writer::ApplicationBootstrap::run_with_shutdown_signals(signals))
}

fn run_storage_reader(signals: flare_im_service_kit::RuntimeShutdownSignals) -> LocalServiceFuture {
    Box::pin(flare_storage_reader::ApplicationBootstrap::run_with_shutdown_signals(signals))
}

fn run_signaling_online(
    signals: flare_im_service_kit::RuntimeShutdownSignals,
) -> LocalServiceFuture {
    Box::pin(flare_signaling_online::ApplicationBootstrap::run_with_shutdown_signals(signals))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_profile_service_has_an_embedded_runner() {
        for service in ALL_RUNTIME_SERVICES {
            assert!(
                EMBEDDED_SERVICE_RUNNERS
                    .iter()
                    .any(|runner| runner.service.service_name == service.service_name),
                "{} must have an embedded runner",
                service.service_name
            );
        }
    }

    #[test]
    fn standard_groups_have_expected_embedded_runner_counts() {
        assert_eq!(
            runners_for_unit(&standard_group_unit(StandardGroup::Edge))
                .unwrap()
                .len(),
            4
        );
        assert_eq!(
            runners_for_unit(&standard_group_unit(StandardGroup::Core))
                .unwrap()
                .len(),
            9
        );
        assert_eq!(
            runners_for_unit(&standard_group_unit(StandardGroup::Data))
                .unwrap()
                .len(),
            3
        );
    }
}

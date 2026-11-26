//! Durable JetStream observer, using the same topology as the accepted main stream.
use crate::{
    application::handler::message_events::MessageEventHandler,
    domain::capability::CapabilityPolicyBackend, infrastructure::capability::PluginRouteBook,
};
use flare_server_core::{
    error::{FlareError, Result},
    mq::{
        NatsConsumerConfig, NatsProducerConfig,
        consumer::{
            ConsumerConfig, ConsumerFailurePublishers, FailureTopic, MqConsumerTask,
            ProducerDeadLetterPublisher,
            dispatcher::{Dispatcher, TopicDispatcher},
        },
        nats::{NatsProducer, NatsStreamSpec, build_nats_consumer_tasks_with_failure_publishers},
    },
};
use std::sync::Arc;

pub(crate) struct EventMqConfig {
    pub(crate) url: String,
    pub(crate) streams: Vec<NatsStreamSpec>,
}
impl NatsConsumerConfig for EventMqConfig {
    fn nats_url(&self) -> &str {
        &self.url
    }
    fn consumer_group(&self) -> &str {
        "capability-plugin-events-v1"
    }
    fn enable_manual_ack(&self) -> bool {
        true
    }
    fn batch_size(&self) -> usize {
        8
    }
    fn batch_timeout_ms(&self) -> u64 {
        1000
    }
    fn max_ack_pending(&self) -> i64 {
        32
    }
    fn enable_durable(&self) -> bool {
        true
    }
    fn stream_specs(&self) -> Vec<NatsStreamSpec> {
        self.streams.clone()
    }
}
impl NatsProducerConfig for EventMqConfig {
    fn nats_url(&self) -> &str {
        &self.url
    }
    fn timeout_ms(&self) -> u64 {
        3000
    }
    fn retries(&self) -> u32 {
        3
    }
    fn retry_backoff_ms(&self) -> u64 {
        100
    }
    fn stream_specs(&self) -> Vec<NatsStreamSpec> {
        self.streams.clone()
    }
}
pub(super) async fn tasks(
    routes: &PluginRouteBook,
    policy: Arc<dyn CapabilityPolicyBackend>,
) -> Result<Vec<MqConsumerTask>> {
    let (Some(authorization), Some(signer)) = (
        routes.authorization.clone(),
        routes.invocation_signer.clone(),
    ) else {
        return Ok(Vec::new());
    };
    let app = flare_im_core_service_kit::load_app_config_from_env();
    if app.mq.default_backend != "nats" {
        tracing::warn!("plugin event delivery requires the JetStream main stream");
        return Ok(Vec::new());
    }
    let source = app.message_ingest_service();
    let url = std::env::var("FLARE_PLUGIN_EVENTS_NATS_URL")
        .ok()
        .or_else(|| std::env::var("MESSAGE_INGEST_JETSTREAM_URL").ok())
        .or_else(|| {
            source
                .jetstream
                .as_deref()
                .and_then(|name| app.jetstream_profile(name))
                .map(|profile| profile.url.clone())
        });
    let Some(url) = url else {
        tracing::warn!("plugin event delivery has no main JetStream profile");
        return Ok(Vec::new());
    };
    let streams = app
        .jetstream_topology()
        .into_iter()
        .map(|spec| {
            NatsStreamSpec::new(spec.stream_name, spec.subjects)
                .with_max_age(spec.max_age)
                .with_duplicate_window(spec.duplicate_window)
                .with_num_replicas(spec.num_replicas)
                .with_max_bytes(spec.max_bytes)
        })
        .collect::<Vec<_>>();
    let streams = if streams.is_empty() {
        flare_server_core::mq::nats::default_stream_specs()
    } else {
        streams
    };
    let config = EventMqConfig { url, streams };
    let handler = Arc::new(MessageEventHandler::new(authorization, policy, signer));
    build_tasks(&config, handler).await
}
pub(crate) async fn build_tasks(
    config: &EventMqConfig,
    handler: Arc<MessageEventHandler>,
) -> Result<Vec<MqConsumerTask>> {
    let producer = Arc::new(
        NatsProducer::new(config)
            .await
            .map_err(|e| FlareError::system(format!("plugin event DLQ: {e}")))?,
    );
    let failures = ConsumerFailurePublishers::new().with_dead_letter(Arc::new(
        ProducerDeadLetterPublisher::new(
            producer,
            FailureTopic::fixed("flare.im.dlq.plugin-events"),
        ),
    ));
    let mut dispatcher = TopicDispatcher::new();
    dispatcher
        .register("flare.im.message.main".into(), handler)
        .map_err(|e| FlareError::system(format!("plugin event consumer: {e}")))?;
    let consumer = flare_im_core_service_kit::consumer_retry::with_downstream_outage_retry(
        ConsumerConfig::default()
            .with_concurrency(8)
            // Preserve each handler's DeadLetter classification. The generic batch
            // fallback maps individual failures to Nack and would retry poison input.
            .with_batch_size(1),
    );
    build_nats_consumer_tasks_with_failure_publishers(
        config,
        consumer,
        Arc::new(dispatcher),
        "capability-plugin-events",
        failures,
    )
    .await
    .map_err(|e| FlareError::system(format!("plugin event JetStream consumer: {e}")))
}

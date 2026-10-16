//! Window events through a real NATS JetStream (`nats-server -js`). Marked `#[ignore]`: run with
//! `FLARE_USAGE_TEST_NATS_URL=nats://127.0.0.1:<port> cargo test -p flare-im-usage -- --ignored`.

use std::time::Duration;

use flare_im_usage::{UsageEvent, UsageReporter, user_hash};

const ENV_NATS_URL: &str = "FLARE_USAGE_TEST_NATS_URL";

fn nats_url() -> String {
    std::env::var(ENV_NATS_URL)
        .unwrap_or_else(|_| panic!("set {ENV_NATS_URL} to a NATS server with JetStream enabled"))
}

#[tokio::test]
#[ignore = "needs FLARE_USAGE_TEST_NATS_URL (nats-server -js)"]
async fn the_shutdown_flush_lands_once_on_the_usage_stream() {
    let url = nats_url();
    let app = format!("it-{}", std::process::id());
    let reporter = UsageReporter::connect("flare-usage-it", &url, Duration::from_secs(60))
        .await
        .expect("usage stream reachable");
    let recorder = reporter.recorder();
    for _ in 0..3 {
        recorder.message_accepted(&app, "normal");
    }
    recorder.media_egress(&app, 4096);
    recorder.user_active(&app, "user-1");
    reporter.run(async {}).await.expect("flush");

    let client = async_nats::connect(&url).await.expect("connect");
    let jetstream = async_nats::jetstream::new(client);
    let mut stream = jetstream.get_stream("flare_usage").await.expect("stream");
    let config = stream.info().await.expect("stream info").config.clone();
    assert_eq!(config.subjects, ["flare.usage.v1.>"]);
    assert_eq!(config.max_age, Duration::from_secs(90 * 24 * 3600));
    assert_eq!(config.duplicate_window, Duration::from_secs(600));

    let subject = format!("flare.usage.v1.{app}");
    let message = stream
        .get_last_raw_message_by_subject(&subject)
        .await
        .expect("window event stored");
    let event: UsageEvent = serde_json::from_slice(&message.payload).expect("event json");
    assert_eq!(event.event, "flare.usage.window.v1");
    assert_eq!(event.producer, "flare-usage-it");
    assert_eq!(event.app, app);
    assert_eq!(event.part, 0);
    let meters: Vec<_> = event
        .meters
        .iter()
        .map(|meter| (meter.meter.as_str(), meter.value))
        .collect();
    assert_eq!(
        meters,
        [("messages.accepted", 3), ("media.bytes_egress", 4096)]
    );
    assert_eq!(
        event
            .active_users
            .as_ref()
            .expect("active users")
            .user_hashes,
        [format!("{:016x}", user_hash(&app, "user-1"))]
    );
    let message_id = message
        .headers
        .get("Nats-Msg-Id")
        .map(|value| value.as_str().to_owned());
    assert_eq!(message_id.as_deref(), Some(event.message_id().as_str()));

    // A retried publish of the same window is a duplicate, not a second event.
    let retry = async_nats::jetstream::message::PublishMessage::build()
        .message_id(event.message_id())
        .payload(message.payload.clone());
    let ack = jetstream
        .send_publish(subject.clone(), retry)
        .await
        .expect("publish")
        .await
        .expect("ack");
    assert!(ack.duplicate);
    let last = stream
        .get_last_raw_message_by_subject(&subject)
        .await
        .expect("still stored");
    assert_eq!(last.sequence, message.sequence);
}

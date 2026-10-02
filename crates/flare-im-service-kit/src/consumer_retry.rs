//! MQ 消费者对下游暂时性失败的重投策略。
//!
//! handler 遇到暂时性失败（下游停机或重启、连接被拒、超时）时返回 Nack 或可重试错误，交给
//! broker 原生重投。不设退避时 JetStream 立即重投：连接被拒在毫秒内返回，默认的 3 次重试在
//! 同一瞬间用完，停机或重启窗口里的消息随即进 DLQ（W6-S2 F4、W7-F2）。这里给出与 push-worker
//! 相同的策略：12 次重试，1 s 起翻倍、封顶 15 s，等待合计 135 s 后仍失败才进 DLQ；下游恢复后，
//! 处于退避中的消息最迟 15 s 内重投。
//!
//! 退避只作用于 broker 原生重投（JetStream 的延迟 NAK）。配了重试发布器（Kafka 的重试 topic）
//! 的消费者按重试 topic 自己的延迟走，不经过它；Kafka 后端也不支持原生重投。

use std::time::Duration;

use flare_server_core::mq::consumer::{ConsumerConfig, RetryBackoff};

/// 重试次数上限：首投加全部重试不得超过 JetStream 消费者的 `max_deliver`（默认 16），否则
/// broker 先停止投递，消息既不送达也不进 DLQ。
pub const DOWNSTREAM_OUTAGE_MAX_RETRIES: u32 = 12;

/// 第一次重投前的等待。
pub const DOWNSTREAM_OUTAGE_RETRY_INITIAL: Duration = Duration::from_secs(1);

/// 单次重投等待的上限。
pub const DOWNSTREAM_OUTAGE_RETRY_MAX: Duration = Duration::from_secs(15);

/// 重试窗口的下限：core 从停止到重新 healthy 约 16 s（W6-S2），窗口至少留出它的数倍。
pub const DOWNSTREAM_OUTAGE_MIN_WINDOW: Duration = Duration::from_secs(120);

/// 下游暂时不可用时的重投退避：1 s 起翻倍，封顶 15 s。
pub fn downstream_outage_retry_backoff() -> RetryBackoff {
    RetryBackoff::new(DOWNSTREAM_OUTAGE_RETRY_INITIAL, DOWNSTREAM_OUTAGE_RETRY_MAX)
}

/// 给消费者配上 [`DOWNSTREAM_OUTAGE_MAX_RETRIES`] 次、按 [`downstream_outage_retry_backoff`]
/// 退避的 broker 原生重投。其余设置不变。
pub fn with_downstream_outage_retry(config: ConsumerConfig) -> ConsumerConfig {
    config
        .with_max_retries(DOWNSTREAM_OUTAGE_MAX_RETRIES)
        .with_retry_backoff(downstream_outage_retry_backoff())
}

/// 消费者的重试预算是否覆盖下游停机或重启的窗口，并落在 JetStream 的最大投递次数之内：
/// 配了退避、首投加全部重试不超过 `max_deliver`、等待合计不少于
/// [`DOWNSTREAM_OUTAGE_MIN_WINDOW`]、单次等待不超过 [`DOWNSTREAM_OUTAGE_RETRY_MAX`]。
/// 不满足时返回原因，供各服务的门禁用例使用。
pub fn check_downstream_outage_retry(
    config: &ConsumerConfig,
    max_deliver: i64,
) -> Result<Duration, String> {
    let backoff = config
        .retry_backoff
        .ok_or_else(|| "consumer retries without backoff (immediate redelivery)".to_string())?;
    if i64::from(config.max_retries) >= max_deliver {
        return Err(format!(
            "first delivery plus {} retries exceeds JetStream max_deliver {max_deliver}",
            config.max_retries
        ));
    }
    let window = backoff.total_for(config.max_retries);
    if window < DOWNSTREAM_OUTAGE_MIN_WINDOW {
        return Err(format!(
            "retry window {window:?} is shorter than {DOWNSTREAM_OUTAGE_MIN_WINDOW:?}"
        ));
    }
    if backoff.delay_for(u32::MAX) > DOWNSTREAM_OUTAGE_RETRY_MAX {
        return Err(format!(
            "single retry delay may exceed {DOWNSTREAM_OUTAGE_RETRY_MAX:?}"
        ));
    }
    if backoff.delay_for(0).is_zero() {
        return Err("first retry is immediate".to_string());
    }
    Ok(window)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn downstream_outage_retry_spans_a_restart_window_inside_default_max_deliver() {
        let config = with_downstream_outage_retry(ConsumerConfig::default());
        let window = check_downstream_outage_retry(&config, 16).expect("budget");
        assert_eq!(window, Duration::from_secs(135));
        assert_eq!(config.max_retries, DOWNSTREAM_OUTAGE_MAX_RETRIES);
        assert!(config.enable_dlq);
    }

    #[test]
    fn the_default_consumer_config_fails_the_check() {
        let error = check_downstream_outage_retry(&ConsumerConfig::default(), 16)
            .expect_err("default retries immediately");
        assert!(error.contains("without backoff"), "{error}");

        let config = with_downstream_outage_retry(ConsumerConfig::default());
        assert!(check_downstream_outage_retry(&config, 12).is_err());
    }
}

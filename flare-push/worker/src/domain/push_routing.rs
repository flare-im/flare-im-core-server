//! 按策略选择设备、按网关分组、合并单网关推送请求（纯编排，无 I/O）。

use std::collections::{HashMap, HashSet};

use flare_proto::access_gateway::{
    PushAckRequest, PushCustomRequest, PushEventRequest, PushMessageRequest,
    PushNotificationRequest, PushOptions,
};
use flare_proto::signaling::online::DeviceInfo;
use flare_proto::signaling::router::PushStrategy;

use super::model::GatewayPushTarget;

fn device_priority_i32(d: &DeviceInfo) -> i32 {
    d.priority
}

fn calculate_quality_score(quality: &Option<flare_proto::ConnectionQuality>) -> f64 {
    match quality {
        Some(q) => {
            let rtt_score = if q.rtt_ms > 0 {
                (1000.0_f64 / q.rtt_ms as f64).min(100.0_f64)
            } else {
                100.0
            };
            let loss_score = (1.0 - q.packet_loss_rate) * 100.0;
            rtt_score * 0.6 + loss_score * 0.4
        }
        None => 50.0,
    }
}

pub fn select_push_targets(
    devices: &[DeviceInfo],
    user_id: &str,
    strategy: PushStrategy,
) -> anyhow::Result<Vec<GatewayPushTarget>> {
    let mut routes: Vec<(GatewayPushTarget, i32, f64)> = devices
        .iter()
        .map(|d| {
            let t = GatewayPushTarget {
                user_id: user_id.to_string(),
                device_id: d.device_id.clone(),
                gateway_id: d.gateway_id.clone(),
            };
            let pq = device_priority_i32(d);
            let qs = calculate_quality_score(&d.connection_quality);
            (t, pq, qs)
        })
        .collect();

    let selected: Vec<GatewayPushTarget> = match strategy {
        PushStrategy::AllDevices => routes.into_iter().map(|(t, _, _)| t).collect(),
        PushStrategy::BestDevice => {
            routes.sort_by(|a, b| {
                b.1.cmp(&a.1)
                    .then_with(|| b.2.partial_cmp(&a.2).unwrap_or(std::cmp::Ordering::Equal))
            });
            routes.into_iter().take(1).map(|(t, _, _)| t).collect()
        }
        PushStrategy::ActiveDevices => routes
            .into_iter()
            .filter(|(_, pq, _)| *pq > 0)
            .map(|(t, _, _)| t)
            .collect(),
        PushStrategy::PrimaryDevice => {
            if let Some(max_p) = routes.iter().map(|(_, pq, _)| *pq).max() {
                routes.retain(|(_, pq, _)| *pq == max_p);
            }
            routes.into_iter().take(1).map(|(t, _, _)| t).collect()
        }
        _ => routes.into_iter().map(|(t, _, _)| t).collect(),
    };

    Ok(selected)
}

pub fn partition_targets_by_gateway(
    targets: &[GatewayPushTarget],
) -> HashMap<String, Vec<GatewayPushTarget>> {
    let mut m: HashMap<String, Vec<GatewayPushTarget>> = HashMap::new();
    for t in targets {
        if t.gateway_id.is_empty() {
            tracing::warn!(
                user_id = %t.user_id,
                device_id = %t.device_id,
                "skip push target: empty gateway_id"
            );
            continue;
        }
        m.entry(t.gateway_id.clone()).or_default().push(t.clone());
    }
    m
}

fn merge_user_ids_for_gateway(
    base_user_ids: &[String],
    gateway_targets: &[GatewayPushTarget],
) -> Vec<String> {
    let user_set: HashSet<String> = gateway_targets.iter().map(|t| t.user_id.clone()).collect();
    if base_user_ids.is_empty() {
        return user_set.into_iter().collect();
    }
    let filtered: Vec<String> = base_user_ids
        .iter()
        .filter(|u| user_set.contains(*u))
        .cloned()
        .collect();
    if filtered.is_empty() {
        user_set.into_iter().collect()
    } else {
        filtered
    }
}

fn device_ids_for_gateway(gateway_targets: &[GatewayPushTarget]) -> Vec<String> {
    gateway_targets
        .iter()
        .map(|t| t.device_id.clone())
        .collect()
}

fn apply_device_filter(opt: Option<PushOptions>, device_ids: Vec<String>) -> Option<PushOptions> {
    let mut o = opt.unwrap_or_default();
    o.device_ids = device_ids;
    Some(o)
}

pub fn merge_push_message_for_gateway(
    mut base: PushMessageRequest,
    gateway_targets: &[GatewayPushTarget],
) -> PushMessageRequest {
    let user_ids = merge_user_ids_for_gateway(&base.user_ids, gateway_targets);
    let device_ids = device_ids_for_gateway(gateway_targets);
    base.user_ids = user_ids;
    base.options = apply_device_filter(base.options.take(), device_ids);
    base
}

pub fn merge_push_event_for_gateway(
    mut base: PushEventRequest,
    gateway_targets: &[GatewayPushTarget],
) -> PushEventRequest {
    let user_ids = merge_user_ids_for_gateway(&base.user_ids, gateway_targets);
    let device_ids = device_ids_for_gateway(gateway_targets);
    base.user_ids = user_ids;
    base.options = apply_device_filter(base.options.take(), device_ids);
    base
}

pub fn merge_push_notification_for_gateway(
    mut base: PushNotificationRequest,
    gateway_targets: &[GatewayPushTarget],
) -> PushNotificationRequest {
    let user_ids = merge_user_ids_for_gateway(&base.user_ids, gateway_targets);
    let device_ids = device_ids_for_gateway(gateway_targets);
    base.user_ids = user_ids;
    base.options = apply_device_filter(base.options.take(), device_ids);
    base
}

pub fn merge_push_ack_for_gateway(
    mut base: PushAckRequest,
    gateway_targets: &[GatewayPushTarget],
) -> PushAckRequest {
    let user_ids = merge_user_ids_for_gateway(&base.user_ids, gateway_targets);
    let device_ids = device_ids_for_gateway(gateway_targets);
    base.user_ids = user_ids;
    base.options = apply_device_filter(base.options.take(), device_ids);
    base
}

pub fn merge_push_custom_for_gateway(
    mut base: PushCustomRequest,
    gateway_targets: &[GatewayPushTarget],
) -> PushCustomRequest {
    let user_ids = merge_user_ids_for_gateway(&base.user_ids, gateway_targets);
    let device_ids = device_ids_for_gateway(gateway_targets);
    base.user_ids = user_ids;
    base.options = apply_device_filter(base.options.take(), device_ids);
    base
}

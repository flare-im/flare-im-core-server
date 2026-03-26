/// 最小推送目标（user / device / gateway），用于按网关分组与 `PushOptions.device_ids`。
#[derive(Debug, Clone)]
pub struct GatewayPushTarget {
    pub user_id: String,
    pub device_id: String,
    pub gateway_id: String,
}

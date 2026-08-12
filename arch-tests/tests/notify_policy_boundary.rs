//! 通知策略的边界守卫。
//!
//! 守的是一条搞错就会「消息收不到」的红线：**通知偏好只能决定要不要推送，
//! 绝不能决定谁收得到消息**。两者目前共用同一份用户列表，一次顺手的过滤就能
//! 把「不弹推送」变成「消息丢了」——而这种问题在测试里往往表现为偶发，
//! 极难归因。所以用源码级断言把它钉住。

use std::fs;
use std::path::PathBuf;

const PUSH_ROUTER: &str = "flare-push/server/src/application/handlers/push_router_handler.rs";
const INGEST_SERVICE: &str = "flare-message-ingest/src/domain/service/message_ingest_service.rs";
const NOTIFY_PORT: &str = "flare-push/server/src/domain/repository/notify_policy_repository.rs";

/// 通知偏好的查询与使用只允许出现在推送侧。
///
/// 投递侧（ingest 解析收件人）一旦引入 mute 判断，被静音的人就再也收不到消息，
/// 而不只是收不到提示音。
#[test]
fn mute_lookup_never_leaks_into_delivery_recipient_resolution() {
    let root = workspace_root();
    let ingest = fs::read_to_string(root.join(INGEST_SERVICE)).expect("read ingest service");

    for forbidden in ["muted_users", "NotifyPolicyRepository", "notify_policy"] {
        assert!(
            !ingest.contains(forbidden),
            "投递侧收件人解析不得引入通知偏好（发现 `{forbidden}`）：\
             通知偏好只决定推不推送，不决定谁收得到消息。\
             需要抑制推送请在 push router 的离线分支做。"
        );
    }
}

/// 过滤只作用于离线任务，且必须放行在线用户。
#[test]
fn mute_filter_only_suppresses_offline_push() {
    let root = workspace_root();
    let router = fs::read_to_string(root.join(PUSH_ROUTER)).expect("read push router");

    assert!(
        router.contains("if !is_online && muted.contains(user_id)"),
        "免打扰过滤必须同时要求「离线」与「已静音」——\
         漏掉 !is_online 会把在线用户的实时投递也一起挡掉"
    );
    assert!(
        router.contains("fn muted_offline_users"),
        "免打扰查询必须收敛在单一入口 `muted_offline_users`，便于审计其调用时机"
    );
}

/// 查询不可用时必须放行（fail-open），并且只在有离线候选时才查询。
#[test]
fn mute_lookup_is_fail_open_and_lazy() {
    let root = workspace_root();
    let router = fs::read_to_string(root.join(PUSH_ROUTER)).expect("read push router");
    let port = fs::read_to_string(root.join(NOTIFY_PORT)).expect("read notify policy port");

    assert!(
        router.contains("HashSet::new()"),
        "通知偏好查询失败必须退化为空静音集（fail-open）：\
         免打扰是偏好不是安全边界，宁可多响一声也不能吞掉推送"
    );
    assert!(
        router.contains("if online_only {") && router.contains("let offline_candidates"),
        "必须先算出离线候选再查询：在线用户不该触发任何通知偏好查询"
    );
    assert!(
        port.contains("fail-open") || port.contains("空集"),
        "端口契约需写明失败时返回空集，避免实现方各自发挥"
    );
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root")
        .to_path_buf()
}

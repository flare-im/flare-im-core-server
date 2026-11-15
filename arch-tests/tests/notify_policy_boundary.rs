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
const PUSH_HANDOFF: &str =
    "flare-push/server/src/application/handlers/push_router_handler/handoff.rs";

/// 通知偏好的查询与使用只允许出现在推送侧。
///
/// 投递侧（ingest 解析收件人）一旦引入 mute 判断，被静音的人就再也收不到消息，
/// 而不只是收不到提示音。
#[test]
fn mute_lookup_never_leaks_into_delivery_recipient_resolution() {
    let root = workspace_root();
    let ingest = fs::read_to_string(root.join(INGEST_SERVICE)).expect("read ingest service");

    for forbidden in [
        "muted_users",
        "preferences_for",
        "NotifyPolicyRepository",
        "notify_policy",
    ] {
        assert!(
            !ingest.contains(forbidden),
            "投递侧收件人解析不得引入通知偏好（发现 `{forbidden}`）：\
             通知偏好只决定推不推送，不决定谁收得到消息。\
             需要抑制推送请在 push router 的离线分支做。"
        );
    }
}

/// 过滤只作用于离线任务：在线任务发给谁由一个碰不到通知偏好的函数决定。
///
/// 离线任务按设备发之后，「在线」与「要推送」不再是同一个用户的二选一——电脑在线的人
/// 手机照样可能要推。所以守卫不再认一行 `if !is_online && ...`，而是认结构：在线名单
/// 在一个独立函数里算出，它的参数与函数体里都没有偏好、提及或设备绑定。
#[test]
fn mute_filter_only_suppresses_offline_push() {
    let root = workspace_root();
    let router =
        production_code(&fs::read_to_string(root.join(PUSH_ROUTER)).expect("read push router"));

    let online_leg = function_source(&router, "\nfn online_task_recipients(");
    for forbidden in [
        "preference",
        "NotifyPreference",
        "muted",
        "mention",
        "devices_to_push",
        "BoundDevices",
        "is_live",
    ] {
        assert!(
            !online_leg.contains(forbidden),
            "在线任务的收件人不得依赖 `{forbidden}`：\
             免打扰、「只接收@我」、设备是否托管只能收窄推送目标，\
             收窄在线名单会让被误判的设备当场丢掉实时消息"
        );
    }
    assert!(
        router.contains(
            "let online_user_ids = online_task_recipients(user_ids, reachability, online_leg);"
        ),
        "在线名单必须只由 online_task_recipients 给出，并且在任何推送过滤之前算出"
    );
    assert_eq!(
        router.matches("push_suppressed_by_preference(").count(),
        2,
        "偏好过滤（一处定义、一处调用）只能出现在组装离线任务的地方"
    );
    assert!(
        router.contains("fn notify_preferences"),
        "通知偏好查询必须收敛在单一入口 `notify_preferences`，便于审计其调用时机"
    );
}

/// 查询不可用时必须放行（fail-open），并且只在有离线候选时才查询。
#[test]
fn mute_lookup_is_fail_open_and_lazy() {
    let root = workspace_root();
    let router = fs::read_to_string(root.join(PUSH_ROUTER)).expect("read push router");
    let handoff = fs::read_to_string(root.join(PUSH_HANDOFF)).expect("read push handoff");
    let port = fs::read_to_string(root.join(NOTIFY_PORT)).expect("read notify policy port");

    let lookup = function_source(&router, "\n    async fn notify_preferences(");
    assert!(
        lookup.contains("Err(error) =>") && lookup.contains("PreferenceLookup::Unavailable"),
        "通知偏好查询失败必须退化为「偏好未知」（fail-open）：\
         免打扰是偏好不是安全边界，宁可多响一声也不能吞掉推送"
    );
    // 免打扰与「只接收@我」读的是同一批参与者行；分开查会让每条消息的 RPC
    // 按偏好种类翻倍——加第二种偏好时踩过一次，用源码断言钉住合并后的形状。
    assert_eq!(
        router.matches(".preferences_for(").count(),
        1,
        "通知偏好只允许有一处查询调用：每加一种偏好就多查一遍，成本按种类翻倍"
    );
    // 设备交接的补提示是同一个路由器的另一个入口，必须走同一个查询入口。
    assert!(
        !handoff.contains(".preferences_for(") && handoff.contains(".notify_preferences("),
        "补提示必须复用 notify_preferences，不得另起一处通知偏好查询"
    );
    assert!(
        router.contains("if online_only {") && router.contains("let offline_candidates"),
        "必须先算出离线候选再查询：没有设备要推的用户不该触发任何通知偏好查询"
    );
    assert!(
        port.contains("fail-open") || port.contains("空集"),
        "端口契约需写明调用方对查询失败 fail-open，避免实现方各自发挥"
    );
}

/// 补提示只产出离线任务，不碰在线一路。
#[test]
fn device_handoff_never_publishes_online_tasks() {
    let root = workspace_root();
    let handoff = fs::read_to_string(root.join(PUSH_HANDOFF)).expect("read push handoff");

    assert!(
        handoff.contains("publish_offline_task(") && !handoff.contains("publish_online_task("),
        "设备交接补的是推送提示：在线投递早已由正常路由完成，再发一遍就是重复消息"
    );
}

/// 去掉文件末尾的测试模块，只留生产代码。
fn production_code(source: &str) -> String {
    match source.find("\n#[cfg(test)]\nmod ") {
        Some(end) => source[..end].to_string(),
        None => source.to_string(),
    }
}

/// 从 `signature` 起到这个函数体结束（与签名同缩进的右花括号）为止的源码。
fn function_source(source: &str, signature: &str) -> String {
    let start = source
        .find(signature)
        .unwrap_or_else(|| panic!("`{}` not found", signature.trim()));
    let body = &source[start + 1..];
    let indent = body.len() - body.trim_start_matches(' ').len();
    let closing = format!("\n{}}}\n", " ".repeat(indent));
    let end = body
        .find(&closing)
        .unwrap_or_else(|| panic!("end of `{}` not found", signature.trim()));
    body[..end + closing.len()].to_string()
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root")
        .to_path_buf()
}

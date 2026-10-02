//! 会话服务的 MQ 事件消费者只按事件自己声明的租户写库（W9-S3 F1）。
//!
//! 消费者曾用 `Context::root().with_tenant_id("0")` 处理消息与已读事件、会话确保在载荷缺租户时
//! 落到 `"0"`：非 0 租户的会话 seq 与成员未读、已读游标从不推进；单聊会话 ID 不含租户，同一
//! user_id 对出现在两个租户时，非 0 租户的事件还会改写租户 0 的同 ID 会话。现在租户只取事件的
//! 信封头、传输层消息头与载荷字段，取不到就拒绝。这里守住生产代码里不再出现把租户写死或兜底成
//! `"0"` 的写法；行为本身由 `flare-conversation` 的单测与真库用例覆盖。

use std::fs;
use std::path::PathBuf;

const CONSUMER: &str = "flare-conversation/src/infrastructure/event_consumer.rs";

#[test]
fn conversation_event_consumer_never_defaults_the_tenant() {
    let path = workspace_root().join(CONSUMER);
    let content = fs::read_to_string(&path).expect("read conversation event consumer");
    let production = strip_inline_test_modules(&content);

    let violations: Vec<&str> = [
        r#"with_tenant_id("0")"#,
        r#"unwrap_or("0")"#,
        r#""0".to_string()"#,
        r#""0".into()"#,
        "DEFAULT_TENANT_ID",
    ]
    .into_iter()
    .filter(|pattern| production.contains(pattern))
    .collect();

    assert!(
        violations.is_empty(),
        "{CONSUMER} must take the tenant from the event and reject events without one, \
         not default to tenant \"0\"; found: {violations:?}"
    );
    assert!(
        production.contains("fn resolve_event_tenant("),
        "{CONSUMER} lost resolve_event_tenant; update this guard together with the consumer"
    );
}

fn strip_inline_test_modules(content: &str) -> String {
    let mut output = String::new();
    let mut pending_test_cfg = false;
    let mut skipping_test_module = false;
    let mut brace_depth = 0isize;

    for line in content.lines() {
        let trimmed = line.trim_start();

        if skipping_test_module {
            brace_depth += brace_delta(line);
            if brace_depth <= 0 {
                skipping_test_module = false;
            }
            continue;
        }

        if trimmed.starts_with("#[cfg(test)]") {
            pending_test_cfg = true;
            continue;
        }

        if pending_test_cfg && (trimmed.starts_with("mod ") || trimmed.starts_with("pub mod ")) {
            skipping_test_module = true;
            brace_depth = brace_delta(line);
            if brace_depth <= 0 {
                brace_depth = 1;
            }
            pending_test_cfg = false;
            continue;
        }

        pending_test_cfg = false;
        output.push_str(line);
        output.push('\n');
    }

    output
}

fn brace_delta(line: &str) -> isize {
    line.chars().filter(|ch| *ch == '{').count() as isize
        - line.chars().filter(|ch| *ch == '}').count() as isize
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root")
        .to_path_buf()
}

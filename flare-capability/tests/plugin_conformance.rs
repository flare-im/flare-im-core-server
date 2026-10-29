//! 插件平台**一致性套件**：断言设计里那几条不变量在代码层面仍然成立。
//!
//! 见 `docs/design/plugin-platform.md` §3。这套东西存在的理由只有一个：
//! 那些不变量是**约定**，而约定在没有护栏时会腐烂。前四项改造刚刚把
//! 「核心不认识具体插件」「声明即边界」立起来，如果只靠人记得，
//! 下一次加插件时最省事的写法仍然是往通用路径里塞一个 if。
//!
//! # 这里为什么有「扫源码」的测试
//!
//! 不变量 3.3（核心不认识任何具体插件）与 3.6（kind 专有数据是不透明载荷）
//! 都是**结构性**约束，没有对应的运行时行为可断言 —— 一个通用分发器里多写
//! 一个 `if capability_id.starts_with("xxx.")`，功能测试全都会通过。
//! 唯一能抓住它的办法就是检查源码本身。
//!
//! 扫描是白名单制：kind 专有的模块（适配器、约定常量、组合根）本来就
//! 应该认识自己的 id，它们不在扫描范围内。白名单越短越好 —— 每加一项都要
//! 说明为什么它不是通用路径。

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

fn src_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("src")
}

/// kind 专有模块：允许认识具体插件的 id。
///
/// 每一项的理由：
/// - `plugin_contract.rs`：约定常量的**唯一**定义处，集中就是为了别处不再出现字面量。
/// - `adapters/`：各 kind 的后端适配器，实现领域端口本就是 kind 专有代码。
/// - `use_case_samples.rs`：示例，不进任何运行路径。
/// - `composition/`：组合根。「本部署装配哪些后端」是**装配决策**而非核心知识，
///   装配处必须能点名它要装什么，否则就没法装配了。
const KIND_SPECIFIC_ALLOWLIST: &[&str] = &[
    "infrastructure/capability/plugin_contract.rs",
    "infrastructure/capability/adapters/",
    "infrastructure/capability/use_case_samples.rs",
    "composition/",
];

/// 具体插件的标识片段。出现在通用路径里即为违反。
const PLUGIN_SPECIFIC_MARKERS: &[&str] = &["\"rtc.", "rtc.media.control", "SfuControl", "sfu_"];

fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).expect("读取源码目录") {
        let path = entry.expect("目录项").path();
        if path.is_dir() {
            rust_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

/// 去掉注释再扫。
///
/// 不去的话，**讲述这条规则的注释本身**会被判成违规 —— 本文件与
/// `capability_dispatch.rs` 的模块注释里都写着「这里曾经写着
/// `starts_with("rtc.")`」，那是说明历史，不是逻辑。
/// 截掉 `#[cfg(test)]` 之后的内容。
///
/// 测试里出现具体插件 id 往往恰恰是为了证明核心**不**特殊对待它 ——
/// `capability_dispatch.rs` 的用例就断言「空路由表下 vendorx.call.video 只是普通
/// 未认领 id」。把这类断言判成违规，等于逼人删掉最有价值的那条测试。
fn strip_test_modules(source: &str) -> String {
    match source.find("#[cfg(test)]") {
        Some(idx) => source[..idx].to_string(),
        None => source.to_string(),
    }
}

/// 整个文件只在测试里编译：父模块用 `#[cfg(test)]` 引入的 `mod x;`（可带 `#[path = "..."]`），
/// 以及这些文件再引入的子模块。这类文件里没有 `#[cfg(test)]` 字样，[`strip_test_modules`]
/// 认不出来（`source_audit_pg_tests.rs` 就是这样被误判的）。只认父模块的引入声明，不按文件名
/// 猜：一个叫 `*_tests.rs` 却被正常引入的文件照样要扫。
fn test_only_files(files: &[PathBuf]) -> BTreeSet<PathBuf> {
    let mut test_only = BTreeSet::new();
    let mut pending: Vec<(PathBuf, bool)> = files.iter().map(|f| (f.clone(), false)).collect();
    // 先从每个文件里找 cfg(test) 引入；被引入的文件里的全部子模块也是测试代码。
    while let Some((file, inherited)) = pending.pop() {
        let Ok(source) = fs::read_to_string(&file) else {
            continue;
        };
        for (child, gated) in module_declarations(&file, &source) {
            if (inherited || gated) && test_only.insert(child.clone()) {
                pending.push((child, true));
            }
        }
    }
    test_only
}

/// `file` 里每一条文件外模块声明（`mod x;`）解析到的文件，以及它是否带 `#[cfg(test)]`。
fn module_declarations(file: &Path, source: &str) -> Vec<(PathBuf, bool)> {
    let dir = file.parent().expect("源码目录");
    let owns_dir = matches!(
        file.file_name().and_then(|n| n.to_str()),
        Some("mod.rs" | "lib.rs" | "main.rs")
    );
    let mut out = Vec::new();
    let mut cfg_test = false;
    let mut path_attr: Option<String> = None;
    for line in source.lines() {
        let t = line.trim();
        if t.starts_with("#[") {
            if t.replace(' ', "") == "#[cfg(test)]" {
                cfg_test = true;
            }
            if let Some(rest) = t.strip_prefix("#[path") {
                path_attr = rest.split('"').nth(1).map(str::to_owned);
            }
            continue;
        }
        if t.is_empty() || t.starts_with("//") {
            continue;
        }
        let decl = t
            .strip_prefix("pub(crate) ")
            .or_else(|| t.strip_prefix("pub(super) "))
            .or_else(|| t.strip_prefix("pub "))
            .unwrap_or(t);
        if let Some(name) = decl.strip_prefix("mod ").and_then(|r| r.strip_suffix(';')) {
            let base = if owns_dir {
                dir.to_path_buf()
            } else {
                dir.join(file.file_stem().expect("文件名"))
            };
            let target = match path_attr.take() {
                // 非 mod.rs 文件里的 `#[path]` 相对该文件所在目录。
                Some(path) => dir.join(path),
                None => {
                    let flat = base.join(format!("{}.rs", name.trim()));
                    if flat.exists() {
                        flat
                    } else {
                        base.join(name.trim()).join("mod.rs")
                    }
                }
            };
            out.push((target, cfg_test));
        }
        cfg_test = false;
        path_attr = None;
    }
    out
}

/// 判断是否是**模块树接线**行：`mod x;` 与 `pub use x::Y;`。
///
/// 模块树必须给子模块起名字，`routing/mod.rs` 里出现 `mod sfu_health_probe;`
/// 是结构性必然，不是「通用路径认识了具体插件」。
///
/// 刻意**不**跳过普通 `use` 导入：通用文件若 `use ...SfuControlClient`，
/// 那它一定要在逻辑里用到，跳过导入行会给这类真违反留一个洞。
fn is_module_wiring(line: &str) -> bool {
    let t = line.trim_start();
    let t = t.strip_prefix("pub ").unwrap_or(t);
    (t.starts_with("mod ") && t.ends_with(';'))
        || (t.starts_with("use ") && line.trim_start().starts_with("pub use "))
}

fn strip_comments(source: &str) -> String {
    let mut out = String::with_capacity(source.len());
    for line in source.lines() {
        let trimmed = line.trim_start();
        if trimmed.starts_with("//") || is_module_wiring(line) {
            out.push('\n');
            continue;
        }
        out.push_str(line);
        out.push('\n');
    }
    out
}

fn relative(path: &Path) -> String {
    path.strip_prefix(src_root())
        .expect("源码路径")
        .to_string_lossy()
        .replace('\\', "/")
}

fn is_kind_specific(rel: &str) -> bool {
    KIND_SPECIFIC_ALLOWLIST
        .iter()
        .any(|allowed| rel == *allowed || rel.starts_with(allowed))
}

/// 不变量 3.3：核心不认识任何具体插件。
///
/// 这条曾经被破过一次 —— `capability_dispatch.rs` 里的
/// `if req.capability_id.starts_with("rtc.")`，以及所有插件共用的健康检查器里的
/// `if capability_id == "rtc.media.control"`。两处都不是 bug，是「加个 if 最省事」
/// 的自然结果，所以必须由门禁而不是自觉来挡。
#[test]
fn generic_paths_do_not_know_specific_plugins() {
    let mut files = Vec::new();
    rust_files(&src_root(), &mut files);
    assert!(files.len() > 20, "扫描没找到源码，测试会空转成假绿");

    let mut violations = BTreeSet::new();
    let mut scanned = 0usize;
    let test_only = test_only_files(&files);
    assert!(
        test_only
            .iter()
            .any(|f| f.ends_with("source_audit_pg_tests.rs")),
        "认不出父模块以 #[cfg(test)] 引入的测试文件，判据是坏的"
    );

    for file in files {
        let rel = relative(&file);
        if is_kind_specific(&rel) || test_only.contains(&file) {
            continue;
        }
        scanned += 1;
        let source = strip_comments(&strip_test_modules(
            &fs::read_to_string(&file).expect("读取源码"),
        ));
        for marker in PLUGIN_SPECIFIC_MARKERS {
            if source.contains(marker) {
                violations.insert(format!("{rel}: 出现具体插件标识 {marker}"));
            }
        }
    }

    assert!(scanned > 15, "白名单太宽，实际只扫了 {scanned} 个文件");
    assert!(
        violations.is_empty(),
        "通用路径里出现了具体插件的知识：\n{}\n\n\
         要加插件专有逻辑，请放到插件自己的服务里，\n\
         或让插件在注册时**声明**它的需求（如 labels 里的 health_protocol），\n\
         而不是在通用路径里加分支。",
        violations
            .iter()
            .map(|v| format!("  - {v}"))
            .collect::<Vec<_>>()
            .join("\n")
    );
}

/// 不变量 3.6：kind 专有数据一律是不透明载荷。
///
/// 通用宿主的类型签名里不得出现 kind 专有类型。删掉的那套骨架就是反例：
/// 通用 trait 的 `descriptor()` 返回 `RtcBackendDescriptor`，加第二个 kind
/// 必然要改签名。
#[test]
fn generic_host_types_carry_no_kind_specific_types() {
    let mut files = Vec::new();
    rust_files(&src_root(), &mut files);

    // 这些类型名一旦出现在通用路径里，就说明 kind 专有**数据/客户端**漏进了宿主。
    //
    // 不变量 3.6 说的是 kind 专有的**载荷类型**（如已删除的 `RtcBackendDescriptor`）
    // 不得出现在通用结构里。
    const KIND_TYPES: &[&str] = &["RtcBackendDescriptor", "SfuControlClient"];

    let mut violations = BTreeSet::new();
    let test_only = test_only_files(&files);
    for file in files {
        let rel = relative(&file);
        if is_kind_specific(&rel) || test_only.contains(&file) {
            continue;
        }
        // 领域端口与注册表是能力的定义与装配处，不算通用宿主。
        if rel.starts_with("domain/capability/")
            || rel.starts_with("infrastructure/capability/registration/")
            || rel.starts_with("composition/")
            || rel.starts_with("interface/grpc/")
        {
            continue;
        }
        let source = strip_comments(&strip_test_modules(
            &fs::read_to_string(&file).expect("读取源码"),
        ));
        for ty in KIND_TYPES {
            if source.contains(ty) {
                violations.insert(format!("{rel}: 通用路径出现 kind 专有类型 {ty}"));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "kind 专有类型漏进了通用宿主：\n{}\n\n\
         kind 专有的数据应当作为不透明载荷（labels / JSON attributes）传递，\n\
         由对应 kind 的适配器解释。",
        violations
            .iter()
            .map(|v| format!("  - {v}"))
            .collect::<Vec<_>>()
            .join("\n")
    );
}

/// 套件自检：故意造一个违反，扫描必须能抓住它。
///
/// 这条是**对门禁本身的门禁**。设计文档里写死了判据：
/// 「套件本身要能抓出故意违反不变量的实现」。没有这条，上面两个测试
/// 可能因为路径写错、白名单过宽而永远绿着 —— 那比没有门禁更糟，
/// 因为它给人以已被守护的错觉。
#[test]
fn the_scanner_actually_catches_a_violation() {
    let injected = r#"
        fn dispatch(capability_id: &str) {
            if capability_id.starts_with("rtc.") {
                unreachable!()
            }
        }
    "#;
    let cleaned = strip_comments(injected);
    assert!(
        PLUGIN_SPECIFIC_MARKERS.iter().any(|m| cleaned.contains(m)),
        "扫描器抓不住一个明显的违反，它是坏的"
    );

    // 反向：讲述规则的注释不该被误判。
    let comment_only = "        // 这里曾经写着 starts_with(\"rtc.\") —— 现在由路由表接管\n";
    assert!(
        !PLUGIN_SPECIFIC_MARKERS
            .iter()
            .any(|m| strip_comments(comment_only).contains(m)),
        "注释被误判成违规，会逼人删掉解释历史的注释"
    );

    // 只跳过父模块以 #[cfg(test)] 引入的文件（及其子模块），不按文件名猜。
    let root = std::env::temp_dir().join(format!("plugin-conformance-{}", std::process::id()));
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(root.join("dispatch")).expect("临时目录");
    let write = |path: &str, text: &str| fs::write(root.join(path), text).expect("写临时文件");
    write(
        "mod.rs",
        "#[cfg(test)]\n#[path = \"gated_tests.rs\"]\nmod gated;\nmod plain_tests;\nmod dispatch;\n",
    );
    write(
        "gated_tests.rs",
        "mod nested;\nconst X: &str = \"rtc.call\";\n",
    );
    write("plain_tests.rs", "const X: &str = \"rtc.call\";\n");
    write("dispatch.rs", "#[cfg(test)]\nmod inner;\n");
    fs::create_dir_all(root.join("gated_tests")).expect("临时目录");
    write("gated_tests/nested.rs", "");
    write("dispatch/inner.rs", "");
    let files: Vec<PathBuf> = ["mod.rs", "gated_tests.rs", "plain_tests.rs", "dispatch.rs"]
        .iter()
        .map(|f| root.join(f))
        .collect();
    let gated = test_only_files(&files);
    let _ = fs::remove_dir_all(&root);
    assert!(gated.contains(&root.join("gated_tests.rs")), "{gated:?}");
    assert!(
        gated.contains(&root.join("gated_tests/nested.rs")),
        "{gated:?}"
    );
    assert!(gated.contains(&root.join("dispatch/inner.rs")), "{gated:?}");
    assert!(
        !gated.contains(&root.join("plain_tests.rs")),
        "正常引入的 *_tests.rs 也被跳过了：名字不是测试的证据"
    );
    assert!(!gated.contains(&root.join("dispatch.rs")), "{gated:?}");
}

/// 每一个**策略变更** RPC 都必须留痕。
///
/// 今天 grant / revoke / tenant_switch 三处都记了 —— 我第一次核对时曾误判成
/// 「只有吊销留痕」，因为按函数名向后取了固定行数的窗口，而审计调用落在窗口外。
/// 这条测试的价值正在于此：把「有没有留痕」变成机器判定，而不是靠人用 grep
/// 取一段窗口来目测。
///
/// 判定方式：策略变更类 RPC 的函数体里必须出现 `record_policy_event`。
/// 新增第四个变更点却忘了审计时，这里会红。
#[test]
fn every_policy_mutation_records_an_audit_event() {
    let service = src_root().join("interface/grpc/capability/service.rs");
    // 去注释：否则把审计调用注释掉也能骗过这条门禁。
    let source = strip_comments(&fs::read_to_string(&service).expect("读取 service.rs"));

    // 策略变更 RPC：改变了「谁能用什么」的持久状态。
    const MUTATIONS: &[&str] = &[
        "async fn grant_user_capability",
        "async fn revoke_user_capability",
        "async fn set_tenant_capability_switch",
    ];

    let mut missing = Vec::new();
    for m in MUTATIONS {
        let start = source
            .find(m)
            .unwrap_or_else(|| panic!("找不到策略变更 RPC：{m}（改名了？测试要跟着改）"));
        // 取到下一个 `    async fn ` 为止，即该函数的完整体，而不是固定行数窗口。
        let rest = &source[start + m.len()..];
        let end = rest.find("\n    async fn ").unwrap_or(rest.len());
        if !rest[..end].contains("record_policy_event") {
            missing.push(*m);
        }
    }

    assert!(
        missing.is_empty(),
        "以下策略变更没有留痕：{missing:?}\n\
         授权的授予/吊销/开关一旦无痕，计费争议就无从对账。"
    );
}

/// 分发路径**不查**端点路由簿。
///
/// 业务分发的目标只来自控制面认证下发、本地持久化的授权投影；插件端点的自注册与
/// 配置发现只用于诊断展示，不能授予执行。`list_filtered` 一旦重新出现在分发链路里，
/// 就说明有人又把诊断读模型当成了路由来源 —— 测试可能照样全绿，授权边界却已经松了，
/// 所以用源码断言钉住。
#[test]
fn dispatch_path_never_routes_through_the_endpoint_book() {
    let dispatch =
        fs::read_to_string(src_root().join("application/handler/capability_dispatch.rs"))
            .expect("读取分发器");
    let remote = fs::read_to_string(src_root().join("application/handler/remote_dispatch.rs"))
        .expect("读取远端分发");

    for (name, source) in [("分发器", &dispatch), ("远端分发", &remote)] {
        let lookups = strip_comments(&strip_test_modules(source))
            .matches("list_filtered")
            .count();
        assert_eq!(
            lookups, 0,
            "{name}里 list_filtered 出现 {lookups} 次；分发目标只能来自授权投影"
        );
    }
}

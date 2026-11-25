use std::path::PathBuf;

#[test]
fn development_tls_material_is_not_checked_in() {
    let root = repository_root();
    let forbidden = [
        "certs/server.key",
        "certs/server.crt",
        "flare-im-transport/certs/server.key",
        "flare-im-transport/certs/server.crt",
        "flare-im-transport/examples/certs/server.key",
        "flare-im-transport/examples/certs/server.crt",
        "flare-im-core-engine/certs/server.key",
        "flare-im-core-engine/certs/server.crt",
        "flare-im-core-engine/examples/certs/server.key",
        "flare-im-core-engine/examples/certs/server.crt",
    ];

    // 判据是「checked in」：本地生成的开发证书留在磁盘上是正常工作流
    // （.gitignore 已覆盖 certs/），守卫真正要拦的是它们进入 git 追踪。
    // 用磁盘存在性判定会误伤本地工作区（2026-11-24 曾因此红在主干）。
    let leaked = forbidden
        .iter()
        .filter(|path| git_tracks(&root, path))
        .copied()
        .collect::<Vec<_>>();

    assert!(
        leaked.is_empty(),
        "development TLS material must be generated locally, not checked in:\n{}",
        leaked.join("\n")
    );
}

/// 该路径是否被 git 追踪（在其所属仓库内执行 `git ls-files --`，非零退出按未追踪处理）。
/// 兄弟仓（flare-im-transport/...）的文件要在它自己的仓库里查：根仓的索引看不到它们。
fn git_tracks(root: &std::path::Path, rel: &str) -> bool {
    let (repo, in_repo_path) = match rel.split_once('/') {
        Some((first, rest)) if root.join(first).join(".git").exists() => {
            (root.join(first), rest.to_string())
        }
        _ => (root.to_path_buf(), rel.to_string()),
    };
    std::process::Command::new("git")
        .args(["ls-files", "--"])
        .arg(&in_repo_path)
        .current_dir(repo)
        .output()
        .map(|output| {
            output.status.success() && !String::from_utf8_lossy(&output.stdout).trim().is_empty()
        })
        .unwrap_or(false)
}

fn repository_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("flare-im-core root")
        .parent()
        .expect("repository root")
        .to_path_buf()
}

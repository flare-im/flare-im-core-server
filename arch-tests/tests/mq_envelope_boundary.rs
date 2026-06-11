use std::fs;
use std::path::{Path, PathBuf};

#[test]
fn mq_envelope_is_the_only_message_and_event_mq_envelope() {
    let root = workspace_root();
    let repo_root = root.parent().expect("flare-im repo root");

    let old_message = ["Message", "Envelope"].join("");
    let old_event = ["TopicEvent", "Envelope"].join("");
    let forbidden = [old_message.as_str(), old_event.as_str()];

    let mut violations = Vec::new();
    collect_rs_files(&root, &root, &mut violations, &forbidden);

    assert!(
        violations.is_empty(),
        "flare-im-core must use MqEnvelope for message/event MQ payloads:\n{}",
        violations.join("\n")
    );

    let proto_path = repo_root.join("flare-proto/proto/topic_envelope.proto");
    let proto = fs::read_to_string(&proto_path).expect("read topic envelope proto");
    for pattern in forbidden {
        assert!(
            !proto.contains(pattern),
            "{} must not define old MQ envelope `{pattern}`",
            proto_path.display()
        );
    }
    assert!(
        proto.contains("message MqEnvelope"),
        "{} must retain the canonical MqEnvelope",
        proto_path.display()
    );
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root")
        .to_path_buf()
}

fn collect_rs_files(root: &Path, dir: &Path, violations: &mut Vec<String>, forbidden: &[&str]) {
    for entry in fs::read_dir(dir).expect("read source directory") {
        let entry = entry.expect("read source entry");
        let path = entry.path();
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("");

        if path.is_dir() {
            if matches!(file_name, "target" | ".git") {
                continue;
            }
            collect_rs_files(root, &path, violations, forbidden);
            continue;
        }

        if path.extension().is_some_and(|ext| ext == "rs") {
            let content = fs::read_to_string(&path).expect("read rust source");
            for pattern in forbidden {
                if content.contains(pattern) {
                    violations.push(format!(
                        "{} contains old MQ envelope `{pattern}`",
                        path.strip_prefix(root).unwrap_or(path.as_path()).display()
                    ));
                }
            }
        }
    }
}

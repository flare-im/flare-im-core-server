use std::fs;
use std::path::{Path, PathBuf};

#[test]
fn conversation_crate_does_not_own_send_or_sequence_allocation() {
    let workspace = workspace_root();
    let conversation = workspace.join("flare-conversation/src");
    let violations = find_forbidden_terms(
        &conversation,
        &[
            "SendMessageRequest",
            "SendMessageResponse",
            "MessageIngestService",
            "MessageIngestClient",
            "SequenceAllocator",
            "allocate_seq",
            "next_seq",
            "send_message(",
        ],
    );

    assert!(
        violations.is_empty(),
        "flare-conversation must stay metadata/read-model only; send/seq terms found:\n{}",
        violations.join("\n")
    );
}

#[test]
fn gateway_send_paths_route_to_message_ingest_boundary() {
    let workspace = workspace_root();

    let api_gateway_handler =
        read_source(&workspace.join("flare-api-gateway/src/interface/http/message_handler.rs"));
    assert!(
        api_gateway_handler.contains("message_ingest")
            || api_gateway_handler.contains("MessageIngest"),
        "api-gateway message handler must route send requests to flare-message-ingest"
    );
    assert!(
        !api_gateway_handler.contains("message_orchestrator"),
        "api-gateway send path must not route new sends to message-orchestrator"
    );

    let signaling_forwarder =
        read_source(&workspace.join("flare-signaling/route/src/infrastructure/forwarder.rs"));
    let forward_message_body = function_body(&signaling_forwarder, "pub async fn forward_message");
    assert!(
        forward_message_body.contains("MESSAGE_INGEST")
            || forward_message_body.contains("message-ingest"),
        "signaling route forwarder must route message frames to flare-message-ingest"
    );
    assert!(
        !forward_message_body.contains("MESSAGE_ORCHESTRATOR")
            && !forward_message_body.contains("message-orchestrator"),
        "signaling route forwarder must not route message frames to message-orchestrator"
    );
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("workspace root")
        .to_path_buf()
}

fn read_source(path: &Path) -> String {
    fs::read_to_string(path).unwrap_or_else(|err| panic!("read {}: {err}", path.display()))
}

fn function_body<'a>(source: &'a str, signature: &str) -> &'a str {
    let start = source
        .find(signature)
        .unwrap_or_else(|| panic!("missing function `{signature}`"));
    let mut depth = 0usize;
    let mut body_start = None;
    for (offset, ch) in source[start..].char_indices() {
        match ch {
            '{' => {
                depth += 1;
                body_start.get_or_insert(start + offset);
            }
            '}' if depth > 0 => {
                depth -= 1;
                if depth == 0 {
                    return &source[body_start.expect("body start")..=start + offset];
                }
            }
            _ => {}
        }
    }
    panic!("unterminated function `{signature}`")
}

fn find_forbidden_terms(root: &Path, terms: &[&str]) -> Vec<String> {
    let mut violations = Vec::new();
    visit_rust_files(root, &mut |path| {
        let source = read_source(path);
        for (line_index, line) in source.lines().enumerate() {
            for term in terms {
                if line.contains(term) {
                    violations.push(format!(
                        "{}:{} contains `{}`",
                        path.display(),
                        line_index + 1,
                        term
                    ));
                }
            }
        }
    });
    violations
}

fn visit_rust_files(root: &Path, visit: &mut impl FnMut(&Path)) {
    for entry in
        fs::read_dir(root).unwrap_or_else(|err| panic!("read dir {}: {err}", root.display()))
    {
        let entry = entry.expect("directory entry");
        let path = entry.path();
        if path.is_dir() {
            visit_rust_files(&path, visit);
        } else if path.extension().and_then(|ext| ext.to_str()) == Some("rs") {
            visit(&path);
        }
    }
}

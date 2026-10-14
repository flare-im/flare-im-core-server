//! Offline contract export from the owning Core HTTP serializers.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let document = flare_admin_gateway::interface::http::admin_openapi();
    let rendered = serde_json::to_string_pretty(&document)? + "\n";
    if let Some(path) = std::env::args().nth(1) {
        std::fs::write(path, rendered)?;
    } else {
        print!("{rendered}");
    }
    Ok(())
}

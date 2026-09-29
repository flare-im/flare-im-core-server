mod error;
mod manifest;
mod supervisor;

use std::{env, path::PathBuf};

use error::{Result, ensure, fail};
use manifest::Manifest;

const DEFAULT_MANIFEST: &str = "/etc/flare/supervise-core.toml";
const DEFAULT_SOCKET: &str = "/tmp/flare-supervise/health.sock";

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = env::args().skip(1);
    let Some(command) = args.next() else {
        return usage_error();
    };
    if command == "--help" || command == "-h" {
        print_usage();
        return Ok(());
    }
    ensure!(
        matches!(command.as_str(), "supervise" | "validate" | "health"),
        "unknown command: {command}"
    );
    let mut manifest = PathBuf::from(
        env::var("FLARE_SUPERVISE_MANIFEST").unwrap_or_else(|_| DEFAULT_MANIFEST.into()),
    );
    let mut socket = PathBuf::from(
        env::var("FLARE_SUPERVISE_HEALTH_SOCKET").unwrap_or_else(|_| DEFAULT_SOCKET.into()),
    );
    let mut selector = "all".to_owned();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--services" => {
                selector = args
                    .next()
                    .ok_or_else(|| error::FlareError::system("--services needs a value"))?
            }
            "--manifest" => {
                manifest = args
                    .next()
                    .ok_or_else(|| error::FlareError::system("--manifest needs a value"))?
                    .into()
            }
            "--socket" => {
                socket = args
                    .next()
                    .ok_or_else(|| error::FlareError::system("--socket needs a value"))?
                    .into()
            }
            _ if arg.starts_with("--services=") => selector = arg[11..].into(),
            _ if arg.starts_with("--manifest=") => manifest = arg[11..].into(),
            _ if arg.starts_with("--socket=") => socket = arg[9..].into(),
            _ => fail!("unknown option: {arg}"),
        }
    }
    if command == "health" {
        return supervisor::health(&socket).await;
    }
    let manifest = Manifest::load(&manifest)?;
    let selected = manifest.select(&selector)?;
    if command == "validate" {
        println!(
            "supervisor manifest valid; selected: {}",
            selected
                .iter()
                .map(|service| service.name.as_str())
                .collect::<Vec<_>>()
                .join(",")
        );
        return Ok(());
    }
    supervisor::supervise(manifest, selected, &socket).await
}

fn usage_error() -> Result<()> {
    print_usage();
    fail!("missing command")
}

fn print_usage() {
    eprintln!(
        "Usage: flare-core supervise|validate [--services=all|a,b] [--manifest=/absolute/path] [--socket=/absolute/path]"
    );
    eprintln!("       flare-core health [--socket=/absolute/path]");
}

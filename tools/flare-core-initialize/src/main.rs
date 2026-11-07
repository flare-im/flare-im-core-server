use anyhow::Result;
use sqlx::{Connection, PgConnection, postgres::PgConnectOptions};
use std::{str::FromStr, time::Duration};

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args == ["--help"] {
        println!(
            "flare-core-initialize\nRequires CORE_INITIAL_DATABASE_URL and CORE_INITIAL_DATABASE. New empty owned database and isolated core_initialize login only; no old import/backfill/adoption."
        );
        return Ok(());
    }
    anyhow::ensure!(args.is_empty(), "usage: flare-core-initialize [--help]");
    let url = std::env::var("CORE_INITIAL_DATABASE_URL")
        .map_err(|_| anyhow::anyhow!("CORE_INITIAL_DATABASE_URL is required"))?;
    let expected = std::env::var("CORE_INITIAL_DATABASE")
        .map_err(|_| anyhow::anyhow!("CORE_INITIAL_DATABASE is required"))?;
    anyhow::ensure!(
        !expected.is_empty()
            && !["postgres", "template0", "template1"].contains(&expected.as_str()),
        "explicit new Core database required"
    );
    let options = PgConnectOptions::from_str(&url)
        .map_err(|_| anyhow::anyhow!("invalid Core initializer configuration"))?;
    anyhow::ensure!(
        options.get_database() == Some(expected.as_str()),
        "Core initial target differs from expected database"
    );
    let mut connection = tokio::time::timeout(
        Duration::from_secs(10),
        PgConnection::connect_with(&options),
    )
    .await
    .map_err(|_| anyhow::anyhow!("Core initializer connection timed out"))?
    .map_err(|_| anyhow::anyhow!("Core initializer database unavailable"))?;
    let result = flare_core_initialize::initialize(&mut connection).await;
    let _ = connection.close().await;
    println!(
        "{}",
        serde_json::json!({"schema":"flare.core.initialization/v1","created":result?})
    );
    Ok(())
}

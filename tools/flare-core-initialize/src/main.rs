use flare_core_initialize::ensure;
use flare_server_core::error::{FlareError, Result};
use sqlx::{Connection, PgConnection, postgres::PgConnectOptions};
use std::{str::FromStr, time::Duration};

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args == ["--help"] {
        println!(
            "flare-core-initialize [upgrade]\nRequires CORE_INITIAL_DATABASE_URL and CORE_INITIAL_DATABASE. No argument initializes a fresh empty owned v4 database; upgrade accepts only the archived initial v1, v2 or v3 model. Isolated database-owner core_initialize login required; no SQLx adoption/import/backfill."
        );
        return Ok(());
    }
    ensure(
        args.is_empty() || args == ["upgrade"],
        "usage: flare-core-initialize [upgrade|--help]",
    )?;
    let upgrading = args == ["upgrade"];
    let url = std::env::var("CORE_INITIAL_DATABASE_URL")
        .map_err(|_| FlareError::system("CORE_INITIAL_DATABASE_URL is required"))?;
    let expected = std::env::var("CORE_INITIAL_DATABASE")
        .map_err(|_| FlareError::system("CORE_INITIAL_DATABASE is required"))?;
    ensure(
        !expected.is_empty()
            && !["postgres", "template0", "template1"].contains(&expected.as_str()),
        "explicit Core target database required",
    )?;
    let options = PgConnectOptions::from_str(&url)
        .map_err(|_| FlareError::system("invalid Core initializer configuration"))?;
    ensure(
        options.get_database() == Some(expected.as_str()),
        "Core initial target differs from expected database",
    )?;
    let mut connection = tokio::time::timeout(
        Duration::from_secs(10),
        PgConnection::connect_with(&options),
    )
    .await
    .map_err(|_| FlareError::system("Core initializer connection timed out"))?
    .map_err(|_| FlareError::system("Core initializer database unavailable"))?;
    let result = if upgrading {
        flare_core_initialize::upgrade(&mut connection).await
    } else {
        flare_core_initialize::initialize(&mut connection).await
    };
    let _ = connection.close().await;
    println!(
        "{}",
        serde_json::json!({"schema":"flare.core.initialization/v2","operation":if upgrading {"upgrade"} else {"initialize"},"changed":result?})
    );
    Ok(())
}

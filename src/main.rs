use rustchat::{config::Config, server, AppState};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    // Fail fast with a readable message on bad configuration.
    let config = Config::from_env()?;
    let addr = config.bind_addr();
    tracing::info!(version = env!("CARGO_PKG_VERSION"), db = %config.db_path, %addr, "starting RustChat");

    let state = AppState::new(config)?;
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!("listening on http://{}", listener.local_addr()?);

    server::run(listener, state, server::os_shutdown_signal()).await?;
    tracing::info!("bye");
    Ok(())
}

//! RustChat: a real-time chat server built on axum, tokio and SQLite.

pub mod config;
pub mod db;
pub mod hub;
pub mod protocol;
pub mod ratelimit;
pub mod routes;
pub mod ws;

use std::sync::Arc;
use tokio::sync::watch;

/// Shared application state, cheap to clone.
#[derive(Clone)]
pub struct AppState {
    pub db: db::Db,
    pub hub: Arc<hub::Hub>,
    pub config: Arc<config::Config>,
    /// Flipped to `true` when the server begins shutting down.
    pub shutdown: Arc<watch::Sender<bool>>,
}

impl AppState {
    pub fn new(config: config::Config) -> anyhow::Result<AppState> {
        let db = db::Db::open(&config.db_path)?;
        Ok(AppState { db, hub: Arc::new(hub::Hub::new()), config: Arc::new(config), shutdown: Arc::new(watch::channel(false).0) })
    }

    /// Tell all live WebSocket connections to close.
    pub fn begin_shutdown(&self) {
        let _ = self.shutdown.send(true);
    }
}

pub use routes::build_app;

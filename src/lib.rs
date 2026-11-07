//! RustChat: a real-time chat server built on axum, tokio and SQLite.
//!
//! Layering (dependencies point downwards only):
//!
//! ```text
//! transport   routes.rs, ws.rs, server.rs     HTTP / WebSocket, origin checks, framing
//! domain      service.rs, hub.rs, registry.rs rooms, presence, admission, retention
//! persistence store.rs (trait) <- sqlite.rs   swappable repository
//! ```

pub mod config;
pub mod error;
pub mod hub;
pub mod protocol;
pub mod ratelimit;
pub mod registry;
pub mod routes;
pub mod server;
pub mod service;
pub mod sqlite;
pub mod store;
pub mod ws;

use std::sync::Arc;
use tokio::sync::watch;

pub use routes::build_app;

/// Current unix time in milliseconds.
pub fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// Cooperative shutdown signal shared by connections and background tasks.
pub struct Shutdown(watch::Sender<bool>);

impl Shutdown {
    fn new() -> Shutdown {
        Shutdown(watch::channel(false).0)
    }

    /// Flip the signal; every subscriber wakes up.
    pub fn trigger(&self) {
        self.0.send_replace(true);
    }

    pub fn subscribe(&self) -> watch::Receiver<bool> {
        self.0.subscribe()
    }
}

/// Shared application state, cheap to clone.
#[derive(Clone)]
pub struct AppState {
    pub chat: Arc<service::ChatService>,
    pub config: Arc<config::Config>,
    pub shutdown: Arc<Shutdown>,
}

impl AppState {
    /// Validate `config`, open the SQLite database it names and build the application state.
    pub fn new(config: config::Config) -> Result<AppState, error::AppError> {
        config.validate()?;
        let store = sqlite::SqliteStore::open(&config.db_path)?;
        Ok(Self::with_store(config, Arc::new(store)))
    }

    /// Build the state on top of any [`store::Store`] implementation.
    pub fn with_store(config: config::Config, store: Arc<dyn store::Store>) -> AppState {
        let config = Arc::new(config);
        let chat = Arc::new(service::ChatService::new(store::Repo::new(store), config.clone()));
        AppState { chat, config, shutdown: Arc::new(Shutdown::new()) }
    }

    /// Tell all live WebSocket connections and background tasks to stop.
    pub fn begin_shutdown(&self) {
        self.shutdown.trigger();
    }
}

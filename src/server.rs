//! Server lifecycle: serve, background pruning and graceful shutdown.

use crate::{routes::build_app, AppState};
use std::{future::Future, net::SocketAddr, time::Duration};
use tokio::{net::TcpListener, task::JoinHandle};

/// Resolves on Ctrl+C (all platforms) or SIGTERM (unix). Signal-registration failures are
/// logged and that source simply never fires.
pub async fn os_shutdown_signal() {
    let ctrl_c = async {
        if let Err(e) = tokio::signal::ctrl_c().await {
            tracing::error!(error = %e, "cannot listen for Ctrl+C");
            std::future::pending::<()>().await;
        }
    };
    #[cfg(unix)]
    let term = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(e) => {
                tracing::error!(error = %e, "cannot listen for SIGTERM");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();
    tokio::select! { _ = ctrl_c => {}, _ = term => {} }
}

/// Periodically apply the retention policy until shutdown.
pub fn spawn_pruner(state: &AppState) -> JoinHandle<()> {
    let chat = state.chat.clone();
    let every = Duration::from_secs(state.config.prune_interval_secs);
    let mut stop = state.shutdown.subscribe();
    tokio::spawn(async move {
        loop {
            if let Err(e) = chat.prune_once().await {
                tracing::error!(error = %e, "prune failed");
            }
            tokio::select! {
                _ = tokio::time::sleep(every) => {}
                _ = stop.changed() => break,
            }
        }
    })
}

/// Serve until `shutdown` resolves, then close WebSockets, drain, and stop background tasks.
/// Shutdown never waits longer than `SHUTDOWN_GRACE_SECS` for stragglers.
pub async fn run(listener: TcpListener, state: AppState, shutdown: impl Future<Output = ()> + Send + 'static) -> std::io::Result<()> {
    let pruner = spawn_pruner(&state);
    let app = build_app(state.clone()).into_make_service_with_connect_info::<SocketAddr>();
    let (begun_tx, begun_rx) = tokio::sync::oneshot::channel::<()>();
    let signal_state = state.clone();
    let serve = axum::serve(listener, app).with_graceful_shutdown(async move {
        shutdown.await;
        tracing::info!("shutdown signal received, draining connections");
        signal_state.begin_shutdown();
        let _ = begun_tx.send(());
    });

    let grace = Duration::from_secs(state.config.shutdown_grace_secs);
    let result = tokio::select! {
        r = serve => r,
        _ = async {
            if begun_rx.await.is_ok() {
                tokio::time::sleep(grace).await;
            } else {
                std::future::pending::<()>().await;
            }
        } => {
            tracing::warn!(?grace, "connections did not drain in time, exiting anyway");
            Ok(())
        }
    };
    state.begin_shutdown();
    if let Err(e) = pruner.await {
        tracing::error!(error = %e, "pruner task failed");
    }
    result
}

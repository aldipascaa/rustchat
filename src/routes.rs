//! HTTP routes: REST API, static web client and the WebSocket endpoint.

use crate::{protocol::validate_room, ws::ws_handler, AppState};
use axum::{
    extract::{Path, Query, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use serde::Deserialize;
use serde_json::json;

const INDEX_HTML: &str = include_str!("../static/index.html");
const STYLE_CSS: &str = include_str!("../static/style.css");
const APP_JS: &str = include_str!("../static/app.js");

pub fn build_app(state: AppState) -> Router {
    Router::new()
        .route("/", get(|| async { asset("text/html; charset=utf-8", INDEX_HTML) }))
        .route("/style.css", get(|| async { asset("text/css; charset=utf-8", STYLE_CSS) }))
        .route("/app.js", get(|| async { asset("text/javascript; charset=utf-8", APP_JS) }))
        .route("/api/health", get(health))
        .route("/api/rooms", get(list_rooms))
        .route("/api/rooms/:room/messages", get(room_messages))
        .route("/ws", get(ws_handler))
        .with_state(state)
}

fn asset(content_type: &'static str, body: &'static str) -> Response {
    ([(header::CONTENT_TYPE, content_type), (header::CACHE_CONTROL, "no-cache")], body).into_response()
}

fn internal(e: anyhow::Error) -> (StatusCode, Json<serde_json::Value>) {
    tracing::error!(error = %e, "request failed");
    (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ "error": "internal error" })))
}

async fn health() -> Json<serde_json::Value> {
    Json(json!({ "status": "ok", "version": env!("CARGO_PKG_VERSION") }))
}

async fn list_rooms(State(st): State<AppState>) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let names = st.db.list_rooms().await.map_err(internal)?;
    Ok(Json(json!({ "rooms": st.hub.room_infos(&names) })))
}

#[derive(Deserialize)]
struct HistoryQuery {
    limit: Option<usize>,
}

async fn room_messages(
    State(st): State<AppState>,
    Path(room): Path<String>,
    Query(q): Query<HistoryQuery>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let not_found = || (StatusCode::NOT_FOUND, Json(json!({ "error": "room not found" })));
    let room = validate_room(&room).map_err(|_| not_found())?;
    if !st.db.room_exists(&room).await.map_err(internal)? {
        return Err(not_found());
    }
    let limit = q.limit.unwrap_or(st.config.history_limit).clamp(1, 200);
    let messages = st.db.recent_messages(&room, limit).await.map_err(internal)?;
    Ok(Json(json!({ "room": room, "messages": messages })))
}

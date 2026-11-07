//! HTTP routes: REST API, static web client, request-id/tracing middleware, security headers.

use crate::{error::ChatError, ws::ws_handler, AppState};
use axum::{
    extract::{Path, Query, Request, State},
    http::{header, HeaderName, HeaderValue, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use serde::Deserialize;
use serde_json::json;
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};
use tracing::Instrument;

const INDEX_HTML: &str = include_str!("../static/index.html");
const STYLE_CSS: &str = include_str!("../static/style.css");
const APP_JS: &str = include_str!("../static/app.js");

/// sha256 of the single inline theme script in `index.html` (checked by a test).
pub const INLINE_SCRIPT_CSP_HASH: &str = "sha256-N7i6Sq2XXJJnYUduLh6lg9Lp179H0vmooS40cwjYgoE=";

/// Per-request correlation id, available to handlers as an extension.
#[derive(Clone, Debug)]
pub struct RequestId(pub String);

static REQ_COUNTER: AtomicU64 = AtomicU64::new(1);
static BOOT_TAG: std::sync::OnceLock<u32> = std::sync::OnceLock::new();

fn new_request_id() -> String {
    let boot = *BOOT_TAG.get_or_init(|| (crate::now_ms() as u64 & 0xffff_ffff) as u32);
    format!("{:08x}-{:06x}", boot, REQ_COUNTER.fetch_add(1, Ordering::Relaxed) & 0xff_ffff)
}

/// Accept a caller-supplied id only if it is short and plain, to keep logs clean.
fn sanitize_request_id(v: &HeaderValue) -> Option<String> {
    let s = v.to_str().ok()?;
    (!s.is_empty() && s.len() <= 64 && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')).then(|| s.to_string())
}

static X_REQUEST_ID: HeaderName = HeaderName::from_static("x-request-id");

async fn request_context(mut req: Request, next: Next) -> Response {
    let id = req.headers().get(&X_REQUEST_ID).and_then(sanitize_request_id).unwrap_or_else(new_request_id);
    req.extensions_mut().insert(RequestId(id.clone()));
    let span = tracing::info_span!("http", request_id = %id, method = %req.method(), path = %req.uri().path());
    let start = Instant::now();
    let mut res = next.run(req).instrument(span.clone()).await;
    span.in_scope(|| tracing::debug!(status = res.status().as_u16(), elapsed_ms = start.elapsed().as_millis() as u64, "request done"));
    if let Ok(v) = HeaderValue::from_str(&id) {
        res.headers_mut().insert(X_REQUEST_ID.clone(), v);
    }
    let h = res.headers_mut();
    h.insert(header::X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff"));
    h.insert(header::REFERRER_POLICY, HeaderValue::from_static("no-referrer"));
    h.insert(header::X_FRAME_OPTIONS, HeaderValue::from_static("DENY"));
    res
}

pub fn build_app(state: AppState) -> Router {
    let csp = format!(
        "default-src 'self'; script-src 'self' '{INLINE_SCRIPT_CSP_HASH}'; style-src 'self'; img-src 'self' data:; connect-src 'self' ws: wss:; frame-ancestors 'none'; base-uri 'none'; form-action 'self'"
    );
    let index = move || {
        let csp = csp.clone();
        async move {
            let mut r = asset("text/html; charset=utf-8", INDEX_HTML);
            if let Ok(v) = HeaderValue::from_str(&csp) {
                r.headers_mut().insert(header::CONTENT_SECURITY_POLICY, v);
            }
            r
        }
    };
    Router::new()
        .route("/", get(index))
        .route("/style.css", get(|| async { asset("text/css; charset=utf-8", STYLE_CSS) }))
        .route("/app.js", get(|| async { asset("text/javascript; charset=utf-8", APP_JS) }))
        .route("/api/health", get(health))
        .route("/api/rooms", get(list_rooms))
        .route("/api/rooms/:room/messages", get(room_messages))
        .route("/ws", get(ws_handler))
        .layer(middleware::from_fn(request_context))
        .with_state(state)
}

fn asset(content_type: &'static str, body: &'static str) -> Response {
    ([(header::CONTENT_TYPE, content_type), (header::CACHE_CONTROL, "no-cache")], body).into_response()
}

/// Map domain errors onto HTTP. Internal details are logged, never returned.
impl IntoResponse for ChatError {
    fn into_response(self) -> Response {
        let status = match &self {
            ChatError::Invalid(_) => StatusCode::BAD_REQUEST,
            ChatError::RoomNotFound(_) => StatusCode::NOT_FOUND,
            ChatError::UsernameTaken(_) => StatusCode::CONFLICT,
            ChatError::UsernameReserved(_) | ChatError::ForbiddenOrigin => StatusCode::FORBIDDEN,
            ChatError::RateLimited => StatusCode::TOO_MANY_REQUESTS,
            ChatError::ServerFull | ChatError::TooManyConnections => StatusCode::SERVICE_UNAVAILABLE,
            ChatError::NotInRoom(_) | ChatError::RoomLimit(_) | ChatError::JoinLimit(_) => StatusCode::UNPROCESSABLE_ENTITY,
            ChatError::Store(e) => {
                tracing::error!(error = %e, "request failed");
                StatusCode::INTERNAL_SERVER_ERROR
            }
        };
        (status, Json(json!({ "error": self.to_string(), "code": self.code() }))).into_response()
    }
}

async fn health(State(st): State<AppState>) -> Response {
    let db_ok = st.chat.repo().ping().await.is_ok();
    let body = json!({
        "status": if db_ok { "ok" } else { "degraded" },
        "version": env!("CARGO_PKG_VERSION"),
        "connections": st.chat.registry().connections(),
    });
    let status = if db_ok { StatusCode::OK } else { StatusCode::SERVICE_UNAVAILABLE };
    (status, Json(body)).into_response()
}

async fn list_rooms(State(st): State<AppState>) -> Result<Json<serde_json::Value>, ChatError> {
    Ok(Json(json!({ "rooms": st.chat.room_list().await? })))
}

#[derive(Deserialize)]
struct HistoryQuery {
    limit: Option<usize>,
}

async fn room_messages(State(st): State<AppState>, Path(room): Path<String>, Query(q): Query<HistoryQuery>) -> Result<Json<serde_json::Value>, ChatError> {
    let (room, messages) = st.chat.history(&room, q.limit).await?;
    Ok(Json(json!({ "room": room, "messages": messages })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    use sha2::{Digest, Sha256};

    #[test]
    fn csp_hash_matches_inline_script() {
        let start = INDEX_HTML.find("<script>").expect("inline script") + "<script>".len();
        let end = start + INDEX_HTML[start..].find("</script>").expect("end of inline script");
        let digest = Sha256::digest(&INDEX_HTML.as_bytes()[start..end]);
        let b64 = base64::engine::general_purpose::STANDARD.encode(digest);
        assert_eq!(INLINE_SCRIPT_CSP_HASH, format!("sha256-{b64}"));
        assert_eq!(INDEX_HTML.matches("<script>").count(), 1, "only one inline script is allowed by the CSP");
    }

    #[test]
    fn request_ids_are_sanitised() {
        assert_eq!(sanitize_request_id(&HeaderValue::from_static("abc-123_X")).as_deref(), Some("abc-123_X"));
        assert!(sanitize_request_id(&HeaderValue::from_static("bad id")).is_none());
        assert!(sanitize_request_id(&HeaderValue::from_str(&"a".repeat(65)).unwrap()).is_none());
        assert_ne!(new_request_id(), new_request_id());
    }
}

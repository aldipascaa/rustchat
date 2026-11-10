use futures_util::{SinkExt, StreamExt};
use rustchat::{build_app, config::Config, server, AppState};
use serde_json::{json, Value};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, Message},
    MaybeTlsStream, WebSocketStream,
};

type Client = WebSocketStream<MaybeTlsStream<TcpStream>>;

async fn start(cfg: Config) -> (String, AppState) {
    let state = AppState::new(Config { db_path: ":memory:".into(), ..cfg }).unwrap();
    let app = build_app(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move { axum::serve(listener, app.into_make_service_with_connect_info::<std::net::SocketAddr>()).await.unwrap() });
    (addr, state)
}

async fn connect(addr: &str, user: &str) -> Client {
    let (ws, _) = connect_async(format!("ws://{addr}/ws?username={user}")).await.unwrap();
    ws
}

async fn send(c: &mut Client, v: Value) {
    c.send(Message::Text(v.to_string().into())).await.unwrap();
}

async fn recv(c: &mut Client) -> Value {
    loop {
        let m = tokio::time::timeout(Duration::from_secs(5), c.next()).await.expect("timeout waiting for message").unwrap().unwrap();
        if let Message::Text(t) = m {
            return serde_json::from_str(t.as_str()).unwrap();
        }
    }
}

/// Read messages until one with the given type (and optional predicate) appears.
async fn recv_type(c: &mut Client, ty: &str) -> Value {
    for _ in 0..20 {
        let v = recv(c).await;
        if v["type"] == ty {
            return v;
        }
    }
    panic!("never received {ty}");
}

async fn http_get(addr: &str, path: &str) -> (u16, String) {
    let mut s = TcpStream::connect(addr).await.unwrap();
    s.write_all(format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
    let mut buf = String::new();
    s.read_to_string(&mut buf).await.unwrap();
    let status = buf.split_whitespace().nth(1).unwrap().parse().unwrap();
    let body = buf.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
    (status, body)
}

#[tokio::test]
async fn two_clients_broadcast_presence_and_typing() {
    let (addr, _s) = start(Config::default()).await;
    let mut alice = connect(&addr, "alice").await;
    assert_eq!(recv(&mut alice).await["type"], "welcome");
    send(&mut alice, json!({"type":"join","room":"general"})).await;
    let joined = recv_type(&mut alice, "joined").await;
    assert_eq!(joined["users"], json!(["alice"]));

    let mut bob = connect(&addr, "bob").await;
    recv_type(&mut bob, "welcome").await;
    send(&mut bob, json!({"type":"join","room":"general"})).await;
    let joined = recv_type(&mut bob, "joined").await;
    assert_eq!(joined["users"], json!(["alice", "bob"]));

    // alice sees bob's join system message + presence
    // (alice's own join notice may arrive first)
    let mut sys = recv_type(&mut alice, "system").await;
    if sys["text"].as_str().unwrap().contains("alice joined") {
        sys = recv_type(&mut alice, "system").await;
    }
    assert!(sys["text"].as_str().unwrap().contains("bob joined"));
    let mut pres = recv_type(&mut alice, "presence").await;
    if pres["users"] == json!(["alice"]) {
        pres = recv_type(&mut alice, "presence").await;
    }
    assert_eq!(pres["users"], json!(["alice", "bob"]));

    send(&mut alice, json!({"type":"message","room":"general","text":"hello bob"})).await;
    let m = recv_type(&mut bob, "message").await;
    assert_eq!(m["user"], "alice");
    assert_eq!(m["text"], "hello bob");
    let m2 = recv_type(&mut alice, "message").await;
    assert_eq!(m2["id"], m["id"]);

    send(&mut bob, json!({"type":"typing","room":"general","typing":true})).await;
    let t = recv_type(&mut alice, "typing").await;
    assert_eq!((t["user"].as_str().unwrap(), t["typing"].as_bool().unwrap()), ("bob", true));

    // bob disconnects -> alice sees leave + presence
    drop(bob);
    let sys = recv_type(&mut alice, "system").await;
    assert!(sys["text"].as_str().unwrap().contains("bob left"));
    let pres = recv_type(&mut alice, "presence").await;
    assert_eq!(pres["users"], json!(["alice"]));
}

#[tokio::test]
async fn messages_not_leaked_across_rooms_and_history_persists() {
    let (addr, _s) = start(Config::default()).await;
    let mut a = connect(&addr, "a").await;
    send(&mut a, json!({"type":"join","room":"general"})).await;
    recv_type(&mut a, "joined").await;
    let mut b = connect(&addr, "b").await;
    send(&mut b, json!({"type":"join","room":"random"})).await;
    recv_type(&mut b, "joined").await;

    send(&mut a, json!({"type":"message","room":"general","text":"secret"})).await;
    recv_type(&mut a, "message").await;

    // b must not receive it: send b's own message and make sure the first message b sees is its own
    send(&mut b, json!({"type":"message","room":"random","text":"mine"})).await;
    let m = recv_type(&mut b, "message").await;
    assert_eq!(m["text"], "mine");

    // late joiner receives history
    let mut c = connect(&addr, "c").await;
    send(&mut c, json!({"type":"join","room":"general"})).await;
    let j = recv_type(&mut c, "joined").await;
    let hist = j["history"].as_array().unwrap();
    assert_eq!(hist.len(), 1);
    assert_eq!(hist[0]["text"], "secret");

    // REST history agrees
    let (status, body) = http_get(&addr, "/api/rooms/general/messages").await;
    assert_eq!(status, 200);
    let v: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["messages"][0]["text"], "secret");
}

#[tokio::test]
async fn validation_and_rate_limit() {
    let cfg = Config { max_message_len: 10, rate_capacity: 5.0, rate_refill_per_sec: 0.01, ..Config::default() };
    let (addr, _s) = start(cfg).await;
    let mut a = connect(&addr, "a").await;
    send(&mut a, json!({"type":"join","room":"general"})).await; // token 1
    recv_type(&mut a, "joined").await;

    send(&mut a, json!({"type":"message","room":"general","text":"this is way too long"})).await; // token 2
    let e = recv_type(&mut a, "error").await;
    assert!(e["message"].as_str().unwrap().contains("too long"));

    send(&mut a, json!({"type":"message","room":"nowhere","text":"x"})).await; // token 3
    assert_eq!(recv_type(&mut a, "error").await["type"], "error");

    send(&mut a, json!({"type":"join","room":"missing"})).await; // token 4
    assert!(recv_type(&mut a, "error").await["message"].as_str().unwrap().contains("does not exist"));

    send(&mut a, json!({"type":"message","room":"general","text":"ok"})).await; // token 5
    recv_type(&mut a, "message").await;

    send(&mut a, json!({"type":"message","room":"general","text":"ok2"})).await; // limited
    let e = recv_type(&mut a, "error").await;
    assert!(e["message"].as_str().unwrap().contains("rate limit"));
}

#[tokio::test]
async fn create_room_is_announced_to_everyone() {
    let (addr, _s) = start(Config::default()).await;
    let mut a = connect(&addr, "a").await;
    let mut b = connect(&addr, "b").await;
    recv_type(&mut a, "welcome").await;
    recv_type(&mut b, "welcome").await;
    send(&mut a, json!({"type":"create_room","room":"Fresh-Room"})).await;
    let r = recv_type(&mut b, "rooms").await;
    let names: Vec<&str> = r["rooms"].as_array().unwrap().iter().map(|x| x["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"fresh-room"));
}

#[tokio::test]
async fn rest_endpoints_and_static_page() {
    let (addr, _s) = start(Config::default()).await;
    let (s, body) = http_get(&addr, "/api/health").await;
    assert_eq!(s, 200);
    assert_eq!(serde_json::from_str::<Value>(&body).unwrap()["status"], "ok");

    let (s, body) = http_get(&addr, "/api/rooms").await;
    assert_eq!(s, 200);
    assert_eq!(serde_json::from_str::<Value>(&body).unwrap()["rooms"].as_array().unwrap().len(), 3);

    let (s, _) = http_get(&addr, "/api/rooms/nope/messages").await;
    assert_eq!(s, 404);

    let (s, body) = http_get(&addr, "/").await;
    assert_eq!(s, 200);
    assert!(body.contains("RustChat"));
}

#[tokio::test]
async fn rejects_bad_username() {
    let (addr, _s) = start(Config::default()).await;
    assert!(connect_async(format!("ws://{addr}/ws?username=bad%20name")).await.is_err());
    assert!(connect_async(format!("ws://{addr}/ws")).await.is_err());
}


// ---------------------------------------------------------------------------------------
// v1.1.0 architecture-review fixes
// ---------------------------------------------------------------------------------------

/// Wait for the close frame and return its code (or None if the stream just ended).
async fn close_code(c: &mut Client) -> Option<u16> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match c.next().await {
                Some(Ok(Message::Close(Some(f)))) => return Some(u16::from(f.code)),
                Some(Ok(Message::Close(None))) | None | Some(Err(_)) => return None,
                _ => {}
            }
        }
    })
    .await
    .expect("timed out waiting for close")
}

async fn wait_closed(c: &mut Client) -> bool {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match c.next().await {
                None | Some(Err(_)) | Some(Ok(Message::Close(_))) => break,
                _ => {}
            }
        }
    })
    .await
    .is_ok()
}

#[tokio::test]
async fn duplicate_username_is_rejected_case_insensitively_and_freed_on_disconnect() {
    let (addr, _s) = start(Config::default()).await;
    let mut first = connect(&addr, "Alice").await;
    assert_eq!(recv(&mut first).await["type"], "welcome");

    for name in ["Alice", "alice", "ALICE"] {
        let mut dup = connect(&addr, name).await;
        let e = recv(&mut dup).await;
        assert_eq!(e["type"], "error");
        assert_eq!(e["code"], "username_taken");
        assert!(e["message"].as_str().unwrap().contains("already in use"));
        assert_eq!(close_code(&mut dup).await, Some(4009));
    }
    // the original session is unaffected
    send(&mut first, json!({"type":"list_rooms"})).await;
    recv_type(&mut first, "rooms").await;

    // once the first connection is gone the name is available again
    drop(first);
    let mut again = None;
    for _ in 0..50 {
        let mut c = connect(&addr, "alice").await;
        if recv(&mut c).await["type"] == "welcome" {
            again = Some(c);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(again.is_some(), "name should be released after disconnect");
}

#[tokio::test]
async fn reserved_usernames_are_rejected() {
    let (addr, _s) = start(Config::default()).await;
    let mut c = connect(&addr, "Admin").await;
    let e = recv(&mut c).await;
    assert_eq!(e["code"], "username_reserved");
    assert_eq!(close_code(&mut c).await, Some(4009));
}

#[tokio::test]
async fn connection_limits_are_enforced() {
    let (addr, _s) = start(Config { max_connections_per_ip: 1, ..Config::default() }).await;
    let mut a = connect(&addr, "a").await;
    assert_eq!(recv(&mut a).await["type"], "welcome");
    // same IP (loopback) already holds its one slot
    let mut b = connect(&addr, "b").await;
    assert_eq!(recv(&mut b).await["code"], "too_many_connections");

    let (addr2, _s2) = start(Config { max_connections: 1, ..Config::default() }).await;
    let mut a = connect(&addr2, "a").await;
    assert_eq!(recv(&mut a).await["type"], "welcome");
    let mut b = connect(&addr2, "b").await;
    assert_eq!(recv(&mut b).await["code"], "server_full");
}

#[tokio::test]
async fn cross_origin_websocket_is_forbidden_but_same_origin_and_no_origin_work() {
    let (addr, _s) = start(Config::default()).await;

    let mut req = format!("ws://{addr}/ws?username=eve").into_client_request().unwrap();
    req.headers_mut().insert("Origin", "https://evil.example".parse().unwrap());
    match connect_async(req).await {
        Err(tokio_tungstenite::tungstenite::Error::Http(resp)) => assert_eq!(resp.status().as_u16(), 403),
        other => panic!("expected 403, got {other:?}"),
    }

    let mut req = format!("ws://{addr}/ws?username=bob").into_client_request().unwrap();
    req.headers_mut().insert("Origin", format!("http://{addr}").parse().unwrap());
    let (mut same, _) = connect_async(req).await.expect("same-origin allowed");
    assert_eq!(recv(&mut same).await["type"], "welcome");

    // no Origin header (CLI / native client)
    let mut c = connect(&addr, "carol").await;
    assert_eq!(recv(&mut c).await["type"], "welcome");
}

#[tokio::test]
async fn allow_listed_origin_is_accepted() {
    let (addr, _s) = start(Config { allowed_origins: vec!["https://app.example".into()], ..Config::default() }).await;
    let mut req = format!("ws://{addr}/ws?username=dave").into_client_request().unwrap();
    req.headers_mut().insert("Origin", "https://app.example".parse().unwrap());
    let (mut c, _) = connect_async(req).await.expect("allow-listed origin");
    assert_eq!(recv(&mut c).await["type"], "welcome");
}

#[tokio::test]
async fn oversized_frame_closes_the_connection() {
    let (addr, _s) = start(Config::default()).await;
    let mut c = connect(&addr, "big").await;
    recv_type(&mut c, "welcome").await;
    let huge = json!({"type":"message","room":"general","text":"x".repeat(64 * 1024)}).to_string();
    let _ = c.send(Message::Text(huge.into())).await;
    assert!(wait_closed(&mut c).await, "server must drop connections that exceed the frame limit");
}

#[tokio::test]
async fn room_cap_is_enforced() {
    let (addr, _s) = start(Config { max_rooms: 4, ..Config::default() }).await;
    let mut a = connect(&addr, "a").await;
    recv_type(&mut a, "welcome").await;
    send(&mut a, json!({"type":"create_room","room":"fourth"})).await;
    recv_type(&mut a, "rooms").await;
    send(&mut a, json!({"type":"create_room","room":"fifth"})).await;
    let e = recv_type(&mut a, "error").await;
    assert_eq!(e["code"], "room_limit");
}

#[tokio::test]
async fn error_frames_carry_codes() {
    let (addr, _s) = start(Config::default()).await;
    let mut a = connect(&addr, "a").await;
    recv_type(&mut a, "welcome").await;
    send(&mut a, json!({"type":"join","room":"missing"})).await;
    assert_eq!(recv_type(&mut a, "error").await["code"], "room_not_found");
    send(&mut a, json!({"type":"message","room":"general","text":"hi"})).await;
    assert_eq!(recv_type(&mut a, "error").await["code"], "not_in_room");
    a.send(Message::Text("not json".into())).await.unwrap();
    assert_eq!(recv_type(&mut a, "error").await["code"], "invalid");
}

#[tokio::test]
async fn background_pruner_enforces_retention() {
    let cfg = Config { history_limit: 3, max_messages_per_room: 3, prune_interval_secs: 1, rate_capacity: 100.0, ..Config::default() };
    let (addr, state) = start(cfg).await;
    let pruner = server::spawn_pruner(&state);

    let mut a = connect(&addr, "a").await;
    send(&mut a, json!({"type":"join","room":"general"})).await;
    recv_type(&mut a, "joined").await;
    for i in 0..8 {
        send(&mut a, json!({"type":"message","room":"general","text":format!("m{i}")})).await;
    }
    let mut kept = 8;
    for _ in 0..40 {
        let (_, body) = http_get(&addr, "/api/rooms/general/messages?limit=200").await;
        kept = serde_json::from_str::<Value>(&body).unwrap()["messages"].as_array().unwrap().len();
        if kept == 3 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert_eq!(kept, 3, "pruner should trim the room to MAX_MESSAGES_PER_ROOM");
    let (_, body) = http_get(&addr, "/api/rooms/general/messages?limit=200").await;
    let v: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["messages"][2]["text"], "m7", "newest messages are the ones kept");

    state.begin_shutdown();
    tokio::time::timeout(Duration::from_secs(5), pruner).await.expect("pruner stops on shutdown").unwrap();
}

#[tokio::test]
async fn stale_connection_is_dropped_and_its_name_freed() {
    // The client never reads, so it never answers the server's pings.
    let (addr, _s) = start(Config { idle_timeout_secs: 3, ..Config::default() }).await;
    let _stale = connect(&addr, "ghost").await;
    let mut freed = false;
    for _ in 0..60 {
        let (_, body) = http_get(&addr, "/api/health").await;
        if serde_json::from_str::<Value>(&body).unwrap()["connections"] == 0 {
            freed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(freed, "idle connection should be dropped by the heartbeat");
}

#[tokio::test]
async fn responses_carry_request_id_and_security_headers() {
    let (addr, _s) = start(Config::default()).await;
    let mut s = TcpStream::connect(&addr).await.unwrap();
    s.write_all(format!("GET / HTTP/1.1\r\nHost: {addr}\r\nX-Request-Id: trace-me-42\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
    let mut raw = String::new();
    s.read_to_string(&mut raw).await.unwrap();
    let lower = raw.to_lowercase();
    assert!(lower.contains("x-request-id: trace-me-42"), "caller's id is echoed: {raw}");
    assert!(lower.contains("x-content-type-options: nosniff"));
    assert!(lower.contains("content-security-policy:"));
    assert!(lower.contains("frame-ancestors 'none'"));

    // generated when absent
    let mut s = TcpStream::connect(&addr).await.unwrap();
    s.write_all(format!("GET /api/health HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
    let mut raw = String::new();
    s.read_to_string(&mut raw).await.unwrap();
    assert!(raw.to_lowercase().contains("x-request-id: "));

    let _c = connect(&addr, "x").await;
    let (_, body) = http_get(&addr, "/api/health").await;
    assert_eq!(serde_json::from_str::<Value>(&body).unwrap()["connections"], 1);
}

#[tokio::test]
async fn server_run_shuts_down_gracefully_and_closes_websockets() {
    // `server::run` is what main() calls, with the OS signal swapped for a oneshot.
    let state = AppState::new(Config { db_path: ":memory:".into(), shutdown_grace_secs: 5, ..Config::default() }).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let serve = tokio::spawn(server::run(listener, state, async move {
        let _ = stop_rx.await;
    }));

    let mut a = connect(&addr, "a").await;
    recv_type(&mut a, "welcome").await;
    send(&mut a, json!({"type":"join","room":"general"})).await;
    recv_type(&mut a, "joined").await;

    stop_tx.send(()).unwrap();
    assert!(wait_closed(&mut a).await, "connection should close on shutdown");
    let res = tokio::time::timeout(Duration::from_secs(8), serve).await.expect("run() must return after shutdown");
    res.unwrap().unwrap();
}

#[tokio::test]
async fn invalid_config_is_rejected_by_app_state() {
    let bad = Config { rate_refill_per_sec: 0.0, ..Config::default() };
    assert!(AppState::new(bad).is_err());
}

#[tokio::test]
async fn history_pages_backwards_with_a_before_cursor() {
    let (addr, state) = start(Config { history_limit: 3, ..Config::default() }).await;
    for i in 1..=7 {
        state.chat.repo().insert_message("general", "alice", &format!("m{i}")).await.unwrap().unwrap();
    }
    let texts = |v: &Value| v.as_array().unwrap().iter().map(|m| m["text"].as_str().unwrap().to_string()).collect::<Vec<_>>();

    // Joining gives the newest page and says that older messages exist.
    let mut bob = connect(&addr, "bob").await;
    recv_type(&mut bob, "welcome").await;
    send(&mut bob, json!({"type":"join","room":"general"})).await;
    let joined = recv_type(&mut bob, "joined").await;
    assert_eq!(texts(&joined["history"]), ["m5", "m6", "m7"]);
    assert_eq!(joined["has_more"], true);

    // Page back using the oldest id on screen as the cursor.
    let oldest = joined["history"][0]["id"].as_i64().unwrap();
    let (status, body) = http_get(&addr, &format!("/api/rooms/general/messages?before={oldest}&limit=3")).await;
    assert_eq!(status, 200);
    let page: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(texts(&page["messages"]), ["m2", "m3", "m4"]);
    assert_eq!(page["has_more"], true);

    let oldest = page["messages"][0]["id"].as_i64().unwrap();
    let (_, body) = http_get(&addr, &format!("/api/rooms/general/messages?before={oldest}&limit=3")).await;
    let page: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(texts(&page["messages"]), ["m1"]);
    assert_eq!(page["has_more"], false);

    // A room that fits in one page has nothing older.
    let (_, body) = http_get(&addr, "/api/rooms/random/messages").await;
    assert_eq!(serde_json::from_str::<Value>(&body).unwrap()["has_more"], false);
    let (status, _) = http_get(&addr, "/api/rooms/general/messages?before=abc").await;
    assert_eq!(status, 400);
}

use futures_util::{SinkExt, StreamExt};
use rustchat::{build_app, config::Config, AppState};
use serde_json::{json, Value};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};

type Client = WebSocketStream<MaybeTlsStream<TcpStream>>;

async fn start(cfg: Config) -> (String, AppState) {
    let state = AppState::new(Config { db_path: ":memory:".into(), ..cfg }).unwrap();
    let app = build_app(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
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

#[tokio::test]
async fn shutdown_closes_connections() {
    let (addr, state) = start(Config::default()).await;
    let mut a = connect(&addr, "a").await;
    recv_type(&mut a, "welcome").await;
    state.begin_shutdown();
    let end = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match a.next().await {
                None | Some(Err(_)) | Some(Ok(Message::Close(_))) => break,
                _ => {}
            }
        }
    })
    .await;
    assert!(end.is_ok(), "connection should close on shutdown");
}

//! Tiny scripted client: connects, joins a room, posts messages, then exits.
//!
//! Usage: `cargo run --example demo_client -- [ws-host:port] [username] [room] [message...]`
//! With no message arguments a short canned conversation is posted.

use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tokio_tungstenite::{connect_async, tungstenite::Message};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let host = args.first().cloned().unwrap_or_else(|| "127.0.0.1:3000".into());
    let user = args.get(1).cloned().unwrap_or_else(|| "demo-bot".into());
    let room = args.get(2).cloned().unwrap_or_else(|| "general".into());
    let msgs: Vec<String> = if args.len() > 3 {
        args[3..].to_vec()
    } else {
        vec!["Hello from the demo client!".into(), "Messages are stored in SQLite and replayed on join.".into()]
    };

    let (mut ws, _) = connect_async(format!("ws://{host}/ws?username={user}")).await?;
    ws.send(Message::Text(json!({"type":"join","room":room}).to_string().into())).await?;
    for m in msgs {
        ws.send(Message::Text(json!({"type":"message","room":room,"text":m}).to_string().into())).await?;
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    }
    // Drain briefly so the server processes everything before we disconnect, printing what we see.
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(500);
    while let Ok(Some(Ok(msg))) = tokio::time::timeout_at(deadline, ws.next()).await {
        if let Message::Text(t) = msg {
            println!("{}", t.as_str());
        }
    }
    ws.close(None).await?;
    Ok(())
}

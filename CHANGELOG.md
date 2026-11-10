# Changelog

All notable changes to this project are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/) and the project uses [Semantic Versioning](https://semver.org/).

## [1.2.0] - 2026-11-10

### Added
- **Older history on demand.** `GET /api/rooms/:room/messages` takes a `before=<message id>` cursor and returns
  `has_more`; the `joined` frame carries `has_more` too. The web client shows a "Load earlier messages" button
  and keeps the scroll position while older pages are prepended. Before this, only the newest 50 messages of a
  room were reachable even though up to 10,000 are kept.
- GitHub Actions workflow: build, clippy with `-D warnings` and tests on every push and pull request.

### Changed
- `Store::recent_messages` takes an optional `before` id (library API).

### Removed
- `src/db.rs`, a leftover of the 1.0 storage module that was no longer compiled.

## [1.1.0] - 2026-11-08

Architecture review release. See [ARCHITECTURE_REVIEW.md](ARCHITECTURE_REVIEW.md) for the findings and decisions.

### Added
- Unique usernames across the server (case-insensitive) and a configurable reserved-name list (`RESERVED_USERNAMES`). A rejected connection receives an `error` frame (`username_taken` / `username_reserved`) and close code 4009; the web client returns to the login form with the message.
- WebSocket `Origin` check (`ALLOWED_ORIGINS`; same-origin and header-less clients always allowed). Cross-origin browsers get HTTP 403.
- Message retention: `MAX_MESSAGES_PER_ROOM` (default 10000), `RETENTION_DAYS`, idle-room expiry `ROOM_TTL_DAYS` (default 7, never default rooms or occupied rooms), run by a background pruner every `PRUNE_INTERVAL_SECS`.
- Limits: `MAX_FRAME_BYTES`, `MAX_CONNECTIONS`, `MAX_CONNECTIONS_PER_IP`, `MAX_ROOMS`, `MAX_JOINED_ROOMS`, `BROADCAST_CAPACITY`.
- Slow-consumer handling: a `lagged {room, skipped}` frame tells a client it missed messages; the web client re-joins to resync.
- Heartbeat: the server pings every `IDLE_TIMEOUT_SECS/3` and drops silent peers; 10 s write timeout for stalled sockets.
- Error frames carry a stable `code`; `/api/health` reports `connections` and returns 503 `degraded` if the database is unreachable.
- `X-Request-Id` on every response (caller's id echoed), per-request and per-connection tracing spans, security headers and a Content-Security-Policy for the web client.
- `SHUTDOWN_GRACE_SECS` bounds how long shutdown waits for connections to drain.

### Changed
- Internal architecture: transport (`ws`, `routes`, `server`) -> domain (`service`, `hub`, `registry`) -> persistence (`Store` trait, `SqliteStore`). Domain logic no longer lives in the WebSocket session.
- Typed errors with `thiserror` (`StoreError`, `ChatError`, `ConfigError`, `AppError`) replace `anyhow` in the library; no `unwrap`/`expect` on runtime paths; poisoned locks are recovered.
- Configuration is validated at startup; unparseable or out-of-range values now abort startup instead of silently falling back to defaults.
- Forwarder tasks are cancelled automatically when a session ends; empty rooms are dropped from the in-memory hub.
- Graceful shutdown lives in `server::run` (injectable signal) and is covered by tests.
- Library API: `rustchat::db` is replaced by `rustchat::store` / `rustchat::sqlite`; `AppState` now holds a `ChatService`.

### Compatibility
- The wire protocol is backward compatible: new frames/fields are additive. The one behavioural change is that a second connection using a name that is already online is refused.

## [1.0.0] - 2026-11-07

First stable release.

### Added
- axum + tokio WebSocket chat server with multiple rooms (create, join, leave), usernames, per-room broadcast channels, presence lists, typing indicators and join/leave system messages.
- SQLite message persistence; the last 50 messages are replayed when joining a room.
- REST endpoints: `/api/health`, `/api/rooms`, `/api/rooms/:room/messages`.
- Per-connection token-bucket rate limiting, message length and name validation, 16 KiB frame limit.
- Graceful shutdown that closes live WebSocket connections, `tracing` logging, environment-variable configuration.
- Embedded web client: room sidebar, avatars, timestamps, presence panel, typing indicator, auto-scroll, automatic reconnect, light/dark theme, responsive layout, accessible labels and focus styles.
- Unit tests and integration tests using real `tokio-tungstenite` clients.
- `demo_client` example.

# Changelog

All notable changes to this project are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/) and the project uses [Semantic Versioning](https://semver.org/).

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

//! Runtime configuration: read from environment variables, validated at startup.

use crate::error::ConfigError;
use std::str::FromStr;

#[derive(Debug, Clone, PartialEq)]
pub struct Config {
    pub host: String,
    pub port: u16,
    pub db_path: String,
    pub max_message_len: usize,
    pub rate_capacity: f64,
    pub rate_refill_per_sec: f64,
    pub history_limit: usize,
    /// Maximum size of a single WebSocket frame/message in bytes.
    pub max_frame_bytes: usize,
    /// Extra allowed `Origin` values (same-origin is always allowed); `*` allows any.
    pub allowed_origins: Vec<String>,
    pub max_connections: usize,
    pub max_connections_per_ip: usize,
    pub max_rooms: usize,
    pub max_joined_rooms: usize,
    /// Per-room broadcast buffer; a consumer further behind than this is "lagged".
    pub broadcast_capacity: usize,
    /// A connection silent for this long (pings are sent at a third of it) is dropped.
    pub idle_timeout_secs: u64,
    /// Delete messages older than this many days (0 = keep forever).
    pub retention_days: u64,
    /// Keep at most this many messages per room (0 = unlimited).
    pub max_messages_per_room: usize,
    /// Delete non-default rooms idle (no messages, nobody online) this many days (0 = never).
    pub room_ttl_days: u64,
    pub prune_interval_secs: u64,
    /// Usernames nobody may take (case-insensitive).
    pub reserved_usernames: Vec<String>,
    /// How long shutdown waits for connections to drain before giving up.
    pub shutdown_grace_secs: u64,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            host: "127.0.0.1".into(),
            port: 3000,
            db_path: "rustchat.db".into(),
            max_message_len: 2000,
            rate_capacity: 10.0,
            rate_refill_per_sec: 3.0,
            history_limit: 50,
            max_frame_bytes: 16 * 1024,
            allowed_origins: Vec::new(),
            max_connections: 1000,
            max_connections_per_ip: 20,
            max_rooms: 200,
            max_joined_rooms: 20,
            broadcast_capacity: 256,
            idle_timeout_secs: 90,
            retention_days: 0,
            max_messages_per_room: 10_000,
            room_ttl_days: 7,
            prune_interval_secs: 300,
            reserved_usernames: ["system", "admin", "administrator", "server", "root", "moderator"].map(String::from).to_vec(),
            shutdown_grace_secs: 10,
        }
    }
}

/// Parse an optional, non-blank value; a present-but-unparseable value is an error.
fn parse<T: FromStr>(get: &impl Fn(&str) -> Option<String>, key: &'static str, default: T, expected: &'static str) -> Result<T, ConfigError> {
    match get(key).map(|s| s.trim().to_string()).filter(|s| !s.is_empty()) {
        None => Ok(default),
        Some(v) => v.parse().map_err(|_| ConfigError::Parse { key, value: v, expected }),
    }
}

fn text(get: &impl Fn(&str) -> Option<String>, key: &str, default: String) -> String {
    get(key).map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).unwrap_or(default)
}

fn list(get: &impl Fn(&str) -> Option<String>, key: &str, default: Vec<String>) -> Vec<String> {
    match get(key) {
        None => default,
        Some(v) => v.split(',').map(|s| s.trim().trim_end_matches('/').to_lowercase()).filter(|s| !s.is_empty()).collect(),
    }
}

fn invalid(key: &'static str, reason: impl Into<String>) -> ConfigError {
    ConfigError::Invalid { key, reason: reason.into() }
}

impl Config {
    /// Build and validate a config from the process environment.
    pub fn from_env() -> Result<Config, ConfigError> {
        Self::from_lookup(|k| std::env::var(k).ok())
    }

    /// Build and validate a config from an arbitrary key lookup (used for testing).
    pub fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Result<Config, ConfigError> {
        let d = Config::default();
        let c = Config {
            host: text(&get, "HOST", d.host),
            port: parse(&get, "PORT", d.port, "0-65535")?,
            db_path: text(&get, "DB_PATH", d.db_path),
            max_message_len: parse(&get, "MAX_MESSAGE_LEN", d.max_message_len, "integer")?,
            rate_capacity: parse(&get, "RATE_CAPACITY", d.rate_capacity, "number")?,
            rate_refill_per_sec: parse(&get, "RATE_REFILL_PER_SEC", d.rate_refill_per_sec, "number")?,
            history_limit: parse(&get, "HISTORY_LIMIT", d.history_limit, "integer")?,
            max_frame_bytes: parse(&get, "MAX_FRAME_BYTES", d.max_frame_bytes, "integer")?,
            allowed_origins: list(&get, "ALLOWED_ORIGINS", d.allowed_origins),
            max_connections: parse(&get, "MAX_CONNECTIONS", d.max_connections, "integer")?,
            max_connections_per_ip: parse(&get, "MAX_CONNECTIONS_PER_IP", d.max_connections_per_ip, "integer")?,
            max_rooms: parse(&get, "MAX_ROOMS", d.max_rooms, "integer")?,
            max_joined_rooms: parse(&get, "MAX_JOINED_ROOMS", d.max_joined_rooms, "integer")?,
            broadcast_capacity: parse(&get, "BROADCAST_CAPACITY", d.broadcast_capacity, "integer")?,
            idle_timeout_secs: parse(&get, "IDLE_TIMEOUT_SECS", d.idle_timeout_secs, "integer")?,
            retention_days: parse(&get, "RETENTION_DAYS", d.retention_days, "integer")?,
            max_messages_per_room: parse(&get, "MAX_MESSAGES_PER_ROOM", d.max_messages_per_room, "integer")?,
            room_ttl_days: parse(&get, "ROOM_TTL_DAYS", d.room_ttl_days, "integer")?,
            prune_interval_secs: parse(&get, "PRUNE_INTERVAL_SECS", d.prune_interval_secs, "integer")?,
            reserved_usernames: list(&get, "RESERVED_USERNAMES", d.reserved_usernames),
            shutdown_grace_secs: parse(&get, "SHUTDOWN_GRACE_SECS", d.shutdown_grace_secs, "integer")?,
        };
        c.validate()?;
        Ok(c)
    }

    /// Reject values that would make the server misbehave. Called on every construction path.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.host.is_empty() {
            return Err(invalid("HOST", "must not be empty"));
        }
        if self.db_path.is_empty() {
            return Err(invalid("DB_PATH", "must not be empty"));
        }
        if !(1..=10_000).contains(&self.max_message_len) {
            return Err(invalid("MAX_MESSAGE_LEN", "must be between 1 and 10000"));
        }
        let min_frame = self.max_message_len * 4 + 256;
        if self.max_frame_bytes < min_frame || self.max_frame_bytes > 1024 * 1024 {
            return Err(invalid("MAX_FRAME_BYTES", format!("must be between {min_frame} (room for a full message) and 1048576")));
        }
        if !self.rate_capacity.is_finite() || self.rate_capacity < 1.0 {
            return Err(invalid("RATE_CAPACITY", "must be a number >= 1"));
        }
        if !self.rate_refill_per_sec.is_finite() || self.rate_refill_per_sec <= 0.0 {
            return Err(invalid("RATE_REFILL_PER_SEC", "must be a number > 0"));
        }
        if !(1..=200).contains(&self.history_limit) {
            return Err(invalid("HISTORY_LIMIT", "must be between 1 and 200"));
        }
        if self.max_connections == 0 || self.max_connections_per_ip == 0 {
            return Err(invalid("MAX_CONNECTIONS", "connection limits must be >= 1"));
        }
        if self.max_rooms < crate::store::DEFAULT_ROOMS.len() {
            return Err(invalid("MAX_ROOMS", format!("must be at least {}", crate::store::DEFAULT_ROOMS.len())));
        }
        if self.max_joined_rooms == 0 {
            return Err(invalid("MAX_JOINED_ROOMS", "must be >= 1"));
        }
        if !(2..=65_536).contains(&self.broadcast_capacity) {
            return Err(invalid("BROADCAST_CAPACITY", "must be between 2 and 65536"));
        }
        if self.idle_timeout_secs < 3 {
            return Err(invalid("IDLE_TIMEOUT_SECS", "must be >= 3"));
        }
        if self.prune_interval_secs == 0 {
            return Err(invalid("PRUNE_INTERVAL_SECS", "must be >= 1"));
        }
        if self.max_messages_per_room != 0 && self.max_messages_per_room < self.history_limit {
            return Err(invalid("MAX_MESSAGES_PER_ROOM", "must be 0 (unlimited) or >= HISTORY_LIMIT"));
        }
        for o in &self.allowed_origins {
            if o != "*" && !(o.starts_with("http://") || o.starts_with("https://")) {
                return Err(invalid("ALLOWED_ORIGINS", format!("{o:?} must be '*' or look like https://host[:port]")));
            }
        }
        Ok(())
    }

    pub fn bind_addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn cfg(pairs: &[(&'static str, &'static str)]) -> Result<Config, ConfigError> {
        let m: HashMap<&str, &str> = pairs.iter().copied().collect();
        Config::from_lookup(|k| m.get(k).map(|s| s.to_string()))
    }

    #[test]
    fn defaults_when_empty() {
        assert_eq!(cfg(&[]).unwrap(), Config::default());
        Config::default().validate().unwrap();
    }

    #[test]
    fn reads_values() {
        let c = cfg(&[("PORT", "8081"), ("DB_PATH", "x.db"), ("RETENTION_DAYS", "30"), ("ALLOWED_ORIGINS", "https://A.example/, http://b.example:8080")]).unwrap();
        assert_eq!(c.port, 8081);
        assert_eq!(c.db_path, "x.db");
        assert_eq!(c.retention_days, 30);
        assert_eq!(c.allowed_origins, vec!["https://a.example", "http://b.example:8080"]);
    }

    #[test]
    fn garbage_is_an_error_not_a_silent_default() {
        assert!(matches!(cfg(&[("MAX_MESSAGE_LEN", "abc")]), Err(ConfigError::Parse { key: "MAX_MESSAGE_LEN", .. })));
        assert!(matches!(cfg(&[("PORT", "70000")]), Err(ConfigError::Parse { .. })));
    }

    #[test]
    fn out_of_range_values_are_rejected() {
        for (k, v) in [
            ("MAX_MESSAGE_LEN", "0"),
            ("MAX_FRAME_BYTES", "100"),
            ("RATE_CAPACITY", "0"),
            ("RATE_REFILL_PER_SEC", "nan"),
            ("RATE_REFILL_PER_SEC", "-1"),
            ("HISTORY_LIMIT", "0"),
            ("HISTORY_LIMIT", "999"),
            ("MAX_CONNECTIONS", "0"),
            ("MAX_ROOMS", "1"),
            ("BROADCAST_CAPACITY", "1"),
            ("IDLE_TIMEOUT_SECS", "1"),
            ("PRUNE_INTERVAL_SECS", "0"),
            ("MAX_MESSAGES_PER_ROOM", "5"),
            ("ALLOWED_ORIGINS", "example.com"),
        ] {
            assert!(cfg(&[(k, v)]).is_err(), "{k}={v} should be rejected");
        }
    }

    #[test]
    fn unlimited_retention_settings_are_valid() {
        let c = cfg(&[("MAX_MESSAGES_PER_ROOM", "0"), ("ROOM_TTL_DAYS", "0"), ("ALLOWED_ORIGINS", "*")]).unwrap();
        assert_eq!(c.max_messages_per_room, 0);
        assert_eq!(c.allowed_origins, vec!["*"]);
    }
}

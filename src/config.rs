//! Runtime configuration, read from environment variables.

#[derive(Debug, Clone, PartialEq)]
pub struct Config {
    pub host: String,
    pub port: u16,
    pub db_path: String,
    pub max_message_len: usize,
    pub rate_capacity: f64,
    pub rate_refill_per_sec: f64,
    pub history_limit: usize,
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
        }
    }
}

fn parse<T: std::str::FromStr>(v: Option<String>, default: T) -> T {
    v.and_then(|s| s.trim().parse().ok()).unwrap_or(default)
}

impl Config {
    /// Build a config from the process environment.
    pub fn from_env() -> Config {
        Self::from_lookup(|k| std::env::var(k).ok())
    }

    /// Build a config from an arbitrary key lookup (used for testing).
    pub fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Config {
        let d = Config::default();
        Config {
            host: get("HOST").unwrap_or(d.host),
            port: parse(get("PORT"), d.port),
            db_path: get("DB_PATH").unwrap_or(d.db_path),
            max_message_len: parse(get("MAX_MESSAGE_LEN"), d.max_message_len),
            rate_capacity: parse(get("RATE_CAPACITY"), d.rate_capacity),
            rate_refill_per_sec: parse(get("RATE_REFILL_PER_SEC"), d.rate_refill_per_sec),
            history_limit: parse(get("HISTORY_LIMIT"), d.history_limit),
        }
    }

    pub fn bind_addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn defaults_when_empty() {
        let c = Config::from_lookup(|_| None);
        assert_eq!(c, Config::default());
    }

    #[test]
    fn reads_values_and_ignores_garbage() {
        let m: HashMap<&str, &str> = [("PORT", "8081"), ("DB_PATH", "x.db"), ("MAX_MESSAGE_LEN", "abc")].into();
        let c = Config::from_lookup(|k| m.get(k).map(|s| s.to_string()));
        assert_eq!(c.port, 8081);
        assert_eq!(c.db_path, "x.db");
        assert_eq!(c.max_message_len, 2000);
    }
}

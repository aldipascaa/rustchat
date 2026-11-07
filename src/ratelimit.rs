//! Simple token-bucket rate limiter, one per connection.

use std::time::Instant;

#[derive(Debug)]
pub struct TokenBucket {
    capacity: f64,
    tokens: f64,
    refill_per_sec: f64,
    last: Instant,
}

impl TokenBucket {
    pub fn new(capacity: f64, refill_per_sec: f64) -> Self {
        Self::new_at(capacity, refill_per_sec, Instant::now())
    }

    pub fn new_at(capacity: f64, refill_per_sec: f64, now: Instant) -> Self {
        TokenBucket { capacity, tokens: capacity, refill_per_sec, last: now }
    }

    /// Try to take one token. Returns false when the caller is rate limited.
    pub fn try_take(&mut self) -> bool {
        self.try_take_at(Instant::now())
    }

    pub fn try_take_at(&mut self, now: Instant) -> bool {
        let elapsed = now.saturating_duration_since(self.last).as_secs_f64();
        self.last = now;
        self.tokens = (self.tokens + elapsed * self.refill_per_sec).min(self.capacity);
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn burst_then_block() {
        let t0 = Instant::now();
        let mut b = TokenBucket::new_at(3.0, 1.0, t0);
        assert!(b.try_take_at(t0));
        assert!(b.try_take_at(t0));
        assert!(b.try_take_at(t0));
        assert!(!b.try_take_at(t0));
    }

    #[test]
    fn refills_over_time_but_caps() {
        let t0 = Instant::now();
        let mut b = TokenBucket::new_at(2.0, 2.0, t0);
        assert!(b.try_take_at(t0));
        assert!(b.try_take_at(t0));
        assert!(!b.try_take_at(t0));
        let t1 = t0 + Duration::from_millis(500);
        assert!(b.try_take_at(t1));
        assert!(!b.try_take_at(t1));
        let t2 = t1 + Duration::from_secs(100);
        assert!(b.try_take_at(t2));
        assert!(b.try_take_at(t2));
        assert!(!b.try_take_at(t2));
    }
}

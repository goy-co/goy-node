use std::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RateLimitReason {
    EventsExhausted,
    BytesExhausted,
}

impl std::fmt::Display for RateLimitReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RateLimitReason::EventsExhausted => write!(f, "events per second limit exceeded"),
            RateLimitReason::BytesExhausted => write!(f, "bytes per second limit exceeded"),
        }
    }
}

/// Token bucket por peer com refinamento contínuo em tempo real de eventos e bytes.
#[derive(Debug)]
pub struct PeerRateLimiter {
    event_capacity: f64,
    event_tokens: f64,
    event_refill_rate: f64,

    byte_capacity: f64,
    byte_tokens: f64,
    byte_refill_rate: f64,

    last_refill: Instant,
    pub warned: bool,
}

impl PeerRateLimiter {
    pub fn new(max_events_per_sec: u32, max_bytes_per_sec: u64) -> Self {
        let event_cap = max_events_per_sec as f64;
        let byte_cap = max_bytes_per_sec as f64;

        Self {
            event_capacity: event_cap,
            event_tokens: event_cap,
            event_refill_rate: event_cap,

            byte_capacity: byte_cap,
            byte_tokens: byte_cap,
            byte_refill_rate: byte_cap,

            last_refill: Instant::now(),
            warned: false,
        }
    }

    /// Repõe os tokens acumulados desde a última verificação.
    fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        if elapsed > 0.0 {
            self.event_tokens =
                (self.event_tokens + elapsed * self.event_refill_rate).min(self.event_capacity);
            self.byte_tokens =
                (self.byte_tokens + elapsed * self.byte_refill_rate).min(self.byte_capacity);
            self.last_refill = now;
        }
    }

    /// Tenta consumir 1 token de evento e `message_bytes` tokens de byte.
    /// Retorna `Ok(())` se houver saldo suficiente, ou `Err(RateLimitReason)` se excedido.
    pub fn try_consume(&mut self, message_bytes: usize) -> Result<(), RateLimitReason> {
        self.refill();

        if self.event_tokens < 1.0 {
            return Err(RateLimitReason::EventsExhausted);
        }

        let needed_bytes = message_bytes as f64;
        if self.byte_tokens < needed_bytes {
            return Err(RateLimitReason::BytesExhausted);
        }

        self.event_tokens -= 1.0;
        self.byte_tokens -= needed_bytes;
        Ok(())
    }
}

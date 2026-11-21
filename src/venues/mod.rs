pub mod binance;
pub mod coinbase;
pub mod polymarket;

use crate::model::{Fill, Venue};
use std::time::Duration;

/// One per venue connection. `parse` is the hot path: it gets a borrowed frame and
/// appends zero or more normalized fills to a reused buffer.
pub trait Normalizer: Send + 'static {
    fn venue(&self) -> Venue;
    fn url(&self) -> String;
    /// Text frames to send after connecting.
    fn subscribe(&self) -> Vec<String> {
        Vec::new()
    }
    /// Application-level keepalive some venues require (Polymarket wants "PING").
    fn keepalive(&self) -> Option<(Duration, &'static str)> {
        None
    }
    /// Reconnect if no frame arrives for this long. A connection that is open but silent
    /// is the worst failure mode for a recorder: nothing errors, data just stops.
    fn stale_after(&self) -> Duration {
        Duration::from_secs(30)
    }
    fn parse(&mut self, frame: &str, recv_us: i64, out: &mut Vec<Fill>) -> anyhow::Result<()>;
}

/// Accepts `"123"` or `123`. Venues are not consistent about quoting integers.
pub(crate) mod flex {
    use serde::de::{self, Deserializer, Visitor};
    use std::fmt;

    pub fn opt_u64<'de, D: Deserializer<'de>>(d: D) -> Result<Option<u64>, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = Option<u64>;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("integer or integer string")
            }
            fn visit_u64<E: de::Error>(self, v: u64) -> Result<Self::Value, E> {
                Ok(Some(v))
            }
            fn visit_i64<E: de::Error>(self, v: i64) -> Result<Self::Value, E> {
                u64::try_from(v).map(Some).map_err(E::custom)
            }
            fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
                crate::fixed::parse_u64(v).map(Some).ok_or_else(|| E::custom("bad integer string"))
            }
            fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(None)
            }
            fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(None)
            }
        }
        d.deserialize_any(V)
    }
}

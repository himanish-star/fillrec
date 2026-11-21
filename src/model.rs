//! The normalized ledger row. One schema for a centralized-exchange spot fill, a perp
//! fill and a prediction-market fill, without flattening away what differs:
//! - `finality` says whether the fill is final at match time (CEX) or matched
//!   off-chain and still settling on-chain (Polymarket CLOB -> Polygon).
//! - `synthetic_id` says whether `trade_id` came from the venue or was derived by us,
//!   which changes how dedupe and gap detection work for that row.
//! - `fee_rate_bps` is present only where the venue publishes it.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(i8)]
pub enum Venue {
    BinanceSpot = 1,
    BinanceUsdm = 2,
    Coinbase = 3,
    Polymarket = 4,
}

impl Venue {
    pub const ALL: [Venue; 4] = [Venue::BinanceSpot, Venue::BinanceUsdm, Venue::Coinbase, Venue::Polymarket];

    pub fn as_str(self) -> &'static str {
        match self {
            Venue::BinanceSpot => "binance_spot",
            Venue::BinanceUsdm => "binance_usdm",
            Venue::Coinbase => "coinbase",
            Venue::Polymarket => "polymarket",
        }
    }

    pub fn parse(s: &str) -> Option<Venue> {
        Venue::ALL.into_iter().find(|v| v.as_str() == s || v.as_str().replace('_', "-") == s)
    }

    #[inline]
    pub fn idx(self) -> usize {
        self as usize - 1
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i8)]
pub enum MarketKind {
    Spot = 1,
    Perp = 2,
    Prediction = 3,
}

/// Aggressor (taker) side. Venues disagree on what "side" means:
/// Binance sends `m` = buyer-is-maker, Coinbase sends the *maker* order's side,
/// Polymarket sends the taker side. All are normalized to taker side here.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i8)]
pub enum Side {
    Unknown = 0,
    Buy = 1,
    Sell = 2,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i8)]
pub enum Finality {
    /// Final at match time (centralized venue).
    Final = 1,
    /// Matched off-chain, settlement transaction not yet confirmed on-chain.
    MatchedPendingSettlement = 2,
}

impl MarketKind {
    pub fn as_str(self) -> &'static str {
        match self {
            MarketKind::Spot => "spot",
            MarketKind::Perp => "perp",
            MarketKind::Prediction => "prediction",
        }
    }
}

impl Side {
    pub fn as_str(self) -> &'static str {
        match self {
            Side::Unknown => "unknown",
            Side::Buy => "buy",
            Side::Sell => "sell",
        }
    }
}

impl Finality {
    pub fn as_str(self) -> &'static str {
        match self {
            Finality::Final => "final",
            Finality::MatchedPendingSettlement => "matched_pending_settlement",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Fill {
    pub venue: Venue,
    pub kind: MarketKind,
    pub symbol: Arc<str>,
    pub trade_id: u64,
    pub synthetic_id: bool,
    pub side: Side,
    /// Price, fixed-point 1e-8.
    pub price: i64,
    /// Quantity, fixed-point 1e-8.
    pub qty: i64,
    pub finality: Finality,
    pub fee_rate_bps: Option<u16>,
    /// Venue's own trade timestamp, microseconds since epoch.
    pub venue_ts_us: i64,
    /// When the frame arrived in this process, microseconds since epoch.
    pub recv_ts_us: i64,
}

#[inline]
pub fn now_us() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_micros() as i64).unwrap_or(0)
}

/// Interns symbol strings so a fill carries an `Arc<str>` (one atomic increment)
/// instead of allocating a fresh `String` per trade.
#[derive(Default)]
pub struct Interner(HashMap<Box<str>, Arc<str>>);

impl Interner {
    #[inline]
    pub fn get(&mut self, s: &str) -> Arc<str> {
        if let Some(a) = self.0.get(s) {
            return a.clone();
        }
        let a: Arc<str> = Arc::from(s);
        self.0.insert(s.into(), a.clone());
        a
    }

    pub fn insert_alias(&mut self, key: &str, value: Arc<str>) {
        self.0.insert(key.into(), value);
    }
}

/// FNV-1a 64. Used for synthetic trade ids, which are persisted, so the hash must be
/// stable across builds and Rust versions (std's `DefaultHasher` is not).
#[inline]
pub fn fnv1a64(parts: &[&[u8]]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for p in parts {
        for &b in *p {
            h ^= b as u64;
            h = h.wrapping_mul(0x0000_0100_0000_01b3);
        }
        h ^= 0xff; // separator so ("ab","c") != ("a","bc")
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

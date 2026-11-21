//! Binance spot (`<sym>@trade`) and USD-M perpetuals (`<sym>@aggTrade`) over the
//! combined-stream endpoint: `{"stream":"btcusdt@trade","data":{...}}`.
//!
//! USD-M market streams live under `/market/`. The legacy `/stream` path still completes
//! the WebSocket handshake but never sends data, which is why every venue connection
//! has a staleness watchdog (see `pipeline.rs`).

use super::Normalizer;
use crate::fixed::parse_fixed;
use crate::model::{Finality, Fill, Interner, MarketKind, Side, Venue};
use anyhow::{bail, Context};
use serde::Deserialize;

pub struct Binance {
    venue: Venue,
    symbols: Vec<String>,
    interner: Interner,
}

impl Binance {
    pub fn spot(symbols: Vec<String>) -> Self {
        Self { venue: Venue::BinanceSpot, symbols, interner: Interner::default() }
    }
    pub fn usdm(symbols: Vec<String>) -> Self {
        Self { venue: Venue::BinanceUsdm, symbols, interner: Interner::default() }
    }
}

#[derive(Deserialize)]
struct Envelope<'a> {
    #[serde(borrow)]
    data: Option<Event<'a>>,
}

#[derive(Deserialize)]
struct Event<'a> {
    e: &'a str,
    s: &'a str,
    /// trade id (spot `trade` stream)
    t: Option<u64>,
    /// aggregate trade id (futures `aggTrade` stream)
    a: Option<u64>,
    p: &'a str,
    q: &'a str,
    #[serde(rename = "T")]
    trade_time_ms: i64,
    /// buyer is maker => the aggressor sold
    m: bool,
}

impl Normalizer for Binance {
    fn venue(&self) -> Venue {
        self.venue
    }

    fn url(&self) -> String {
        let (host, stream) = match self.venue {
            Venue::BinanceUsdm => ("wss://fstream.binance.com/market", "aggTrade"),
            _ => ("wss://stream.binance.com:9443", "trade"),
        };
        let streams: Vec<String> =
            self.symbols.iter().map(|s| format!("{}@{}", s.to_lowercase(), stream)).collect();
        format!("{host}/stream?streams={}", streams.join("/"))
    }

    #[inline]
    fn parse(&mut self, frame: &str, recv_us: i64, out: &mut Vec<Fill>) -> anyhow::Result<()> {
        let env: Envelope = serde_json::from_str(frame).context("binance envelope")?;
        let Some(ev) = env.data else { return Ok(()) };
        let (kind, id) = match (ev.e, ev.t, ev.a) {
            ("trade", Some(t), _) => (MarketKind::Spot, t),
            ("aggTrade", _, Some(a)) => (MarketKind::Perp, a),
            _ => bail!("unexpected binance event {}", ev.e),
        };
        out.push(Fill {
            venue: self.venue,
            kind,
            symbol: self.interner.get(ev.s),
            trade_id: id,
            synthetic_id: false,
            side: if ev.m { Side::Sell } else { Side::Buy },
            price: parse_fixed(ev.p).context("price")?,
            qty: parse_fixed(ev.q).context("qty")?,
            finality: Finality::Final,
            fee_rate_bps: None,
            venue_ts_us: ev.trade_time_ms * 1_000,
            recv_ts_us: recv_us,
        });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spot_trade() {
        let f = r#"{"stream":"btcusdt@trade","data":{"e":"trade","E":1763700000001,"s":"BTCUSDT","t":5000000001,"p":"91234.56000000","q":"0.00120000","T":1763700000000,"m":true,"M":true}}"#;
        let mut b = Binance::spot(vec!["BTCUSDT".into()]);
        let mut out = vec![];
        b.parse(f, 1, &mut out).unwrap();
        assert_eq!(out.len(), 1);
        let x = &out[0];
        assert_eq!((x.trade_id, x.side, x.price, x.qty), (5_000_000_001, Side::Sell, 9_123_456_000_000, 120_000));
        assert_eq!(x.venue_ts_us, 1_763_700_000_000_000);
        assert_eq!(&*x.symbol, "BTCUSDT");
    }

    #[test]
    fn perp_agg_trade() {
        let f = r#"{"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","E":1763700000001,"a":2900000000,"s":"BTCUSDT","p":"91200.10","q":"0.015","f":1,"l":3,"T":1763700000000,"m":false}}"#;
        let mut b = Binance::usdm(vec!["BTCUSDT".into()]);
        let mut out = vec![];
        b.parse(f, 1, &mut out).unwrap();
        assert_eq!((out[0].kind, out[0].side, out[0].trade_id), (MarketKind::Perp, Side::Buy, 2_900_000_000));
    }

    #[test]
    fn ignores_control_frames() {
        let mut b = Binance::spot(vec![]);
        let mut out = vec![];
        b.parse(r#"{"result":null,"id":1}"#, 1, &mut out).unwrap();
        assert!(out.is_empty());
    }
}

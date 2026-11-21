//! Polymarket CLOB public market channel (`last_trade_price` events).
//!
//! Differences from the CEX feeds, all kept in the row rather than flattened away:
//! - Trades match off-chain on the CLOB and settle on Polygon afterwards, so the
//!   public feed only proves "matched": finality = MatchedPendingSettlement.
//! - The event has no trade id, so we derive a stable one (FNV-1a over the event's
//!   fields) and mark it `synthetic_id`. Dedupe uses a bounded seen-set instead of
//!   a sequence high-water mark, and gaps cannot be detected.
//! - Fee rate is published per trade, so it is recorded.

use super::{flex, Normalizer};
use crate::fixed::parse_fixed;
use crate::model::{fnv1a64, Finality, Fill, Interner, MarketKind, Side, Venue};
use anyhow::Context;
use serde::Deserialize;
use std::time::Duration;

pub struct Polymarket {
    /// (clob token id, human label "slug:Outcome")
    assets: Vec<(String, String)>,
    interner: Interner,
}

impl Polymarket {
    pub fn new(assets: Vec<(String, String)>) -> Self {
        let mut interner = Interner::default();
        for (id, label) in &assets {
            let l = interner.get(label);
            interner.insert_alias(id, l);
        }
        Self { assets, interner }
    }

    /// Top active markets by 24h volume from the public Gamma API (cold path).
    pub async fn discover(n: usize) -> anyhow::Result<Vec<(String, String)>> {
        let url = format!(
            "https://gamma-api.polymarket.com/markets?active=true&closed=false&order=volume24hr&ascending=false&limit={n}"
        );
        let client = reqwest::Client::builder().user_agent(concat!("fillrec/", env!("CARGO_PKG_VERSION"))).build()?;
        let mut attempt = 0;
        let v: serde_json::Value = loop {
            attempt += 1;
            let r = async { client.get(&url).send().await?.error_for_status()?.json().await }.await;
            match r {
                Ok(v) => break v,
                Err(e) if attempt < 8 => {
                    tracing::warn!("polymarket discovery attempt {attempt} failed: {e}");
                    tokio::time::sleep(std::time::Duration::from_millis(250 * attempt)).await;
                }
                Err(e) => return Err(e.into()),
            }
        };
        let mut out = Vec::new();
        for m in v.as_array().context("gamma: expected array")? {
            let slug = m["slug"].as_str().unwrap_or("?");
            let (Some(outcomes), Some(ids)) = (m["outcomes"].as_str(), m["clobTokenIds"].as_str()) else {
                continue;
            };
            let outcomes: Vec<String> = serde_json::from_str(outcomes).unwrap_or_default();
            let ids: Vec<String> = serde_json::from_str(ids).unwrap_or_default();
            for (id, o) in ids.into_iter().zip(outcomes) {
                out.push((id, format!("{slug}:{o}")));
            }
        }
        Ok(out)
    }
}

#[derive(Deserialize)]
struct Ev<'a> {
    event_type: Option<&'a str>,
    asset_id: Option<&'a str>,
    price: Option<&'a str>,
    size: Option<&'a str>,
    side: Option<&'a str>,
    #[serde(default, deserialize_with = "flex::opt_u64")]
    timestamp: Option<u64>,
    #[serde(default, deserialize_with = "flex::opt_u64")]
    fee_rate_bps: Option<u64>,
}

impl Polymarket {
    #[inline]
    fn one(&mut self, ev: Ev, recv_us: i64, out: &mut Vec<Fill>) -> anyhow::Result<()> {
        if ev.event_type != Some("last_trade_price") {
            return Ok(()); // book / price_change / tick_size_change
        }
        let asset = ev.asset_id.context("asset_id")?;
        let price = ev.price.context("price")?;
        let size = ev.size.context("size")?;
        let side = ev.side.unwrap_or("");
        let ts_ms = ev.timestamp.context("timestamp")?;
        // Masked to 63 bits: the ledger (Pinot LONG, Kafka JSON consumers) is signed 64-bit.
        let id = fnv1a64(&[asset.as_bytes(), &ts_ms.to_le_bytes(), price.as_bytes(), size.as_bytes(), side.as_bytes()])
            & 0x7FFF_FFFF_FFFF_FFFF;
        out.push(Fill {
            venue: Venue::Polymarket,
            kind: MarketKind::Prediction,
            symbol: self.interner.get(asset),
            trade_id: id,
            synthetic_id: true,
            side: match side {
                "BUY" => Side::Buy,
                "SELL" => Side::Sell,
                _ => Side::Unknown,
            },
            price: parse_fixed(price).context("price")?,
            qty: parse_fixed(size).context("size")?,
            finality: Finality::MatchedPendingSettlement,
            fee_rate_bps: ev.fee_rate_bps.and_then(|f| u16::try_from(f).ok()),
            venue_ts_us: ts_ms as i64 * 1_000,
            recv_ts_us: recv_us,
        });
        Ok(())
    }
}

impl Normalizer for Polymarket {
    fn venue(&self) -> Venue {
        Venue::Polymarket
    }

    fn url(&self) -> String {
        "wss://ws-subscriptions-clob.polymarket.com/ws/market".into()
    }

    fn subscribe(&self) -> Vec<String> {
        let ids: Vec<&str> = self.assets.iter().map(|(id, _)| id.as_str()).collect();
        vec![serde_json::json!({ "assets_ids": ids, "type": "market" }).to_string()]
    }

    fn stale_after(&self) -> Duration {
        Duration::from_secs(120) // PONGs count as frames; prediction markets can be quiet
    }

    fn keepalive(&self) -> Option<(Duration, &'static str)> {
        Some((Duration::from_secs(10), "PING"))
    }

    fn parse(&mut self, frame: &str, recv_us: i64, out: &mut Vec<Fill>) -> anyhow::Result<()> {
        let t = frame.trim_start();
        if t.starts_with('[') {
            let evs: Vec<Ev> = serde_json::from_str(t).context("polymarket array")?;
            for ev in evs {
                self.one(ev, recv_us, out)?;
            }
            Ok(())
        } else if t.starts_with('{') {
            let ev: Ev = serde_json::from_str(t).context("polymarket event")?;
            self.one(ev, recv_us, out)
        } else {
            Ok(()) // "PONG"
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const F: &str = r#"{"asset_id":"1143","event_type":"last_trade_price","fee_rate_bps":"0","market":"0x6a","price":"0.456","side":"BUY","size":"219.217767","timestamp":"1750428146322"}"#;

    #[test]
    fn last_trade_price() {
        let mut p = Polymarket::new(vec![("1143".into(), "will-it-rain:Yes".into())]);
        let mut out = vec![];
        p.parse(F, 1, &mut out).unwrap();
        let x = &out[0];
        assert_eq!(&*x.symbol, "will-it-rain:Yes");
        assert_eq!((x.price, x.qty, x.side), (45_600_000, 21_921_776_700, Side::Buy));
        assert_eq!(x.finality, Finality::MatchedPendingSettlement);
        assert!(x.synthetic_id);
        assert_eq!(x.fee_rate_bps, Some(0));
    }

    #[test]
    fn synthetic_id_is_stable() {
        let mut p = Polymarket::new(vec![]);
        let (mut a, mut b) = (vec![], vec![]);
        p.parse(F, 1, &mut a).unwrap();
        p.parse(F, 999, &mut b).unwrap();
        assert_eq!(a[0].trade_id, b[0].trade_id);
        assert!(a[0].trade_id <= i64::MAX as u64, "must fit a signed LONG column");
    }

    #[test]
    fn arrays_and_pong() {
        let mut p = Polymarket::new(vec![]);
        let mut out = vec![];
        p.parse(&format!("[{F},{{\"event_type\":\"book\",\"asset_id\":\"1\",\"bids\":[{{\"price\":\"0.1\",\"size\":\"5\"}}]}}]"), 1, &mut out).unwrap();
        p.parse("PONG", 1, &mut out).unwrap();
        assert_eq!(out.len(), 1);
    }
}

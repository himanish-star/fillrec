//! Coinbase Exchange public `matches` channel.
//! Note: Coinbase's `side` is the *maker* order's side, so the taker side is the opposite.

use super::Normalizer;
use crate::fixed::{parse_fixed, parse_rfc3339_us};
use crate::model::{Finality, Fill, Interner, MarketKind, Side, Venue};
use anyhow::{bail, Context};
use serde::Deserialize;

pub struct Coinbase {
    products: Vec<String>,
    interner: Interner,
}

impl Coinbase {
    pub fn new(products: Vec<String>) -> Self {
        Self { products, interner: Interner::default() }
    }
}

#[derive(Deserialize)]
struct Msg<'a> {
    #[serde(rename = "type")]
    ty: &'a str,
    trade_id: Option<u64>,
    side: Option<&'a str>,
    size: Option<&'a str>,
    price: Option<&'a str>,
    product_id: Option<&'a str>,
    time: Option<&'a str>,
    message: Option<&'a str>,
}

impl Normalizer for Coinbase {
    fn venue(&self) -> Venue {
        Venue::Coinbase
    }

    fn url(&self) -> String {
        "wss://ws-feed.exchange.coinbase.com".into()
    }

    fn subscribe(&self) -> Vec<String> {
        vec![serde_json::json!({
            "type": "subscribe",
            "product_ids": self.products,
            "channels": ["matches"],
        })
        .to_string()]
    }

    #[inline]
    fn parse(&mut self, frame: &str, recv_us: i64, out: &mut Vec<Fill>) -> anyhow::Result<()> {
        let m: Msg = serde_json::from_str(frame).context("coinbase msg")?;
        match m.ty {
            "match" | "last_match" => {}
            "error" => bail!("coinbase error: {}", m.message.unwrap_or("")),
            _ => return Ok(()), // subscriptions, heartbeat
        }
        let maker_side = m.side.context("side")?;
        let taker = match maker_side {
            "buy" => Side::Sell,
            "sell" => Side::Buy,
            _ => Side::Unknown,
        };
        out.push(Fill {
            venue: Venue::Coinbase,
            kind: MarketKind::Spot,
            symbol: self.interner.get(m.product_id.context("product_id")?),
            trade_id: m.trade_id.context("trade_id")?,
            synthetic_id: false,
            side: taker,
            price: parse_fixed(m.price.context("price")?).context("price")?,
            qty: parse_fixed(m.size.context("size")?).context("size")?,
            finality: Finality::Final,
            fee_rate_bps: None,
            venue_ts_us: parse_rfc3339_us(m.time.context("time")?).context("time format")?,
            recv_ts_us: recv_us,
        });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn match_flips_maker_side() {
        let f = r#"{"type":"match","trade_id":10,"maker_order_id":"ac","taker_order_id":"13","side":"sell","size":"5.23512","price":"400.23","product_id":"BTC-USD","sequence":50,"time":"2014-11-07T08:19:27.028459Z"}"#;
        let mut c = Coinbase::new(vec![]);
        let mut out = vec![];
        c.parse(f, 1, &mut out).unwrap();
        let x = &out[0];
        assert_eq!(x.side, Side::Buy);
        assert_eq!((x.trade_id, x.price, x.qty), (10, 40_023_000_000, 523_512_000));
        assert_eq!(x.venue_ts_us, 1_415_348_367_028_459);
    }

    #[test]
    fn skips_subscriptions() {
        let mut c = Coinbase::new(vec![]);
        let mut out = vec![];
        c.parse(r#"{"type":"subscriptions","channels":[{"name":"matches","product_ids":["BTC-USD"]}]}"#, 1, &mut out).unwrap();
        assert!(out.is_empty());
    }
}

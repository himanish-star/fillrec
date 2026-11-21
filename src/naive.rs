//! Baseline parsers, kept only for benchmarking: the "obvious" implementation
//! (`serde_json::Value` tree + `f64` prices + a fresh `String` per symbol).
//! The real parsers in `venues/` replace each of those choices; `benches/normalize.rs`
//! measures what that is worth.

use anyhow::Context;
use serde_json::Value;

#[derive(Debug)]
pub struct NaiveFill {
    pub symbol: String,
    pub trade_id: u64,
    pub price: f64,
    pub qty: f64,
    pub ts_ms: i64,
}

pub fn binance(frame: &str) -> anyhow::Result<NaiveFill> {
    let v: Value = serde_json::from_str(frame)?;
    let d = &v["data"];
    Ok(NaiveFill {
        symbol: d["s"].as_str().context("s")?.to_string(),
        trade_id: d["t"].as_u64().or_else(|| d["a"].as_u64()).context("id")?,
        price: d["p"].as_str().context("p")?.parse()?,
        qty: d["q"].as_str().context("q")?.parse()?,
        ts_ms: d["T"].as_i64().context("T")?,
    })
}

pub fn coinbase(frame: &str) -> anyhow::Result<NaiveFill> {
    let v: Value = serde_json::from_str(frame)?;
    Ok(NaiveFill {
        symbol: v["product_id"].as_str().context("product_id")?.to_string(),
        trade_id: v["trade_id"].as_u64().context("trade_id")?,
        price: v["price"].as_str().context("price")?.parse()?,
        qty: v["size"].as_str().context("size")?.parse()?,
        ts_ms: 0,
    })
}

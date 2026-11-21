//! Sinks.
//! - `Kafka`: JSON rows keyed by `venue|symbol|trade_id` into a topic that an Apache Pinot
//!   REALTIME upsert table consumes (see `pinot/`). Keying by the primary key keeps every
//!   version of a trade in one partition, which Pinot upsert requires.
//! - `Ndjson`: one JSON row per line to a file.
//! - `Null`: measure the pipeline without storage.

use crate::model::Fill;
use anyhow::Context;
use rdkafka::config::ClientConfig;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::time::Duration;

pub enum Sink {
    Null,
    Ndjson(BufWriter<File>),
    Kafka(KafkaSink),
}

pub struct KafkaSink {
    producer: FutureProducer,
    topic: String,
    key: String,
    val: String,
}

impl KafkaSink {
    pub fn new(brokers: &str, topic: &str) -> anyhow::Result<Self> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            // Ledger semantics: no duplicates from producer retries, wait for all in-sync replicas.
            .set("enable.idempotence", "true")
            .set("acks", "all")
            .set("linger.ms", "2")
            .set("compression.type", "lz4")
            .set("message.timeout.ms", "30000")
            .create()
            .context("kafka producer")?;
        Ok(Self { producer, topic: topic.into(), key: String::with_capacity(128), val: String::with_capacity(512) })
    }
}

/// Hand-written JSON encoder for one row. Field order matches `pinot/fills_schema.json`.
/// Prices/quantities go out as exact integers scaled 1e8 (`price_e8`), never floats.
#[inline]
pub fn encode_json(out: &mut String, f: &Fill, ingest_us: i64) {
    use std::fmt::Write as _;
    out.clear();
    let _ = write!(
        out,
        r#"{{"venue":"{}","market_kind":"{}","symbol":"{}","trade_id":{},"synthetic_id":{},"side":"{}","price_e8":{},"qty_e8":{},"finality":"{}","fee_rate_bps":{},"venue_ts_us":{},"recv_ts_us":{},"ingest_ts_us":{},"venue_ts_ms":{}}}"#,
        f.venue.as_str(),
        f.kind.as_str(),
        JsonStr(&f.symbol),
        f.trade_id,
        f.synthetic_id,
        f.side.as_str(),
        f.price,
        f.qty,
        f.finality.as_str(),
        match f.fee_rate_bps {
            Some(v) => v as i32,
            None => -1,
        },
        f.venue_ts_us,
        f.recv_ts_us,
        ingest_us,
        f.venue_ts_us / 1000,
    );
}

/// Escapes a string for JSON (symbols are venue-controlled, so never trust them).
struct JsonStr<'a>(&'a str);
impl std::fmt::Display for JsonStr<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for c in self.0.chars() {
            match c {
                '"' => f.write_str("\\\"")?,
                '\\' => f.write_str("\\\\")?,
                c if (c as u32) < 0x20 => write!(f, "\\u{:04x}", c as u32)?,
                c => std::fmt::Write::write_char(f, c)?,
            }
        }
        Ok(())
    }
}

#[inline]
pub fn encode_key(out: &mut String, f: &Fill) {
    use std::fmt::Write as _;
    out.clear();
    let _ = write!(out, "{}|{}|{}", f.venue.as_str(), f.symbol, f.trade_id);
}

impl Sink {
    /// Returns once every row in `fills` is durably acknowledged by the sink.
    pub async fn write(&mut self, fills: &[Fill], ingest_us: i64) -> anyhow::Result<()> {
        match self {
            Sink::Null => Ok(()),
            Sink::Ndjson(w) => {
                let mut s = String::with_capacity(512);
                for f in fills {
                    encode_json(&mut s, f, ingest_us);
                    w.write_all(s.as_bytes())?;
                    w.write_all(b"\n")?;
                }
                w.flush()?;
                Ok(())
            }
            Sink::Kafka(k) => {
                // Enqueue the whole batch, then await all delivery reports.
                let mut pending = Vec::with_capacity(fills.len());
                for f in fills {
                    encode_key(&mut k.key, f);
                    encode_json(&mut k.val, f, ingest_us);
                    loop {
                        let rec = FutureRecord::to(&k.topic).key(k.key.as_str()).payload(k.val.as_str());
                        match k.producer.send_result(rec) {
                            Ok(delivery) => {
                                pending.push(delivery);
                                break;
                            }
                            // Local queue full: let librdkafka drain, then retry this row.
                            Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), _)) => {
                                tokio::time::sleep(Duration::from_millis(1)).await;
                            }
                            Err((e, _)) => return Err(e).context("kafka enqueue"),
                        }
                    }
                }
                for p in pending {
                    p.await.context("kafka delivery cancelled")?.map_err(|(e, _)| e).context("kafka delivery")?;
                }
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::*;
    use std::sync::Arc;

    #[test]
    fn json_row_is_valid_and_exact() {
        let f = Fill {
            venue: Venue::Polymarket,
            kind: MarketKind::Prediction,
            symbol: Arc::from("will-\"x\"-happen:Yes"),
            trade_id: 42,
            synthetic_id: true,
            side: Side::Buy,
            price: 45_600_000,
            qty: 21_921_776_700,
            finality: Finality::MatchedPendingSettlement,
            fee_rate_bps: Some(0),
            venue_ts_us: 1_750_428_146_322_000,
            recv_ts_us: 1_750_428_146_400_000,
        };
        let mut s = String::new();
        encode_json(&mut s, &f, 7);
        let v: serde_json::Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["symbol"], "will-\"x\"-happen:Yes");
        assert_eq!(v["price_e8"], 45_600_000);
        assert_eq!(v["finality"], "matched_pending_settlement");
        assert_eq!(v["venue_ts_ms"], 1_750_428_146_322i64);
    }
}

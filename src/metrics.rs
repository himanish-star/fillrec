//! Per-venue counters and latency histograms (HdrHistogram, 3 significant digits).

use crate::model::Venue;
use hdrhistogram::Histogram;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Mutex;
use std::time::Instant;

fn hist() -> Histogram<u64> {
    Histogram::new_with_bounds(1, 3_600_000_000, 3).expect("hist bounds")
}

pub struct Hists {
    /// Time to parse + normalize one frame, nanoseconds.
    pub parse_ns: Histogram<u64>,
    /// Venue trade timestamp -> frame received here, microseconds (includes clock skew).
    pub wire_us: Histogram<u64>,
    /// Frame received -> row acknowledged by the sink, microseconds.
    pub commit_us: Histogram<u64>,
}

#[derive(Default)]
pub struct VenueCounters {
    pub frames: AtomicU64,
    pub fills: AtomicU64,
    pub dupes: AtomicU64,
    pub gap_events: AtomicU64,
    pub missed_ids: AtomicU64,
    pub parse_errors: AtomicU64,
    pub reconnects: AtomicU64,
    pub stale_reconnects: AtomicU64,
    pub negative_wire: AtomicU64,
}

pub struct VenueMetrics {
    pub c: VenueCounters,
    pub h: Mutex<Hists>,
}

pub struct Metrics {
    pub started: Instant,
    pub venues: [VenueMetrics; 4],
    pub batches: AtomicU64,
    pub rows: AtomicU64,
    pub insert_errors: AtomicU64,
    pub insert_us: Mutex<Histogram<u64>>,
}

#[inline]
pub fn rec(h: &mut Histogram<u64>, v: i64) {
    let _ = h.record(v.max(1) as u64);
}

impl Metrics {
    pub fn new() -> Self {
        let vm = || VenueMetrics {
            c: VenueCounters::default(),
            h: Mutex::new(Hists { parse_ns: hist(), wire_us: hist(), commit_us: hist() }),
        };
        Self {
            started: Instant::now(),
            venues: [vm(), vm(), vm(), vm()],
            batches: AtomicU64::new(0),
            rows: AtomicU64::new(0),
            insert_errors: AtomicU64::new(0),
            insert_us: Mutex::new(hist()),
        }
    }

    pub fn venue(&self, v: Venue) -> &VenueMetrics {
        &self.venues[v.idx()]
    }

    pub fn report_line(&self) -> String {
        let secs = self.started.elapsed().as_secs_f64().max(1e-9);
        let mut s = format!(
            "{:<13} {:>8} {:>8} {:>5} {:>6} {:>4} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9}\n",
            "venue", "fills", "fills/s", "dup", "missed", "rc", "parse p50", "parse p99", "wire p50", "wire p99", "commit p50", "commit p99"
        );
        for v in Venue::ALL {
            let m = self.venue(v);
            let fills = m.c.fills.load(Relaxed);
            if m.c.frames.load(Relaxed) == 0 {
                continue;
            }
            let h = m.h.lock().unwrap();
            s += &format!(
                "{:<13} {:>8} {:>8.1} {:>5} {:>6} {:>4} {:>7}ns {:>7}ns {:>7.1}ms {:>7.1}ms {:>7.1}ms {:>7.1}ms\n",
                v.as_str(),
                fills,
                fills as f64 / secs,
                m.c.dupes.load(Relaxed),
                m.c.missed_ids.load(Relaxed),
                m.c.reconnects.load(Relaxed),
                h.parse_ns.value_at_quantile(0.5),
                h.parse_ns.value_at_quantile(0.99),
                h.wire_us.value_at_quantile(0.5) as f64 / 1e3,
                h.wire_us.value_at_quantile(0.99) as f64 / 1e3,
                h.commit_us.value_at_quantile(0.5) as f64 / 1e3,
                h.commit_us.value_at_quantile(0.99) as f64 / 1e3,
            );
        }
        let ins = self.insert_us.lock().unwrap();
        s += &format!(
            "sink: {} batches, {} rows, {} errors, insert p50 {:.1}ms p99 {:.1}ms",
            self.batches.load(Relaxed),
            self.rows.load(Relaxed),
            self.insert_errors.load(Relaxed),
            ins.value_at_quantile(0.5) as f64 / 1e3,
            ins.value_at_quantile(0.99) as f64 / 1e3,
        );
        s
    }

    pub fn summary_json(&self) -> Value {
        let q = |h: &Histogram<u64>| {
            json!({
                "count": h.len(),
                "p50": h.value_at_quantile(0.5),
                "p90": h.value_at_quantile(0.9),
                "p99": h.value_at_quantile(0.99),
                "p999": h.value_at_quantile(0.999),
                "max": h.max(),
            })
        };
        let mut venues = serde_json::Map::new();
        for v in Venue::ALL {
            let m = self.venue(v);
            if m.c.frames.load(Relaxed) == 0 {
                continue;
            }
            let h = m.h.lock().unwrap();
            venues.insert(
                v.as_str().into(),
                json!({
                    "frames": m.c.frames.load(Relaxed),
                    "fills": m.c.fills.load(Relaxed),
                    "dupes": m.c.dupes.load(Relaxed),
                    "gap_events": m.c.gap_events.load(Relaxed),
                    "missed_ids": m.c.missed_ids.load(Relaxed),
                    "parse_errors": m.c.parse_errors.load(Relaxed),
                    "reconnects": m.c.reconnects.load(Relaxed),
                    "stale_reconnects": m.c.stale_reconnects.load(Relaxed),
                    "negative_wire_latency": m.c.negative_wire.load(Relaxed),
                    "parse_ns": q(&h.parse_ns),
                    "wire_us": q(&h.wire_us),
                    "commit_us": q(&h.commit_us),
                }),
            );
        }
        json!({
            "duration_s": self.started.elapsed().as_secs_f64(),
            "venues": venues,
            "sink": {
                "batches": self.batches.load(Relaxed),
                "rows": self.rows.load(Relaxed),
                "errors": self.insert_errors.load(Relaxed),
                "insert_us": q(&self.insert_us.lock().unwrap()),
            }
        })
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

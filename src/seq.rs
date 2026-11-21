//! Dedupe and gap detection, per connection.
//!
//! Venue-issued ids (Binance trade / aggTrade id, Coinbase trade_id) are consecutive
//! per symbol, so a high-water mark per symbol catches both duplicates (replayed
//! `last_match` after reconnect) and gaps (trades missed while disconnected).
//! Synthetic ids carry no order, so they get a bounded seen-set and no gap detection.

use crate::model::Fill;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq)]
pub enum Check {
    New,
    Dup,
    /// New, but this many ids were skipped before it.
    Gap(u64),
}

pub struct SeqTracker {
    hwm: HashMap<Arc<str>, u64>,
    seen: HashSet<u64>,
    order: VecDeque<u64>,
    cap: usize,
}

impl SeqTracker {
    pub fn new(cap: usize) -> Self {
        Self { hwm: HashMap::new(), seen: HashSet::with_capacity(cap), order: VecDeque::with_capacity(cap), cap }
    }

    #[inline]
    pub fn check(&mut self, f: &Fill) -> Check {
        if f.synthetic_id {
            if !self.seen.insert(f.trade_id) {
                return Check::Dup;
            }
            self.order.push_back(f.trade_id);
            if self.order.len() > self.cap {
                if let Some(old) = self.order.pop_front() {
                    self.seen.remove(&old);
                }
            }
            return Check::New;
        }
        match self.hwm.get_mut(&*f.symbol) {
            None => {
                self.hwm.insert(f.symbol.clone(), f.trade_id);
                Check::New
            }
            Some(h) if f.trade_id <= *h => Check::Dup,
            Some(h) => {
                let gap = f.trade_id - *h - 1;
                *h = f.trade_id;
                if gap == 0 {
                    Check::New
                } else {
                    Check::Gap(gap)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::*;

    fn fill(sym: &str, id: u64, synthetic: bool) -> Fill {
        Fill {
            venue: Venue::Coinbase,
            kind: MarketKind::Spot,
            symbol: Arc::from(sym),
            trade_id: id,
            synthetic_id: synthetic,
            side: Side::Buy,
            price: 1,
            qty: 1,
            finality: Finality::Final,
            fee_rate_bps: None,
            venue_ts_us: 0,
            recv_ts_us: 0,
        }
    }

    #[test]
    fn hwm() {
        let mut s = SeqTracker::new(4);
        assert_eq!(s.check(&fill("A", 10, false)), Check::New);
        assert_eq!(s.check(&fill("A", 11, false)), Check::New);
        assert_eq!(s.check(&fill("A", 11, false)), Check::Dup);
        assert_eq!(s.check(&fill("A", 15, false)), Check::Gap(3));
        assert_eq!(s.check(&fill("B", 1, false)), Check::New);
    }

    #[test]
    fn synthetic_bounded() {
        let mut s = SeqTracker::new(2);
        assert_eq!(s.check(&fill("P", 7, true)), Check::New);
        assert_eq!(s.check(&fill("P", 7, true)), Check::Dup);
        s.check(&fill("P", 8, true));
        s.check(&fill("P", 9, true)); // evicts 7
        assert_eq!(s.check(&fill("P", 7, true)), Check::New);
    }
}

//! Venue connection loops and the batching writer.
//!
//! Per venue: connect -> subscribe -> read frames -> parse into a reused buffer ->
//! dedupe/gap check -> bounded channel. The channel is the backpressure point: if the
//! sink stalls, venue readers block rather than drop rows, and the stall shows up as
//! a gap (missed ids) on reconnect instead of silent loss.
//!
//! Writer: batches by size or time, retries a failed insert forever with capped
//! backoff (never drops a batch), and records frame-received -> committed latency.

use crate::metrics::{rec, Metrics};
use crate::model::{now_us, Fill, Venue};
use crate::seq::{Check, SeqTracker};
use crate::sink::Sink;
use crate::venues::Normalizer;
use futures_util::{SinkExt, StreamExt};
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{info, warn};

pub type CaptureTx = mpsc::UnboundedSender<(Venue, i64, String)>;

pub async fn run_venue(
    mut n: Box<dyn Normalizer>,
    tx: mpsc::Sender<Fill>,
    metrics: Arc<Metrics>,
    capture: Option<CaptureTx>,
    mut shutdown: watch::Receiver<bool>,
) {
    let venue = n.venue();
    let m = metrics.venue(venue);
    let mut seq = SeqTracker::new(200_000);
    let mut buf: Vec<Fill> = Vec::with_capacity(64);
    let mut backoff = Duration::from_millis(250);
    let mut first = true;

    loop {
        if *shutdown.borrow() {
            return;
        }
        if !first {
            m.c.reconnects.fetch_add(1, Relaxed);
            tokio::select! {
                _ = tokio::time::sleep(backoff) => {}
                _ = shutdown.changed() => return,
            }
            backoff = (backoff * 2).min(Duration::from_secs(10));
        }
        first = false;

        let url = n.url();
        let ws = match tokio::time::timeout(Duration::from_secs(10), connect_async(&url)).await {
            Ok(Ok((ws, _))) => ws,
            Ok(Err(e)) => {
                warn!(venue = venue.as_str(), "connect failed: {e}");
                continue;
            }
            Err(_) => {
                warn!(venue = venue.as_str(), "connect timed out after 10s");
                continue;
            }
        };
        info!(venue = venue.as_str(), "connected");
        let (mut write, mut read) = ws.split();
        let mut ok = true;
        for s in n.subscribe() {
            if let Err(e) = write.send(Message::Text(s)).await {
                warn!(venue = venue.as_str(), "subscribe failed: {e}");
                ok = false;
            }
        }
        if !ok {
            continue;
        }
        let ka = n.keepalive();
        let mut ka_tick = tokio::time::interval(ka.map(|k| k.0).unwrap_or(Duration::from_secs(3600)));
        ka_tick.tick().await;
        let stale_after = n.stale_after();
        let stale = tokio::time::sleep(stale_after);
        tokio::pin!(stale);

        loop {
            tokio::select! {
                _ = &mut stale => {
                    m.c.stale_reconnects.fetch_add(1, Relaxed);
                    warn!(venue = venue.as_str(), "no frames for {stale_after:?}; reconnecting");
                    break;
                }
                msg = read.next() => {
                    let text = match msg {
                        Some(Ok(Message::Text(t))) => t,
                        Some(Ok(Message::Binary(b))) => match String::from_utf8(b) { Ok(t) => t, Err(_) => continue },
                        Some(Ok(Message::Ping(p))) => { let _ = write.send(Message::Pong(p)).await; continue }
                        Some(Ok(Message::Close(c))) => { warn!(venue = venue.as_str(), "closed by venue: {c:?}"); break }
                        Some(Ok(_)) => continue,
                        Some(Err(e)) => { warn!(venue = venue.as_str(), "read error: {e}"); break }
                        None => { warn!(venue = venue.as_str(), "stream ended"); break }
                    };
                    let recv_us = now_us();
                    stale.as_mut().reset(tokio::time::Instant::now() + stale_after);
                    m.c.frames.fetch_add(1, Relaxed);
                    let t0 = Instant::now();
                    buf.clear();
                    let res = n.parse(&text, recv_us, &mut buf);
                    let parse_ns = t0.elapsed().as_nanos() as i64;
                    if let Err(e) = res {
                        m.c.parse_errors.fetch_add(1, Relaxed);
                        warn!(venue = venue.as_str(), "parse error: {e:#}; frame={}", &text[..text.len().min(300)]);
                    }
                    if let Some(c) = &capture {
                        let _ = c.send((venue, recv_us, text.clone()));
                    }
                    {
                        let mut h = m.h.lock().unwrap();
                        rec(&mut h.parse_ns, parse_ns);
                        for f in &buf {
                            let wire = recv_us - f.venue_ts_us;
                            if wire < 0 { m.c.negative_wire.fetch_add(1, Relaxed); }
                            rec(&mut h.wire_us, wire);
                        }
                    }
                    for f in buf.drain(..) {
                        match seq.check(&f) {
                            Check::Dup => { m.c.dupes.fetch_add(1, Relaxed); continue }
                            Check::Gap(k) => {
                                m.c.gap_events.fetch_add(1, Relaxed);
                                m.c.missed_ids.fetch_add(k, Relaxed);
                                warn!(venue = venue.as_str(), symbol = &*f.symbol, missed = k, "sequence gap");
                            }
                            Check::New => {}
                        }
                        m.c.fills.fetch_add(1, Relaxed);
                        if tx.send(f).await.is_err() { return }
                    }
                    backoff = Duration::from_millis(250);
                }
                _ = ka_tick.tick(), if ka.is_some() => {
                    if let Err(e) = write.send(Message::Text(ka.unwrap().1.to_string())).await {
                        warn!(venue = venue.as_str(), "keepalive failed: {e}");
                        break;
                    }
                }
                _ = shutdown.changed() => {
                    let _ = write.send(Message::Close(None)).await;
                    return;
                }
            }
        }
    }
}

pub async fn run_writer(
    mut rx: mpsc::Receiver<Fill>,
    mut sink: Sink,
    batch: usize,
    flush_every: Duration,
    metrics: Arc<Metrics>,
) {
    let mut buf: Vec<Fill> = Vec::with_capacity(batch);
    let mut tick = tokio::time::interval(flush_every);
    loop {
        let closed = tokio::select! {
            biased;
            f = rx.recv() => match f {
                Some(f) => { buf.push(f); if buf.len() < batch { continue } false }
                None => true,
            },
            _ = tick.tick() => false,
        };
        if !buf.is_empty() {
            flush(&mut sink, &mut buf, &metrics).await;
        }
        if closed {
            return;
        }
    }
}

pub async fn flush(sink: &mut Sink, buf: &mut Vec<Fill>, metrics: &Metrics) {
    let mut backoff = Duration::from_millis(100);
    loop {
        let t0 = Instant::now();
        match sink.write(buf, now_us()).await {
            Ok(()) => {
                rec(&mut metrics.insert_us.lock().unwrap(), t0.elapsed().as_micros() as i64);
                break;
            }
            Err(e) => {
                metrics.insert_errors.fetch_add(1, Relaxed);
                warn!("sink write failed ({} rows), retrying in {backoff:?}: {e:#}", buf.len());
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(5));
            }
        }
    }
    let committed = now_us();
    metrics.batches.fetch_add(1, Relaxed);
    metrics.rows.fetch_add(buf.len() as u64, Relaxed);
    for v in Venue::ALL {
        if !buf.iter().any(|f| f.venue == v) {
            continue;
        }
        let mut h = metrics.venue(v).h.lock().unwrap();
        for f in buf.iter().filter(|f| f.venue == v) {
            rec(&mut h.commit_us, committed - f.recv_ts_us);
        }
    }
    buf.clear();
}

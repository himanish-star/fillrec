# Benchmarks

All numbers were measured on 21 Nov 2026 on an Apple M2 Max (12 cores, 32 GB), macOS, rustc 1.97.0, release build
(`lto = "thin"`, `codegen-units = 1`), over a home internet connection in India. The raw outputs are in `results/`.

## 1. Parse + normalize (Criterion)

`cargo bench --bench normalize`, over 2,000 real frames per venue captured from the live feeds
(`benches/fixtures/sample.tsv`).

| Venue | Production parser | Naive baseline | Speedup |
|---|---|---|---|
| Binance spot | **298 ns/frame** (3.35 M frames/s) | 847 ns/frame | 2.8× |
| Coinbase | **322 ns/frame** (3.10 M frames/s) | 797 ns/frame | 2.5× |
| Binance USD-M | 356 ns/frame (2.81 M frames/s) | – | – |
| Polymarket | 579 ns/frame (1.73 M frames/s)¹ | – | – |
| Encode one row as JSON for Kafka | 232 ns/row | – | – |

The production parser and the naive baseline (`src/naive.rs`) differ in three ways:

1. They deserialize into typed structs whose fields borrow `&str` from the frame, instead of building a `serde_json::Value` tree.
2. Prices and sizes are parsed straight into `i64 × 10^8`, never through `f64`.
3. Symbols are interned as `Arc<str>` instead of allocating a fresh `String` per row.

¹ Most Polymarket frames are order-book snapshots, which the parser must skip over but never turns into rows.

## 2. Replay throughput (`fillrec replay`)

This replays the full 90-second capture: 81,162 frames, which produce 32,126 fills.

| Sink | Batch | Throughput |
|---|---|---|
| none (parse only, 5 passes) | – | 2.37 M frames/s, 421 ns/frame mean |
| Kafka, `acks=all`, idempotent | 500 | 38 k fills/s |
| Kafka, `acks=all`, idempotent | 2,000 | 50 k fills/s |
| Kafka, `acks=all`, idempotent | 10,000 | 248 k fills/s |

The writer waits for every row in a batch to be acknowledged before it builds the next batch, so small batches are
bound by round trips. Live mode keeps small batches (500 rows / 20 ms) on purpose, because latency matters more there
than throughput. The next improvement is to keep several batches in flight while still committing them in order.

## 3. Live run into Kafka → Pinot (2 minutes, all four venues)

`fillrec run --sink kafka --duration 120`

| Venue | Fills | Parse p50 / p99 | Recv → committed p50 / p99 / p99.9 |
|---|---|---|---|
| Binance spot | 22,488 | 0.8 / 14.8 µs | 16.0 / 29.8 / 36.0 ms |
| Binance USD-M | 13,828 | 1.5 / 20.7 µs | 16.4 / 29.5 / 33.9 ms |
| Coinbase | 4,149 | 1.9 / 24.9 µs | 16.0 / 30.0 / 73.9 ms |
| Polymarket | 258 | 1.3 / 12.9 µs | 14.7 / 29.4 / 37.4 ms |

- **Total:** 40,723 rows in 4,166 batches, with 0 sink errors, 0 duplicates, 0 sequence gaps and 0 parse errors.
- **Pinot check:** `SELECT venue, COUNT(*) FROM fills GROUP BY venue` matched every venue's count exactly.
- **Kafka produce time per batch:** p50 4.5 ms, p99 10.8 ms.
- **Where the commit time goes:** recv → committed is mostly the 20 ms flush timer (rows wait half of it on average) plus the Kafka round trip.
- **Live parse vs Criterion:** live parse p50 (0.8–1.9 µs) is higher than in Criterion (~0.3 µs). The live parser runs cold, interleaved with network I/O on the same thread, and the measurement includes `Instant::now()` overhead.

## 4. Idempotency check

The same capture was replayed into Kafka three times (3 × 32,126 rows).

- `SELECT COUNT(*) FROM fills OPTION(skipUpsert=true)` returns **96,378**: every raw row that Pinot stored.
- `SELECT COUNT(*) FROM fills` returns **32,126**: exactly one copy of each trade, because of upsert on `(venue, symbol, trade_id)`.

## What is not measured, and why

**Exchange → recv latency.** This machine's clock runs about 120 ms behind Binance's, so for 98% of Binance spot rows
`recv_ts < venue_ts`. Without NTP- or PTP-disciplined clocks on both sides, this number is not meaningful, so it is
reported as skewed rather than quoted.

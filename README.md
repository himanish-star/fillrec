# fillrec

A multi-venue trade recorder in Rust. It listens to live public trade feeds from **Binance spot**, **Binance USD-M
perpetuals**, **Coinbase** and **Polymarket** and normalizes every fill into one ledger schema. Rows go out through an
idempotent Kafka producer into an **Apache Pinot** REALTIME upsert table, where they are queryable with SQL within
seconds.

```
venue websockets ─► per-venue tokio task ─► bounded channel ─► batching writer ─► Kafka "fills" ─► Pinot upsert table
                    connect / keepalive        (backpressure)    500 rows | 20 ms    key = venue|symbol|trade_id
                    stale watchdog                               retry forever       idempotent, acks=all
                    parse (zero-copy)
                    dedupe + gap check
```

Measured results are in [BENCHMARKS.md](BENCHMARKS.md).

## One schema, three kinds of truth

| Feed | Trade id | What "side" means | Timestamp | `finality` |
|---|---|---|---|---|
| Binance spot | venue id, consecutive | `m` = buyer is maker | ms | `final` |
| Binance USD-M | aggregate id, consecutive | `m` = buyer is maker | ms | `final` |
| Coinbase | venue id, consecutive | the **maker's** side | RFC 3339 µs | `final` |
| Polymarket | none: stable FNV-1a hash, `synthetic_id = true` | taker side | ms (string) | `matched_pending_settlement` |

Every row stores the **taker** side. Prices and sizes are exact integers scaled by 10^8 (`price_e8`, `qty_e8`). Input
with more precision than that is rejected, never rounded. Pinot derives `price`, `qty` and `notional` as doubles for
convenience.

## Correctness

- **Duplicates.** There are three layers:
  - a per-symbol high-water mark in the recorder (it catches Coinbase's `last_match` replays after a reconnect);
  - Kafka's idempotent producer;
  - Pinot upsert on `(venue, symbol, trade_id)`.

  Replaying one capture three times leaves exactly one row per trade (see BENCHMARKS.md §4).
- **Gaps.** Venue ids are consecutive per symbol, so any jump is counted as missed trades and logged. Polymarket has no
  venue id, so gaps there cannot be detected; this is a known limitation.
- **No silent loss.** A full channel makes readers wait instead of dropping rows, and failed writes retry with capped
  backoff.
- **No silent sockets.** If a connection sends nothing for 30 s (120 s for Polymarket), the recorder counts it and
  reconnects.

## Run it

```bash
./scripts/setup.sh                       # Kafka (KRaft) + Apache Pinot 1.3.0, topic + table
cargo run --release -- run               # all venues -> Kafka -> Pinot; Ctrl-C to stop
open http://localhost:9000               # Pinot query console
```

```sql
SELECT symbol, SUM(notional) / SUM(qty) AS vwap, COUNT(*) AS fills
FROM fills WHERE venue_ts_ms > ago('PT5M')
GROUP BY symbol ORDER BY fills DESC
```

Other modes:

```bash
fillrec run --sink ndjson --out fills.ndjson --capture raw.tsv --duration 60   # no infra; record raw frames
fillrec replay --file raw.tsv --sink null --repeat 5                           # parser throughput
fillrec replay --file raw.tsv --sink kafka --batch 10000                       # sink throughput
cargo bench --bench normalize                                                  # Criterion, vs naive baseline
cargo test
```

## Things the live feeds taught me

- **A socket that connects but never speaks.** On Binance's legacy USD-M path the WebSocket handshake completes, but no
  data ever arrives. Market streams now live under `/market`. Hence the stale watchdog.
- **A p99 that was not the pipeline.** Coinbase's recv→commit p99 was 1.9 s because slow Polymarket discovery ran
  before the writer had started. After reordering startup, p99 is about 21 ms.
- **An id that overflowed the ledger.** 64-bit hash ids landed in Pinot's signed `LONG` as negative numbers. They are
  now masked to 63 bits, and a test covers it.
- **TLS 1.3 only.** Polymarket's endpoints refuse macOS's native TLS stack, so the recorder uses rustls throughout.
- **Clocks.** This machine's clock runs about 120 ms behind Binance's, so exchange→recv latency is reported as skewed,
  not quoted.

## Layout

| Path | What it holds |
|---|---|
| `src/venues/` | one parser per venue, unit-tested on real frame shapes |
| `src/model.rs` | the `Fill` row, enums, symbol interner, stable hash |
| `src/fixed.rs` | exact decimal and RFC 3339 parsing |
| `src/seq.rs` | dedupe and gap detection |
| `src/pipeline.rs` | connection loop, watchdog, batching writer |
| `src/sink.rs` | Kafka / NDJSON / null sinks |
| `src/naive.rs` | baseline parser, used only by the benchmarks |
| `pinot/` | schema and REALTIME upsert table config |

## Next

- Keep several Kafka batches in flight while committing them in order. At batch 500 the writer is round-trip bound (38k fills/s).
- Backfill detected gaps from the venues' REST trade endpoints.
- Track Polymarket on-chain settlement and upsert rows from `matched_pending_settlement` to `final`.
- Compare simdjson (C++, over FFI) with serde for the parse stage.

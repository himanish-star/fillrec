//! Parse+normalize cost per frame, on real frames captured from the live feeds
//! (`benches/fixtures/sample.tsv`, written by `fillrec run --capture`).
//! Compares the production parsers against the naive baseline in `src/naive.rs`.

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use fillrec::model::{Fill, Venue};
use fillrec::naive;
use fillrec::sink::encode_json;
use fillrec::venues::{binance::Binance, coinbase::Coinbase, polymarket::Polymarket, Normalizer};

fn load(v: Venue) -> Vec<String> {
    let s = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/benches/fixtures/sample.tsv"))
        .expect("run `fillrec run --capture benches/fixtures/sample.tsv` first");
    s.lines()
        .filter_map(|l| {
            let mut it = l.splitn(3, '\t');
            let (vv, _, f) = (it.next()?, it.next()?, it.next()?);
            (Venue::parse(vv)? == v).then(|| f.to_string())
        })
        .collect()
}

fn bench_parser(c: &mut Criterion, name: &str, frames: &[String], mut n: Box<dyn Normalizer>) {
    if frames.is_empty() {
        return;
    }
    let mut g = c.benchmark_group(name);
    g.throughput(Throughput::Elements(frames.len() as u64));
    let mut out: Vec<Fill> = Vec::with_capacity(64);
    g.bench_function("typed_borrowed_fixedpoint", |b| {
        b.iter(|| {
            for f in frames {
                out.clear();
                let _ = n.parse(black_box(f), 0, &mut out);
                black_box(&out);
            }
        })
    });
    g.finish();
}

fn benches(c: &mut Criterion) {
    let spot = load(Venue::BinanceSpot);
    let perp = load(Venue::BinanceUsdm);
    let cb: Vec<String> = load(Venue::Coinbase).into_iter().filter(|f| f.contains("\"match\"")).collect();
    let pm = load(Venue::Polymarket);

    bench_parser(c, "binance_spot", &spot, Box::new(Binance::spot(vec![])));
    bench_parser(c, "binance_usdm", &perp, Box::new(Binance::usdm(vec![])));
    bench_parser(c, "coinbase", &cb, Box::new(Coinbase::new(vec![])));
    bench_parser(c, "polymarket", &pm, Box::new(Polymarket::new(vec![])));

    // Baseline: serde_json::Value + f64 + String per row.
    if !spot.is_empty() {
        let mut g = c.benchmark_group("binance_spot");
        g.throughput(Throughput::Elements(spot.len() as u64));
        g.bench_function("naive_value_f64", |b| {
            b.iter(|| {
                for f in &spot {
                    black_box(naive::binance(black_box(f)).ok());
                }
            })
        });
        g.finish();
    }
    if !cb.is_empty() {
        let mut g = c.benchmark_group("coinbase");
        g.throughput(Throughput::Elements(cb.len() as u64));
        g.bench_function("naive_value_f64", |b| {
            b.iter(|| {
                for f in &cb {
                    black_box(naive::coinbase(black_box(f)).ok());
                }
            })
        });
        g.finish();
    }

    // Encoding one row for the Kafka -> Pinot path.
    let mut n = Binance::spot(vec![]);
    let mut fills = Vec::new();
    for f in &spot {
        let _ = n.parse(f, 0, &mut fills);
    }
    if !fills.is_empty() {
        let mut g = c.benchmark_group("encode");
        g.throughput(Throughput::Elements(fills.len() as u64));
        let mut s = String::with_capacity(512);
        g.bench_function("json_row", |b| {
            b.iter(|| {
                for f in &fills {
                    encode_json(&mut s, black_box(f), 0);
                    black_box(&s);
                }
            })
        });
        g.finish();
    }
}

criterion_group!(normalize, benches);
criterion_main!(normalize);

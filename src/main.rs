use anyhow::{bail, Context};
use clap::{Parser, Subcommand, ValueEnum};
use fillrec::metrics::{rec, Metrics};
use fillrec::model::{now_us, Fill, Venue};
use fillrec::pipeline::{flush, run_venue, run_writer, CaptureTx};
use fillrec::sink::{KafkaSink, Sink};
use fillrec::venues::{binance::Binance, coinbase::Coinbase, polymarket::Polymarket, Normalizer};
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, watch};
use tracing::{info, warn};

#[derive(Parser)]
#[command(about = "Record trades from Binance, Coinbase and Polymarket into one ledger schema")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Clone, Copy, ValueEnum)]
enum SinkKind {
    Kafka,
    Ndjson,
    Null,
}

#[derive(clap::Args)]
struct SinkArgs {
    #[arg(long, value_enum, default_value = "kafka")]
    sink: SinkKind,
    #[arg(long, default_value = "localhost:9092")]
    brokers: String,
    #[arg(long, default_value = "fills")]
    topic: String,
    /// Output file for --sink ndjson
    #[arg(long, default_value = "results/fills.ndjson")]
    out: String,
    /// Max rows per sink write
    #[arg(long, default_value_t = 500)]
    batch: usize,
    /// Flush a partial batch after this many ms
    #[arg(long, default_value_t = 20)]
    flush_ms: u64,
}

#[derive(Subcommand)]
enum Cmd {
    /// Connect to venues and record live fills
    Run {
        #[arg(long, value_delimiter = ',', default_value = "binance-spot,binance-usdm,coinbase,polymarket")]
        venues: Vec<String>,
        #[arg(long, value_delimiter = ',', default_value = "BTCUSDT,ETHUSDT,SOLUSDT,XRPUSDT,DOGEUSDT")]
        binance_symbols: Vec<String>,
        #[arg(long, value_delimiter = ',', default_value = "BTC-USD,ETH-USD,SOL-USD,XRP-USD,DOGE-USD")]
        coinbase_products: Vec<String>,
        /// Number of top Polymarket markets (by 24h volume) to follow
        #[arg(long, default_value_t = 25)]
        polymarket_markets: usize,
        #[command(flatten)]
        sink: SinkArgs,
        /// Stop after N seconds (default: run until Ctrl-C)
        #[arg(long)]
        duration: Option<u64>,
        #[arg(long, default_value_t = 10)]
        report_secs: u64,
        /// Also write every raw frame to this file (for `replay` and benchmarks)
        #[arg(long)]
        capture: Option<String>,
        /// Write a JSON summary of all metrics here on exit
        #[arg(long)]
        summary: Option<String>,
    },
    /// Replay a capture file through the parsers (and optionally a sink) as fast as possible
    Replay {
        #[arg(long)]
        file: String,
        #[arg(long, default_value_t = 1)]
        repeat: usize,
        #[command(flatten)]
        sink: SinkArgs,
        #[arg(long)]
        summary: Option<String>,
    },
}

fn make_sink(a: &SinkArgs) -> anyhow::Result<Sink> {
    Ok(match a.sink {
        SinkKind::Null => Sink::Null,
        SinkKind::Ndjson => {
            if let Some(dir) = std::path::Path::new(&a.out).parent() {
                std::fs::create_dir_all(dir)?;
            }
            Sink::Ndjson(BufWriter::new(File::create(&a.out)?))
        }
        SinkKind::Kafka => Sink::Kafka(KafkaSink::new(&a.brokers, &a.topic)?),
    })
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Several venues only offer TLS 1.3; pin one rustls crypto provider for the whole process.
    let _ = rustls::crypto::ring::default_provider().install_default();
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with_target(false)
        .init();
    match Cli::parse().cmd {
        Cmd::Run { venues, binance_symbols, coinbase_products, polymarket_markets, sink, duration, report_secs, capture, summary } => {
            run(venues, binance_symbols, coinbase_products, polymarket_markets, sink, duration, report_secs, capture, summary).await
        }
        Cmd::Replay { file, repeat, sink, summary } => replay(file, repeat, sink, summary).await,
    }
}

#[allow(clippy::too_many_arguments)]
async fn run(
    venues: Vec<String>,
    binance_symbols: Vec<String>,
    coinbase_products: Vec<String>,
    polymarket_markets: usize,
    sink_args: SinkArgs,
    duration: Option<u64>,
    report_secs: u64,
    capture: Option<String>,
    summary: Option<String>,
) -> anyhow::Result<()> {
    let metrics = Arc::new(Metrics::new());
    let sink = make_sink(&sink_args)?;
    let (tx, rx) = mpsc::channel::<Fill>(65_536);
    let (stop_tx, stop_rx) = watch::channel(false);

    let cap_tx: Option<CaptureTx> = match &capture {
        Some(path) => {
            let (ctx, mut crx) = mpsc::unbounded_channel::<(Venue, i64, String)>();
            let mut w = BufWriter::new(File::create(path).with_context(|| format!("create {path}"))?);
            tokio::task::spawn_blocking(move || {
                while let Some((v, ts, frame)) = crx.blocking_recv() {
                    let frame = frame.replace('\n', " ");
                    let _ = writeln!(w, "{}\t{}\t{}", v.as_str(), ts, frame);
                }
                let _ = w.flush();
            });
            Some(ctx)
        }
        None => None,
    };

    // Start the writer first, and do slow discovery before any feed starts producing,
    // so startup work never shows up as commit latency.
    let writer = tokio::spawn(run_writer(rx, sink, sink_args.batch, Duration::from_millis(sink_args.flush_ms), metrics.clone()));
    let mut tasks = Vec::new();
    let mut normalizers: Vec<Box<dyn Normalizer>> = Vec::new();
    for v in &venues {
        let venue = Venue::parse(v).with_context(|| format!("unknown venue {v}"))?;
        let n: Box<dyn Normalizer> = match venue {
            Venue::BinanceSpot => Box::new(Binance::spot(binance_symbols.clone())),
            Venue::BinanceUsdm => Box::new(Binance::usdm(binance_symbols.clone())),
            Venue::Coinbase => Box::new(Coinbase::new(coinbase_products.clone())),
            Venue::Polymarket => {
                let assets = Polymarket::discover(polymarket_markets).await.context("polymarket discovery")?;
                info!("polymarket: following {} outcome tokens", assets.len());
                Box::new(Polymarket::new(assets))
            }
        };
        normalizers.push(n);
    }
    for n in normalizers {
        tasks.push(tokio::spawn(run_venue(n, tx.clone(), metrics.clone(), cap_tx.clone(), stop_rx.clone())));
    }
    drop(tx);
    drop(cap_tx);

    let m2 = metrics.clone();
    let reporter = tokio::spawn(async move {
        let mut t = tokio::time::interval(Duration::from_secs(report_secs));
        t.tick().await;
        loop {
            t.tick().await;
            println!("\n{}", m2.report_line());
        }
    });

    match duration {
        Some(s) => tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(s)) => {}
            _ = tokio::signal::ctrl_c() => {}
        },
        None => {
            tokio::signal::ctrl_c().await?;
        }
    }
    info!("shutting down: closing venues, draining writer");
    let _ = stop_tx.send(true);
    for t in tasks {
        let _ = t.await;
    }
    writer.await?; // all senders dropped -> writer flushes the tail and exits
    reporter.abort();
    println!("\n=== final ===\n{}", metrics.report_line());
    if let Some(p) = summary {
        std::fs::write(&p, serde_json::to_string_pretty(&metrics.summary_json())?)?;
        info!("summary written to {p}");
    }
    Ok(())
}

async fn replay(file: String, repeat: usize, sink_args: SinkArgs, summary: Option<String>) -> anyhow::Result<()> {
    // Load everything first so disk I/O is not in the measurement.
    let mut frames: Vec<(Venue, String)> = Vec::new();
    for line in BufReader::new(File::open(&file)?).lines() {
        let line = line?;
        let mut it = line.splitn(3, '\t');
        let (Some(v), Some(_ts), Some(frame)) = (it.next(), it.next(), it.next()) else { continue };
        let Some(v) = Venue::parse(v) else { continue };
        frames.push((v, frame.to_string()));
    }
    if frames.is_empty() {
        bail!("no frames in {file}");
    }
    info!("loaded {} frames from {file}", frames.len());

    let mut ns: [Box<dyn Normalizer>; 4] = [
        Box::new(Binance::spot(vec![])),
        Box::new(Binance::usdm(vec![])),
        Box::new(Coinbase::new(vec![])),
        Box::new(Polymarket::new(vec![])),
    ];
    let metrics = Metrics::new();
    let mut sink = make_sink(&sink_args)?;
    let mut batch: Vec<Fill> = Vec::with_capacity(sink_args.batch);
    let mut out: Vec<Fill> = Vec::with_capacity(64);
    let (mut n_frames, mut n_fills, mut errors) = (0u64, 0u64, 0u64);
    let mut parse_total = Duration::ZERO;

    let t_all = Instant::now();
    for _ in 0..repeat {
        for (v, frame) in &frames {
            let recv = now_us();
            let t0 = Instant::now();
            out.clear();
            if ns[v.idx()].parse(frame, recv, &mut out).is_err() {
                errors += 1;
            }
            let el = t0.elapsed();
            parse_total += el;
            n_frames += 1;
            {
                let mut h = metrics.venue(*v).h.lock().unwrap();
                rec(&mut h.parse_ns, el.as_nanos() as i64);
            }
            metrics.venue(*v).c.frames.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            metrics.venue(*v).c.fills.fetch_add(out.len() as u64, std::sync::atomic::Ordering::Relaxed);
            n_fills += out.len() as u64;
            if !matches!(sink_args.sink, SinkKind::Null) {
                batch.extend(out.drain(..));
                if batch.len() >= sink_args.batch {
                    flush(&mut sink, &mut batch, &metrics).await;
                }
            }
        }
    }
    if !batch.is_empty() {
        flush(&mut sink, &mut batch, &metrics).await;
    }
    let wall = t_all.elapsed();
    if errors > 0 {
        warn!("{errors} frames failed to parse");
    }
    println!(
        "replayed {n_frames} frames -> {n_fills} fills in {:.3}s wall\n  parse only: {:.0} ns/frame mean, {:.2} M frames/s\n  end to end (incl. sink): {:.0} fills/s",
        wall.as_secs_f64(),
        parse_total.as_nanos() as f64 / n_frames as f64,
        n_frames as f64 / parse_total.as_secs_f64() / 1e6,
        n_fills as f64 / wall.as_secs_f64(),
    );
    println!("{}", metrics.report_line());
    if let Some(p) = summary {
        let mut j = metrics.summary_json();
        j["replay"] = serde_json::json!({
            "frames": n_frames, "fills": n_fills, "errors": errors,
            "wall_s": wall.as_secs_f64(),
            "parse_ns_per_frame_mean": parse_total.as_nanos() as f64 / n_frames as f64,
            "fills_per_s_end_to_end": n_fills as f64 / wall.as_secs_f64(),
        });
        std::fs::write(&p, serde_json::to_string_pretty(&j)?)?;
    }
    Ok(())
}

//! Shared Java-exported input, actual Rust parse/commit/wait/encode implementation.
use kline_core::{Bar, Interval, Market, Source, Update};
use kline_service::{BulkQuery, Catalog, Clock, Engine, Instrument, Settings};
use parking_lot::RwLock;
use serde::Deserialize;
use std::{
    num::NonZeroUsize,
    sync::Arc,
    time::{Duration, Instant},
};
const H: i64 = 3_600_000;
#[derive(Deserialize)]
struct Fixture {
    base: i64,
    future_symbols: usize,
    spot_symbols: usize,
    history: usize,
    updates: u32,
    flood: bool,
    waiters: usize,
    frames: Vec<Template>,
}
#[derive(Deserialize)]
struct Template {
    market: String,
    template: String,
    open_offset: i64,
    close_offset: i64,
    event_offset: i64,
    at_ms: f64,
}
struct Input {
    market: Market,
    raw: Vec<u8>,
    at_ms: f64,
}
struct ReplayClock(RwLock<(i64, Instant)>);
impl Clock for ReplayClock {
    fn now_ms(&self) -> i64 {
        let (base, start) = *self.0.read();
        base + start.elapsed().as_millis() as i64
    }
}
fn bar(open: i64, interval: i64, trades: u32, close: f64) -> Bar {
    Bar {
        open_time: open,
        close_time: open + interval - 1,
        trades,
        values: [100., 110., 90., close, 1000., 100000., 500., 50000.],

        ..Bar::default()
    }
}
fn percentile(values: &[f64], q: f64) -> f64 {
    if values.is_empty() {
        return -1.;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    sorted[((q * sorted.len() as f64) as usize).min(sorted.len() - 1)]
}
fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.
}
fn process_cpu_ms() -> f64 {
    let t = rustix::time::clock_gettime(rustix::time::ClockId::ProcessCPUTime);
    t.tv_sec as f64 * 1000. + t.tv_nsec as f64 / 1e6
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() != 5 {
        return Err("usage: replay fixture.json output.json warmups runs".into());
    }
    let fixture: Fixture = serde_json::from_slice(&std::fs::read(&args[1])?)?;
    let warmups: usize = args[3].parse()?;
    let runs: usize = args[4].parse()?;
    let mut instruments = Vec::new();
    for (market, count, prefix) in [
        (Market::Future, fixture.future_symbols, "F"),
        (Market::Spot, fixture.spot_symbols, "S"),
    ] {
        for i in 0..count {
            for interval in ["1h", "1d"] {
                let symbol = format!("{prefix}{i:04}USDT");
                instruments.push(Instrument {
                    market,
                    symbol: symbol.clone(),
                    interval: Interval::parse(interval).unwrap(),
                    trading: true,
                    continuous: if market == Market::Future {
                        Some((
                            symbol,
                            if i < 528 {
                                "PERPETUAL"
                            } else {
                                "TRADIFI_PERPETUAL"
                            }
                            .into(),
                        ))
                    } else {
                        None
                    },
                    capacity: NonZeroUsize::new(fixture.history).unwrap(),
                });
            }
        }
    }
    let clock = Arc::new(ReplayClock(RwLock::new((
        fixture.base - 20,
        Instant::now(),
    ))));
    let engine = Engine::new(
        Catalog::new(instruments)?,
        clock.clone(),
        Settings::default(),
    );
    for (id, slot) in engine.catalog.slots().iter().enumerate() {
        let duration = slot.interval.millis();
        let latest = if slot.interval.code() == "1h" {
            fixture.base - H
        } else {
            fixture.base / duration * duration
        };
        for j in (0..fixture.history).rev() {
            engine.commit(
                id,
                Update {
                    bar: bar(latest - j as i64 * duration, duration, 1, 100.),
                    closed: j != 0,
                    source: Source::Rest,
                    event_time: None,
                    sequence: 0,
                },
            )?;
        }
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(4)
        .enable_all()
        .build()?;
    let mut results = Vec::new();
    for round in 0..warmups + runs {
        let boundary = fixture.base + round as i64 * H;
        let inputs: Vec<_> = fixture
            .frames
            .iter()
            .map(|t| Input {
                market: if t.market == "future" {
                    Market::Future
                } else {
                    Market::Spot
                },
                raw: t
                    .template
                    .replace("@OPEN@", &(boundary + t.open_offset).to_string())
                    .replace("@CLOSE@", &(boundary + t.close_offset).to_string())
                    .replace("@EVENT@", &(boundary + t.event_offset).to_string())
                    .into_bytes(),
                at_ms: t.at_ms,
            })
            .collect();
        let start = Instant::now();
        *clock.0.write() = (boundary - 20, start);
        let cpu = process_cpu_ms();
        let mut requests = Vec::new();
        for index in 0..fixture.waiters {
            let e = engine.clone();
            let count = fixture.future_symbols;
            requests.push(runtime.spawn(async move {
                tokio::time::sleep_until(tokio::time::Instant::from_std(
                    start + Duration::from_millis(25),
                ))
                .await;
                let symbols = (0..6)
                    .map(|j| format!("F{:04}USDT", (index * 7 + j) % count))
                    .collect();
                let request_start = Instant::now();
                let reply = e
                    .bulk(BulkQuery {
                        market: Market::Future,
                        interval: "1h".into(),
                        limit: Some(1),
                        closed_only: true,
                        symbols,
                    })
                    .await
                    .unwrap();
                (
                    ms(request_start.elapsed()),
                    ms(start.elapsed()) - 20.,
                    reply,
                )
            }));
        }
        let mut close_offsets = Vec::new();
        let mut close_latencies = Vec::new();
        for input in &inputs {
            if !fixture.flood {
                let deadline = start + Duration::from_secs_f64((input.at_ms + 20.) / 1000.);
                while let Some(remaining) = deadline.checked_duration_since(Instant::now()) {
                    std::thread::sleep(remaining.min(Duration::from_micros(200)));
                }
            }
            let receive = Instant::now();
            let ingested = engine
                .ingest(input.market, &input.raw)?
                .expect("every fixture frame must map");
            if ingested.closed {
                close_latencies.push(ms(receive.elapsed()));
                close_offsets.push(ms(start.elapsed()) - 20.);
            }
        }
        let enqueue_end = ms(start.elapsed()) - 20.;
        // Same 10ms stable-idle interval as the Java harness; Rust has no ingress queue.
        std::thread::sleep(Duration::from_millis(10));
        let mut bulk_samples = Vec::new();
        let mut bulk_offsets = Vec::new();
        let mut replies = Vec::new();
        for request in requests {
            let (latency, offset, reply) = runtime.block_on(request)?;
            bulk_samples.push(latency);
            bulk_offsets.push(offset);
            replies.push(reply);
        }
        let cpu_ms = process_cpu_ms() - cpu;
        let expected = fixture.future_symbols + fixture.spot_symbols;
        assert_eq!(close_offsets.len(), expected);
        for slot in engine
            .catalog
            .slots()
            .iter()
            .filter(|s| s.interval.code() == "1h")
        {
            let (closed, is_final) = slot.get(boundary - H).unwrap();
            assert!(is_final);
            assert_eq!(closed.trades, 2 + fixture.updates);
            assert_eq!(closed.values[3], 109.);
            let (forming, is_final) = slot.get(boundary).unwrap();
            assert!(!is_final);
            assert_eq!(
                forming.trades,
                if fixture.flood { fixture.updates } else { 1 }
            );
            assert!(slot.len() <= fixture.history);
        }
        for reply in &replies {
            assert!(reply.finalized);
            let body: serde_json::Value = serde_json::from_slice(&reply.body)?;
            let bars = body["klines"].as_object().unwrap();
            assert_eq!(bars.len(), 6);
            for v in bars.values() {
                assert_eq!(v[0][4], "109");
                assert_eq!(v[0][0], boundary - H);
            }
        }
        let result = serde_json::json!({"round":round,"warmup":round<warmups,"cpu_ms":cpu_ms,"input":inputs.len(),
            "close_done_max_ms":percentile(&close_offsets,1.),"receive_to_close_p99_ms":percentile(&close_latencies,0.99),
            "receive_to_close_samples_ms":close_latencies,"bulk_request_samples_ms":bulk_samples,"bulk_done_max_ms":percentile(&bulk_offsets,1.),
            "bulk_response_bytes":replies.iter().map(|r|r.body.len()).sum::<usize>(),"expected_closes":expected,"handled_closes":close_offsets.len(),
            "missing_finals":0,"wrong_final_values":0,"wrong_latest_forming":0,"bulk_unfinalized":0,"enqueue_end_ms":enqueue_end});
        eprintln!(
            "round {round} cpu={cpu_ms:.3}ms ready={:.3}ms",
            percentile(&close_offsets, 1.)
        );
        results.push(result);
    }
    let output = serde_json::json!({"implementation":"rust-core-prototype","history":fixture.history,"retained_bars":engine.catalog.slots().iter().map(|s|s.len()).sum::<usize>(),
        "proc_status":std::fs::read_to_string("/proc/self/status").unwrap_or_default(),"rounds":results});
    std::fs::write(&args[2], serde_json::to_vec(&output)?)?;
    Ok(())
}

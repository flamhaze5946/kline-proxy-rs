use kline_core::{Bar, Interval, Market, Source, Update};
use kline_service::{Catalog, Clock, Engine, Instrument, Settings};
use std::{num::NonZeroUsize, sync::Arc};
const BASE: i64 = 1_789_308_000_000;
struct Fixed;
impl Clock for Fixed {
    fn now_ms(&self) -> i64 {
        BASE + 10000
    }
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let port = std::env::args().nth(1).unwrap_or("0".into());
    let mut instruments = Vec::new();
    for i in 0..718 {
        for interval in ["1h", "1d"] {
            instruments.push(Instrument {
                market: Market::Future,
                symbol: format!("F{i:04}USDT"),
                interval: Interval::parse(interval).unwrap(),
                trading: true,
                continuous: None,
                capacity: NonZeroUsize::new(1000).unwrap(),
            });
        }
    }
    let engine = Engine::new(
        Catalog::new(instruments)?,
        Arc::new(Fixed),
        Settings::default(),
    );
    for (id, slot) in engine.catalog.slots().iter().enumerate() {
        let duration = slot.interval.millis();
        let latest = if slot.interval.code() == "1h" {
            BASE - duration
        } else {
            BASE / duration * duration
        };
        for j in (0..1000).rev() {
            let open = latest - j * duration;
            engine.commit(
                id,
                Update {
                    bar: Bar {
                        open_time: open,
                        close_time: open + duration - 1,
                        trades: 1,
                        values: [100., 110., 90., 100., 1000., 100000., 500., 50000.],

                        ..Bar::default()
                    },
                    closed: true,
                    source: Source::Rest,
                    event_time: None,
                    sequence: 0,
                },
            )?;
        }
    }
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(4)
        .enable_all()
        .build()?
        .block_on(async {
            let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{port}")).await?;
            println!("READY {}", listener.local_addr()?.port());
            axum::serve(
                kline_service::http::low_latency_listener(listener),
                kline_service::http::router(engine),
            )
            .await?;
            Ok::<_, Box<dyn std::error::Error>>(())
        })
}

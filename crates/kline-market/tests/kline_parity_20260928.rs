use kline_core::{Bar, Interval, Market, Source, Update};
use kline_service::{Catalog, Engine, Instrument, Settings, SystemClock};
use std::{num::NonZeroUsize, sync::Arc};

#[test]
fn recent_window_fallback_keeps_actual_weekly_and_monthly_last_open() {
    // 2024-02-05 Monday (weekly) and 2024-02-01 (monthly) are not
    // multiples of seven/thirty days from the Unix epoch.
    for (code, latest) in [("1w", 1_707_091_200_000), ("1M", 1_706_745_600_000)] {
        let interval = Interval::parse(code).unwrap();
        let engine = Engine::new(
            Catalog::new(vec![Instrument {
                market: Market::Future,
                symbol: "BTCUSDT".into(),
                interval,
                trading: true,
                continuous: None,
                capacity: NonZeroUsize::new(4).unwrap(),
            }])
            .unwrap(),
            Arc::new(SystemClock { offset_ms: 0 }),
            Settings::default(),
        );
        for open in [latest - interval.millis(), latest] {
            engine
                .commit(
                    0,
                    Update {
                        bar: Bar {
                            open_time: open,
                            close_time: open + interval.millis() - 1,
                            values: [100.; 8],
                            ..Bar::default()
                        },
                        closed: true,
                        source: Source::Stream,
                        event_time: Some(open),
                        sequence: open as u64,
                    },
                )
                .unwrap();
        }
        let rows = kline_market::klines::cached(
            &engine,
            Market::Future,
            "BTCUSDT",
            interval,
            None,
            None,
            1,
        );
        assert_eq!(
            rows.iter().map(|r| r.open_time).collect::<Vec<_>>(),
            vec![latest],
            "{code}"
        );
        let rows = kline_market::klines::cached(
            &engine,
            Market::Future,
            "BTCUSDT",
            interval,
            None,
            None,
            2,
        );
        assert_eq!(rows.len(), 2, "{code}");
        assert_eq!(rows.last().unwrap().open_time, latest);
    }
}

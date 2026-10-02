use kline_core::{Bar, Interval, Market, Source, Update};
use kline_runtime::{
    config::{Config, guarded},
    storage::Store,
};
use kline_service::{Catalog, Clock, Engine, Instrument, Settings};
use std::{num::NonZeroUsize, sync::Arc};
const H: i64 = 3_600_000;
struct Fixed;
impl Clock for Fixed {
    fn now_ms(&self) -> i64 {
        10 * H
    }
}
fn engine() -> Arc<Engine> {
    engine_mode(kline_core::NumberType::Double)
}
fn engine_mode(mode: kline_core::NumberType) -> Arc<Engine> {
    Engine::new(
        Catalog::new(vec![Instrument {
            market: Market::Future,
            symbol: "BTCUSDT".into(),
            interval: Interval::parse("1h").unwrap(),
            trading: true,
            continuous: None,
            capacity: NonZeroUsize::new(5).unwrap(),
        }])
        .unwrap(),
        Arc::new(Fixed),
        Settings {
            number_type: mode,
            ..Settings::default()
        },
    )
}
fn commit(e: &Engine, open: i64, price: f64, closed: bool, sequence: u64) {
    e.commit(
        0,
        Update {
            bar: Bar {
                open_time: open,
                close_time: open + H - 1,
                trades: sequence as u32,
                values: [price, price, price, price, 1., 2., 3., 4.],

                ..Bar::default()
            },
            closed,
            source: Source::Stream,
            event_time: Some(sequence as i64),
            sequence,
        },
    )
    .unwrap();
}
#[test]
fn snapshots_restore_only_finals_bit_exactly_and_fail_closed_on_corruption() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine();
    let mut store = Store::new(dir.path(), 1).unwrap();
    commit(&e, 0, -0., true, 1);
    commit(&e, H, 1.234567890123, true, 2);
    commit(&e, 2 * H, 999., false, 3);
    assert_eq!(store.dump(&e, || true).written, 1);
    assert_eq!(store.dump(&e, || true).written, 0);
    let restored = engine();
    let mut reader = Store::new(dir.path(), 1).unwrap();
    let report = reader.restore(&restored);
    assert_eq!(report.series, 1);
    assert_eq!(report.bars, 2);
    assert_eq!(
        restored.catalog.slot(0).get(0).unwrap().0.values[0].to_bits(),
        (-0f64).to_bits()
    );
    assert_eq!(restored.catalog.slot(0).get(H), e.catalog.slot(0).get(H));
    assert!(restored.catalog.slot(0).get(2 * H).is_none());
    let path = store.path(&e.catalog.slot(0));
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[40] ^= 1;
    std::fs::write(path, bytes).unwrap();
    let clean = engine();
    let report = reader.restore(&clean);
    assert_eq!(report.corrupt, 1);
    assert!(clean.catalog.slot(0).is_empty());
}
#[test]
fn failed_dump_preserves_last_snapshot_and_dirty_state_until_retry_then_corruption_fails_closed() {
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("data");
    let displaced = parent.path().join("last-good");
    let e = engine();
    let mut store = Store::new(&root, 1).unwrap();
    commit(&e, 0, 1., true, 1);
    assert_eq!(store.dump(&e, || true).written, 1);
    let filename = store
        .path(&e.catalog.slot(0))
        .file_name()
        .unwrap()
        .to_owned();
    let before = std::fs::read(root.join(&filename)).unwrap();
    commit(&e, 0, 2., true, 2);
    // A file occupying the directory path forces a real filesystem error,
    // including when tests run as root (chmod would not reliably do that).
    std::fs::rename(&root, &displaced).unwrap();
    std::fs::write(&root, b"blocked").unwrap();
    let failure = store.dump(&e, || true);
    assert_eq!((failure.written, failure.failures), (0, 1));
    assert!(store.is_dirty(&e));
    assert_eq!(std::fs::read(displaced.join(&filename)).unwrap(), before);
    assert_eq!(e.catalog.slot(0).get(0).unwrap().0.values[3], 2.);
    std::fs::remove_file(&root).unwrap();
    std::fs::rename(&displaced, &root).unwrap();
    assert_eq!(store.dump(&e, || true).written, 1);
    assert!(!store.is_dirty(&e));
    let restored = engine();
    let mut reader = Store::new(&root, 1).unwrap();
    assert_eq!(reader.restore(&restored).series, 1);
    assert_eq!(restored.catalog.slot(0).get(0).unwrap().0.values[3], 2.);
    let mut damaged = std::fs::read(root.join(&filename)).unwrap();
    damaged.truncate(damaged.len() - 1);
    std::fs::write(root.join(&filename), damaged).unwrap();
    let clean = engine();
    assert_eq!(reader.restore(&clean).corrupt, 1);
    assert!(clean.catalog.slot(0).is_empty());
}

#[test]
fn deferred_dumps_remain_dirty_and_a_final_revision_is_written_next_pass() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine();
    let mut store = Store::new(dir.path(), 1).unwrap();
    commit(&e, 0, 1., true, 1);
    assert_eq!(store.dump(&e, || false).written, 0);
    assert!(store.is_dirty(&e));
    assert_eq!(store.dump(&e, || true).written, 1);
    let (old_generation, old_snapshot) = e.catalog.slot(0).durable_snapshot(10 * H);
    commit(&e, 0, 2., true, 2);
    assert!(e.catalog.slot(0).durable_generation() > old_generation);
    assert_eq!(old_snapshot[0].values[3], 1.);
    assert_eq!(store.dump(&e, || true).written, 1);
    let clean = engine();
    let mut reader = Store::new(dir.path(), 1).unwrap();
    reader.restore(&clean);
    assert_eq!(clean.catalog.slot(0).get(0).unwrap().0.values[3], 2.);
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
}

#[test]
fn persistence_selection_applies_to_restore_dump_and_dirty_without_deleting_excluded_files() {
    struct Later;
    impl Clock for Later {
        fn now_ms(&self) -> i64 {
            96 * H
        }
    }
    let definitions: Vec<_> = [
        (Market::Future, "1h"),
        (Market::Future, "1d"),
        (Market::Spot, "1h"),
    ]
    .into_iter()
    .map(|(market, period)| Instrument {
        market,
        symbol: "BTCUSDT".into(),
        interval: Interval::parse(period).unwrap(),
        trading: true,
        continuous: None,
        capacity: NonZeroUsize::new(5).unwrap(),
    })
    .collect();
    let make = || {
        Engine::new(
            Catalog::new(definitions.clone()).unwrap(),
            Arc::new(Later),
            Settings::default(),
        )
    };
    let populate = |engine: &Engine, id: usize, n: u32| {
        let slot = engine.catalog.slot(id);
        engine
            .commit(
                id,
                Update {
                    bar: Bar {
                        open_time: 0,
                        close_time: slot.interval.millis() - 1,
                        trades: n,
                        values: [n as f64; 8],
                        ..Bar::default()
                    },
                    closed: true,
                    source: Source::Stream,
                    event_time: Some(n as i64),
                    sequence: n as u64,
                },
            )
            .unwrap();
    };
    let directory = tempfile::tempdir().unwrap();
    let engine = make();
    for id in 0..3 {
        populate(&engine, id, 1);
    }
    let allowed = engine
        .catalog
        .find(Market::Future, Interval::parse("1h").unwrap(), "BTCUSDT")
        .unwrap();
    let mut store = Store::new(directory.path(), 3).unwrap();
    assert_eq!(store.dump(&engine, || true).written, 3); // Native configs retain the old default.
    let original: Vec<_> = engine
        .catalog
        .slots()
        .iter()
        .map(|s| std::fs::read(store.path(s)).unwrap())
        .collect();
    let config: kline_runtime::config::PersistenceConfig = serde_json::from_value(serde_json::json!({
        "directory": directory.path(), "enabled_intervals": [{"market":"future","interval":"1h"}]
    })).unwrap();
    store.configure(config.clone());
    let restored = make();
    assert_eq!(store.restore(&restored).series, 1);
    for id in 0..3 {
        assert_eq!(!restored.catalog.slot(id).is_empty(), id == allowed);
        if id != allowed {
            populate(&restored, id, 2);
        }
    }
    assert!(!store.is_dirty(&restored));
    assert_eq!(store.dump(&restored, || true).written, 0);
    populate(&restored, allowed, 3);
    assert!(store.is_dirty(&restored));
    assert_eq!(store.dump(&restored, || true).written, 1);
    for (id, bytes) in original.iter().enumerate() {
        if id != allowed {
            assert_eq!(
                &std::fs::read(store.path(&engine.catalog.slot(id))).unwrap(),
                bytes
            );
        }
    }
    let fresh_dir = tempfile::tempdir().unwrap();
    let mut fresh = Store::new(fresh_dir.path(), 3).unwrap();
    fresh.configure(config.clone());
    assert_eq!(fresh.dump(&engine, || true).written, 1);
    assert_eq!(std::fs::read_dir(fresh_dir.path()).unwrap().count(), 1);

    // Explicit empty selection must not try existing corrupt files or the legacy importer.
    let mut none = config;
    none.enabled_intervals = Some(vec![]);
    none.legacy_directory = Some(directory.path().join("missing-java-cache"));
    store.configure(none);
    let path = store.path(&engine.catalog.slot(allowed));
    std::fs::write(&path, b"deliberately corrupt").unwrap();
    let empty = make();
    let report = store.restore(&empty);
    assert_eq!((report.series, report.corrupt), (0, 0));
    assert!(!store.is_dirty(&engine));
    assert_eq!(store.dump(&engine, || true).written, 0);
    assert_eq!(std::fs::read(&path).unwrap(), b"deliberately corrupt");
    engine.refresh_catalog(vec![]).unwrap();
    store.dump(&engine, || true);
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 3);
}

#[test]
fn persistence_selection_validates_market_period_and_duplicates() {
    let valid = serde_json::json!({
        "listen":"127.0.0.1:0",
        "instruments":[{"market":"future","symbol":"BTCUSDT","intervals":["1h"]}],
        "persistence":{"directory":"unused","enabled_intervals":[{"market":"future","interval":"1h"}]}
    });
    assert!(
        serde_json::from_value::<Config>(valid.clone())
            .unwrap()
            .catalog()
            .is_ok()
    );
    for rules in [
        serde_json::json!([{"market":"bad","interval":"1h"}]),
        serde_json::json!([{"market":"future","interval":"bad"}]),
        serde_json::json!([{"market":"future","interval":"1h"},{"market":"future","interval":"1h"}]),
    ] {
        let mut value = valid.clone();
        value["persistence"]["enabled_intervals"] = rules;
        let config: Config = serde_json::from_value(value).unwrap();
        assert!(config.catalog().is_err());
    }
}
#[test]
fn interval_guard_checks_every_enabled_interval_and_rejects_impossible_windows() {
    let intervals = [
        Interval::parse("1h").unwrap(),
        Interval::parse("1d").unwrap(),
    ];
    assert!(guarded(H - 500, intervals.into_iter(), 1000, 2000));
    assert!(guarded(H + 1000, intervals.into_iter(), 1000, 2000));
    assert!(!guarded(H + 2000, intervals.into_iter(), 1000, 2000));
    let config:Config=serde_json::from_value(serde_json::json!({"listen":"127.0.0.1:0","instruments":[{"market":"spot","symbol":"BTCUSDT","intervals":["1s"]}],"rest":{}})).unwrap();
    assert!(config.catalog().is_err());
}

#[test]
fn disk_retention_survives_memory_eviction_and_java_shards_import_without_mutation() {
    let dir = tempfile::tempdir().unwrap();
    let legacy = tempfile::tempdir().unwrap();
    let e = engine();
    let mut definitions = e.catalog.instruments();
    definitions[0].capacity = NonZeroUsize::new(2).unwrap();
    e.refresh_catalog(definitions).unwrap();
    let config:kline_runtime::config::PersistenceConfig=serde_json::from_value(serde_json::json!({"directory":dir.path(),"legacy_directory":legacy.path(),"max_store_count":6})).unwrap();
    let shard = legacy.path().join("future/1h/BTCUSDT");
    std::fs::create_dir_all(&shard).unwrap();
    let row = |n: i64| serde_json::json!({"openTime":n*H,"closeTime":(n+1)*H-1,"tradeNum":1,"openPrice":"1.2300","highPrice":"2","lowPrice":"1","closePrice":"1.5","volume":"3","quoteVolume":"4","activeBuyVolume":"1","activeBuyQuoteVolume":"2"});
    let bytes=serde_json::to_vec(&serde_json::json!({"version":1,"service":"future","interval":"1h","symbol":"BTCUSDT","rows":[row(0),row(1),row(2),row(3)]})).unwrap();
    let original = shard.join("1970-01-01.json");
    std::fs::write(&original, &bytes).unwrap();
    let mut store = Store::new(dir.path(), 1).unwrap();
    store.configure(config.clone());
    let report = store.restore(&e);
    assert_eq!(report.bars, 2);
    assert_eq!(e.catalog.slot(0).get(3 * H).unwrap().0.values[0], 1.23);
    assert_eq!(std::fs::read(original).unwrap(), bytes);
    commit(&e, 4 * H, 4., true, 5);
    assert_eq!(store.dump(&e, || true).written, 1);
    let restored = engine();
    let mut definitions = restored.catalog.instruments();
    definitions[0].capacity = NonZeroUsize::new(6).unwrap();
    restored.refresh_catalog(definitions).unwrap();
    let mut reader = Store::new(dir.path(), 1).unwrap();
    reader.configure(config);
    assert_eq!(reader.restore(&restored).bars, 5);
    assert!(restored.catalog.slot(0).get(0).is_some());
    // A future-dated final stays dirty until its close time; it cannot disappear on shutdown.
    commit(&e, 11 * H, 5., true, 6);
    store.dump(&e, || true);
    assert!(store.is_dirty(&e));
}

#[test]
fn absent_legacy_data_does_not_hide_a_corrupt_rust_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let legacy = tempfile::tempdir().unwrap();
    let e = engine();
    let mut store = Store::new(dir.path(), 1).unwrap();
    store.configure(
        serde_json::from_value(
            serde_json::json!({"directory":dir.path(),"legacy_directory":legacy.path()}),
        )
        .unwrap(),
    );
    assert_eq!(store.restore(&e).corrupt, 0);
    assert_eq!(store.restore(&e).series, 0);
    std::fs::write(store.path(&e.catalog.slot(0)), b"corrupt").unwrap();
    assert_eq!(store.restore(&e).corrupt, 1);
    assert!(e.catalog.slot(0).is_empty());
}

#[test]
fn all_numeric_modes_survive_restart_and_exact_scale_is_preserved() {
    use kline_core::NumberType;
    for mode in [
        NumberType::Double,
        NumberType::Float,
        NumberType::String,
        NumberType::BigDecimal,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let e = engine_mode(mode);
        let mut bar = Bar {
            close_time: H - 1,
            trades: 1,
            ..Bar::default()
        };
        bar.set_numbers(
            mode,
            [
                "-0.0000",
                "1.00E2",
                "0E+5",
                "0.000000001",
                "100.50000000",
                "123456789.123456789",
                "001.2300",
                "2E-20",
            ],
        )
        .unwrap();
        let expected = bar.clone();
        e.commit(
            0,
            Update {
                bar,
                closed: true,
                source: Source::Rest,
                event_time: None,
                sequence: 0,
            },
        )
        .unwrap();
        let mut store = Store::new(dir.path(), 1).unwrap();
        assert_eq!(store.dump(&e, || true).written, 1);
        let restored = engine_mode(mode);
        assert_eq!(store.restore(&restored).bars, 1);
        assert_eq!(restored.catalog.slot(0).get(0).unwrap().0, expected);
        assert_eq!(
            serde_json::to_value(binance_wire::DisplayBar(
                restored.catalog.slot(0).get(0).unwrap().0
            ))
            .unwrap(),
            serde_json::to_value(binance_wire::DisplayBar(expected)).unwrap()
        );
    }
}

#[test]
fn incompatible_numeric_restore_rejects_the_whole_file_without_partial_publication() {
    use kline_core::NumberType;
    let dir = tempfile::tempdir().unwrap();
    let e = engine_mode(NumberType::BigDecimal);
    for (i, price) in ["1.0", "1e1000"].into_iter().enumerate() {
        let mut bar = Bar {
            open_time: i as i64 * H,
            close_time: (i as i64 + 1) * H - 1,
            trades: 1,
            ..Bar::default()
        };
        bar.set_numbers(NumberType::BigDecimal, [price; 8]).unwrap();
        e.commit(
            0,
            Update {
                bar,
                closed: true,
                source: Source::Rest,
                event_time: None,
                sequence: 0,
            },
        )
        .unwrap();
    }
    let mut store = Store::new(dir.path(), 1).unwrap();
    assert_eq!(store.dump(&e, || true).written, 1);
    let restored = engine_mode(NumberType::Double);
    assert_eq!(store.restore(&restored).corrupt, 1);
    assert!(restored.catalog.slot(0).is_empty());
}

#[test]
fn java_snapshot_import_uses_the_selected_numeric_policy() {
    use kline_core::NumberType;
    let legacy = tempfile::tempdir().unwrap();
    let shard = legacy.path().join("future/1h/BTCUSDT");
    std::fs::create_dir_all(&shard).unwrap();
    let original=serde_json::to_vec(&serde_json::json!({"version":1,"service":"future","interval":"1h","symbol":"BTCUSDT","rows":[{
        "openTime":0,"closeTime":H-1,"tradeNum":1,"openPrice":"1.00E2","highPrice":"100.50000000","lowPrice":"0.000000001","closePrice":"0.000000001","volume":"100.50000000","quoteVolume":"100.50000000","activeBuyVolume":"0E+5","activeBuyQuoteVolume":"-0.0000"
    }]})).unwrap();
    std::fs::write(shard.join("1970-01-01.json"), &original).unwrap();
    for mode in [
        NumberType::Double,
        NumberType::Float,
        NumberType::String,
        NumberType::BigDecimal,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let e = engine_mode(mode);
        let mut store = Store::new(dir.path(), 1).unwrap();
        store.configure(
            serde_json::from_value(
                serde_json::json!({"directory":dir.path(),"legacy_directory":legacy.path()}),
            )
            .unwrap(),
        );
        assert_eq!(store.restore(&e).bars, 1);
        let loaded = e.catalog.slot(0).get(0).unwrap().0;
        let expected = match mode {
            NumberType::Double | NumberType::Float => "0",
            _ => "0.000000001",
        };
        assert_eq!(
            serde_json::to_value(binance_wire::DisplayBar(loaded)).unwrap()[4],
            expected
        );
        assert_eq!(
            std::fs::read(shard.join("1970-01-01.json")).unwrap(),
            original
        );
    }
}

#[test]
fn a_bad_java_numeric_row_does_not_discard_other_rows_in_the_day() {
    let legacy = tempfile::tempdir().unwrap();
    let directory = legacy.path().join("future/1h/BTCUSDT");
    std::fs::create_dir_all(&directory).unwrap();
    let row = |n: i64, price: &str| serde_json::json!({"openTime":n*H,"closeTime":(n+1)*H-1,"tradeNum":1,"openPrice":price,"highPrice":"2","lowPrice":"1","closePrice":"1.5","volume":"3","quoteVolume":"4","activeBuyVolume":"1","activeBuyQuoteVolume":"2"});
    let bytes = serde_json::to_vec(&serde_json::json!({"version":1,"service":"future","interval":"1h","symbol":"BTCUSDT","rows":[row(0,"1.0"),row(1,"bad"),row(2,"1.2")]})).unwrap();
    let path = directory.join("1970-01-01.json");
    std::fs::write(&path, &bytes).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let e = engine();
    let mut store = Store::new(dir.path(), 1).unwrap();
    store.configure(
        serde_json::from_value(
            serde_json::json!({"directory":dir.path(),"legacy_directory":legacy.path()}),
        )
        .unwrap(),
    );
    assert_eq!(store.restore(&e).bars, 2);
    assert!(e.catalog.slot(0).get(0).is_some());
    assert!(e.catalog.slot(0).get(H).is_none());
    assert!(e.catalog.slot(0).get(2 * H).is_some());
    assert_eq!(std::fs::read(path).unwrap(), bytes);
}

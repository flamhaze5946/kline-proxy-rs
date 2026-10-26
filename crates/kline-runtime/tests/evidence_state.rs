use kline_core::{
    Bar, Interval, Market, NumberType,
    evidence::{EvidenceState, EvidenceUpdate},
};
use kline_runtime::{
    config::Config,
    evidence_directory,
    evidence_state::{self, StateDirectory},
    storage::Store,
};
use kline_service::{Clock, Engine, evidence::BreakerState};
use serde_json::{Value, json};
use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
};
const NOW: i64 = 6_000_000_500;
struct TestClock(AtomicI64);
impl Clock for TestClock {
    fn now_ms(&self) -> i64 {
        self.0.load(Ordering::Relaxed)
    }
}
fn config(root: &Path) -> Config {
    serde_json::from_value(json!({"listen":"127.0.0.1:0","number_type":"string","history_capacity":241,"evidence":{"ws_trust_window_ms":60000,"not_trading_retention_min":1},"bulk_admission_wait_ms":100,"http_admission_wait_ms":100,"bulk_pre_boundary_wait_ms":0,"subscriptions":[{"market":"future","interval":"1m","continuous":true,"history_capacity":241}],"rest":{"latest_repair_workers":2},"market_api":{"enabled":false},"persistence":{"directory":root,"boundary_guard_before_ms":2000,"boundary_guard_after_ms":15000}})).unwrap()
}
fn engine(c: &Config) -> Arc<Engine> {
    Engine::new(
        c.catalog().unwrap(),
        Arc::new(TestClock(AtomicI64::new(NOW))),
        c.engine_settings().unwrap(),
    )
}
fn exchange(status: &str, pair: &str, contract: &str) -> Value {
    json!({"symbols":[{"symbol":"BTCUSDT","status":status,"pair":pair,"contractType":contract,"deliveryDate":5_999_000_000_i64}]})
}
fn merge(c: &Config, e: &Engine, v: &Value, now: i64) {
    let definitions = evidence_directory::merge(c, v, now).unwrap();
    e.refresh_catalog_with_retained(definitions, evidence_state::retained_records(c))
        .unwrap();
}
fn write_breaker(root: &Path, last: i64, total: u64) {
    std::fs::write(
        root.join("breaker.json"),
        json!({"last_conflict_ms":last,"revised_total":total}).to_string(),
    )
    .unwrap();
}
#[test]
fn missing_breaker_fails_closed_and_missing_retained_is_empty() {
    let t = tempfile::tempdir().unwrap();
    let c = config(t.path());
    let e = engine(&c);
    let state = StateDirectory::load(&c, &e).unwrap();
    assert!(!e.ws_final_trusted());
    assert_eq!(
        e.breaker_state(),
        BreakerState {
            last_conflict_ms: NOW,
            revised_total: 0
        }
    );
    assert!(evidence_state::retained_records(&c).is_empty());
    state.flush(&c, &e).unwrap();
    assert!(t.path().join("retained.json").is_file());
}
#[test]
fn unreadable_breaker_fails_closed_but_unreadable_retained_refuses_startup() {
    let t = tempfile::tempdir().unwrap();
    let c = config(t.path());
    write_breaker(t.path(), 0, 8);
    std::fs::write(t.path().join("breaker.json"), b"invalid").unwrap();
    let e = engine(&c);
    let state = StateDirectory::load(&c, &e).unwrap();
    assert!(!e.ws_final_trusted());
    assert_eq!(e.breaker_state().revised_total, 0);
    drop(state);
    std::fs::write(t.path().join("retained.json"), b"invalid").unwrap();
    assert!(StateDirectory::load(&c, &engine(&c)).is_err());
    std::fs::remove_file(t.path().join("retained.json")).unwrap();
    std::fs::create_dir(t.path().join("retained.json")).unwrap();
    assert!(StateDirectory::load(&c, &engine(&c)).is_err());
}
#[test]
fn advisory_lock_blocks_service_and_reset_then_releases_without_removing_lock_file() {
    let t = tempfile::tempdir().unwrap();
    let c = config(t.path());
    write_breaker(t.path(), NOW, 7);
    let e = engine(&c);
    let state = StateDirectory::load(&c, &e).unwrap();
    assert!(StateDirectory::load(&c, &engine(&c)).is_err());
    assert!(evidence_state::reset_breaker(&c).is_err());
    drop(state);
    assert!(t.path().join("evidence.lock").exists());
    let reset = evidence_state::reset_breaker(&c).unwrap();
    assert_eq!(
        reset,
        BreakerState {
            last_conflict_ms: 0,
            revised_total: 7
        }
    );
    let _lock = StateDirectory::lock(t.path()).unwrap();
}
#[test]
fn reset_unreadable_count_is_explicit_and_deleting_breaker_cannot_clear_trip() {
    let t = tempfile::tempdir().unwrap();
    let c = config(t.path());
    std::fs::write(t.path().join("breaker.json"), b"bad").unwrap();
    assert_eq!(
        evidence_state::reset_breaker(&c).unwrap(),
        BreakerState::default()
    );
    std::fs::remove_file(t.path().join("breaker.json")).unwrap();
    let e = engine(&c);
    let _state = StateDirectory::load(&c, &e).unwrap();
    assert!(!e.ws_final_trusted());
}
#[test]
fn failed_atomic_writes_remain_dirty_and_retry_both_files() {
    let t = tempfile::tempdir().unwrap();
    let c = config(t.path());
    let e = engine(&c);
    let state = StateDirectory::load(&c, &e).unwrap();
    std::fs::create_dir(t.path().join("breaker.json")).unwrap();
    std::fs::create_dir(t.path().join("retained.json")).unwrap();
    assert!(state.flush(&c, &e).is_err());
    let runtime = c.evidence.as_ref().unwrap().runtime.lock();
    assert_ne!(runtime.acknowledged, Some(runtime.generation));
    drop(runtime);
    std::fs::remove_dir(t.path().join("breaker.json")).unwrap();
    std::fs::remove_dir(t.path().join("retained.json")).unwrap();
    state.flush(&c, &e).unwrap();
    let value: Value =
        serde_json::from_slice(&std::fs::read(t.path().join("breaker.json")).unwrap()).unwrap();
    assert_eq!(value["last_conflict_ms"], NOW);
    let runtime = c.evidence.as_ref().unwrap().runtime.lock();
    assert_eq!(runtime.acknowledged, Some(runtime.generation));
    assert!(
        t.path().read_dir().unwrap().all(|p| !p
            .unwrap()
            .file_name()
            .to_string_lossy()
            .contains("tmp-"))
    );
}
#[test]
fn retained_creation_merge_preserves_identity_since_deadline_capacity_and_delivery() {
    let t = tempfile::tempdir().unwrap();
    let c = config(t.path());
    let e = engine(&c);
    merge(&c, &e, &exchange("TRADING", "BTCUSDT", "PERPETUAL"), NOW);
    let slot = e.catalog.slot(0);
    merge(
        &c,
        &e,
        &exchange("BREAK", "DIFFERENT", "CURRENT_QUARTER"),
        NOW + 1000,
    );
    let record = evidence_state::retained_records(&c).remove(0);
    assert_eq!(record.pair, "BTCUSDT");
    assert_eq!(record.contract_type, "PERPETUAL");
    assert_eq!(record.since, NOW + 1000);
    assert_eq!(record.expires_ms, NOW + 61000);
    assert_eq!(record.delivery_ms, Some(5_999_000_000));
    assert_eq!(e.catalog.slot(0).not_trading_since(), Some(NOW + 1000));
    assert!(!slot.is_trading());
    assert!(Arc::ptr_eq(&slot, &e.catalog.slot(0)));
    assert_eq!(slot.capacity(), 241);
    merge(&c, &e, &json!({"symbols":[]}), NOW + 2000);
    assert_eq!(evidence_state::retained_records(&c), vec![record]);
}
#[test]
fn disappeared_symbol_uses_last_known_delivery_date() {
    let t = tempfile::tempdir().unwrap();
    let c = config(t.path());
    let e = engine(&c);
    merge(&c, &e, &exchange("TRADING", "BTCUSDT", "PERPETUAL"), NOW);
    merge(&c, &e, &json!({"symbols":[]}), NOW + 1000);
    assert_eq!(
        evidence_state::retained_records(&c)[0].delivery_ms,
        Some(5_999_000_000)
    );
}
#[test]
fn returning_trading_cancels_record_by_catalog_key_even_if_contract_type_changes() {
    let t = tempfile::tempdir().unwrap();
    let c = config(t.path());
    let e = engine(&c);
    merge(&c, &e, &exchange("TRADING", "BTCUSDT", "PERPETUAL"), NOW);
    let slot = e.catalog.slot(0);
    merge(
        &c,
        &e,
        &exchange("BREAK", "BTCUSDT", "PERPETUAL"),
        NOW + 1000,
    );
    merge(
        &c,
        &e,
        &exchange("TRADING", "BTCUSDT", "TRADIFI_PERPETUAL"),
        NOW + 2000,
    );
    assert!(evidence_state::retained_records(&c).is_empty());
    assert!(slot.is_trading());
    assert_eq!(slot.not_trading_since(), None);
    assert!(Arc::ptr_eq(&slot, &e.catalog.slot(0)));
    merge(
        &c,
        &e,
        &exchange("TRADING", "BTCUSDT", "TRADIFI_PERPETUAL"),
        NOW + 100000,
    );
    assert!(slot.is_tracked());
    let records: Value =
        serde_json::from_slice(&std::fs::read(t.path().join("retained.json")).unwrap()).unwrap();
    assert_eq!(records, json!([]));
}
#[test]
fn ineligible_return_to_trading_also_revokes_retained_and_publishes_unknown() {
    for (pair, contract) in [("OTHER", "PERPETUAL"), ("BTCUSDT", "CURRENT_QUARTER")] {
        let t = tempfile::tempdir().unwrap();
        let c = config(t.path());
        let e = engine(&c);
        merge(&c, &e, &exchange("TRADING", "BTCUSDT", "PERPETUAL"), NOW);
        merge(
            &c,
            &e,
            &exchange("BREAK", "BTCUSDT", "PERPETUAL"),
            NOW + 1000,
        );
        merge(&c, &e, &exchange("TRADING", pair, contract), NOW + 2000);
        assert!(evidence_state::retained_records(&c).is_empty());
        assert!(
            e.catalog
                .find(Market::Future, Interval::parse("1m").unwrap(), "BTCUSDT")
                .is_none()
        );
    }
}
#[test]
fn expiry_cleans_snapshot_without_a_live_slot_and_never_restarts_the_deadline() {
    let t = tempfile::tempdir().unwrap();
    let c = config(t.path());
    let e = engine(&c);
    merge(&c, &e, &exchange("TRADING", "BTCUSDT", "PERPETUAL"), NOW);
    let store = Store::new(t.path(), 1).unwrap();
    let path = store.path(&e.catalog.slot(0));
    std::fs::write(&path, b"stub").unwrap();
    merge(
        &c,
        &e,
        &exchange("BREAK", "BTCUSDT", "PERPETUAL"),
        NOW + 1000,
    );
    let restarted = config(t.path());
    let new_engine = engine(&restarted);
    let _state = StateDirectory::load(&restarted, &new_engine).unwrap();
    merge(&restarted, &new_engine, &json!({"symbols":[]}), NOW + 61000);
    assert!(!path.exists());
    assert!(evidence_state::retained_records(&restarted).is_empty());
    merge(
        &restarted,
        &new_engine,
        &json!({"symbols":[]}),
        NOW + 1000000,
    );
    assert!(evidence_state::retained_records(&restarted).is_empty());
}
#[test]
fn restart_merge_preserves_retained_since_or_cancels_if_trading_resumed_offline() {
    for trading in [false, true] {
        let t = tempfile::tempdir().unwrap();
        let c = config(t.path());
        let e = engine(&c);
        merge(&c, &e, &exchange("TRADING", "BTCUSDT", "PERPETUAL"), NOW);
        merge(
            &c,
            &e,
            &exchange("BREAK", "BTCUSDT", "PERPETUAL"),
            NOW + 1000,
        );
        let restarted = config(t.path());
        let new_engine = engine(&restarted);
        let state = StateDirectory::load(&restarted, &new_engine).unwrap();
        merge(
            &restarted,
            &new_engine,
            &exchange(
                if trading { "TRADING" } else { "BREAK" },
                "BTCUSDT",
                "PERPETUAL",
            ),
            NOW + 2000,
        );
        let records = evidence_state::retained_records(&restarted);
        if trading {
            assert!(records.is_empty());
            assert!(new_engine.catalog.slot(0).is_trading());
        } else {
            assert_eq!(records[0].since, NOW + 1000);
            assert_eq!(records[0].expires_ms, NOW + 61000);
        }
        state.flush(&restarted, &new_engine).unwrap();
    }
}
#[test]
fn snapshots_keep_v024_bytes_restore_all_rows_pending_and_never_resurrect_absence() {
    let t = tempfile::tempdir().unwrap();
    let c = config(t.path());
    let e = engine(&c);
    merge(&c, &e, &exchange("TRADING", "BTCUSDT", "PERPETUAL"), NOW);
    let mut bar = Bar {
        open_time: NOW.div_euclid(60000) * 60000 - 60000,
        close_time: NOW.div_euclid(60000) * 60000 - 1,
        ..Bar::default()
    };
    bar.set_numbers(NumberType::String, ["1.000"; 8]).unwrap();
    let open = bar.open_time;
    e.commit_evidence(0, EvidenceUpdate::Rest(bar)).unwrap();
    let mut store = Store::new(t.path(), 1).unwrap();
    assert_eq!(store.dump(&e, || true).written, 1);
    let bytes = std::fs::read(store.path(&e.catalog.slot(0))).unwrap();
    assert_eq!(&bytes[..8], b"KLINE\0\x02\0");
    let restored = engine(&c);
    restored.refresh_catalog(e.catalog.instruments()).unwrap();
    assert_eq!(Store::new(t.path(), 1).unwrap().restore(&restored).bars, 1);
    assert_eq!(
        restored.catalog.slot(0).evidence_record(open).state,
        EvidenceState::Pending
    );
    e.commit_evidence(0, EvidenceUpdate::Absent(open)).unwrap();
    assert_eq!(store.dump(&e, || true).written, 1);
    let restored = engine(&c);
    restored.refresh_catalog(e.catalog.instruments()).unwrap();
    assert_eq!(Store::new(t.path(), 1).unwrap().restore(&restored).bars, 0);
}

#[tokio::test]
async fn discover_uses_the_same_retention_merge_on_initial_and_subsequent_metadata_refreshes() {
    use axum::{Json, Router, extract::State, routing::get};
    let t = tempfile::tempdir().unwrap();
    let c = config(t.path());
    let e = engine(&c);
    let current = Arc::new(parking_lot::RwLock::new(exchange(
        "TRADING",
        "BTCUSDT",
        "PERPETUAL",
    )));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let app =
        Router::new()
            .route(
                "/fapi/v1/exchangeInfo",
                get(
                    |State(s): State<Arc<parking_lot::RwLock<Value>>>| async move {
                        Json(s.read().clone())
                    },
                ),
            )
            .with_state(current.clone());
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let metadata = kline_market::metadata::Metadata::new(
        kline_runtime::rest::RestApi::new(&url, &url, 6000).unwrap(),
        300,
    );
    let first = kline_runtime::directory::discover(&c, &metadata)
        .await
        .unwrap();
    assert_eq!(first.len(), 1);
    e.refresh_catalog_with_retained(first, evidence_state::retained_records(&c))
        .unwrap();
    current.write()["symbols"][0]["status"] = json!("BREAK");
    metadata.refresh(Market::Future).await.unwrap();
    let second = kline_runtime::directory::discover(&c, &metadata)
        .await
        .unwrap();
    assert_eq!(second.len(), 1);
    assert!(!second[0].trading);
    let since = evidence_state::retained_records(&c)[0].since;
    e.refresh_catalog_with_retained(second, evidence_state::retained_records(&c))
        .unwrap();
    assert_eq!(e.catalog.slot(0).not_trading_since(), Some(since));
    current.write()["symbols"][0]["status"] = json!("TRADING");
    current.write()["symbols"][0]["pair"] = json!("OTHER");
    metadata.refresh(Market::Future).await.unwrap();
    assert!(
        kline_runtime::directory::discover(&c, &metadata)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(evidence_state::retained_records(&c).is_empty());
    task.abort();
}
#[test]
fn failed_expiry_cleanup_does_not_partially_commit_retention_revocations() {
    let t = tempfile::tempdir().unwrap();
    let c = config(t.path());
    let e = engine(&c);
    let trading = json!({"symbols":[{"symbol":"BTCUSDT","status":"TRADING","pair":"BTCUSDT","contractType":"PERPETUAL"},{"symbol":"ETHUSDT","status":"TRADING","pair":"ETHUSDT","contractType":"PERPETUAL"}]});
    merge(&c, &e, &trading, NOW);
    merge(&c, &e, &json!({"symbols":[]}), NOW + 1000);
    let original = evidence_state::retained_records(&c);
    let store = Store::new(t.path(), 2).unwrap();
    let path = store.path(
        &e.catalog.slot(
            e.catalog
                .find(Market::Future, Interval::parse("1m").unwrap(), "BTCUSDT")
                .unwrap(),
        ),
    );
    std::fs::create_dir(&path).unwrap();
    let eth_only = json!({"symbols":[{"symbol":"ETHUSDT","status":"TRADING","pair":"ETHUSDT","contractType":"PERPETUAL"}]});
    assert!(evidence_directory::merge(&c, &eth_only, NOW + 61000).is_err());
    assert_eq!(evidence_state::retained_records(&c), original);
    std::fs::remove_dir(path).unwrap();
    merge(&c, &e, &eth_only, NOW + 61000);
    assert!(evidence_state::retained_records(&c).is_empty());
}
#[test]
fn state_loader_rejects_duplicate_catalog_keys_and_config_stays_strict() {
    let t = tempfile::tempdir().unwrap();
    let c = config(t.path());
    let record = json!({"symbol":"BTCUSDT","pair":"BTCUSDT","contract_type":"PERPETUAL","since":NOW,"expires_ms":NOW+60000,"delivery_ms":null});
    std::fs::write(
        t.path().join("retained.json"),
        json!([record.clone(), record]).to_string(),
    )
    .unwrap();
    assert!(StateDirectory::load(&c, &engine(&c)).is_err());
    assert!(
        serde_json::from_value::<Config>(json!({"listen":"127.0.0.1:0","unexpected":1})).is_err()
    );
    assert!(
        serde_json::from_value::<Config>(
            json!({"listen":"127.0.0.1:0","evidence":{"ws_trust_window_ms":60000,"runtime":{}}})
        )
        .is_err()
    );
}

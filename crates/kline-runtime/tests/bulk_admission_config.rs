use kline_runtime::config::Config;
use serde_json::json;

#[test]
fn bulk_admission_settings_default_to_the_old_cap_with_queueing_and_parse_when_given() {
    let config: Config = serde_json::from_value(json!({"listen": "127.0.0.1:0"})).unwrap();
    assert_eq!(config.bulk_inflight_limit, 256);
    assert_eq!(config.bulk_admission_queue, 4096);
    assert_eq!(config.bulk_admission_wait_ms, None);
    assert_eq!(config.http_concurrency_limit, 512);
    assert_eq!(config.http_admission_queue, 4096);
    assert_eq!(config.http_admission_wait_ms, None);
    let config: Config = serde_json::from_value(json!({
        "listen": "127.0.0.1:0",
        "final_wait_ms": 6000,
        "bulk_inflight_limit": 1024,
        "bulk_admission_queue": 0,
        "bulk_admission_wait_ms": 2500,
        "http_concurrency_limit": 2048,
        "http_admission_queue": 16,
        "http_admission_wait_ms": 1500
    }))
    .unwrap();
    assert_eq!(config.bulk_inflight_limit, 1024);
    assert_eq!(config.bulk_admission_queue, 0);
    assert_eq!(config.bulk_admission_wait_ms, Some(2500));
    assert_eq!(config.http_concurrency_limit, 2048);
    assert_eq!(config.http_admission_queue, 16);
    assert_eq!(config.http_admission_wait_ms, Some(1500));
    assert!(
        serde_json::from_value::<Config>(json!({"listen": "127.0.0.1:0", "bulk_inflight": 1}))
            .is_err(),
        "unknown keys stay rejected"
    );
}

#[test]
fn engine_settings_fall_back_to_the_final_wait_and_engine_clamps_apply() {
    let config: Config =
        serde_json::from_value(json!({"listen": "127.0.0.1:0", "final_wait_ms": 6000})).unwrap();
    let settings = config.engine_settings().unwrap();
    assert_eq!(settings.admission_wait_ms, 6000);
    assert_eq!(settings.http_admission_wait_ms, 6000);
    assert_eq!(settings.inflight_limit, 256);
    assert_eq!(settings.http_concurrency_limit, 512);

    let config: Config = serde_json::from_value(json!({
        "listen": "127.0.0.1:0",
        "final_wait_ms": 6000,
        "bulk_admission_wait_ms": 0,
        "http_admission_wait_ms": 1500,
        "bulk_inflight_limit": 0,
        "http_concurrency_limit": 1_000_000,
        "bulk_admission_queue": 0,
        "http_admission_queue": 100_000_000
    }))
    .unwrap();
    let settings = config.engine_settings().unwrap();
    assert_eq!(settings.admission_wait_ms, 0);
    assert_eq!(settings.http_admission_wait_ms, 1500);
    let engine = kline_service::Engine::new(
        kline_service::Catalog::new(vec![]).unwrap(),
        std::sync::Arc::new(kline_service::SystemClock { offset_ms: 0 }),
        kline_service::Settings {
            admission_wait_ms: 99_999,
            ..settings
        },
    );
    let effective = engine.settings();
    assert_eq!(effective.inflight_limit, 1, "zero becomes one");
    assert_eq!(effective.http_concurrency_limit, 65_536);
    assert_eq!(effective.admission_queue, 0);
    assert_eq!(effective.http_admission_queue, 1 << 20);
    assert_eq!(effective.admission_wait_ms, 30_000);
    assert_eq!(effective.http_admission_wait_ms, 1500);
}

#[test]
fn the_pre_boundary_wait_defaults_to_javas_250_ms_and_can_be_disabled() {
    let config: Config = serde_json::from_value(json!({"listen": "127.0.0.1:0"})).unwrap();
    assert_eq!(config.engine_settings().unwrap().pre_boundary_wait_ms, 250);
    let config: Config =
        serde_json::from_value(json!({"listen": "127.0.0.1:0", "bulk_pre_boundary_wait_ms": 0}))
            .unwrap();
    assert_eq!(config.engine_settings().unwrap().pre_boundary_wait_ms, 0);
}

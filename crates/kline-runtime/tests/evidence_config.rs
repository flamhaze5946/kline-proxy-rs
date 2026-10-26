use kline_runtime::config::Config;
use serde_json::{Value, json};
fn config() -> Value {
    json!({"listen":"127.0.0.1:0","number_type":"string","history_capacity":241,
        "evidence":{"ws_trust_window_ms":86400000},
        "bulk_admission_wait_ms":100,"http_admission_wait_ms":100,"bulk_pre_boundary_wait_ms":250,
        "subscriptions":[{"market":"future","interval":"1m","continuous":true,"history_capacity":241}],
        "rest":{"latest_repair_workers":2},
        "persistence":{"directory":"state","boundary_guard_before_ms":2000,"boundary_guard_after_ms":15000}})
}
fn load(v: &Value) -> anyhow::Result<Config> {
    let file = tempfile::NamedTempFile::new()?;
    std::fs::write(file.path(), serde_json::to_vec(v)?)?;
    Config::load(file.path())
}
#[test]
fn enabled_settings_require_explicit_guard_workers_admission_and_valid_evidence_contract() {
    let v = config();
    let c = load(&v).unwrap();
    c.catalog().unwrap();
    let settings = c.engine_settings().unwrap();
    let e = settings.evidence.unwrap();
    assert_eq!(e.ws_trust_window_ms, 86400000);
    assert_eq!(e.fallback_weight_per_minute, 120);
    for field in ["boundary_guard_before_ms", "boundary_guard_after_ms"] {
        let mut v = config();
        v["persistence"].as_object_mut().unwrap().remove(field);
        assert!(load(&v).is_err());
    }
    let mut v = config();
    v["rest"]
        .as_object_mut()
        .unwrap()
        .remove("latest_repair_workers");
    assert!(load(&v).is_err());
    let mut v = config();
    v["evidence"]
        .as_object_mut()
        .unwrap()
        .remove("ws_trust_window_ms");
    assert!(load(&v).is_err());
    for (path, value) in [
        (vec!["number_type"], json!("double")),
        (vec!["evidence", "ws_trust_window_ms"], json!(59999)),
        (vec!["evidence", "fallback_weight_per_minute"], json!(0)),
        (vec!["final_wait_ms"], json!(0)),
        (vec!["bulk_admission_wait_ms"], Value::Null),
        (vec!["http_admission_wait_ms"], Value::Null),
        (vec!["bulk_pre_boundary_wait_ms"], Value::Null),
    ] {
        let mut v = config();
        if path.len() == 1 {
            v[path[0]] = value;
        } else {
            v[path[0]][path[1]] = value;
        }
        assert!(load(&v).unwrap().catalog().is_err(), "{path:?}");
    }
    for (field, value) in [
        ("interval", json!("1h")),
        ("continuous", json!(false)),
        ("history_capacity", json!(240)),
    ] {
        let mut v = config();
        v["subscriptions"][0][field] = value;
        assert!(load(&v).unwrap().catalog().is_err());
    }
}
#[test]
fn absent_evidence_config_preserves_legacy_defaults_and_does_not_require_new_explicit_fields() {
    let c = load(&json!({"listen":"127.0.0.1:0","rest":{}})).unwrap();
    c.catalog().unwrap();
    assert!(c.engine_settings().unwrap().evidence.is_none());
}

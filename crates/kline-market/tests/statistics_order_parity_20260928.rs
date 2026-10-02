use kline_core::{Bar, ExactNumbers};
use kline_market::statistics;
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc};

#[test]
fn java_statistic_values_match_for_collision_resizes_and_replaced_dates() {
    let cases: Value =
        serde_json::from_str(include_str!("fixtures/statistics-completion-cases.json")).unwrap();
    let expected: Value =
        serde_json::from_str(include_str!("fixtures/statistics-completion-java.json")).unwrap();
    let mut differences = vec![];
    for case in cases.as_array().unwrap() {
        let rows: Vec<_> = case["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|series| {
                let bars = series["bars"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|row| Bar {
                        open_time: row["t"].as_i64().unwrap(),
                        exact: Some(Arc::new(ExactNumbers::Strings([
                            row["open"].as_str().unwrap().into(),
                            "0".into(),
                            "0".into(),
                            "0".into(),
                            "0".into(),
                            row["quote_volume"].as_str().unwrap().into(),
                            "0".into(),
                            "0".into(),
                        ]))),
                        ..Bar::default()
                    })
                    .collect();
                (series["symbol"].as_str().unwrap().into(), bars)
            })
            .collect();
        let (a, b) = statistics::calculate(
            &rows,
            case["days"].as_u64().unwrap() as usize,
            case["volume_days"].as_u64().unwrap() as usize,
            case["rank"].as_u64().unwrap() as usize,
        )
        .unwrap();
        // Preserve the f32 representation used by Java's JSON serializer.
        let actual: Value = serde_json::from_slice(
            &serde_json::to_vec(&BTreeMap::from([("yama01", a), ("yama02", b)])).unwrap(),
        )
        .unwrap();
        let name = case["name"].as_str().unwrap();
        if actual != expected[name] {
            differences.push(json!({"case":name,"rust":actual,"java":expected[name]}));
        }
    }
    assert!(
        differences.is_empty(),
        "{}",
        serde_json::to_string_pretty(&differences).unwrap()
    );
}

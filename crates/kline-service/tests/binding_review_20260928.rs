//! Golden results from the real Java SerializeConfig + BulkKlinesRequest record.
//! The HTTP controller's private DTO uses the same field/schema declarations.
use kline_service::http_compat::{self, JsonBody};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Deserialize, Serialize)]
struct Body {
    #[serde(
        default,
        deserialize_with = "http_compat::optional_string",
        skip_serializing_if = "Option::is_none"
    )]
    interval: Option<String>,
    #[serde(
        default,
        deserialize_with = "http_compat::optional_i32",
        skip_serializing_if = "Option::is_none"
    )]
    limit: Option<i32>,
    #[serde(
        default,
        deserialize_with = "http_compat::json_bool",
        skip_serializing_if = "Option::is_none"
    )]
    closed_only: Option<bool>,
    #[serde(
        default,
        deserialize_with = "http_compat::optional_strings",
        skip_serializing_if = "Option::is_none"
    )]
    symbols: Option<Vec<Option<String>>>,
}
impl JsonBody for Body {
    const RECORD: bool = true;
    const STRING_FIELDS: &'static [&'static str] = &["interval"];
    const STRING_LIST_FIELDS: &'static [&'static str] = &["symbols"];
    const BOOLEAN_FIELDS: &'static [&'static str] = &["closed_only"];
    const INTEGER_FIELDS: &'static [&'static str] = &["limit"];
}

#[test]
fn binding_matches_java_record_and_number_length_oracle() {
    let cases: Vec<Value> =
        serde_json::from_str(include_str!("fixtures/binding_review_20260928.json")).unwrap();
    assert_eq!(cases.len(), 149);
    for case in cases {
        let name = case["name"].as_str().unwrap();
        let actual = http_compat::json_body::<Body>(case["body"].as_str().unwrap().as_bytes());
        if case["status"] == "error" {
            assert!(actual.is_err(), "Java rejects {name}");
        } else {
            assert_eq!(
                serde_json::to_value(actual.unwrap_or_else(|error| panic!("{name}: {error}")))
                    .unwrap(),
                case["value"],
                "{name}"
            );
        }
    }
}

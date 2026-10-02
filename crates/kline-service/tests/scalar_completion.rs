//! Expected values come from JDK 21 Spring NumberUtils and Jackson, not Rust.
use kline_service::http_compat::{self, JsonBody};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Deserialize, Serialize, Debug)]
struct ScalarBody {
    #[serde(default, deserialize_with = "http_compat::optional_string")]
    interval: Option<String>,
    #[serde(default, deserialize_with = "http_compat::optional_strings")]
    symbols: Option<Vec<Option<String>>>,
    #[serde(default, deserialize_with = "http_compat::optional_i32")]
    limit: Option<i32>,
    #[serde(default, deserialize_with = "http_compat::optional_i64")]
    since_ms: Option<i64>,
    #[serde(default, deserialize_with = "http_compat::json_bool")]
    closed_only: Option<bool>,
}
impl JsonBody for ScalarBody {
    const STRING_FIELDS: &'static [&'static str] = &["interval"];
    const STRING_LIST_FIELDS: &'static [&'static str] = &["symbols"];
    const BOOLEAN_FIELDS: &'static [&'static str] = &["closed_only"];
    const INTEGER_FIELDS: &'static [&'static str] = &["limit", "since_ms"];
}
fn oracle() -> Value {
    serde_json::from_str(include_str!("fixtures/java-scalar-oracle.json")).unwrap()
}

#[test]
fn spring_unicode_numbers_match_java_oracle() {
    let oracle = oracle();
    for (input, expected) in oracle["query_numbers"].as_object().unwrap() {
        let actual = http_compat::optional_number::<i32>(Some(input));
        if expected == "ERROR" {
            assert!(actual.is_err(), "{input}: {actual:?}");
        } else {
            assert_eq!(
                serde_json::to_value(actual.unwrap()).unwrap(),
                *expected,
                "{input}"
            );
        }
    }
    for zero in oracle["digit_zero_bmp"].as_array().unwrap() {
        let zero = zero.as_u64().unwrap() as u32;
        for n in 0..10 {
            let s = char::from_u32(zero + n).unwrap().to_string();
            assert_eq!(
                http_compat::optional_number::<i32>(Some(&s)),
                Ok(Some(n as i32))
            );
        }
    }
}

#[test]
fn jackson_raw_token_coercions_match_java_oracle() {
    let oracle = oracle();
    for (input, expected) in oracle["bodies"].as_object().unwrap() {
        let actual = http_compat::json_body::<ScalarBody>(input.as_bytes());
        if expected == "ERROR" {
            assert!(actual.is_err(), "{input}: {actual:?}");
        } else {
            assert_eq!(
                serde_json::to_value(actual.unwrap()).unwrap(),
                *expected,
                "{input}"
            );
        }
    }
    for input in [
        r#"{"limit":18446744073709551616}"#,
        r#"{"since_ms":18446744073709551616}"#,
    ] {
        assert!(http_compat::json_body::<ScalarBody>(input.as_bytes()).is_err());
    }
    let body = http_compat::json_body::<ScalarBody>(br#"{"interval":"1h","unused":1e400}"#)
        .unwrap()
        .unwrap();
    assert_eq!(body.interval.as_deref(), Some("1h"));
}

#[test]
fn ticker_symbols_preserve_number_tokens_and_read_one_array() {
    assert_eq!(
        http_compat::json_string_list("[1.2300,-0,1E+3,null] trailing").unwrap(),
        Some(vec![
            Some("1.2300".into()),
            Some("-0".into()),
            Some("1E+3".into()),
            None
        ])
    );
    assert!(http_compat::json_string_list("[[1]]").is_err());
}

#[test]
fn jackson_number_length_constraints_apply_to_unknown_fields_too() {
    let oracle: Value =
        serde_json::from_str(include_str!("fixtures/java-number-length-oracle.json")).unwrap();
    for (case, expected) in oracle.as_object().unwrap() {
        let (kind, digits) = case.rsplit_once('-').unwrap();
        let digits = "1".repeat(digits.parse().unwrap());
        let value = match kind {
            "negative" => format!("-{digits}"),
            "fraction" => format!("{digits}.0"),
            "exponent" => format!("{digits}e0"),
            "negative-fraction" => format!("-{digits}.0"),
            "negative-exponent" => format!("-{digits}e0"),
            _ => digits,
        };
        for field in ["interval", "unknown"] {
            let body = format!("{{\"{field}\":{value}}}");
            assert_eq!(
                http_compat::json_body::<ScalarBody>(body.as_bytes()).is_ok(),
                expected == "OK",
                "{case}/{field}"
            );
        }
    }
    for input in [
        format!("{{\"interval\":\"{}\"}}", "1".repeat(2000)),
        format!("{{}} {}", "1".repeat(2000)),
    ] {
        assert!(http_compat::json_body::<ScalarBody>(input.as_bytes()).is_ok());
    }
}

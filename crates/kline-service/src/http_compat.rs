//! Shared request-boundary compatibility. Domain queries stay strongly typed.
use axum::{
    Json,
    body::Bytes,
    extract::{FromRequest, FromRequestParts, Request},
    http::{StatusCode, header, request::Parts},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Deserializer, de::DeserializeOwned};
use serde_json::{Value, value::RawValue};
use std::borrow::Cow;
use std::str::FromStr;

fn error(status: StatusCode, message: impl ToString) -> Response {
    let mut response = (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(serde_json::json!({"code":-1000,"msg":message.to_string()})),
    )
        .into_response();
    response
        .extensions_mut()
        .insert(crate::management::NegotiationFallback(status));
    response
}

/// Keep extractor failures inside the same JSON error envelope as business errors.
pub struct Query<T>(pub T);
impl<S: Send + Sync, T: DeserializeOwned + Send> FromRequestParts<S> for Query<T> {
    type Rejection = Response;
    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let axum::extract::Query(pairs) =
            axum::extract::Query::<Vec<(String, String)>>::from_request_parts(parts, state)
                .await
                .map_err(|e| error(e.status(), e.body_text()))?;
        let mut values = serde_json::Map::new();
        for (key, value) in pairs {
            // Servlet binds repeated String parameters as comma-joined values;
            // scalar numbers and booleans use their first occurrence.
            let scalar = matches!(
                key.as_str(),
                "limit"
                    | "startTime"
                    | "endTime"
                    | "since_ms"
                    | "until_ms"
                    | "closed_only"
                    | "pageNo"
                    | "pageSize"
            ) || (key == "type" && parts.uri.path().starts_with("/bapi/composite/"));
            if let Some(Value::String(previous)) = values.get_mut(&key) {
                if !scalar {
                    previous.push(',');
                    previous.push_str(&value);
                }
            } else {
                values.insert(key, Value::String(value));
            }
        }
        serde_json::from_value(Value::Object(values))
            .map(Self)
            .map_err(|e| error(StatusCode::BAD_REQUEST, e))
    }
}

/// Spring's optional request body: empty bytes and JSON null both mean no value.
/// Bytes extraction retains the router's body limit. Whitespace around null is JSON.
pub struct OptionalJson<T>(pub Option<T>);
/// Request DTO field kinds whose Jackson coercion depends on the original JSON
/// token. Keeping this schema at the HTTP boundary leaves domain types unchanged.
/// These optional request DTOs must default absent fields and bind each field
/// independently; that allows overwritten members to be validated in isolation.
pub trait JsonBody: DeserializeOwned {
    const RECORD: bool = false;
    const STRING_FIELDS: &'static [&'static str] = &[];
    const STRING_LIST_FIELDS: &'static [&'static str] = &[];
    const BOOLEAN_FIELDS: &'static [&'static str] = &[];
    const INTEGER_FIELDS: &'static [&'static str] = &[];
}

fn raw_integer(s: &str) -> bool {
    let digits = s.strip_prefix('-').unwrap_or(s);
    !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit())
}
fn raw_string(raw: &RawValue) -> Result<Value, serde_json::Error> {
    let s = raw.get();
    match s.as_bytes()[0] {
        b'"' | b'n' => serde_json::from_str(s),
        b'[' | b'{' => Err(serde::de::Error::custom("Expected string or scalar value")),
        _ => Ok(Value::String(s.to_owned())),
    }
}
struct RawMembers<'a>(Vec<(String, &'a RawValue)>);
impl<'de> Deserialize<'de> for RawMembers<'de> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct Members;
        impl<'de> serde::de::Visitor<'de> for Members {
            type Value = RawMembers<'de>;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a JSON object")
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut map: A,
            ) -> Result<Self::Value, A::Error> {
                let mut members = Vec::with_capacity(map.size_hint().unwrap_or(4));
                while let Some(member) = map.next_entry()? {
                    members.push(member);
                }
                Ok(RawMembers(members))
            }
        }
        d.deserialize_map(Members)
    }
}
fn number_lengths(bytes: &[u8]) -> Result<(), serde_json::Error> {
    // Jackson's default StreamReadConstraints counts numeric digits, excluding
    // signs, decimal points and exponent markers. Skip this scan for normal
    // small bodies, and skip strings so long symbol/text values stay valid.
    if bytes.len() <= 1000 {
        return Ok(());
    }
    let mut at = 0;
    while at < bytes.len() {
        if bytes[at] == b'"' {
            at += 1;
            while at < bytes.len() {
                let c = bytes[at];
                at += 1;
                if c == b'\\' {
                    at += 1;
                } else if c == b'"' {
                    break;
                }
            }
        } else if bytes[at].is_ascii_digit() {
            let mut digits = 0;
            while at < bytes.len()
                && (bytes[at].is_ascii_digit()
                    || matches!(bytes[at], b'.' | b'e' | b'E' | b'+' | b'-'))
            {
                digits += usize::from(bytes[at].is_ascii_digit());
                at += 1;
            }
            if digits > 1000 {
                return Err(serde::de::Error::custom(
                    "Number length exceeds the maximum length (1000)",
                ));
            }
        } else {
            at += 1;
        }
    }
    Ok(())
}
/// Jackson reads one value (FAIL_ON_TRAILING_TOKENS is disabled), and duplicate
/// object members keep their last value. RawValue also preserves 1e3, 1.2300 and
/// arbitrarily large numeric tokens when binding a String field.
pub fn json_body<T: JsonBody>(body: &[u8]) -> Result<Option<T>, serde_json::Error> {
    let mut decoder =
        serde_json::Deserializer::from_slice(body).into_iter::<Option<RawMembers<'_>>>();
    let raw = decoder
        .next()
        .transpose()?
        .ok_or_else(|| <serde_json::Error as serde::de::Error>::custom("Expected JSON object"))?;
    number_lengths(&body[..decoder.byte_offset()])?;
    let Some(raw) = raw else { return Ok(None) };
    let mut values = serde_json::Map::new();
    let mut overwritten = Vec::new();
    for (key, value) in raw.0 {
        let s = value.get();
        let value = if T::STRING_FIELDS.contains(&key.as_str()) {
            raw_string(value)?
        } else if T::STRING_LIST_FIELDS.contains(&key.as_str()) {
            let list: Option<Vec<&RawValue>> = serde_json::from_str(s)?;
            match list {
                Some(rows) => {
                    Value::Array(rows.into_iter().map(raw_string).collect::<Result<_, _>>()?)
                }
                None => Value::Null,
            }
        } else if T::BOOLEAN_FIELDS.contains(&key.as_str()) {
            if raw_integer(s) {
                Value::Bool(s.bytes().any(|b| matches!(b, b'1'..=b'9')))
            } else {
                serde_json::from_str(s)?
            }
        } else if T::INTEGER_FIELDS.contains(&key.as_str()) {
            if raw_integer(s) {
                Value::String(s.to_owned())
            } else {
                serde_json::from_str(s)?
            }
        } else {
            // Jackson ignores unknown DTO fields, including numbers beyond f64.
            continue;
        };
        // Jackson 2.15 constructs a record once every creator property has
        // arrived. A later known property has no setter, even when its earlier
        // occurrence was valid. Earlier duplicates still bind normally.
        if T::RECORD
            && values.len()
                == T::STRING_FIELDS.len()
                    + T::STRING_LIST_FIELDS.len()
                    + T::BOOLEAN_FIELDS.len()
                    + T::INTEGER_FIELDS.len()
        {
            return Err(serde::de::Error::custom(
                "No fallback setter for record creator property",
            ));
        }
        match values.entry(key) {
            serde_json::map::Entry::Vacant(entry) => {
                entry.insert(value);
            }
            serde_json::map::Entry::Occupied(mut entry) => {
                let key = entry.key().clone();
                overwritten.push((key, entry.insert(value)));
            }
        }
    }
    // Jackson validates overwritten values too: {"limit":"bad","limit":2}
    // fails, even though valid duplicate members otherwise keep their last value.
    // The ordinary, duplicate-free path needs no cloning or extra deserialization.
    for (key, previous) in overwritten {
        // A repeated small field must not copy a large symbols list each time.
        // Every old token is visited once; total work remains proportional to
        // the bounded request body even when a record is not yet constructed.
        let mut old = serde_json::Map::new();
        old.insert(key, previous);
        let _: T = serde_json::from_value(Value::Object(old))?;
    }
    serde_json::from_value(Value::Object(values)).map(Some)
}

/// Java's String[] binding for the symbols query parameter also reads one value.
pub fn json_string_list(s: &str) -> Result<Option<Vec<Option<String>>>, serde_json::Error> {
    let mut decoder = serde_json::Deserializer::from_str(s).into_iter::<Option<Vec<&RawValue>>>();
    let rows = decoder
        .next()
        .transpose()?
        .ok_or_else(|| <serde_json::Error as serde::de::Error>::custom("Expected JSON array"))?;
    number_lengths(&s.as_bytes()[..decoder.byte_offset()])?;
    rows.map(|rows| {
        rows.into_iter()
            .map(|r| scalar_string(raw_string(r)?).map_err(serde::de::Error::custom))
            .collect()
    })
    .transpose()
}

impl<S: Send + Sync, T: JsonBody + Send> FromRequest<S> for OptionalJson<T> {
    type Rejection = Response;
    async fn from_request(request: Request, state: &S) -> Result<Self, Self::Rejection> {
        let json = request
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|h| h.to_str().ok())
            .and_then(|s| s.split(';').next())
            .is_some_and(|s| {
                let media = s.trim().to_ascii_lowercase();
                media == "application/json"
                    || (media.starts_with("application/") && media.ends_with("+json"))
            });
        let body = Bytes::from_request(request, state)
            .await
            .map_err(|e| error(e.status(), e.body_text()))?;
        if body.is_empty() {
            return Ok(Self(None));
        }
        if !json {
            return Err(error(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "Expected application/json",
            ));
        }
        json_body(&body)
            .map(Self)
            .map_err(|e| error(StatusCode::BAD_REQUEST, e))
    }
}

pub fn java_whitespace(c: char) -> bool {
    matches!(c, '\u{0009}'..='\u{000d}' | '\u{001c}'..='\u{0020}' | '\u{1680}' | '\u{2000}'..='\u{2006}' | '\u{2008}'..='\u{200a}' | '\u{2028}' | '\u{2029}' | '\u{205f}' | '\u{3000}')
}
pub fn is_blank(s: &str) -> bool {
    s.chars().all(java_whitespace)
}
pub fn java_trim(s: &str) -> &str {
    s.trim_matches(|c| c <= '\u{0020}')
}
fn java_digits(text: &str, hex: bool) -> Cow<'_, str> {
    if text.is_ascii() {
        return Cow::Borrowed(text);
    }
    // Character.digit(char, radix), JDK 21: supplementary characters are not
    // accepted by parseInt/parseLong, which iterate UTF-16 char values.
    const ZEROES: [u32; 37] = [
        48, 1632, 1776, 1984, 2406, 2534, 2662, 2790, 2918, 3046, 3174, 3302, 3430, 3558, 3664,
        3792, 3872, 4160, 4240, 6112, 6160, 6470, 6608, 6784, 6800, 6992, 7088, 7232, 7248, 42528,
        43216, 43264, 43472, 43504, 43600, 44016, 65296,
    ];
    Cow::Owned(
        text.chars()
            .map(|c| {
                let n = u32::from(c);
                if let Some(zero) = ZEROES.iter().find(|&&z| n >= z && n < z + 10) {
                    char::from_u32(u32::from(b'0') + n - zero).unwrap()
                } else if hex && matches!(c, '\u{ff21}'..='\u{ff26}' | '\u{ff41}'..='\u{ff46}') {
                    char::from_u32(n - 0xfee0).unwrap()
                } else {
                    c
                }
            })
            .collect(),
    )
}
pub fn optional_number<T: FromStr>(value: Option<&str>) -> Result<Option<T>, &'static str> {
    let Some(value) = value else {
        return Ok(None);
    };
    // Spring NumberUtils removes all Java whitespace and accepts hexadecimal.
    let text = if value.chars().any(java_whitespace) {
        std::borrow::Cow::Owned(
            value
                .chars()
                .filter(|c| !java_whitespace(*c))
                .collect::<String>(),
        )
    } else {
        std::borrow::Cow::Borrowed(value)
    };
    if text.is_empty() {
        return Ok(None);
    }
    let unsigned = text.strip_prefix(['-', '+']).unwrap_or(&text);
    if unsigned.starts_with(['-', '+']) {
        return Err("Invalid number");
    }
    let hex = unsigned
        .strip_prefix("0x")
        .or_else(|| unsigned.strip_prefix("0X"))
        .or_else(|| unsigned.strip_prefix('#'));
    let text = if let Some(hex) = hex {
        if text.starts_with('+') || hex.starts_with(['-', '+']) {
            return Err("Invalid number");
        }
        let magnitude =
            i128::from_str_radix(&java_digits(hex, true), 16).map_err(|_| "Invalid number")?;
        std::borrow::Cow::Owned(
            (if text.starts_with('-') {
                -magnitude
            } else {
                magnitude
            })
            .to_string(),
        )
    } else {
        text
    };
    java_digits(&text, false)
        .parse()
        .map(Some)
        .map_err(|_| "Invalid number")
}
pub fn query_i32<'de, D: Deserializer<'de>>(d: D) -> Result<Option<i32>, D::Error> {
    let value = Option::<String>::deserialize(d)?;
    optional_number(value.as_deref()).map_err(serde::de::Error::custom)
}

fn integer<'de, T: FromStr, D: Deserializer<'de>>(
    d: D,
    min: f64,
    max: f64,
) -> Result<Option<T>, D::Error> {
    let text = match Value::deserialize(d)? {
        Value::Null => return Ok(None),
        Value::String(s) => s,
        // Jackson accepts JSON fractional numbers for Integer/Long and truncates toward zero.
        Value::Number(n) if n.is_f64() => {
            let value = n.as_f64().unwrap();
            if value < min || value > max {
                return Err(serde::de::Error::custom("Integer out of range"));
            }
            format!("{:.0}", value.trunc())
        }
        Value::Number(n) => n.to_string(),
        _ => {
            return Err(serde::de::Error::custom(
                "Expected integer or numeric string",
            ));
        }
    };
    match java_trim(&text) {
        "" | "null" => Ok(None),
        text => java_digits(text, false)
            .parse()
            .map(Some)
            .map_err(|_| serde::de::Error::custom("Invalid number")),
    }
}
pub fn optional_i32<'de, D: Deserializer<'de>>(d: D) -> Result<Option<i32>, D::Error> {
    integer(d, f64::from(i32::MIN), f64::from(i32::MAX))
}
pub fn optional_i64<'de, D: Deserializer<'de>>(d: D) -> Result<Option<i64>, D::Error> {
    let value = Value::deserialize(d)?;
    if let Value::Number(n) = &value
        && n.is_f64()
    {
        let value = n.as_f64().unwrap();
        if value < i64::MIN as f64 || value > i64::MAX as f64 {
            return Err(serde::de::Error::custom("Integer out of range"));
        }
        // Jackson compares double bounds before its saturating double-to-long
        // conversion, including the representable value immediately at 2^63.
        return Ok(Some(value as i64));
    }
    integer(value, i64::MIN as f64, i64::MAX as f64).map_err(serde::de::Error::custom)
}
fn scalar_string(value: Value) -> Result<Option<String>, &'static str> {
    match value {
        Value::Null => Ok(None),
        Value::String(s) => Ok(Some(s)),
        Value::Bool(v) => Ok(Some(v.to_string())),
        Value::Number(v) => Ok(Some(v.to_string())),
        _ => Err("Expected string or scalar value"),
    }
}
pub fn optional_string<'de, D: Deserializer<'de>>(d: D) -> Result<Option<String>, D::Error> {
    scalar_string(Value::deserialize(d)?).map_err(serde::de::Error::custom)
}
pub fn string_list(value: Value) -> Result<Option<Vec<Option<String>>>, &'static str> {
    match value {
        Value::Null => Ok(None),
        Value::Array(rows) => rows
            .into_iter()
            .map(scalar_string)
            .collect::<Result<Vec<_>, _>>()
            .map(Some),
        _ => Err("Expected string array"),
    }
}
pub fn optional_strings<'de, D: Deserializer<'de>>(
    d: D,
) -> Result<Option<Vec<Option<String>>>, D::Error> {
    string_list(Value::deserialize(d)?).map_err(serde::de::Error::custom)
}

/// Spring query Boolean conversion also accepts on/off, yes/no, and 1/0.
pub fn query_bool<'de, D: Deserializer<'de>>(d: D) -> Result<Option<bool>, D::Error> {
    let s = Option::<String>::deserialize(d)?;
    match s
        .as_deref()
        .map(java_trim)
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        None | Some("") => Ok(None),
        Some("true" | "on" | "yes" | "1") => Ok(Some(true)),
        Some("false" | "off" | "no" | "0") => Ok(Some(false)),
        _ => Err(serde::de::Error::custom("Invalid boolean")),
    }
}

/// Jackson body Boolean conversion is distinct from Spring's query conversion.
pub fn json_bool<'de, D: Deserializer<'de>>(d: D) -> Result<Option<bool>, D::Error> {
    match Value::deserialize(d)? {
        Value::Null => Ok(None),
        Value::Bool(v) => Ok(Some(v)),
        Value::Number(n) if n.is_i64() || n.is_u64() => Ok(Some(n.to_string() != "0")),
        Value::String(s) => match java_trim(&s) {
            "" | "null" => Ok(None),
            "true" | "True" | "TRUE" => Ok(Some(true)),
            "false" | "False" | "FALSE" => Ok(Some(false)),
            _ => Err(serde::de::Error::custom("Invalid boolean")),
        },
        _ => Err(serde::de::Error::custom("Invalid boolean")),
    }
}

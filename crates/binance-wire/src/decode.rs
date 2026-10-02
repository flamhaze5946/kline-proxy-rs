use kline_core::{Bar, Interval, NumberType, Source, Update};
use serde::{
    Deserialize, Deserializer,
    de::{self, Visitor},
};
use std::{borrow::Cow, fmt};

#[derive(Debug)]
pub enum Identity<'a> {
    Symbol(Cow<'a, str>),
    Continuous {
        pair: Cow<'a, str>,
        contract_type: Cow<'a, str>,
    },
}
#[derive(Debug)]
pub struct Parsed<'a> {
    pub identity: Identity<'a>,
    pub interval: Interval,
    pub update: Update,
}
#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("invalid JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("missing or invalid {0}")]
    Field(&'static str),
    #[error(transparent)]
    Bar(#[from] kline_core::InvalidBar),
}

// Deliberately no `flatten`, `untagged`, Value tree, or per-number String.
// Inline optional data handles combined streams without an allocation or retry parse.
#[derive(Deserialize)]
#[serde(bound(deserialize = "N: Deserialize<'de>"))]
struct Frame<'a, N> {
    #[serde(borrow, default, deserialize_with = "borrow_optional")]
    e: Option<Cow<'a, str>>,
    #[serde(borrow, default, deserialize_with = "borrow_optional")]
    s: Option<Cow<'a, str>>,
    #[serde(borrow, default, deserialize_with = "borrow_optional")]
    ps: Option<Cow<'a, str>>,
    #[serde(borrow, default, deserialize_with = "borrow_optional")]
    ct: Option<Cow<'a, str>>,
    #[serde(rename = "E", default, deserialize_with = "event_time")]
    event: Option<i64>,
    #[serde(borrow)]
    k: Option<Candle<'a, N>>,
    #[serde(borrow)]
    data: Option<Payload<'a, N>>,
}
#[derive(Deserialize)]
#[serde(bound(deserialize = "N: Deserialize<'de>"))]
struct Payload<'a, N> {
    #[serde(borrow, default, deserialize_with = "borrow_optional")]
    e: Option<Cow<'a, str>>,
    #[serde(borrow, default, deserialize_with = "borrow_optional")]
    s: Option<Cow<'a, str>>,
    #[serde(borrow, default, deserialize_with = "borrow_optional")]
    ps: Option<Cow<'a, str>>,
    #[serde(borrow, default, deserialize_with = "borrow_optional")]
    ct: Option<Cow<'a, str>>,
    #[serde(rename = "E", default, deserialize_with = "event_time")]
    event: Option<i64>,
    #[serde(borrow)]
    k: Option<Candle<'a, N>>,
}
fn borrow_optional<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Cow<'de, str>>, D::Error> {
    #[derive(Deserialize)]
    struct Text<'a>(#[serde(borrow)] Cow<'a, str>);
    Option::<Text<'de>>::deserialize(d).map(|s| s.map(|s| s.0))
}
// Event time is optional ordering/diagnostic metadata. A malformed E must not
// discard a valid candle. RawValue keeps this tolerant path allocation-free.
fn event_time<'de, D: Deserializer<'de>>(d: D) -> Result<Option<i64>, D::Error> {
    let raw = <&serde_json::value::RawValue>::deserialize(d)?.get();
    if raw.starts_with('"') {
        if raw.contains('\\') {
            Ok(serde_json::from_str::<String>(raw)
                .ok()
                .and_then(|s| s.trim().parse().ok()))
        } else {
            Ok(raw[1..raw.len() - 1].trim().parse().ok())
        }
    } else {
        Ok(raw.parse().ok())
    }
}

#[derive(Deserialize)]
#[serde(bound(deserialize = "N: Deserialize<'de>"))]
struct Candle<'a, N> {
    t: i64,
    #[serde(rename = "T")]
    close_time: i64,
    #[serde(borrow)]
    i: Cow<'a, str>,
    o: N,
    h: N,
    l: N,
    c: N,
    v: N,
    q: N,
    #[serde(rename = "V")]
    taker_base: N,
    #[serde(rename = "Q")]
    taker_quote: N,
    n: u32,
    x: bool,
}
pub(crate) struct Numeric(pub(crate) f64);
impl<'de> Deserialize<'de> for Numeric {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct NumberVisitor;
        impl Visitor<'_> for NumberVisitor {
            type Value = Numeric;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a finite decimal string")
            }
            fn visit_str<E: de::Error>(self, s: &str) -> Result<Self::Value, E> {
                let n: f64 = s.parse().map_err(E::custom)?;
                self.visit_f64(n)
            }
            fn visit_f64<E: de::Error>(self, n: f64) -> Result<Self::Value, E> {
                if n.is_finite() {
                    Ok(Numeric(n))
                } else {
                    Err(E::custom("non-finite number"))
                }
            }
            fn visit_i64<E: de::Error>(self, n: i64) -> Result<Self::Value, E> {
                self.visit_f64(n as f64)
            }
            fn visit_u64<E: de::Error>(self, n: u64) -> Result<Self::Value, E> {
                self.visit_f64(n as f64)
            }
        }
        d.deserialize_any(NumberVisitor)
    }
}

/// Control/other event frames return None. Malformed kline frames return an error.
pub fn parse(raw: &[u8], sequence: u64) -> Result<Option<Parsed<'_>>, ParseError> {
    parse_as::<Numeric>(raw, sequence, NumberType::Double)
}
pub fn parse_with_type(
    raw: &[u8],
    sequence: u64,
    mode: NumberType,
) -> Result<Option<Parsed<'_>>, ParseError> {
    match mode {
        NumberType::Double => parse(raw, sequence),
        NumberType::Float => parse_as::<crate::numbers::FloatNumber>(raw, sequence, mode),
        _ => parse_as::<crate::numbers::RawNumber<'_>>(raw, sequence, mode),
    }
}
fn parse_as<'a, N: crate::numbers::DecodeNumber<'a>>(
    raw: &'a [u8],
    sequence: u64,
    mode: NumberType,
) -> Result<Option<Parsed<'a>>, ParseError> {
    let frame: Frame<'_, N> = serde_json::from_slice(raw)?;
    let payload = if let Some(data) = frame.data {
        if frame.k.is_some() {
            return Err(ParseError::Field("ambiguous combined payload"));
        }
        data
    } else {
        Payload {
            e: frame.e,
            s: frame.s,
            ps: frame.ps,
            ct: frame.ct,
            event: frame.event,
            k: frame.k,
        }
    };
    let identity = match payload.e.as_deref() {
        Some("kline") => Identity::Symbol(payload.s.ok_or(ParseError::Field("s"))?),
        Some("continuous_kline") => {
            let contract_type = payload.ct.ok_or(ParseError::Field("ct"))?;
            if !matches!(contract_type.as_ref(), "PERPETUAL" | "TRADIFI_PERPETUAL") {
                return Ok(None);
            }
            Identity::Continuous {
                pair: payload.ps.ok_or(ParseError::Field("ps"))?,
                contract_type,
            }
        }
        _ => return Ok(None),
    };
    let k = payload.k.ok_or(ParseError::Field("k"))?;
    let interval = Interval::parse(&k.i).ok_or(ParseError::Field("interval"))?;
    let mut bar = Bar {
        open_time: k.t,
        close_time: k.close_time,
        trades: k.n,
        values: [0.; 8],

        ..Bar::default()
    };
    N::fill(
        [k.o, k.h, k.l, k.c, k.v, k.q, k.taker_base, k.taker_quote],
        &mut bar,
        mode,
    )?;
    bar.validate()?;
    Ok(Some(Parsed {
        identity,
        interval,
        update: Update {
            bar,
            closed: k.x,
            source: Source::Stream,
            event_time: payload.event,
            sequence,
        },
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    const FRAME: &str = r#"{"e":"kline","s":"BTCUSDT","E":1234,"k":{"t":0,"T":3599999,"i":"1h","o":"1.2","h":"2","l":"1","c":"1.5","v":"100","q":"150","V":"50","Q":"75","n":10,"x":true}}"#;
    #[test]
    fn raw_and_combined_are_equivalent_and_borrow_identifiers() {
        let combined = format!(r#"{{"stream":"btcusdt@kline_1h","data":{FRAME}}}"#);
        let a = parse(FRAME.as_bytes(), 8).unwrap().unwrap();
        let b = parse(combined.as_bytes(), 8).unwrap().unwrap();
        assert_eq!(a.update.bar, b.update.bar);
        assert!(matches!(
            a.identity,
            Identity::Symbol(Cow::Borrowed("BTCUSDT"))
        ));
        assert!(a.update.closed);
        assert_eq!(a.update.sequence, 8);
    }
    #[test]
    fn malformed_numbers_missing_fields_and_bad_ranges_fail() {
        for broken in [
            FRAME.replace("1.2", "NaN"),
            FRAME.replace("1.2", "1e999"),
            FRAME.replace("\"x\":true", "\"z\":true"),
            FRAME.replace("\"t\":0", "\"t\":-1"),
            FRAME.replace("\"n\":10", "\"n\":2147483648"),
        ] {
            assert!(parse(broken.as_bytes(), 1).is_err(), "{broken}");
        }
        assert!(parse(br#"{"result":null,"id":1}"#, 1).unwrap().is_none());
    }
    #[test]
    fn continuous_contracts_are_explicit() {
        let frame = FRAME.replace(
            "\"e\":\"kline\",\"s\":\"BTCUSDT\"",
            "\"e\":\"continuous_kline\",\"ps\":\"PAIR\",\"ct\":\"TRADIFI_PERPETUAL\"",
        );
        assert!(
            matches!(parse(frame.as_bytes(), 1).unwrap().unwrap().identity,
            Identity::Continuous { pair, .. } if pair == "PAIR")
        );
        assert!(
            parse(
                frame
                    .replace("TRADIFI_PERPETUAL", "CURRENT_QUARTER")
                    .as_bytes(),
                1
            )
            .unwrap()
            .is_none()
        );
    }
    #[test]
    fn invalid_event_time_does_not_drop_a_valid_candle() {
        for (raw, expected) in [
            ("null", None),
            ("\"bad\"", None),
            ("1.5", None),
            ("9223372036854775808", None),
            ("\" 123 \"", Some(123)),
            ("123", Some(123)),
            ("\"\\u0031\"", Some(1)),
        ] {
            let frame = FRAME.replace("\"E\":1234", &format!("\"E\":{raw}"));
            let parsed = parse(frame.as_bytes(), 1).unwrap().unwrap();
            assert_eq!(parsed.update.event_time, expected);
            assert!(parsed.update.closed);
        }
    }
}

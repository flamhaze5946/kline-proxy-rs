//! Borrow stream fields while decoding; materialize only the retained public values.
use super::{DECIMALS, INTEGERS};
use crate::{
    decimal,
    error::{ApiError, Result},
};
use serde::Deserialize;
use serde_json::{Map, Value, value::RawValue};
use std::borrow::Cow;

#[derive(Deserialize)]
pub(super) struct Event<'a> {
    #[serde(default, borrow)]
    data: Option<&'a RawValue>,
    #[serde(default, borrow, rename = "e")]
    pub kind: Option<Cow<'a, str>>,
    #[serde(default, borrow, rename = "s")]
    pub symbol: Option<Cow<'a, str>>,
    #[serde(default, borrow, rename = "E")]
    event_time: Option<&'a RawValue>,
    #[serde(default, borrow, rename = "T")]
    transaction_time: Option<&'a RawValue>,
    #[serde(default, borrow, rename = "p")]
    price_change: Option<&'a RawValue>,
    #[serde(default, borrow, rename = "P")]
    price_change_percent: Option<&'a RawValue>,
    #[serde(default, borrow, rename = "w")]
    weighted_average: Option<&'a RawValue>,
    #[serde(default, borrow, rename = "x")]
    previous_close: Option<&'a RawValue>,
    #[serde(default, borrow, rename = "c")]
    last_price: Option<&'a RawValue>,
    #[serde(default, borrow, rename = "Q")]
    last_quantity: Option<&'a RawValue>,
    #[serde(default, borrow, rename = "b")]
    bid_price: Option<&'a RawValue>,
    #[serde(default, borrow, rename = "B")]
    bid_quantity: Option<&'a RawValue>,
    // `a` is the ask price in ticker events and the sequence in aggTrade events.
    #[serde(default, borrow, rename = "a")]
    ask_or_sequence: Option<&'a RawValue>,
    #[serde(default, borrow, rename = "A")]
    ask_quantity: Option<&'a RawValue>,
    #[serde(default, borrow, rename = "o")]
    open_price: Option<&'a RawValue>,
    #[serde(default, borrow, rename = "h")]
    high_price: Option<&'a RawValue>,
    #[serde(default, borrow, rename = "l")]
    low_price: Option<&'a RawValue>,
    #[serde(default, borrow, rename = "v")]
    volume: Option<&'a RawValue>,
    #[serde(default, borrow, rename = "q")]
    quote_volume: Option<&'a RawValue>,
    #[serde(default, borrow, rename = "O")]
    open_time: Option<&'a RawValue>,
    #[serde(default, borrow, rename = "C")]
    close_time: Option<&'a RawValue>,
    #[serde(default, borrow, rename = "F")]
    first_id: Option<&'a RawValue>,
    #[serde(default, borrow, rename = "L")]
    last_id: Option<&'a RawValue>,
    #[serde(default, borrow, rename = "n")]
    count: Option<&'a RawValue>,
}

pub(super) struct Trade<'a> {
    pub symbol: &'a str,
    pub event_ms: i64,
    pub time: i64,
    pub sequence: i64,
    pub price: Cow<'a, str>,
}

#[derive(Deserialize)]
#[serde(transparent)]
struct Text<'a>(#[serde(borrow)] Cow<'a, str>);

impl Event<'_> {
    pub fn event_ms(&self) -> Option<i64> {
        self.event_time
            .and_then(|raw| serde_json::from_str(raw.get()).ok())
    }
    pub fn trade(&self) -> Result<Trade<'_>> {
        let integer = |raw: Option<&RawValue>, error: &'static str| {
            raw.and_then(|raw| serde_json::from_str(raw.get()).ok())
                .ok_or_else(|| ApiError::internal(error))
        };
        let price = self
            .price_change
            .ok_or_else(|| ApiError::internal("trade missing price"))?;
        let Text(price) = serde_json::from_str(price.get()).map_err(ApiError::internal)?;
        Ok(Trade {
            symbol: self
                .symbol
                .as_deref()
                .ok_or_else(|| ApiError::internal("trade missing symbol"))?,
            event_ms: integer(self.event_time, "trade missing event time")?,
            time: integer(self.transaction_time, "trade missing transaction time")?,
            sequence: integer(self.ask_or_sequence, "trade missing sequence")?,
            price,
        })
    }
    pub fn display(&self) -> Result<Value> {
        let mut out = Map::new();
        if let Some(symbol) = &self.symbol {
            out.insert("symbol".into(), symbol.as_ref().into());
        }
        let decimals = [
            self.price_change,
            self.price_change_percent,
            self.weighted_average,
            self.previous_close,
            self.last_price,
            self.last_quantity,
            self.bid_price,
            self.bid_quantity,
            self.ask_or_sequence,
            self.ask_quantity,
            self.open_price,
            self.high_price,
            self.low_price,
            self.volume,
            self.quote_volume,
        ];
        for ((name, _), raw) in DECIMALS.iter().zip(decimals) {
            if let Some(raw) = raw {
                let value = if raw.get().starts_with('"') {
                    let Text(text) = serde_json::from_str(raw.get()).map_err(ApiError::internal)?;
                    decimal(&text)?
                } else {
                    // Preserve the previous JSON number formatting for numeric inputs.
                    let value: Value =
                        serde_json::from_str(raw.get()).map_err(ApiError::internal)?;
                    decimal(&value.to_string())?
                };
                out.insert((*name).into(), value.into());
            }
        }
        let integers = [
            self.open_time,
            self.close_time,
            self.first_id,
            self.last_id,
            self.count,
        ];
        for ((name, _), raw) in INTEGERS.iter().zip(integers) {
            if let Some(raw) = raw {
                out.insert(
                    (*name).into(),
                    serde_json::from_str(raw.get()).map_err(ApiError::internal)?,
                );
            }
        }
        Ok(out.into())
    }
}

pub(super) fn decode<'a>(
    raw: &'a [u8],
    mut accept: impl FnMut(Event<'a>) -> Result<()>,
) -> Result<()> {
    fn payload<'a>(raw: &'a [u8], accept: &mut impl FnMut(Event<'a>) -> Result<()>) -> Result<()> {
        if raw.iter().find(|c| !c.is_ascii_whitespace()) == Some(&b'[') {
            let events: Vec<Event<'a>> = serde_json::from_slice(raw).map_err(ApiError::internal)?;
            for event in events {
                accept(event)?;
            }
            Ok(())
        } else {
            accept(serde_json::from_slice(raw).map_err(ApiError::internal)?)
        }
    }
    if raw.iter().find(|c| !c.is_ascii_whitespace()) == Some(&b'{') {
        let event: Event<'a> = serde_json::from_slice(raw).map_err(ApiError::internal)?;
        if let Some(data) = event.data {
            payload(data.get().as_bytes(), &mut accept)
        } else {
            accept(event)
        }
    } else {
        payload(raw, &mut accept)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn borrowed_stream_fields_match_the_previous_display_contract() {
        let raw = br#"{"e":"24hrTicker","s":"BTCUSDT","E":1789640000000,"p":"-0.000","P":"1E-8","w":"001.2500","x":null,"c":"123.45000000","Q":"0.001","b":"123.4","B":"1.00","a":"123.5","A":"2.00","o":"100","h":"130","l":"90","v":"1200.00","q":"1e5","O":10,"C":20,"F":30,"L":40,"n":50,"unused":{"x":[1,2,3]}}"#;
        let previous: Value = serde_json::from_slice(raw).unwrap();
        let expected = super::super::display(&previous, true).unwrap();
        for encoded in [
            raw.to_vec(),
            format!(
                "{{\"stream\":\"btcusdt@ticker\",\"data\":{}}}",
                std::str::from_utf8(raw).unwrap()
            )
            .into_bytes(),
            format!("[{}]", std::str::from_utf8(raw).unwrap()).into_bytes(),
            format!("{{\"data\":[{}]}}", std::str::from_utf8(raw).unwrap()).into_bytes(),
        ] {
            let mut received = vec![];
            decode(&encoded, |event| {
                received.push(event.display()?);
                Ok(())
            })
            .unwrap();
            assert_eq!(received, vec![expected.clone()]);
        }
        for raw in [
            r#"{"e":"24hrTicker","s":"B\u0054CUSDT","c":"1.2\u0030","p":1e0,"q":null,"C":7}"#,
            r#"{"e":"24hrTicker","s":"BTCUSDT","c":"-000.0100","p":1.2345678901234567}"#,
        ] {
            let previous: Value = serde_json::from_str(raw).unwrap();
            let event: Event<'_> = serde_json::from_str(raw).unwrap();
            assert_eq!(
                event.display().unwrap(),
                super::super::display(&previous, true).unwrap()
            );
        }
    }
}

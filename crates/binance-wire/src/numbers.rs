use crate::{ParseError, decode::Numeric};
use kline_core::{Bar, ExactNumbers, NumberType};
use serde::{
    Deserialize, Deserializer,
    de::{self, Visitor},
};
use std::{borrow::Cow, fmt};

pub(crate) trait DecodeNumber<'de>: Deserialize<'de> + Sized {
    fn fill(fields: [Self; 8], bar: &mut Bar, mode: NumberType) -> Result<(), ParseError>;
}
impl<'de> DecodeNumber<'de> for Numeric {
    fn fill(fields: [Self; 8], bar: &mut Bar, _: NumberType) -> Result<(), ParseError> {
        bar.values = fields.map(|n| n.0);
        Ok(())
    }
}
pub(crate) struct FloatNumber(f32);
impl<'de> Deserialize<'de> for FloatNumber {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl Visitor<'_> for V {
            type Value = FloatNumber;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a finite float")
            }
            fn visit_str<E: de::Error>(self, s: &str) -> Result<Self::Value, E> {
                let n = s.parse::<f32>().map_err(E::custom)?;
                if n.is_finite() {
                    Ok(FloatNumber(n))
                } else {
                    Err(E::custom("non-finite float"))
                }
            }
            fn visit_f64<E: de::Error>(self, n: f64) -> Result<Self::Value, E> {
                let n = n as f32;
                if n.is_finite() {
                    Ok(FloatNumber(n))
                } else {
                    Err(E::custom("non-finite float"))
                }
            }
            fn visit_i64<E: de::Error>(self, n: i64) -> Result<Self::Value, E> {
                self.visit_f64(n as f64)
            }
            fn visit_u64<E: de::Error>(self, n: u64) -> Result<Self::Value, E> {
                self.visit_f64(n as f64)
            }
        }
        d.deserialize_any(V)
    }
}
impl<'de> DecodeNumber<'de> for FloatNumber {
    fn fill(fields: [Self; 8], bar: &mut Bar, _: NumberType) -> Result<(), ParseError> {
        bar.values = fields.map(|n| f64::from(n.0));
        bar.number_type = NumberType::Float;
        Ok(())
    }
}
#[derive(Deserialize)]
pub(crate) struct RawNumber<'a>(#[serde(borrow)] &'a serde_json::value::RawValue);
impl<'a> RawNumber<'a> {
    fn text(&self) -> Result<Cow<'a, str>, ParseError> {
        let s = self.0.get();
        if s.starts_with('"') {
            if s.contains('\\') {
                return Ok(Cow::Owned(serde_json::from_str(s)?));
            }
            return Ok(Cow::Borrowed(&s[1..s.len() - 1]));
        }
        Ok(Cow::Borrowed(s))
    }
}
impl<'de> DecodeNumber<'de> for RawNumber<'de> {
    fn fill(fields: [Self; 8], bar: &mut Bar, mode: NumberType) -> Result<(), ParseError> {
        let texts: Vec<_> = fields.iter().map(|n| n.text()).collect::<Result<_, _>>()?;
        bar.set_numbers(mode, std::array::from_fn(|i| texts[i].as_ref()))?;
        Ok(())
    }
}

/// Wire representation for exact modes; primitive modes retain the stack-only formatter.
pub fn exact_display(bar: &Bar, index: usize) -> Option<Cow<'_, str>> {
    match bar.exact.as_deref()? {
        ExactNumbers::Strings(a) => Some(Cow::Borrowed(&a[index])),
        ExactNumbers::Decimals(a) => {
            Some(Cow::Owned(kline_core::decimal_to_plain_string(&a[index])))
        }
    }
}

/// A native snapshot keeps BigDecimal's scale as well as its coefficient.
pub fn stored_numbers(bar: &Bar) -> [String; 8] {
    std::array::from_fn(|i| match bar.exact.as_deref() {
        Some(ExactNumbers::Strings(a)) => a[i].clone(),
        Some(ExactNumbers::Decimals(a)) => {
            let (n, scale) = a[i].as_bigint_and_exponent();
            format!("{n}e{}", -scale)
        }
        None => bar.values[i].to_string(),
    })
}
pub fn convert_bar(mut bar: Bar, mode: NumberType) -> Result<Bar, ParseError> {
    if bar.number_type != mode {
        let raw = stored_numbers(&bar);
        bar.set_numbers(mode, std::array::from_fn(|i| raw[i].as_str()))?;
    }
    Ok(bar)
}

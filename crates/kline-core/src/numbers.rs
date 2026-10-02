//! Numeric policy is a domain concern; protocol parsers supply the original decimal lexemes.
use crate::{Bar, InvalidBar};
use bigdecimal::BigDecimal;
use num_traits::{ToPrimitive, Zero};
use std::{str::FromStr, sync::Arc};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(u8)]
pub enum NumberType {
    #[default]
    Double = 0,
    Float = 1,
    String = 2,
    BigDecimal = 3,
}
impl NumberType {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "double" => Some(Self::Double),
            "float" => Some(Self::Float),
            "string" => Some(Self::String),
            "bigDecimal" => Some(Self::BigDecimal),
            _ => None,
        }
    }
    pub fn code(self) -> &'static str {
        match self {
            Self::Double => "double",
            Self::Float => "float",
            Self::String => "string",
            Self::BigDecimal => "bigDecimal",
        }
    }
}
#[derive(Clone, Debug)]
pub enum ExactNumbers {
    Strings([String; 8]),
    Decimals([BigDecimal; 8]),
}
/// BigDecimal.toPlainString suppresses a negative scale for zero.
pub fn decimal_to_plain_string(value: &BigDecimal) -> String {
    if value.is_zero() && value.as_bigint_and_exponent().1 <= 0 {
        "0".into()
    } else {
        value.to_plain_string()
    }
}
impl Bar {
    pub fn set_numbers(&mut self, mode: NumberType, raw: [&str; 8]) -> Result<(), InvalidBar> {
        if raw.iter().any(|s| s.len() > 4096) {
            return Err(InvalidBar::Number);
        }
        let mut values = [0.; 8];
        let exact = match mode {
            NumberType::Double | NumberType::Float => {
                for (out, input) in values.iter_mut().zip(raw) {
                    *out = if mode == NumberType::Float {
                        f64::from(input.parse::<f32>().map_err(|_| InvalidBar::Number)?)
                    } else {
                        input.parse().map_err(|_| InvalidBar::Number)?
                    };
                    if !out.is_finite() {
                        return Err(InvalidBar::Number);
                    }
                }
                None
            }
            NumberType::String => {
                for (out, input) in values.iter_mut().zip(raw) {
                    *out = input.parse().unwrap_or(0.);
                }
                Some(Arc::new(ExactNumbers::Strings(raw.map(str::to_owned))))
            }
            NumberType::BigDecimal => {
                let mut decimals = Vec::with_capacity(8);
                for (out, input) in values.iter_mut().zip(raw) {
                    let value = BigDecimal::from_str(input).map_err(|_| InvalidBar::Number)?;
                    if value.as_bigint_and_exponent().1.unsigned_abs() > 4096
                        || value.as_bigint_and_exponent().0.to_string().len() > 1024
                    {
                        return Err(InvalidBar::Number);
                    }
                    *out = value.to_f64().unwrap_or(0.);
                    decimals.push(value);
                }
                Some(Arc::new(ExactNumbers::Decimals(
                    decimals.try_into().unwrap(),
                )))
            }
        };
        self.values = values;
        self.exact = exact;
        self.number_type = mode;
        Ok(())
    }
    pub(crate) fn valid_numbers(&self) -> bool {
        match (self.number_type, self.exact.as_deref()) {
            (NumberType::Double | NumberType::Float, None) => {
                self.values.iter().all(|v| v.is_finite())
            }
            (NumberType::String, Some(ExactNumbers::Strings(_))) => true,
            (NumberType::BigDecimal, Some(ExactNumbers::Decimals(_))) => true,
            _ => false,
        }
    }
    pub(crate) fn numbers_equal(&self, other: &Self) -> bool {
        if self.number_type != other.number_type {
            return false;
        }
        match (self.exact.as_deref(), other.exact.as_deref()) {
            (Some(ExactNumbers::Strings(a)), Some(ExactNumbers::Strings(b))) => a == b,
            (Some(ExactNumbers::Decimals(a)), Some(ExactNumbers::Decimals(b))) => {
                a.iter().zip(b).all(|(a, b)| {
                    // Java BigDecimal.equals includes scale; Rust's numerical equality does not.
                    a.as_bigint_and_exponent() == b.as_bigint_and_exponent()
                })
            }
            (None, None) => self
                .values
                .iter()
                .zip(other.values)
                .all(|(a, b)| a.to_bits() == b.to_bits()),
            _ => false,
        }
    }
    /// Java synthetic fills copy the previous close and use an exact zero for volumes.
    pub fn synthetic_after(&self, open_time: i64, period: i64) -> Self {
        let exact = self.exact.as_deref().map(|numbers| {
            Arc::new(match numbers {
                ExactNumbers::Strings(a) => ExactNumbers::Strings(std::array::from_fn(|i| {
                    if i < 4 { a[3].clone() } else { "0".into() }
                })),
                ExactNumbers::Decimals(a) => ExactNumbers::Decimals(std::array::from_fn(|i| {
                    if i < 4 {
                        a[3].clone()
                    } else {
                        BigDecimal::from(0)
                    }
                })),
            })
        });
        Self {
            open_time,
            close_time: open_time + period - 1,
            trades: 0,
            values: [
                self.values[3],
                self.values[3],
                self.values[3],
                self.values[3],
                0.,
                0.,
                0.,
                0.,
            ],
            number_type: self.number_type,
            exact,
        }
    }
}

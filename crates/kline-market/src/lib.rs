mod cms_shape;
pub mod config;
pub mod cpu;
pub mod error;
pub mod funding;
pub mod metadata;
pub mod transport;
/// Total time a market query (ticker, or exchange metadata on its first load) may take, counted
/// from the request's arrival so that HTTP admission queueing is part of it.
pub const QUERY_BUDGET: std::time::Duration = std::time::Duration::from_secs(5);
/// BigDecimal.toPlainString compatibility for non-Kline decimal wire fields.
pub fn decimal(raw: &str) -> error::Result<String> {
    use std::str::FromStr;
    if raw.len() > 1024 {
        return Err(error::ApiError::internal("decimal exceeds size limit"));
    }
    // Binance normally sends already canonical plain decimals. Preserve their scale
    // without constructing a BigInt and formatting the same digits again.
    if canonical_plain_decimal(raw) {
        return Ok(raw.to_owned());
    }
    let value = bigdecimal::BigDecimal::from_str(raw).map_err(error::ApiError::internal)?;
    let (_, scale) = value.as_bigint_and_exponent();
    if scale.unsigned_abs() > 4096 {
        return Err(error::ApiError::internal("decimal exponent exceeds limit"));
    }
    Ok(kline_core::decimal_to_plain_string(&value))
}
fn canonical_plain_decimal(raw: &str) -> bool {
    let digits = raw.strip_prefix('-').unwrap_or(raw).as_bytes();
    if digits.is_empty() || !digits[0].is_ascii_digit() {
        return false;
    }
    let mut point = false;
    let mut nonzero = false;
    for (i, &c) in digits.iter().enumerate() {
        if c == b'.' {
            if point || i + 1 == digits.len() {
                return false;
            }
            point = true;
        } else if c.is_ascii_digit() {
            if i == 1 && !point && digits[0] == b'0' {
                return false;
            }
            nonzero |= c != b'0';
        } else {
            return false;
        }
    }
    !raw.starts_with('-') || nonzero
}
pub mod http;
pub mod klines;
mod metadata_shape;
pub mod statistics;
pub mod ticker;
pub mod vision;

#[cfg(test)]
mod decimal_tests {
    use super::*;
    use std::str::FromStr;
    #[test]
    fn fast_plain_decimals_preserve_bigdecimal_formatting_and_validation() {
        let reference = |raw: &str| {
            bigdecimal::BigDecimal::from_str(raw).map(|v| kline_core::decimal_to_plain_string(&v))
        };
        for raw in [
            "0",
            "0.00",
            "-0",
            "-0.000",
            "+001.20",
            "001.200",
            ".25",
            "1.",
            "-123.45000",
            "0.0000000012300",
            "12345678901234567890.12345678901234567890",
            "1E-8",
            "0E+5",
            "-0e-5",
            "1e+20",
            "",
            "-",
            "--1",
            "1..0",
            "NaN",
            "inf",
            "1.2x",
            " 1.0",
        ] {
            match reference(raw) {
                Ok(value) => assert_eq!(decimal(raw).unwrap(), value, "{raw}"),
                Err(_) => assert!(decimal(raw).is_err(), "{raw}"),
            }
        }
        for integer in 0..100 {
            for fraction in ["", ".0", ".0001", ".01000", ".99999999999999999"] {
                for sign in ["", "-", "+"] {
                    let raw = format!("{sign}{integer}{fraction}");
                    assert_eq!(decimal(&raw).unwrap(), reference(&raw).unwrap(), "{raw}");
                }
            }
        }
        assert!(decimal(&"1".repeat(1025)).is_err());
        assert!(decimal("1e4097").is_err());
    }
}

mod pacing;

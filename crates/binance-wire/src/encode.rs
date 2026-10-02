use arrayvec::ArrayString;
use kline_core::Bar;
use serde::{
    Serialize, Serializer,
    ser::{Error, SerializeTuple},
};
use std::fmt::Write;

/// Eight fractional places, trailing zero removal, signed zero preserved.
/// Capacity covers every finite f64 in fixed notation (including f64::MAX).
pub fn display_number(value: f64) -> Result<ArrayString<384>, &'static str> {
    if !value.is_finite() {
        return Err("non-finite number");
    }
    let negative = value.is_sign_negative();
    let absolute = value.abs();
    let mut result = ArrayString::new();
    if negative {
        result.push('-');
    }
    // Preserve the deployed JDK 21 formatter's underflow tie at 5e-9.
    if absolute <= 0.000000005 {
        result.push('0');
        return Ok(result);
    }
    let binary_exponent = ((absolute.to_bits() >> 52) & 0x7ff) as i32 - 1023;
    if absolute.fract() == 0.0 && binary_exponent <= 62 {
        let mut integer = absolute as u64;
        if binary_exponent > 53 {
            let half_ulp = 1_u64 << (binary_exponent - 54);
            let insignificant = half_ulp.ilog10();
            let power = 10_u64.pow(insignificant);
            integer =
                (integer / power + u64::from(integer % power >= power / 2 && power > 1)) * power;
        }
        write!(&mut result, "{integer}").map_err(|_| "number exceeds buffer")?;
        return Ok(result);
    }
    let mut buffer = ryu::Buffer::new();
    let shortest = buffer.format_finite(absolute);
    let legacy;
    let shortest = if binary_exponent > 62 {
        legacy = legacy_large(absolute, binary_exponent, shortest)?;
        legacy.as_str()
    } else {
        shortest
    };
    let (mantissa, exponent) = shortest.split_once('e').map_or((shortest, 0), |(m, e)| {
        (m, e.parse::<i32>().expect("ryu exponent"))
    });
    let dot = mantissa.find('.').unwrap_or(mantissa.len()) as i32;
    let mut digits = ArrayString::<24>::new();
    for c in mantissa.chars().filter(|c| *c != '.') {
        digits.push(c);
    }
    let decimal_at = dot + exponent;
    let fractional_digits = digits.trim_end_matches('0').len() as i32 - decimal_at;
    if fractional_digits > 8 {
        // When precision is actually reduced, use binary half-even rounding.
        result.clear();
        write!(&mut result, "{value:.8}").map_err(|_| "number exceeds buffer")?;
        let trimmed = result.trim_end_matches('0').trim_end_matches('.').len();
        result.truncate(trimmed);
        return Ok(result);
    }
    if decimal_at <= 0 {
        result.push_str("0.");
        for _ in decimal_at..0 {
            result.push('0');
        }
        result.push_str(&digits);
    } else {
        for i in 0..decimal_at as usize {
            result.push(digits.as_bytes().get(i).copied().unwrap_or(b'0') as char);
        }
        if (decimal_at as usize) < digits.len() {
            result.push('.');
            result.push_str(&digits[decimal_at as usize..]);
        }
    }
    if result.contains('.') {
        let trimmed = result.trim_end_matches('0').trim_end_matches('.').len();
        result.truncate(trimmed);
    }
    Ok(result)
}

// Legacy FloatingDecimal uses open (not inclusive) rounding boundaries, and
// symmetric margins even at powers of two. Ryū can select a shorter digit
// sequence on those boundaries. Large integers need a rare compatibility path;
// ordinary exchange prices/volumes stay on the allocation-free fast path above.
fn legacy_large(
    value: f64,
    exponent: i32,
    shortest: &str,
) -> Result<ArrayString<384>, &'static str> {
    let mut exact = ArrayString::<384>::new();
    write!(&mut exact, "{value:.0}").map_err(|_| "number exceeds buffer")?;
    let power_of_two = value.to_bits() & ((1_u64 << 52) - 1) == 0;
    let margin_exp = exponent - if power_of_two { 54 } else { 53 };
    let margin_value = f64::from_bits(((margin_exp + 1023) as u64) << 52);
    let mut margin = ArrayString::<384>::new();
    write!(&mut margin, "{margin_value:.0}").map_err(|_| "number exceeds buffer")?;
    let mut candidate = ArrayString::<384>::new();
    candidate.push_str(shortest);
    let significant = shortest
        .split('e')
        .next()
        .unwrap()
        .bytes()
        .filter(u8::is_ascii_digit)
        .count();
    for precision in significant..=18 {
        if within_margin(&exact, &candidate, &margin) {
            return Ok(candidate);
        }
        candidate.clear();
        write!(&mut candidate, "{value:.precision$e}").map_err(|_| "number exceeds buffer")?;
    }
    Ok(candidate)
}
fn within_margin(exact: &str, candidate: &str, margin: &str) -> bool {
    let (mantissa, exponent) = candidate.split_once('e').map_or((candidate, 0), |(m, e)| {
        (m, e.parse::<usize>().expect("numeric exponent"))
    });
    let point = mantissa.find('.').unwrap_or(mantissa.len());
    let mut decimal = ArrayString::<384>::new();
    for c in mantissa.chars().filter(|c| *c != '.') {
        decimal.push(c);
    }
    while decimal.len() < point + exponent {
        decimal.push('0');
    }
    let a = exact.as_bytes();
    let b = decimal.as_bytes();
    let (large, small) = if (a.len(), a) >= (b.len(), b) {
        (a, b)
    } else {
        (b, a)
    };
    let mut difference = [b'0'; 384];
    let mut borrow = 0_i16;
    for i in 0..large.len() {
        let x = i16::from(large[large.len() - 1 - i] - b'0');
        let y = if i < small.len() {
            i16::from(small[small.len() - 1 - i] - b'0')
        } else {
            0
        };
        let d = x - y - borrow;
        borrow = i16::from(d < 0);
        difference[383 - i] = (d.rem_euclid(10) as u8) + b'0';
    }
    let first = difference.iter().position(|c| *c != b'0').unwrap_or(383);
    let d = &difference[first..];
    (d.len(), d) < (margin.len(), margin.as_bytes())
}

pub struct DisplayBar(pub Bar);
impl Serialize for DisplayBar {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let b = &self.0;
        let mut tuple = serializer.serialize_tuple(12)?;
        tuple.serialize_element(&b.open_time)?;
        for i in 0..5 {
            serialize_number::<S>(&mut tuple, b, i)?;
        }
        tuple.serialize_element(&b.close_time)?;
        serialize_number::<S>(&mut tuple, b, 5)?;
        tuple.serialize_element(&b.trades)?;
        for i in 6..8 {
            serialize_number::<S>(&mut tuple, b, i)?;
        }
        tuple.serialize_element("0")?;
        tuple.end()
    }
}
fn serialize_number<S: Serializer>(
    tuple: &mut S::SerializeTuple,
    bar: &Bar,
    i: usize,
) -> Result<(), S::Error> {
    if let Some(value) = crate::exact_display(bar, i) {
        tuple.serialize_element(value.as_ref())
    } else {
        tuple.serialize_element(
            display_number(bar.values[i])
                .map_err(S::Error::custom)?
                .as_str(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn formatting_boundaries() {
        for (value, expected) in [
            (0., "0"),
            (-0., "-0"),
            (100., "100"),
            (0.5, "0.5"),
            (0.000000001, "0"),
            (1.234567891, "1.23456789"),
            (1.234567899, "1.2345679"),
        ] {
            assert_eq!(display_number(value).unwrap().as_str(), expected);
        }
        assert!(display_number(f64::MAX).is_ok());
        assert!(display_number(f64::NAN).is_err());
    }
    #[test]
    fn java21_legacy_decimal_boundaries() {
        for (value, expected) in [
            (5e-9, "0"),
            (1.5e-8, "0.00000001"),
            (650556376.779132, "650556376.779132"),
            (1000000000000000128_f64, "1000000000000000130"),
            (1e23, "99999999999999990000000"),
            (2_f64.powi(69), "590295810358705650000"),
            (2_f64.powi(119), "664613997892457900000000000000000000"),
        ] {
            assert_eq!(display_number(value).unwrap().as_str(), expected);
        }
    }
    #[test]
    fn binance_array_positions_and_types() {
        let bar = Bar {
            open_time: 0,
            close_time: 999,
            trades: 9,
            values: [1., 2., 3., 4., 5., 6., 7., 8.],

            ..Bar::default()
        };
        assert_eq!(
            serde_json::to_string(&DisplayBar(bar)).unwrap(),
            r#"[0,"1","2","3","4","5",999,"6",9,"7","8","0"]"#
        );
    }
}

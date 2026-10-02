use crate::{ParseError, decode::Numeric};
use kline_core::{Bar, NumberType};
use serde::{Deserialize, de::IgnoredAny};

#[derive(Deserialize)]
struct Row<N>(i64, N, N, N, N, N, i64, N, u32, N, N, IgnoredAny);

/// Decode the REST array directly; no DOM and no decimal-string allocations.
pub fn parse_rest_bars(raw: &[u8]) -> Result<Vec<Bar>, ParseError> {
    parse_as::<Numeric>(raw, NumberType::Double)
}
pub fn parse_rest_bars_with_type(raw: &[u8], mode: NumberType) -> Result<Vec<Bar>, ParseError> {
    match mode {
        NumberType::Double => parse_rest_bars(raw),
        NumberType::Float => parse_as::<crate::numbers::FloatNumber>(raw, mode),
        _ => parse_as::<crate::numbers::RawNumber<'_>>(raw, mode),
    }
}
fn parse_as<'a, N: crate::numbers::DecodeNumber<'a>>(
    raw: &'a [u8],
    mode: NumberType,
) -> Result<Vec<Bar>, ParseError> {
    let rows: Vec<Row<N>> = serde_json::from_slice(raw)?;
    rows.into_iter()
        .map(|r| {
            let mut bar = Bar {
                open_time: r.0,
                close_time: r.6,
                trades: r.8,
                values: [0.; 8],

                ..Bar::default()
            };
            N::fill([r.1, r.2, r.3, r.4, r.5, r.7, r.9, r.10], &mut bar, mode)?;
            bar.validate()?;
            Ok(bar)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rest_layout_preserves_values_and_rejects_partial_or_invalid_rows() {
        let raw = br#"[[0,"1","2","0.5","1.5","100",59999,"150",7,"50","75","0"]]"#;
        let bar = parse_rest_bars(raw).unwrap().remove(0);
        assert_eq!(bar.values, [1., 2., 0.5, 1.5, 100., 150., 50., 75.]);
        assert_eq!(bar.trades, 7);
        assert_eq!(bar.close_time, 59999);
        assert!(parse_rest_bars(br#"[[0,"1"]]"#).is_err());
        assert!(
            parse_rest_bars(
                &String::from_utf8_lossy(raw)
                    .replace("\"1\"", "\"NaN\"")
                    .into_bytes()
            )
            .is_err()
        );
        assert!(parse_rest_bars(br#"{"code":-1121,"msg":"Invalid symbol"}"#).is_err());
    }
}

//! Binance protocol adapter; borrowed identifiers and direct numeric decoding.
mod decode;
mod encode;
mod numbers;
mod rest;
pub use decode::{Identity, ParseError, Parsed, parse, parse_with_type};
pub use encode::{DisplayBar, display_number};
pub use numbers::{convert_bar, exact_display, stored_numbers};
pub use rest::{parse_rest_bars, parse_rest_bars_with_type};

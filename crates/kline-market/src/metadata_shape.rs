//! Fields mirror the deployed Java metadata models; unknown upstream fields are omitted.
use crate::error::{ApiError, Result};
use num_traits::ToPrimitive;
use serde_json::{Map, Value};
pub fn shape(class: &str, raw: &Value) -> Result<Value> {
    if raw.is_null() {
        return Ok(Value::Null);
    }
    match class {
        "String" => {
            return match raw {
                Value::String(_) => Ok(raw.clone()),
                Value::Number(_) | Value::Bool(_) => Ok(Value::String(raw.to_string())),
                _ => Err(ApiError::upstream_io("invalid metadata string")),
            };
        }
        "Integer" => return integer(raw, i32::MIN as i64, i32::MAX as i64),
        "Long" => return integer(raw, i64::MIN, i64::MAX),
        "Boolean" => return boolean(raw),
        "BigDecimal" => return decimal(raw, false),
        "DecimalString" => return decimal(raw, true),
        _ => {}
    }
    let (parent, fields): (&str, &[(&str, &str)]) = match class {
        "BinanceExchange" => (
            "",
            &[
                ("timezone", "String"),
                ("serverTime", "Long"),
                ("rateLimits", "List<BinanceRateLimit>"),
            ],
        ),
        "BinanceFutureExchange" => (
            "BinanceExchange",
            &[
                ("futuresType", "String"),
                ("exchangeFilters", "List<BinanceFutureExchangeFilter>"),
                ("assets", "List<BinanceFutureAsset>"),
                ("symbols", "List<BinanceFutureSymbol>"),
            ],
        ),
        "BinanceSpotExchange" => (
            "BinanceExchange",
            &[
                ("exchangeFilters", "List<BinanceSpotExchangeFilter>"),
                ("symbols", "List<BinanceSpotSymbol>"),
            ],
        ),
        "BinanceFutureSymbol" => (
            "",
            &[
                ("symbol", "String"),
                ("pair", "String"),
                ("contractType", "String"),
                ("deliveryDate", "Long"),
                ("onboardDate", "Long"),
                ("status", "String"),
                ("maintMarginPercent", "BigDecimal"),
                ("requiredMarginPercent", "BigDecimal"),
                ("baseAsset", "String"),
                ("quoteAsset", "String"),
                ("marginAsset", "String"),
                ("pricePrecision", "Integer"),
                ("quantityPrecision", "Integer"),
                ("baseAssetPrecision", "Integer"),
                ("quotePrecision", "Integer"),
                ("underlyingType", "String"),
                ("underlyingSubType", "List<String>"),
                ("settlePlan", "Integer"),
                ("triggerProtect", "BigDecimal"),
                ("filters", "List<BinanceFutureSymbolFilter>"),
                ("orderTypes", "List<String>"),
                ("timeInForce", "List<String>"),
                ("liquidationFee", "BigDecimal"),
                ("marketTakeBound", "BigDecimal"),
                ("maxMoveOrderLimit", "Long"),
            ],
        ),
        "BinanceSpotSymbol" => (
            "",
            &[
                ("symbol", "String"),
                ("status", "String"),
                ("baseAsset", "String"),
                ("baseAssetPrecision", "Integer"),
                ("quoteAsset", "String"),
                ("quoteAssetPrecision", "Integer"),
                ("quotePrecision", "Integer"),
                ("baseCommissionPrecision", "Integer"),
                ("quoteCommissionPrecision", "Integer"),
                ("orderTypes", "List<String>"),
                ("icebergAllowed", "Boolean"),
                ("ocoAllowed", "Boolean"),
                ("quoteOrderQtyMarketAllowed", "Boolean"),
                ("allowTrailingStop", "Boolean"),
                ("isSpotTradingAllowed", "Boolean"),
                ("isMarginTradingAllowed", "Boolean"),
                ("cancelReplaceAllowed", "Boolean"),
                ("filters", "List<BinanceSpotSymbolFilter>"),
                ("permissions", "List<String>"),
                ("defaultSelfTradePreventionMode", "String"),
                ("allowedSelfTradePreventionModes", "List<String>"),
            ],
        ),
        "BinanceSymbolFilter" => (
            "",
            &[
                ("filterType", "String"),
                ("minPrice", "DecimalString"),
                ("maxPrice", "DecimalString"),
                ("tickSize", "DecimalString"),
                ("minQty", "DecimalString"),
                ("maxQty", "DecimalString"),
                ("stepSize", "DecimalString"),
                ("limit", "Long"),
                ("multiplierUp", "DecimalString"),
                ("multiplierDown", "DecimalString"),
            ],
        ),
        "BinanceFutureSymbolFilter" => (
            "BinanceSymbolFilter",
            &[
                ("notional", "DecimalString"),
                ("multiplierDecimal", "Integer"),
            ],
        ),
        "BinanceSpotSymbolFilter" => (
            "BinanceSymbolFilter",
            &[
                ("avgPriceMins", "Long"),
                ("bidMultiplierUp", "DecimalString"),
                ("bidMultiplierDown", "DecimalString"),
                ("askMultiplierUp", "DecimalString"),
                ("askMultiplierDown", "DecimalString"),
                ("minNotional", "DecimalString"),
                ("maxNotional", "DecimalString"),
                ("applyToMarket", "Boolean"),
                ("applyMinToMarket", "Boolean"),
                ("applyMaxToMarket", "Boolean"),
                ("maxNumOrders", "Long"),
                ("maxNumAlgoOrders", "Long"),
                ("maxNumIcebergOrders", "Long"),
                ("maxPosition", "DecimalString"),
                ("minTrailingAboveDelta", "DecimalString"),
                ("maxTrailingAboveDelta", "DecimalString"),
                ("minTrailingBelowDelta", "DecimalString"),
                ("maxTrailingBelowDelta", "DecimalString"),
            ],
        ),
        "BinanceRateLimit" => (
            "",
            &[
                ("rateLimitType", "String"),
                ("interval", "String"),
                ("intervalNum", "Long"),
                ("limit", "Long"),
            ],
        ),
        "BinanceFutureExchangeFilter" => ("", &[]),
        "BinanceSpotExchangeFilter" => (
            "",
            &[
                ("filterType", "String"),
                ("maxNumOrders", "Long"),
                ("maxNumAlgoOrders", "Long"),
                ("maxNumIcebergOrders", "Long"),
            ],
        ),
        "BinanceFutureAsset" => (
            "",
            &[
                ("asset", "String"),
                ("marginAvailable", "Boolean"),
                ("autoAssetExchange", "BigDecimal"),
            ],
        ),
        _ => return Ok(raw.clone()),
    };
    if !raw.is_object() {
        return Err(ApiError::upstream_io("invalid metadata object"));
    }
    let mut out = if parent.is_empty() {
        Map::new()
    } else {
        shape(parent, raw)?.as_object().cloned().unwrap_or_default()
    };
    for (key, ty) in fields {
        let Some(value) = raw.get(*key).filter(|v| !v.is_null()) else {
            continue;
        };
        let value = if let Some(child) = ty.strip_prefix("List<").and_then(|s| s.strip_suffix('>'))
        {
            Value::Array(
                value
                    .as_array()
                    .ok_or_else(|| ApiError::upstream_io("metadata list shape"))?
                    .iter()
                    .map(|v| shape(child, v))
                    .collect::<Result<_>>()?,
            )
        } else {
            shape(ty, value)?
        };
        if !value.is_null() {
            out.insert((*key).into(), value);
        }
    }
    Ok(Value::Object(out))
}

fn java_trim(raw: &str) -> &str {
    raw.trim_matches(|c| c <= '\u{20}')
}

fn decimal(raw: &Value, as_string: bool) -> Result<Value> {
    let text = match raw {
        Value::String(raw) => {
            let raw = java_trim(raw);
            if raw.is_empty() || raw == "null" {
                return Ok(Value::Null);
            }
            raw.to_owned()
        }
        Value::Number(raw) => raw.to_string(),
        _ => return Err(ApiError::upstream_io("invalid metadata decimal")),
    };
    let decimal = crate::decimal(&text).map_err(ApiError::upstream_io)?;
    if as_string {
        Ok(Value::String(decimal))
    } else {
        serde_json::from_str(&decimal).map_err(ApiError::upstream_io)
    }
}

fn integer(raw: &Value, min: i64, max: i64) -> Result<Value> {
    let invalid = || ApiError::upstream_io("invalid metadata integer");
    let number = match raw {
        Value::String(raw) => {
            let raw = java_trim(raw);
            if raw.is_empty() || raw == "null" {
                return Ok(Value::Null);
            }
            raw.parse::<i64>().map_err(|_| invalid())?
        }
        Value::Number(raw) => {
            if let Some(value) = raw.as_i64() {
                value
            } else {
                // Jackson accepts fractional JSON numbers for integer DTO fields,
                // truncating toward zero only after checking the target range.
                let value = raw
                    .to_string()
                    .parse::<bigdecimal::BigDecimal>()
                    .map_err(|_| invalid())?;
                if value < min || value > max {
                    return Err(invalid());
                }
                value.to_i64().ok_or_else(invalid)?
            }
        }
        _ => return Err(invalid()),
    };
    if !(min..=max).contains(&number) {
        return Err(invalid());
    }
    Ok(number.into())
}

fn boolean(raw: &Value) -> Result<Value> {
    match raw {
        Value::Bool(_) => Ok(raw.clone()),
        Value::Number(number) if number.is_i64() || number.is_u64() => {
            Ok((number.as_i64() != Some(0)).into())
        }
        Value::String(raw) => match java_trim(raw) {
            "" | "null" => Ok(Value::Null),
            "true" | "True" | "TRUE" => Ok(true.into()),
            "false" | "False" | "FALSE" => Ok(false.into()),
            _ => Err(ApiError::upstream_io("invalid metadata boolean")),
        },
        _ => Err(ApiError::upstream_io("invalid metadata boolean")),
    }
}

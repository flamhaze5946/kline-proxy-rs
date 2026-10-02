//! The Java CMS DTO schema, including scalar coercions and nullable list rows.
use crate::error::{ApiError, Result};
use serde_json::{Map, Value};

pub(super) fn response(raw: &Value, articles: bool) -> Result<Value> {
    shape(
        if articles {
            "ResponseArticles"
        } else {
            "ResponseCatalogs"
        },
        raw,
    )
}
fn shape(kind: &str, raw: &Value) -> Result<Value> {
    if raw.is_null() || kind == "Object" {
        return Ok(raw.clone());
    }
    if let Some(item) = kind.strip_prefix("List:") {
        return raw
            .as_array()
            .ok_or_else(|| ApiError::upstream_io("Expected CMS list"))?
            .iter()
            .map(|row| shape(item, row))
            .collect::<Result<Vec<_>>>()
            .map(Value::Array);
    }
    let fields: &[(&str, &str)] = match kind {
        "ResponseArticles" => &[
            ("code", "String"),
            ("message", "String"),
            ("messageDetail", "String"),
            ("success", "Boolean"),
            ("data", "Articles"),
        ],
        "ResponseCatalogs" => &[
            ("code", "String"),
            ("message", "String"),
            ("messageDetail", "String"),
            ("success", "Boolean"),
            ("data", "Catalogs"),
        ],
        "Articles" => &[("total", "Long"), ("articles", "List:Article")],
        "Catalogs" => &[("catalogs", "List:Catalog")],
        "Article" => &[
            ("id", "Long"),
            ("code", "String"),
            ("title", "String"),
            ("imageLink", "Object"),
            ("shortLink", "Object"),
            ("body", "Object"),
            ("type", "Object"),
            ("catalogId", "Object"),
            ("catalogName", "Object"),
            ("publishDate", "Object"),
            ("footer", "Object"),
            ("releaseDate", "Long"),
        ],
        "Catalog" => &[
            ("catalogId", "Long"),
            ("parentCatalogId", "Long"),
            ("icon", "String"),
            ("catalogName", "String"),
            ("description", "String"),
            ("catalogType", "Integer"),
            ("total", "Long"),
            ("articles", "List:Article"),
        ],
        primitive => {
            return crate::metadata_shape::shape(primitive, raw).map_err(ApiError::upstream_io);
        }
    };
    let raw = raw
        .as_object()
        .ok_or_else(|| ApiError::upstream_io("Expected CMS object"))?;
    let mut out = Map::new();
    for (field, kind) in fields {
        if let Some(value) = raw.get(*field).filter(|v| !v.is_null()) {
            let value = shape(kind, value)?;
            if !value.is_null() {
                out.insert((*field).into(), value);
            }
        }
    }
    Ok(out.into())
}

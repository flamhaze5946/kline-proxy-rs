//! Spring's message-converter behavior for JSON and String controllers.
//! The normal UTF-8 path only changes a header; alternate encodings stream.
use super::{Media, accepted, generic_json, html, suppress_head};
use axum::{
    body::{Body, Bytes},
    extract::Request,
    http::{HeaderValue, Method, StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use futures_util::StreamExt;

/// Status Spring uses if its exception advice cannot write the requested media.
/// Binding errors normally render as Java's 500/-1000 JSON, but their underlying
/// framework error is 400 (or 415 for an unsupported request Content-Type).
#[derive(Clone, Copy)]
pub struct NegotiationFallback(pub StatusCode);

#[derive(Clone, Copy)]
struct FixedContentType;

/// Java's cached all-market ticker ResponseEntity has an explicit Content-Type.
/// Like PNG responses, it is independent of Accept, including malformed Accept.
pub fn fixed_content_type(mut response: Response) -> Response {
    response.extensions_mut().insert(FixedContentType);
    response
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Charset {
    Utf8,
    Utf16,
    Utf16Be,
    Utf16Le,
    Utf32Be,
    Utf32Le,
    Latin1,
    Ascii,
}
impl Charset {
    pub(super) fn parse(value: &str) -> Option<Self> {
        if value.eq_ignore_ascii_case("utf-8") || value.eq_ignore_ascii_case("utf8") {
            Some(Self::Utf8)
        } else if value.eq_ignore_ascii_case("utf-16") || value.eq_ignore_ascii_case("utf16") {
            Some(Self::Utf16)
        } else if value.eq_ignore_ascii_case("utf-16be")
            || value.eq_ignore_ascii_case("unicodebigunmarked")
        {
            Some(Self::Utf16Be)
        } else if value.eq_ignore_ascii_case("utf-16le")
            || value.eq_ignore_ascii_case("unicodelittleunmarked")
        {
            Some(Self::Utf16Le)
        } else if value.eq_ignore_ascii_case("utf-32be") || value.eq_ignore_ascii_case("utf-32") {
            Some(Self::Utf32Be)
        } else if value.eq_ignore_ascii_case("utf-32le") {
            Some(Self::Utf32Le)
        } else if value.eq_ignore_ascii_case("iso-8859-1")
            || value.eq_ignore_ascii_case("iso8859-1")
            || value.eq_ignore_ascii_case("latin1")
        {
            Some(Self::Latin1)
        } else if value.eq_ignore_ascii_case("us-ascii") || value.eq_ignore_ascii_case("ascii") {
            Some(Self::Ascii)
        } else {
            None
        }
    }
    pub(super) fn json_supported(self) -> bool {
        !matches!(self, Self::Utf16 | Self::Latin1)
    }
    pub(super) fn json_encoding(self) -> Self {
        // Jackson maps US-ASCII to its UTF-8 writer, preserving its wire behavior.
        if self == Self::Ascii {
            Self::Utf8
        } else {
            self
        }
    }
}
pub(super) fn charset(content_type: &str) -> Charset {
    content_type
        .split(';')
        .filter_map(|p| p.trim().split_once('='))
        .find(|(name, _)| name.trim().eq_ignore_ascii_case("charset"))
        .and_then(|(_, value)| Charset::parse(value.trim().trim_matches('"')))
        .unwrap_or(Charset::Utf8)
}
pub(super) fn encode_response(mut response: Response, encoding: Charset) -> Response {
    if encoding == Charset::Utf8 {
        return response;
    }
    response.headers_mut().remove(header::CONTENT_LENGTH);
    let body = std::mem::replace(response.body_mut(), Body::empty());
    let stream = futures_util::stream::try_unfold(
        (body.into_data_stream(), Vec::<u8>::new(), true),
        move |(mut source, mut pending, first)| async move {
            let Some(chunk) = source.next().await else {
                return if pending.is_empty() {
                    Ok(None)
                } else {
                    Err(std::io::Error::other("incomplete UTF-8 response"))
                };
            };
            pending.extend_from_slice(&chunk.map_err(std::io::Error::other)?);
            let valid_len = match std::str::from_utf8(&pending) {
                Ok(_) => pending.len(),
                Err(error) if error.error_len().is_none() => error.valid_up_to(),
                Err(_) => return Err(std::io::Error::other("invalid UTF-8 response")),
            };
            let text = std::str::from_utf8(&pending[..valid_len]).map_err(std::io::Error::other)?;
            let mut bytes = Vec::with_capacity(text.len().saturating_mul(2));
            if first && encoding == Charset::Utf16 {
                bytes.extend_from_slice(&[0xfe, 0xff]);
            }
            match encoding {
                Charset::Utf8 => bytes.extend_from_slice(text.as_bytes()),
                Charset::Utf16 | Charset::Utf16Be | Charset::Utf16Le => {
                    for unit in text.encode_utf16() {
                        bytes.extend_from_slice(&if encoding == Charset::Utf16Le {
                            unit.to_le_bytes()
                        } else {
                            unit.to_be_bytes()
                        });
                    }
                }
                Charset::Utf32Be | Charset::Utf32Le => {
                    for ch in text.chars() {
                        bytes.extend_from_slice(&if encoding == Charset::Utf32Le {
                            (ch as u32).to_le_bytes()
                        } else {
                            (ch as u32).to_be_bytes()
                        });
                    }
                }
                Charset::Latin1 | Charset::Ascii => {
                    let max = if encoding == Charset::Latin1 {
                        255
                    } else {
                        127
                    };
                    bytes.extend(
                        text.chars()
                            .map(|ch| if ch as u32 <= max { ch as u8 } else { b'?' }),
                    );
                }
            }
            pending.drain(..valid_len);
            Ok(Some((Bytes::from(bytes), (source, pending, false))))
        },
    );
    *response.body_mut() = Body::from_stream(stream);
    response
}
fn text_media(media: &[Media]) -> String {
    let first = &media[0];
    let offered = if first.kind == "application/*" || first.kind == "application/*+json" {
        "application/json"
    } else if first.kind.contains('*') || first.kind == "application/octet-stream" {
        "text/plain"
    } else {
        &first.kind
    };
    let mut selected = first.concrete(offered);
    if !first.params.iter().any(|(name, _)| name == "charset")
        && offered != "application/json"
        && !offered.ends_with("+json")
    {
        selected.push_str(";charset=UTF-8");
    }
    selected
}

enum Accept {
    Default,
    Json,
    Other(Result<Vec<Media>, ()>),
}
/// Apply to business routes; management endpoints negotiate their own produces.
/// Default Accept, */*, and application/json do not allocate or inspect bodies.
pub async fn negotiate(request: Request, next: Next) -> Response {
    let method = request.method().clone();
    let path = request.uri().path();
    if path.starts_with("/actuator")
        || path.starts_with("/health/")
        || path == "/metrics"
        || path == "/error"
    {
        return suppress_head(&method, next.run(request).await);
    }
    let mut all = request.headers().get_all(header::ACCEPT).iter();
    let first = all.next();
    let accept = if all.next().is_none() {
        match first.and_then(|v| v.to_str().ok()) {
            None | Some("") | Some("*/*") | Some("*") => Accept::Default,
            Some("application/json") => Accept::Json,
            _ => Accept::Other(accepted(request.headers())),
        }
    } else {
        Accept::Other(accepted(request.headers()))
    };
    let mut response = next.run(request).await;
    if response
        .extensions_mut()
        .remove::<FixedContentType>()
        .is_some()
    {
        return suppress_head(&method, response);
    }
    let Some(content_type) = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
    else {
        return suppress_head(&method, response);
    };
    let text = content_type.starts_with("text/plain");
    if content_type != "application/json" && !text {
        return suppress_head(&method, response);
    }
    let content_type = match accept {
        Accept::Default => {
            if text {
                response.headers_mut().insert(
                    header::CONTENT_TYPE,
                    HeaderValue::from_static("text/plain;charset=UTF-8"),
                );
            }
            return suppress_head(&method, response);
        }
        Accept::Json => {
            if text {
                response.headers_mut().insert(
                    header::CONTENT_TYPE,
                    HeaderValue::from_static("application/json"),
                );
            }
            return suppress_head(&method, response);
        }
        Accept::Other(Err(())) => {
            let status = if response.status().is_success() {
                StatusCode::INTERNAL_SERVER_ERROR
            } else {
                response.status()
            };
            return suppress_head(&method, status.into_response());
        }
        Accept::Other(Ok(media)) => {
            if text {
                text_media(&media)
            } else if let Some(content_type) = generic_json(&media) {
                content_type
            } else {
                let status = response
                    .extensions()
                    .get::<NegotiationFallback>()
                    .map(|s| s.0)
                    .unwrap_or(if response.status().is_success() {
                        StatusCode::NOT_ACCEPTABLE
                    } else {
                        StatusCode::INTERNAL_SERVER_ERROR
                    });
                let response = if media.iter().any(|m| m.matches("text/html")) {
                    html(
                        status,
                        status.as_u16(),
                        status.canonical_reason().unwrap_or("None"),
                    )
                } else {
                    status.into_response()
                };
                return suppress_head(&method, response);
            }
        }
    };
    let encoding = charset(&content_type);
    let Ok(value) = content_type.parse() else {
        return suppress_head(&method, StatusCode::INTERNAL_SERVER_ERROR.into_response());
    };
    response.headers_mut().insert(header::CONTENT_TYPE, value);
    let response = encode_response(
        response,
        if text {
            encoding
        } else {
            encoding.json_encoding()
        },
    );
    if method == Method::HEAD {
        suppress_head(&method, response)
    } else {
        response
    }
}

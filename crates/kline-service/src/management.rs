//! Spring Boot's exposed management and framework HTTP contracts.
//!
//! Runtime values stay native: disk health is measured locally and Prometheus
//! exposes this process's metrics. Market recovery readiness has its own endpoint.
use axum::{
    Json,
    body::Body,
    extract::Request,
    http::{HeaderMap, Method, StatusCode, Uri, header},
    response::{IntoResponse, Response},
};
use serde_json::{Value, json};
use std::collections::BTreeSet;

mod negotiation;
pub use negotiation::{NegotiationFallback, fixed_content_type, negotiate};

const V3: &str = "application/vnd.spring-boot.actuator.v3+json";
const V2: &str = "application/vnd.spring-boot.actuator.v2+json";
const JSON: &str = "application/json";
const TEXT: &str = "text/plain;version=0.0.4;charset=utf-8";
const OPENMETRICS: &str = "application/openmetrics-text;version=1.0.0;charset=utf-8";
const DISK_THRESHOLD: u64 = 10 * 1024 * 1024;

#[derive(Debug)]
struct Media {
    kind: String,
    params: Vec<(String, String)>,
    quality: f32,
}
impl Media {
    fn matches(&self, offered: &str) -> bool {
        self.kind == "*/*"
            || self.kind == offered
            || self
                .kind
                .strip_suffix("/*")
                .is_some_and(|t| offered.starts_with(&format!("{t}/")))
            || self
                .kind
                .strip_suffix("*+json")
                .is_some_and(|t| offered.starts_with(t) && offered.ends_with("+json"))
    }
    fn concrete(&self, offered: &str) -> String {
        let mut out = offered.to_owned();
        for (name, value) in &self.params {
            if name != "q" {
                out.push(';');
                out.push_str(name);
                out.push('=');
                out.push_str(value);
            }
        }
        out
    }
    fn specificity(&self) -> u8 {
        if self.kind == "*/*" {
            0
        } else if self.kind.contains('*') {
            1
        } else {
            2
        }
    }
}
fn accepted(headers: &HeaderMap) -> Result<Vec<Media>, ()> {
    let mut values = Vec::new();
    for header in headers.get_all(header::ACCEPT) {
        for part in header.to_str().map_err(|_| ())?.split(',') {
            if part.trim().is_empty() {
                continue;
            }
            let mut pieces = part.split(';');
            let kind = pieces
                .next()
                .unwrap_or_default()
                .trim()
                .to_ascii_lowercase();
            let kind = if kind == "*" { "*/*".into() } else { kind };
            let Some((top, sub)) = kind.split_once('/') else {
                return Err(());
            };
            if top.is_empty()
                || sub.is_empty()
                || sub.contains('/')
                || !kind
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~/".contains(&b))
            {
                return Err(());
            }
            let mut params = Vec::new();
            let mut quality = 1.;
            for piece in pieces {
                if piece.trim().is_empty() {
                    continue;
                }
                let Some((name, value)) = piece.trim().split_once('=') else {
                    return Err(());
                };
                let name = name.trim().to_ascii_lowercase();
                let value = value.trim().to_owned();
                if name == "charset"
                    && negotiation::Charset::parse(value.trim_matches('"')).is_none()
                {
                    return Err(());
                }
                if name == "q" {
                    quality = value.trim_matches('"').parse::<f32>().map_err(|_| ())?;
                    if !quality.is_finite() || !(0.0..=1.0).contains(&quality) {
                        return Err(());
                    }
                }
                params.push((name, value));
            }
            values.push(Media {
                kind,
                params,
                quality,
            });
        }
    }
    if values.is_empty() {
        values.push(Media {
            kind: "*/*".into(),
            params: vec![],
            quality: 1.,
        });
    }
    values.sort_by(|a, b| {
        b.quality
            .total_cmp(&a.quality)
            .then_with(|| b.specificity().cmp(&a.specificity()))
    });
    Ok(values)
}
fn response(status: StatusCode, content_type: String, value: Value) -> Response {
    let charset = negotiation::charset(&content_type);
    let response = (status, [(header::CONTENT_TYPE, content_type)], Json(value)).into_response();
    negotiation::encode_response(response, charset.json_encoding())
}
fn generic_json(accepted: &[Media]) -> Option<String> {
    accepted.iter().find_map(|m| {
        if !negotiation::charset(&m.concrete(JSON)).json_supported() {
            return None;
        }
        if m.kind == JSON
            || m.kind.starts_with("application/")
                && m.kind.ends_with("+json")
                && !m.kind.contains('*')
        {
            Some(m.concrete(&m.kind))
        } else if m.matches(JSON) || m.kind == "application/*+json" {
            Some(m.concrete(JSON))
        } else {
            None
        }
    })
}
fn html(status: StatusCode, code: u16, reason: &str) -> Response {
    let created = chrono::Local::now().format("%a %b %d %T %Z %Y");
    (status, [(header::CONTENT_TYPE, "text/html;charset=UTF-8")], format!("<html><body><h1>Whitelabel Error Page</h1><p>This application has no explicit mapping for /error, so you are seeing this as a fallback.</p><div id='created'>{created}</div><div>There was an unexpected error (type={reason}, status={code}).</div></body></html>")).into_response()
}
fn rejected(headers: &HeaderMap, fallback: StatusCode, message: String) -> Response {
    let Ok(accepted) = accepted(headers) else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    if let Some(media) = generic_json(&accepted) {
        response(
            StatusCode::INTERNAL_SERVER_ERROR,
            media,
            json!({"code":-1000,"msg":message}),
        )
    } else if accepted.iter().any(|m| m.matches("text/html")) {
        html(
            fallback,
            fallback.as_u16(),
            fallback.canonical_reason().unwrap_or("None"),
        )
    } else {
        fallback.into_response()
    }
}
fn actuator_media(headers: &HeaderMap) -> Result<String, Box<Response>> {
    let accepted = accepted(headers)
        .map_err(|_| Box::new(StatusCode::INTERNAL_SERVER_ERROR.into_response()))?;
    for m in &accepted {
        for offered in [V3, V2, JSON] {
            if m.matches(offered) {
                if !negotiation::charset(&m.concrete(offered)).json_supported() {
                    return Err(Box::new(StatusCode::INTERNAL_SERVER_ERROR.into_response()));
                }
                return Ok(m.concrete(offered));
            }
        }
    }
    Err(Box::new(rejected(
        headers,
        StatusCode::NOT_ACCEPTABLE,
        "No acceptable representation".into(),
    )))
}
pub async fn index(headers: HeaderMap, uri: Uri) -> Response {
    let media = match actuator_media(&headers) {
        Ok(m) => m,
        Err(r) => return *r,
    };
    let host = headers
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
        .or_else(|| uri.authority().map(|a| a.as_str()))
        .unwrap_or("localhost");
    // Java does not enable forwarded-header processing in this application.
    let scheme = uri.scheme_str().unwrap_or("http");
    let base = format!("{scheme}://{host}/actuator");
    let links: serde_json::Map<_, _> = [
        ("self", "", false),
        ("health", "/health", false),
        ("health-path", "/health/{*path}", true),
        ("info", "/info", false),
        ("prometheus", "/prometheus", false),
    ]
    .into_iter()
    .map(|(name, suffix, templated)| {
        (
            name.into(),
            json!({"href":format!("{base}{suffix}"),"templated":templated}),
        )
    })
    .collect();
    response(StatusCode::OK, media, json!({"_links":links}))
}
pub async fn info(headers: HeaderMap) -> Response {
    match actuator_media(&headers) {
        Ok(m) => response(StatusCode::OK, m, json!({})),
        Err(r) => *r,
    }
}
fn disk_is_up(available: Option<u64>) -> bool {
    available.is_some_and(|free| free >= DISK_THRESHOLD)
}
pub async fn health(headers: HeaderMap) -> Response {
    let media = match actuator_media(&headers) {
        Ok(m) => m,
        Err(r) => return *r,
    };
    let available = rustix::fs::statvfs(".")
        .ok()
        .map(|s| s.f_bavail.saturating_mul(s.f_frsize));
    let up = disk_is_up(available);
    response(
        if up {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        },
        media,
        json!({"status":if up { "UP" } else { "DOWN" }}),
    )
}
pub async fn component(headers: HeaderMap, uri: Uri) -> Response {
    if uri.path().trim_end_matches('/') == "/actuator/health" {
        return health(headers).await;
    }
    match actuator_media(&headers) {
        Ok(_) => StatusCode::NOT_FOUND.into_response(),
        Err(r) => *r,
    }
}
fn suppress_head(method: &Method, mut response: Response) -> Response {
    if method == Method::HEAD {
        response.headers_mut().remove(header::CONTENT_LENGTH);
        // Spring's HEAD responses have no Content-Length. An unknown-size empty
        // stream prevents Axum's outer route from inventing Content-Length: 0.
        *response.body_mut() = Body::from_stream(futures_util::stream::empty::<
            Result<axum::body::Bytes, std::convert::Infallible>,
        >());
    }
    response
}
/// Match Spring's method fallback without Axum adding an Allow header to its
/// Java-style 500 response. OPTIONS still supplies its own exact Allow value.
pub fn get<H, T, S>(handler: H) -> axum::routing::MethodRouter<S>
where
    H: axum::handler::Handler<T, S>,
    T: 'static,
    S: Clone + Send + Sync + 'static,
{
    axum::routing::get(handler).merge(axum::routing::any(method_not_allowed))
}
/// Can also be installed on the market router, avoiding separate method policies.
pub async fn method_not_allowed(request: Request) -> Response {
    if request.method() == Method::OPTIONS {
        let allow = if request.uri().path().ends_with("/klines/bulk")
            || request.uri().path().ends_with("/fundingRate/bulk")
        {
            "GET,HEAD,POST,OPTIONS"
        } else {
            "GET,HEAD,OPTIONS"
        };
        return (StatusCode::OK, [(header::ALLOW, allow)]).into_response();
    }
    suppress_head(
        request.method(),
        rejected(
            request.headers(),
            StatusCode::METHOD_NOT_ALLOWED,
            format!("Request method '{}' is not supported", request.method()),
        ),
    )
}
pub async fn not_found(request: Request) -> Response {
    let path = request.uri().path().trim_matches('/');
    suppress_head(
        request.method(),
        rejected(
            request.headers(),
            StatusCode::NOT_FOUND,
            format!("No static resource {path}."),
        ),
    )
}
/// Direct access to Boot's error endpoint has no servlet error attributes.
/// Preserve that observable JSON contract; normal failures use {code,msg}.
pub async fn error_controller(request: Request) -> Response {
    if request.method() == Method::OPTIONS {
        return (
            StatusCode::OK,
            [(header::ALLOW, "GET,HEAD,POST,PUT,PATCH,DELETE,OPTIONS")],
        )
            .into_response();
    }
    let Ok(media) = accepted(request.headers()) else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let result = if media.iter().any(|m| m.kind == "text/html") {
        html(StatusCode::INTERNAL_SERVER_ERROR, 999, "None")
    } else if let Some(content_type) = generic_json(&media) {
        response(
            StatusCode::INTERNAL_SERVER_ERROR,
            content_type,
            json!({"timestamp":chrono::Utc::now().timestamp_millis(),"status":999,"error":"None"}),
        )
    } else {
        StatusCode::INTERNAL_SERVER_ERROR.into_response()
    };
    suppress_head(request.method(), result)
}
fn included_names(params: &[(String, String)]) -> Option<BTreeSet<String>> {
    let values: Vec<_> = params
        .iter()
        .filter(|(key, _)| key == "includedNames")
        .map(|(_, value)| value.as_str())
        .collect();
    match values.as_slice() {
        [] | [""] => None,
        [one] => Some(
            one.split(',')
                .map(|s| crate::http_compat::java_trim(s).to_owned())
                .collect(),
        ),
        _ => Some(values.into_iter().map(str::to_owned).collect()),
    }
}
fn sample_name(line: &str) -> &str {
    line.split(['{', ' ']).next().unwrap_or_default()
}
fn family(name: &str) -> &str {
    ["_bucket", "_count", "_sum", "_total", "_created"]
        .iter()
        .find_map(|suffix| name.strip_suffix(suffix))
        .unwrap_or(name)
}
fn filter_metrics(text: String, included: Option<BTreeSet<String>>) -> String {
    let Some(included) = included else {
        return text;
    };
    let matched: BTreeSet<_> = text
        .lines()
        .filter(|line| !line.starts_with('#') && included.contains(sample_name(line)))
        .map(sample_name)
        .collect();
    let mut out = String::new();
    for line in text.lines() {
        let keep = if line.starts_with('#') {
            let name = line.split_whitespace().nth(2).unwrap_or_default();
            matched
                .iter()
                .any(|sample| *sample == name || family(sample) == name)
        } else {
            matched.contains(sample_name(line))
        };
        if keep {
            out.push_str(line);
            out.push('\n');
        }
    }
    out
}
pub fn metrics_response(
    text: String,
    headers: &HeaderMap,
    params: &[(String, String)],
) -> Response {
    let media = match accepted(headers) {
        Ok(m) => m,
        Err(()) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    let compatible = media.iter().any(|m| {
        [
            ("text/plain", "0.0.4"),
            ("application/openmetrics-text", "1.0.0"),
        ]
        .iter()
        .any(|(kind, version)| {
            m.matches(kind)
                && m.params
                    .iter()
                    .all(|(key, value)| key != "version" || value == version)
        })
    });
    if !compatible {
        return rejected(
            headers,
            StatusCode::NOT_ACCEPTABLE,
            "No acceptable representation".into(),
        );
    }
    // The Java exporter prefers OpenMetrics whenever that format is present,
    // after the endpoint's produces condition has accepted the request.
    let open = media
        .iter()
        .any(|m| m.kind == "application/openmetrics-text");
    let text = filter_metrics(text, included_names(params));
    let text = if open {
        let mut out = String::new();
        for line in text.lines() {
            if line.starts_with("# TYPE ") && line.ends_with(" counter") {
                let name = line.split_whitespace().nth(2).unwrap_or_default();
                out.push_str("# TYPE ");
                out.push_str(name.strip_suffix("_total").unwrap_or(name));
                out.push_str(" counter\n");
            } else {
                out.push_str(line);
                out.push('\n');
            }
        }
        out.push_str("# EOF\n");
        out
    } else {
        text
    };
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, if open { OPENMETRICS } else { TEXT })],
        text,
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn health_reports_actual_disk_failure_threshold() {
        assert!(!disk_is_up(None));
        assert!(!disk_is_up(Some(DISK_THRESHOLD - 1)));
        assert!(disk_is_up(Some(DISK_THRESHOLD)));
    }
    #[test]
    fn metric_inclusion_matches_spring_set_binding_and_exact_samples() {
        let text = "# TYPE a histogram\na_bucket{le=\"1\"} 2\na_count 2\na_sum 1\nb 8\n";
        let p = |values: &[&str]| {
            values
                .iter()
                .map(|s| ("includedNames".into(), (*s).into()))
                .collect::<Vec<_>>()
        };
        assert_eq!(filter_metrics(text.into(), included_names(&p(&[""]))), text);
        assert!(filter_metrics(text.into(), included_names(&p(&[" "]))).is_empty());
        assert_eq!(
            filter_metrics(text.into(), included_names(&p(&["a_count"]))),
            "# TYPE a histogram\na_count 2\n"
        );
        assert!(filter_metrics(text.into(), included_names(&p(&["a"]))).is_empty());
        assert_eq!(
            filter_metrics(text.into(), included_names(&p(&["a_count,a_sum", "b"]))),
            "b 8\n"
        );
        assert_eq!(
            filter_metrics(text.into(), included_names(&p(&["a_count", "a_sum"]))),
            "# TYPE a histogram\na_count 2\na_sum 1\n"
        );
    }
}

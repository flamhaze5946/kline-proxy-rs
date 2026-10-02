use crate::http_compat::{self, OptionalJson, Query};
use crate::management::get;
use crate::{BulkQuery, Engine, ServiceError};
use axum::{
    Json, Router,
    body::Body,
    extract::{DefaultBodyLimit, Request, State},
    http::{HeaderMap, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::any,
};
use kline_core::Market;
use serde::Deserialize;
use std::sync::{Arc, atomic::Ordering};

/// Small bulk responses should not wait for Nagle/delayed-ACK interaction.
pub fn low_latency_listener(
    listener: tokio::net::TcpListener,
) -> axum::serve::TapIo<
    tokio::net::TcpListener,
    impl FnMut(&mut tokio::net::TcpStream) + Send + 'static,
> {
    use axum::serve::ListenerExt;
    listener.tap_io(|stream| {
        if let Err(error) = stream.set_nodelay(true) {
            tracing::warn!(%error, "could not enable TCP_NODELAY");
        }
    })
}

#[derive(Clone)]
struct HttpState {
    engine: Arc<Engine>,
    requests: Arc<crate::admission::Gate>,
    readiness: Option<Arc<dyn Readiness>>,
    strict_readiness: bool,
}
/// Lifecycle supplies an in-memory health snapshot. Never run recovery from HTTP.
pub trait Readiness: Send + Sync {
    fn is_ready(&self) -> bool;
    fn details(&self) -> serde_json::Value;
    fn is_serving(&self) -> bool {
        self.is_ready()
    }
    fn query_ready(&self, _query: &BulkQuery) -> bool {
        self.is_ready()
    }
}
pub fn router(engine: Arc<Engine>) -> Router {
    router_with_readiness(engine, None)
}
pub fn router_with_readiness(engine: Arc<Engine>, readiness: Option<Arc<dyn Readiness>>) -> Router {
    router_with_policy(engine, readiness, true)
}
pub fn router_with_policy(
    engine: Arc<Engine>,
    readiness: Option<Arc<dyn Readiness>>,
    strict_readiness: bool,
) -> Router {
    let state = HttpState {
        requests: Arc::new(engine.http_gate()),
        engine,
        readiness,
        strict_readiness,
    };
    Router::new()
        .route("/fapi/v1/klines/bulk", get(future_get).post(future_post))
        .route("/api/v3/klines/bulk", get(spot_get).post(spot_post))
        .route(
            "/health/live",
            get(|| async { Json(serde_json::json!({"status":"up"})) }),
        )
        .route("/health/ready", get(ready))
        .route("/health/serving", get(serving))
        .route("/metrics", get(metrics))
        .route("/actuator", get(crate::management::index))
        .route("/actuator/prometheus", get(metrics))
        .route("/actuator/health", get(crate::management::health))
        .route("/actuator/health/", get(crate::management::health))
        .route("/actuator/health/{*path}", get(crate::management::component))
        .route(
            "/actuator/info",
            get(crate::management::info),
        )
        .route(
            "/health/diagnostics",
            get(|State(s): State<HttpState>| async move { Json(s.engine.diagnostics.summaries()) }),
        )
        .route("/health/performance",get(|State(s):State<HttpState>|async move{Json(serde_json::json!({"closed_bars":s.engine.diagnostics.summaries(),"work":s.engine.metrics.work.summary(),"diagnostic_dropped":s.engine.diagnostics.dropped.load(std::sync::atomic::Ordering::Relaxed),"bulk_http":s.engine.metrics.http.hourly(),"measurement":"application completion; excludes Nginx/TLS/network; boundary requests start within first 10 seconds"}))}))
        .route("/error", any(crate::management::error_controller))
        .method_not_allowed_fallback(crate::management::method_not_allowed)
        .fallback(crate::management::not_found)
        .layer(DefaultBodyLimit::max(64 * 1024))
        .layer(middleware::from_fn_with_state(state.clone(), admit))
        .layer(middleware::from_fn(crate::management::negotiate))
        .with_state(state)
}
async fn admit(State(state): State<HttpState>, request: Request, next: Next) -> Response {
    let path = request.uri().path().to_owned();
    let started = std::time::Instant::now();
    let entry = tokio::time::Instant::now();
    let run = |request| crate::admission::REQUEST_ENTRY.scope(entry, next.run(request));
    // A bulk request's wait for an in-flight slot shares one budget with this queue.
    let budget = path
        .ends_with("/klines/bulk")
        .then(|| std::time::Duration::from_millis(state.engine.settings().admission_wait_ms));
    let response = if crate::admission::is_management(&path) {
        run(request).await
    } else if let Some(_permit) = state
        .requests
        .enter(&state.engine.metrics, entry, budget)
        .await
    {
        run(request).await
    } else {
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            -1008,
            "Request capacity exceeded",
        )
    };
    state.engine.metrics.http.record_at(
        &path,
        started.elapsed(),
        response.status().as_u16(),
        state.engine.now_ms(),
        response
            .extensions()
            .get::<crate::diagnostics::AnsweredFor>()
            .map(|a| a.0),
    );
    response
}

#[derive(Deserialize)]
struct Params {
    interval: Option<String>,
    #[serde(default, deserialize_with = "http_compat::query_i32")]
    limit: Option<i32>,
    #[serde(default, deserialize_with = "http_compat::query_bool")]
    closed_only: Option<bool>,
    symbols: Option<String>,
}
#[derive(Deserialize)]
struct PostBody {
    #[serde(default, deserialize_with = "http_compat::optional_string")]
    interval: Option<String>,
    #[serde(default, deserialize_with = "http_compat::optional_i32")]
    limit: Option<i32>,
    #[serde(default, deserialize_with = "http_compat::json_bool")]
    closed_only: Option<bool>,
    #[serde(default, deserialize_with = "http_compat::optional_strings")]
    symbols: Option<Vec<Option<String>>>,
}
impl http_compat::JsonBody for PostBody {
    const RECORD: bool = true;
    const STRING_FIELDS: &'static [&'static str] = &["interval"];
    const STRING_LIST_FIELDS: &'static [&'static str] = &["symbols"];
    const BOOLEAN_FIELDS: &'static [&'static str] = &["closed_only"];
    const INTEGER_FIELDS: &'static [&'static str] = &["limit"];
}
impl From<Params> for PostBody {
    fn from(p: Params) -> Self {
        Self {
            interval: p.interval,
            limit: p.limit,
            closed_only: p.closed_only,
            symbols: p.symbols.map(|s| {
                s.split(',')
                    .filter(|s| !http_compat::is_blank(s))
                    .map(|s| Some(s.to_owned()))
                    .collect()
            }),
        }
    }
}
async fn future_get(State(s): State<HttpState>, Query(p): Query<Params>) -> Response {
    if p.interval.is_none() {
        let mut response = error(
            StatusCode::INTERNAL_SERVER_ERROR,
            -1000,
            "Required request parameter 'interval' for method parameter type String is not present",
        );
        response
            .extensions_mut()
            .insert(crate::management::NegotiationFallback(
                StatusCode::BAD_REQUEST,
            ));
        return response;
    }
    answer(s, Market::Future, p.into()).await
}
async fn spot_get(State(s): State<HttpState>, Query(p): Query<Params>) -> Response {
    if p.interval.is_none() {
        let mut response = error(
            StatusCode::INTERNAL_SERVER_ERROR,
            -1000,
            "Required request parameter 'interval' for method parameter type String is not present",
        );
        response
            .extensions_mut()
            .insert(crate::management::NegotiationFallback(
                StatusCode::BAD_REQUEST,
            ));
        return response;
    }
    answer(s, Market::Spot, p.into()).await
}
async fn future_post(
    State(s): State<HttpState>,
    OptionalJson(body): OptionalJson<PostBody>,
) -> Response {
    match body {
        Some(p)
            if p.interval
                .as_deref()
                .is_some_and(|s| !http_compat::is_blank(s)) =>
        {
            answer(s, Market::Future, p).await
        }
        _ => missing(),
    }
}
async fn spot_post(
    State(s): State<HttpState>,
    OptionalJson(body): OptionalJson<PostBody>,
) -> Response {
    match body {
        Some(p)
            if p.interval
                .as_deref()
                .is_some_and(|s| !http_compat::is_blank(s)) =>
        {
            answer(s, Market::Spot, p).await
        }
        _ => missing(),
    }
}
fn missing() -> Response {
    error(StatusCode::BAD_REQUEST, -1102, "interval is required")
}
async fn answer(state: HttpState, market: Market, p: PostBody) -> Response {
    let Some(interval) = p.interval else {
        return missing();
    };
    let query = BulkQuery {
        market,
        interval,
        limit: p.limit,
        closed_only: p.closed_only.unwrap_or(true),
        symbols: p
            .symbols
            .unwrap_or_default()
            .into_iter()
            .flatten()
            .filter(|s| !http_compat::is_blank(s))
            .collect(),
    };
    if state.strict_readiness
        && state
            .readiness
            .as_ref()
            .is_some_and(|r| !r.query_ready(&query))
    {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            -1008,
            "Requested market data is initializing or recovering",
        );
    }
    match state.engine.bulk(query).await {
        Ok(reply) => {
            let mut response = (
                [(header::CONTENT_TYPE, "application/json")],
                Body::from(reply.body.clone()),
            )
                .into_response();
            // For the hourly statistics: the period this request was answered for.
            response
                .extensions_mut()
                .insert(crate::diagnostics::AnsweredFor(reply.boundary));
            response
        }
        Err(ServiceError::Interval) => error(StatusCode::BAD_REQUEST, -1120, "Invalid interval."),
        Err(ServiceError::Busy) => error(
            StatusCode::SERVICE_UNAVAILABLE,
            -1008,
            "Response capacity exceeded",
        ),
        Err(e) => {
            tracing::error!(error = %e, "bulk failed");
            error(StatusCode::INTERNAL_SERVER_ERROR, -1000, "Internal error")
        }
    }
}
fn error(status: StatusCode, code: i32, message: &str) -> Response {
    (status, Json(serde_json::json!({"code":code,"msg":message}))).into_response()
}
async fn ready(State(s): State<HttpState>) -> Response {
    if let Some(readiness) = &s.readiness {
        return (
            if readiness.is_ready() {
                StatusCode::OK
            } else {
                StatusCode::SERVICE_UNAVAILABLE
            },
            Json(readiness.details()),
        )
            .into_response();
    }
    let total = s.engine.catalog.slots().len();
    let populated = s
        .engine
        .catalog
        .slots()
        .iter()
        .filter(|s| !s.is_empty())
        .count();
    let ready = total > 0 && populated == total;
    (if ready { StatusCode::OK } else { StatusCode::SERVICE_UNAVAILABLE },
        Json(serde_json::json!({"cache_populated":ready,"populated_series":populated,"configured_series":total,
            "mode":"core-prototype","checks":"cache population only; no reconnect gap recovery"}))).into_response()
}
async fn serving(State(s): State<HttpState>) -> Response {
    if let Some(readiness) = &s.readiness {
        let serving = readiness.is_serving();
        let mut details = readiness.details();
        details["serving"] = serving.into();
        return (
            if serving {
                StatusCode::OK
            } else {
                StatusCode::SERVICE_UNAVAILABLE
            },
            Json(details),
        )
            .into_response();
    }
    ready(State(s)).await
}
async fn metrics(
    State(s): State<HttpState>,
    headers: HeaderMap,
    axum::extract::Query(params): axum::extract::Query<Vec<(String, String)>>,
) -> Response {
    let m = &s.engine.metrics;
    let (entries, flights, bytes) = s.engine.cache_sizes();
    let mut text = String::from(
        "# TYPE http_server_requests_seconds histogram\n# TYPE http_server_requests_errors_total counter\n",
    );
    m.http.render(&mut text);
    use std::fmt::Write;
    for (name, value) in [
        ("frames_total", &m.frames),
        ("ignored_total", &m.ignored),
        ("invalid_total", &m.invalid),
        ("forming_total", &m.forming),
        ("finals_total", &m.finals),
        ("final_revisions_total", &m.final_revisions),
        ("responses_built_total", &m.responses_built),
        ("cache_hits_total", &m.cache_hits),
        ("payloads_built_total", &m.payloads_built),
        ("payload_reuses_total", &m.payload_reuses),
        ("bulk_admission_queued_total", &m.admission_queued),
        ("bulk_admission_rejected_total", &m.admission_rejected),
        ("http_admission_queued_total", &m.http_admission_queued),
        ("http_admission_rejected_total", &m.http_admission_rejected),
        ("bulk_pre_boundary_waits_total", &m.pre_boundary_waits),
    ] {
        let _ = writeln!(
            text,
            "# TYPE kline_{name} counter\nkline_{name} {}",
            value.load(Ordering::Relaxed)
        );
    }
    for (name, value) in [
        ("cache_entries", entries),
        ("inflight_keys", flights),
        ("cache_bytes", bytes),
        ("bulk_admission_waiting", s.engine.admission_waiting()),
        (
            "http_admission_waiting",
            m.http_admission_waiting.load(Ordering::Relaxed),
        ),
    ] {
        let _ = writeln!(text, "# TYPE kline_{name} gauge\nkline_{name} {value}");
    }
    crate::management::metrics_response(text, &headers, &params)
}

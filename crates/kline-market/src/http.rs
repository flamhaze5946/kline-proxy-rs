//! Remaining Java HTTP contracts. Network/cache/statistical services stay independent.
use crate::{
    config::Config,
    error::{ApiError, Result},
    funding::{Funding, Query as FundingQuery},
    metadata::Metadata,
    statistics::Statistics,
    ticker::Tickers,
    transport::RestApi,
};
use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{Request, State},
    http::header,
    middleware::{self, Next},
    response::{IntoResponse, Response},
};
use kline_core::Market;
use kline_service::Engine;
use kline_service::http_compat::{self, OptionalJson, Query};
use kline_service::management::get;
use moka::future::Cache;
use serde_json::Value;
use std::{collections::BTreeMap, net::SocketAddr, sync::Arc, time::Duration};
pub struct App {
    pub engine: Arc<Engine>,
    pub api: Arc<RestApi>,
    pub metadata: Arc<Metadata>,
    pub funding: Arc<Funding>,
    pub tickers: Arc<Tickers>,
    pub statistics: Arc<Statistics>,
    pub config: Config,
    cms: Cache<String, Bytes>,
    requests: kline_service::admission::Gate,
}
impl App {
    /// This route group's admission gate (tests occupy it to exercise queueing).
    pub fn admission(&self) -> &kline_service::admission::Gate {
        &self.requests
    }
}
impl App {
    pub fn new(engine: Arc<Engine>, api: Arc<RestApi>, config: Config) -> Arc<Self> {
        let metadata = Metadata::new(api.clone(), config.metadata_refresh_seconds);
        let funding = Funding::new(api.clone(), metadata.clone(), config.funding.clone());
        let tickers = Tickers::with_engine(api.clone(), metadata.clone(), engine.clone());
        let statistics = Statistics::new(
            engine.clone(),
            metadata.clone(),
            api.clone(),
            config.statistics.clone(),
        );
        let requests = engine.http_gate();
        Arc::new(Self {
            engine,
            api,
            metadata,
            funding,
            tickers,
            statistics,
            config,
            cms: Cache::builder()
                .max_capacity(128)
                .time_to_live(Duration::from_secs(300))
                .build(),
            requests,
        })
    }
}
type Params = BTreeMap<String, String>;
pub fn router(app: Arc<App>) -> Router {
    Router::new()
        .route("/health/market",get(|State(app):State<Arc<App>>|async move{Json(serde_json::json!({"tickers":app.tickers.status(),"metadata":app.metadata.status(),"funding":{"upstream_loads":app.funding.upstream_loads.load(std::sync::atomic::Ordering::Relaxed),"cached_hours":app.funding.chunks.entry_count()}}))}))
        .route("/fapi/v1/exchangeInfo", get(future_exchange))
        .route("/api/v3/exchangeInfo", get(spot_exchange))
        .route("/fapi/v1/time", get(time))
        .route("/api/v3/time", get(time))
        .route("/fapi/v1/klines", get(future_klines))
        .route("/api/v3/klines", get(spot_klines))
        .route("/fapi/v1/fundingRate", get(funding))
        .route(
            "/fapi/v1/fundingRate/bulk",
            get(funding_get).post(funding_post),
        )
        .route("/fapi/v1/premiumIndex", get(premium))
        .route("/fapi/v1/ticker/price", get(future_price))
        .route("/api/v3/ticker/price", get(spot_price))
        .route("/fapi/v1/ticker/24hr", get(future_ticker))
        .route("/api/v3/ticker/24hr", get(spot_ticker))
        .route(
            "/bapi/composite/v1/public/cms/article/catalog/list/query",
            get(catalogs),
        )
        .route(
            "/bapi/composite/v1/public/cms/article/list/query",
            get(articles),
        )
        .route("/statistic/getAltCoinIndex", get(alt))
        .route("/statistic/getYama01AltCoinIndex", get(yama01))
        .route("/statistic/getYama02AltCoinIndex", get(yama02))
        .route("/statistic/getYamaAggAltCoinIndex", get(aggregate))
        .route("/statistic/pic/getYama01AltCoinIndex", get(yama01_image))
        .route("/statistic/pic/getYama02AltCoinIndex", get(yama02_image))
        .route(
            "/statistic/pic/getYamaAggAltCoinIndex",
            get(aggregate_image),
        )
        .route("/hello/helloWorld", get(|| async { "Hello World!" }))
        .route("/hello/whatsMyIp", get(ip))
        .method_not_allowed_fallback(kline_service::management::method_not_allowed)
        .layer(axum::extract::DefaultBodyLimit::max(64 * 1024))
        .layer(middleware::from_fn_with_state(app.clone(), admit))
        .layer(middleware::from_fn(kline_service::management::negotiate))
        .with_state(app)
}
async fn admit(State(app): State<Arc<App>>, request: Request, next: Next) -> Response {
    let path = request.uri().path().to_owned();
    let started = std::time::Instant::now();
    let entry = tokio::time::Instant::now();
    let run = |request| kline_service::admission::REQUEST_ENTRY.scope(entry, next.run(request));
    let response = if kline_service::admission::is_management(&path) {
        run(request).await
    } else if let Some(_permit) = app
        .requests
        .enter(&app.engine.metrics, entry, route_budget(&path))
        .await
    {
        run(request).await
    } else {
        ApiError {
            status: 503,
            code: -1008,
            message: "Request capacity exceeded".into(),
            negotiation_fallback: None,
        }
        .into_response()
    };
    app.engine.metrics.http.record_at(
        &path,
        started.elapsed(),
        response.status().as_u16(),
        app.engine.now_ms(),
        None,
    );
    response
}
/// The total budget a route's handler enforces from arrival, if any, so that HTTP admission never
/// queues the request past it: the ticker query always has [`crate::QUERY_BUDGET`]. A first
/// metadata load (until that market's metadata has loaded once) has the same budget, but it is not
/// applied here, because whether a request needs metadata depends on its parameters and body.
fn route_budget(path: &str) -> Option<Duration> {
    matches!(
        path,
        "/fapi/v1/ticker/price"
            | "/api/v3/ticker/price"
            | "/fapi/v1/ticker/24hr"
            | "/api/v3/ticker/24hr"
    )
    .then_some(crate::QUERY_BUDGET)
}

fn json_bytes(body: Bytes) -> Response {
    (
        [(header::CONTENT_TYPE, "application/json")],
        Body::from(body),
    )
        .into_response()
}
fn required<'a>(q: &'a Params, name: &str) -> Result<&'a str> {
    q.get(name)
        .map(String::as_str)
        .ok_or_else(|| ApiError::binding(format!("{name} is required")))
}
fn number<T: std::str::FromStr>(q: &Params, name: &str) -> Result<Option<T>> {
    http_compat::optional_number(q.get(name).map(String::as_str))
        .map_err(|_| ApiError::binding(format!("Invalid {name}")))
}
fn nonblank<'a>(q: &'a Params, key: &str) -> Option<&'a str> {
    q.get(key)
        .map(String::as_str)
        .filter(|s| !http_compat::is_blank(s))
}
async fn exchange(app: Arc<App>, market: Market) -> Result<Response> {
    let (prefix, body) = app.metadata.body(market, app.engine.now_ms()).await?;
    let size = prefix.len() + body.len();
    let stream = futures_util::stream::iter([Ok::<_, std::convert::Infallible>(prefix), Ok(body)]);
    Ok((
        [
            (header::CONTENT_TYPE, "application/json".to_owned()),
            (header::CONTENT_LENGTH, size.to_string()),
        ],
        Body::from_stream(stream),
    )
        .into_response())
}
async fn future_exchange(State(app): State<Arc<App>>) -> Result<Response> {
    exchange(app, Market::Future).await
}
async fn spot_exchange(State(app): State<Arc<App>>) -> Result<Response> {
    exchange(app, Market::Spot).await
}
async fn time(State(app): State<Arc<App>>) -> Json<Value> {
    Json(serde_json::json!({"serverTime":app.engine.now_ms()}))
}
async fn ordinary(app: Arc<App>, market: Market, q: Params) -> Result<Response> {
    let query = crate::klines::Query {
        symbol: required(&q, "symbol")?.into(),
        interval: required(&q, "interval")?.into(),
        start_time: number(&q, "startTime")?,
        end_time: number(&q, "endTime")?,
        limit: number(&q, "limit")?,
        time_zone: q.get("timeZone").cloned(),
    };
    Ok(json_bytes(
        crate::klines::query_bytes(&app.engine, &app.api, market, query).await?,
    ))
}
async fn future_klines(State(app): State<Arc<App>>, Query(q): Query<Params>) -> Result<Response> {
    ordinary(app, Market::Future, q).await
}
async fn spot_klines(State(app): State<Arc<App>>, Query(q): Query<Params>) -> Result<Response> {
    ordinary(app, Market::Spot, q).await
}
async fn funding(State(app): State<Arc<App>>, Query(q): Query<Params>) -> Result<Json<Value>> {
    // Java validates only nonblank symbols but forwards the original optional
    // parameter. An explicit empty symbol is distinct from omitting it upstream.
    let symbol = q.get("symbol").map(String::as_str);
    if let Some(s) = symbol.filter(|s| !http_compat::is_blank(s)) {
        app.metadata.validate(Market::Future, &[s.into()]).await?
    }
    let rows = app
        .funding
        .raw(
            symbol,
            number(&q, "startTime")?,
            number(&q, "endTime")?,
            number(&q, "limit")?,
        )
        .await?;
    Ok(Json(
        serde_json::to_value(rows).map_err(ApiError::internal)?,
    ))
}
async fn funding_get(State(app): State<Arc<App>>, Query(q): Query<Params>) -> Result<Response> {
    let query = FundingQuery {
        symbols: nonblank(&q, "symbols").map(|s| s.split(',').map(|s| Some(s.into())).collect()),
        since_ms: number(&q, "since_ms")?,
        until_ms: number(&q, "until_ms")?,
        limit: number(&q, "limit")?,
    };
    Ok(json_bytes(app.funding.bulk(query).await?))
}
async fn funding_post(
    State(app): State<Arc<App>>,
    OptionalJson(body): OptionalJson<FundingQuery>,
) -> Result<Response> {
    let query = body.unwrap_or_default();
    Ok(json_bytes(app.funding.bulk(query).await?))
}
async fn premium(State(app): State<Arc<App>>, Query(q): Query<Params>) -> Result<Json<Value>> {
    let symbol = nonblank(&q, "symbol");
    let args = if let Some(s) = symbol {
        app.metadata.validate(Market::Future, &[s.into()]).await?;
        vec![("symbol".into(), s.into())]
    } else {
        vec![]
    };
    let raw: Value = app
        .api
        .json(
            Market::Future,
            "/fapi/v1/premiumIndex",
            &args,
            if symbol.is_some() { 1 } else { 10 },
        )
        .await
        .map_err(ApiError::from)?;
    let convert = |row: &Value| -> Result<Value> {
        if row.is_null() {
            return Ok(serde_json::json!([]));
        }
        if !row.is_object() {
            return Err(ApiError::upstream_io("Expected premium index object"));
        }
        let mut out = serde_json::Map::new();
        for key in ["symbol", "nextFundingTime", "time"] {
            if let Some(value) = row.get(key).filter(|v| !v.is_null()) {
                let value = crate::metadata_shape::shape(
                    if key == "symbol" { "String" } else { "Long" },
                    value,
                )
                .map_err(ApiError::upstream_io)?;
                if !value.is_null() {
                    out.insert(key.into(), value);
                }
            }
        }
        for key in [
            "markPrice",
            "indexPrice",
            "estimatedSettlePrice",
            "lastFundingRate",
            "interestRate",
        ] {
            if let Some(value) = row.get(key).filter(|v| !v.is_null()) {
                out.insert(
                    key.into(),
                    crate::decimal(
                        &value
                            .as_str()
                            .map(str::to_owned)
                            .unwrap_or_else(|| value.to_string()),
                    )
                    .map_err(ApiError::upstream_io)?
                    .into(),
                );
            }
        }
        Ok(out.into())
    };
    Ok(Json(if let Some(rows) = raw.as_array() {
        Value::Array(rows.iter().map(convert).collect::<Result<_>>()?)
    } else {
        convert(&raw)?
    }))
}
fn ticker_symbols(market: Market, q: &Params) -> Result<(Option<String>, Vec<String>)> {
    let symbol = nonblank(q, "symbol").map(str::to_owned);
    let payload = if market == Market::Spot {
        nonblank(q, "symbols")
    } else {
        None
    };
    if symbol.is_some() && payload.is_some() {
        return Err(ApiError::bad(
            -1128,
            "Combination of optional parameters invalid. Recommendation: 'symbol' and 'symbols' cannot both be sent.",
        ));
    }
    let mut symbols = if let Some(s) = &symbol {
        vec![s.clone()]
    } else if let Some(s) = payload {
        if !s.starts_with('[') {
            return Err(ApiError::bad(
                -1100,
                "Illegal characters found in parameter 'symbols'.",
            ));
        }
        http_compat::json_string_list(s)
            .map_err(|_| ApiError::bad(-1100, "Illegal characters found in parameter 'symbols'."))?
            .ok_or_else(|| {
                ApiError::bad(-1100, "Illegal characters found in parameter 'symbols'.")
            })?
            .into_iter()
            .flatten()
            .filter(|s| !http_compat::is_blank(s))
            .collect()
    } else {
        vec![]
    };
    let mut seen = std::collections::HashSet::new();
    symbols.retain(|s| seen.insert(s.clone()));
    Ok((symbol, symbols))
}
async fn ticker(app: Arc<App>, market: Market, price: bool, q: Params) -> Result<Response> {
    let kind = if market == Market::Spot && !price {
        nonblank(&q, "type")
    } else {
        None
    };
    if kind.is_some_and(|s| !matches!(s, "FULL" | "MINI")) {
        return Err(ApiError::bad(-1139, "Invalid ticker type."));
    }
    let status = if market == Market::Spot && !price {
        nonblank(&q, "symbolStatus")
    } else {
        None
    };
    if status.is_some_and(|s| !matches!(s, "TRADING" | "HALT" | "BREAK")) {
        return Err(ApiError::bad(-1122, "Invalid symbolStatus."));
    }
    let (symbol, symbols) = ticker_symbols(market, &q)?;
    if market == Market::Spot && !price && nonblank(&q, "symbols").is_some() && symbols.is_empty() {
        return Ok(json_bytes(Bytes::from_static(b"[]")));
    }
    let fixed_media = if price {
        symbols.is_empty()
    } else {
        symbol.is_none()
            && (market == Market::Future || (nonblank(&q, "symbols").is_none() && status.is_none()))
    };
    let response = json_bytes(
        app.tickers
            .query(
                market,
                price,
                symbol.as_deref(),
                symbols,
                kind == Some("MINI"),
                status,
            )
            .await?,
    );
    Ok(if fixed_media {
        kline_service::management::fixed_content_type(response)
    } else {
        response
    })
}
async fn future_price(State(a): State<Arc<App>>, Query(q): Query<Params>) -> Result<Response> {
    ticker(a, Market::Future, true, q).await
}
async fn spot_price(State(a): State<Arc<App>>, Query(q): Query<Params>) -> Result<Response> {
    ticker(a, Market::Spot, true, q).await
}
async fn future_ticker(State(a): State<Arc<App>>, Query(q): Query<Params>) -> Result<Response> {
    ticker(a, Market::Future, false, q).await
}
async fn spot_ticker(State(a): State<Arc<App>>, Query(q): Query<Params>) -> Result<Response> {
    ticker(a, Market::Spot, false, q).await
}
async fn cms(app: Arc<App>, q: Params, catalog: bool) -> Result<Response> {
    let mut args = vec![];
    for key in if catalog {
        vec!["catalogId", "pageNo", "pageSize"]
    } else {
        vec!["catalogId", "type", "pageNo", "pageSize"]
    } {
        let value = if key == "catalogId" {
            required(&q, key)?.to_owned()
        } else {
            number::<i32>(&q, key)?
                .ok_or_else(|| ApiError::binding(format!("{key} is required")))?
                .to_string()
        };
        args.push((key.to_owned(), value));
    }
    let path = if catalog {
        "bapi/composite/v1/public/cms/article/catalog/list/query"
    } else {
        "bapi/composite/v1/public/cms/article/list/query"
    };
    let key = serde_json::to_string(&(path, &args)).map_err(ApiError::internal)?;
    let bytes = app
        .cms
        .try_get_with(key, async {
            let raw = app
                .api
                .external(
                    &format!("{}/{path}", app.config.cms_url.trim_end_matches('/')),
                    &args,
                    8 * 1024 * 1024,
                )
                .await
                .map_err(ApiError::from)?;
            app.api
                .cpu
                .run_large(move || {
                    let value: Value =
                        crate::transport::decode_json(&raw).map_err(ApiError::from)?;
                    if !value.is_object() {
                        return Err(ApiError::upstream_io("Expected CMS response object"));
                    }
                    let body = serde_json::to_vec(&crate::cms_shape::response(&value, catalog)?)
                        .map_err(ApiError::internal)?;
                    Ok::<_, ApiError>(Bytes::from(body))
                })
                .await
                .map_err(ApiError::from)?
        })
        .await
        .map_err(|e| (*e).clone())?;
    Ok(json_bytes(bytes))
}
async fn catalogs(State(a): State<Arc<App>>, Query(q): Query<Params>) -> Result<Response> {
    cms(a, q, true).await
}
async fn articles(State(a): State<Arc<App>>, Query(q): Query<Params>) -> Result<Response> {
    cms(a, q, false).await
}
async fn statistic(app: Arc<App>, kind: &'static str) -> Result<Json<Value>> {
    Ok(Json((*app.statistics.query(kind).await?).clone()))
}
async fn alt(State(a): State<Arc<App>>) -> Result<Json<Value>> {
    statistic(a, "alt").await
}
async fn yama01(State(a): State<Arc<App>>) -> Result<Json<Value>> {
    statistic(a, "yama01").await
}
async fn yama02(State(a): State<Arc<App>>) -> Result<Json<Value>> {
    statistic(a, "yama02").await
}
async fn aggregate(State(a): State<Arc<App>>) -> Result<Json<Value>> {
    statistic(a, "agg").await
}
async fn picture(app: Arc<App>, kind: &'static str) -> Result<Response> {
    Ok((
        [
            (header::CONTENT_TYPE, "image/png"),
            (
                header::CONTENT_DISPOSITION,
                "inline; filename=\"image.png\"",
            ),
        ],
        Body::from(app.statistics.image(kind).await?),
    )
        .into_response())
}
async fn yama01_image(State(a): State<Arc<App>>) -> Result<Response> {
    picture(a, "yama01").await
}
async fn yama02_image(State(a): State<Arc<App>>) -> Result<Response> {
    picture(a, "yama02").await
}
async fn aggregate_image(State(a): State<Arc<App>>) -> Result<Response> {
    picture(a, "agg").await
}
async fn ip(request: Request) -> String {
    for key in ["x-forwarded-for", "x-real-ip", "forwarded"] {
        if let Some(s) = request
            .headers()
            .get(key)
            .and_then(|h| h.to_str().ok())
            .filter(|s| !http_compat::is_blank(s))
        {
            return s.into();
        }
    }
    request
        .extensions()
        .get::<axum::extract::ConnectInfo<SocketAddr>>()
        .map(|a| a.0.ip().to_string())
        .unwrap_or_else(|| "unknown".into())
}

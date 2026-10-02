//! Same current production HTTP routers, with identical ready market data and no live feeds.
use axum::{Router, extract::{Query, State}, http::Uri, response::{IntoResponse, Response}};
use kline_core::{Bar, Interval, Market, Source, Update};
use kline_market::{http::App, transport::RestApi};
use kline_service::{Catalog, Clock, Engine, Instrument, Settings};
use serde_json::{Value, json};
use std::{collections::BTreeMap, net::SocketAddr, num::NonZeroUsize, sync::{Arc, atomic::{AtomicU64, Ordering}}};

struct FixedClock(i64);
impl Clock for FixedClock { fn now_ms(&self) -> i64 { self.0 } }
#[derive(Clone)]
struct Fixture { data: Arc<Value>, calls: Arc<AtomicU64> }
async fn upstream(State(f): State<Fixture>, uri: Uri, Query(q): Query<BTreeMap<String,String>>) -> Response {
    f.calls.fetch_add(1, Ordering::Relaxed);
    let value = if uri.path().ends_with("exchangeInfo") {
        f.data["exchange"][if uri.path().starts_with("/fapi") { "future" } else { "spot" }].clone()
    } else if uri.path().ends_with("fundingRate") {
        let start = q.get("startTime").and_then(|s|s.parse::<i64>().ok()).unwrap_or(i64::MIN);
        let end = q.get("endTime").and_then(|s|s.parse::<i64>().ok()).unwrap_or(i64::MAX);
        let limit = q.get("limit").and_then(|s|s.parse::<usize>().ok()).unwrap_or(100);
        Value::Array(f.data["funding"].as_array().unwrap().iter().filter(|r|
            q.get("symbol").is_none_or(|s|r["symbol"]==s.as_str())
            && r["fundingTime"].as_i64().unwrap()>=start && r["fundingTime"].as_i64().unwrap()<=end
        ).take(limit).cloned().collect())
    } else {
        return (axum::http::StatusCode::INTERNAL_SERVER_ERROR,format!("Unexpected upstream: {uri}")).into_response();
    };
    axum::Json(value).into_response()
}
fn main() -> anyhow::Result<()> {
    let args: Vec<_> = std::env::args().collect();
    let data: Arc<Value> = Arc::new(serde_json::from_slice(&std::fs::read(&args[1])?)?);
    let now = data["now"].as_i64().unwrap();
    let interval = Interval::parse("1h").unwrap();
    let mut definitions = Vec::new();
    for (market, key) in [(Market::Future,"future"),(Market::Spot,"spot")] {
        for symbol in data["bars"][key].as_object().unwrap().keys() {
            let trading = data["exchange"][key]["symbols"].as_array().unwrap().iter()
                .find(|s|s["symbol"]==symbol.as_str()).is_some_and(|s|s["status"]=="TRADING");
            definitions.push(Instrument { market, symbol:symbol.clone(), interval,
                trading, continuous:None, capacity:NonZeroUsize::new(18_000).unwrap() });
        }
    }
    let engine = Engine::new(Catalog::new(definitions).map_err(anyhow::Error::msg)?, Arc::new(FixedClock(now)), Settings::default());
    for (market,key) in [(Market::Future,"future"),(Market::Spot,"spot")] {
        for (symbol,rows) in data["bars"][key].as_object().unwrap() {
            let id = engine.catalog.find(market,interval,symbol).unwrap();
            let rows = rows.as_array().unwrap();
            for index in (rows.len() as i64-1000)..rows.len() as i64 {
                let row = &rows[index.max(0) as usize];
                let mut bar = Bar { open_time:row[0].as_i64().unwrap(), close_time:row[6].as_i64().unwrap(),
                    trades:row[8].as_u64().unwrap() as u32, ..Bar::default() };
                if index<0 {bar.open_time+=index*3_600_000;bar.close_time+=index*3_600_000;}
                for (i,n) in [1,2,3,4,5,7,9,10].into_iter().enumerate() {
                    bar.values[i] = row[n].as_str().unwrap().parse()?;
                }
                engine.commit(id,Update{bar,closed:true,source:Source::Stream,event_time:Some(1),sequence:1})?;
            }
        }
    }
    tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build()?.block_on(async {
        let calls = Arc::new(AtomicU64::new(0));
        let fixture = Fixture { data:data.clone(), calls:calls.clone() };
        let upstream_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let root = format!("http://{}",upstream_listener.local_addr()?);
        tokio::spawn(async move { axum::serve(upstream_listener,Router::new().fallback(upstream).with_state(fixture)).await.unwrap(); });
        let mut config = kline_market::config::Config::default();
        config.cms_url = root.clone(); config.funding.vision_enabled = false;
        config.statistics.altcoin_url = root.clone();
        let app = App::new(engine.clone(),RestApi::new(&root,&root,6000)?,config);
        let router = kline_service::http::router_with_policy(engine,None,false)
            .merge(kline_market::http::router(app))
            .route("/__fixture/status",axum::routing::get(move || { let c=calls.clone(); async move {
                let stat = std::fs::read_to_string("/proc/self/stat").unwrap();
                let columns: Vec<_> = stat.rsplit_once(')').unwrap().1.split_whitespace().collect();
                let ticks = columns[11].parse::<u64>().unwrap()+columns[12].parse::<u64>().unwrap();
                axum::Json(json!({"now":now,"upstream_calls":c.load(Ordering::Relaxed),
                    "cpu_ns":ticks*10_000_000,"rss_bytes":columns[21].parse::<u64>().unwrap()*4096,
                    "pid":std::process::id()}))
            }}));
        let listener = kline_runtime::listener::bind(format!("0.0.0.0:{}",args[2]),1024).await?;
        println!("READY {}",listener.local_addr()?);
        axum::serve(kline_service::http::low_latency_listener(listener),
            router.into_make_service_with_connect_info::<SocketAddr>()).await?;
        Ok::<_,anyhow::Error>(())
    })
}

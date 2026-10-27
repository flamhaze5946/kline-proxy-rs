//! Minimal HTTP/1.1 Binance REST mock on the paused runtime. Every response closes its
//! connection, and the server waits for the client's EOF, so the controller can see the whole
//! exchange. Latency and completion order are expressed only by `tokio::time::sleep` here.
use crate::control::{Control, VirtualClock};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Clone)]
pub struct Symbol {
    pub seed: i64,
    /// First open time the exchange has rows for (new listings).
    pub listed_from: i64,
    pub status: String,
    pub listed: bool,
    pub pair: String,
    pub contract: String,
    /// Open times the exchange has no row for (real exchange gaps).
    pub holes: Vec<i64>,
}
#[derive(Clone, Debug)]
pub enum Fault {
    Status(u16),
    Empty,
    Drop,
    /// Answer normally, but only after this many milliseconds.
    Delay(u64),
}
#[derive(Default)]
pub struct Plan {
    pub symbols: BTreeMap<String, Symbol>,
    /// Applied in order to the next kline requests of a symbol.
    pub faults: BTreeMap<String, VecDeque<Fault>>,
}
pub struct Mock {
    pub plan: Mutex<Plan>,
    trace: Mutex<Vec<(i64, String, String)>>,
    control: Arc<Control>,
    clock: Arc<VirtualClock>,
}

pub fn interval_ms(code: &str) -> i64 {
    kline_core::Interval::parse(code)
        .map(|i| i.millis())
        .unwrap_or(60_000)
}
fn price(cents: i64) -> String {
    format!("{}.{:02}", cents / 100, cents % 100)
}
/// Deterministic bar for one opening time; a forming bar is a prefix of its final values.
pub fn bar_fields(seed: i64, open: i64, period: i64, now: i64) -> (Vec<String>, u32, bool) {
    let k = open / period;
    let o = 10_000 + seed * 1000 + (k % 17) * 25;
    let final_trades = 5 + (k % 11) as u32;
    let closed = open + period - 1 <= now;
    let (c, h, l, trades) = if closed {
        let c = o + ((k % 5) - 2) * 10;
        (c, o.max(c) + 30, o.min(c) - 20, final_trades)
    } else {
        let elapsed = ((now - open) * 4 / period).clamp(0, 3);
        let c = o + (elapsed - 1) * 5;
        (c, o.max(c) + 10, o.min(c) - 5, 1 + elapsed as u32)
    };
    let v = 1000 + i64::from(trades) * 100;
    let q = v * o / 100;
    let fields = vec![
        price(o),
        price(h),
        price(l),
        price(c),
        price(v),
        price(q),
        price(v / 2),
        price(q / 2),
    ];
    (fields, trades, closed)
}

impl Mock {
    pub async fn start(
        control: Arc<Control>,
        clock: Arc<VirtualClock>,
        plan: Plan,
    ) -> (Arc<Self>, String) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let mock = Arc::new(Self {
            plan: Mutex::new(plan),
            trace: Mutex::new(vec![]),
            control,
            clock,
        });
        let server = mock.clone();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    continue;
                };
                server.control.accepted();
                tokio::spawn(server.clone().connection(stream));
            }
        });
        (mock, url)
    }
    fn relative(&self) -> i64 {
        self.clock.read() - self.clock.base_ms()
    }
    async fn connection(self: Arc<Self>, mut stream: tokio::net::TcpStream) {
        let mut head = Vec::new();
        let mut buffer = [0_u8; 4096];
        let complete = loop {
            if head.windows(4).any(|w| w == b"\r\n\r\n") {
                break true;
            }
            match stream.read(&mut buffer).await {
                Ok(0) | Err(_) => break false,
                Ok(n) => head.extend_from_slice(&buffer[..n]),
            }
        };
        self.control.io_done();
        if !complete {
            return;
        }
        let text = String::from_utf8_lossy(&head).into_owned();
        let target = text
            .lines()
            .next()
            .and_then(|line| line.split(' ').nth(1))
            .unwrap_or("/")
            .to_owned();
        let (path, query) = target.split_once('?').unwrap_or((&target, ""));
        let query: BTreeMap<String, String> = query
            .split('&')
            .filter(|p| !p.is_empty())
            .map(|p| {
                let (k, v) = p.split_once('=').unwrap_or((p, ""));
                (k.to_owned(), v.to_owned())
            })
            .collect();
        let now = self.clock.read();
        let (delay, response, summary) = self.respond(now, path, &query);
        self.trace.lock().unwrap().push((
            self.relative(),
            target.clone(),
            json!({"t":self.relative(),"path":path,"query":query,"reply":summary}).to_string(),
        ));
        if delay > 0 {
            tokio::time::sleep(Duration::from_millis(delay)).await;
        }
        let _ = self.control.ready(self.clock.read(), target).await;
        // Released by the controller, which counted this write and the close as I/O.
        if let Some(bytes) = response {
            let _ = stream.write_all(&bytes).await;
        }
        let _ = stream.shutdown().await;
        loop {
            match stream.read(&mut buffer).await {
                Ok(0) | Err(_) => break,
                Ok(_) => {}
            }
        }
        self.control.io_done();
    }
    fn respond(
        &self,
        now: i64,
        path: &str,
        query: &BTreeMap<String, String>,
    ) -> (u64, Option<Vec<u8>>, String) {
        let mut plan = self.plan.lock().unwrap();
        if path == "/fapi/v1/exchangeInfo" {
            let symbols: Vec<Value> = plan
                .symbols
                .iter()
                .filter(|(_, s)| s.listed)
                .map(|(name, s)| {
                    json!({"symbol":name,"pair":s.pair,"contractType":s.contract,"status":s.status,
                        "baseAsset":name.trim_end_matches("USDT"),"quoteAsset":"USDT","marginAsset":"USDT",
                        "pricePrecision":2,"quantityPrecision":3,"onboardDate":0,"deliveryDate":4133404800000_i64})
                })
                .collect();
            let body = json!({"timezone":"UTC","serverTime":now,"futuresType":"U_MARGINED",
                "rateLimits":[],"exchangeFilters":[],"assets":[],"symbols":symbols});
            let names: Vec<_> = symbols.iter().map(|s| s["symbol"].clone()).collect();
            return (
                0,
                Some(http(200, &body.to_string())),
                format!("exchangeInfo {}", Value::from(names)),
            );
        }
        if path != "/fapi/v1/klines" {
            return (
                0,
                Some(http(404, r#"{"code":-1,"msg":"not found"}"#)),
                "404".into(),
            );
        }
        let name = query.get("symbol").cloned().unwrap_or_default();
        let fault = plan.faults.get_mut(&name).and_then(VecDeque::pop_front);
        let mut delay = 0;
        match fault {
            Some(Fault::Status(code)) => {
                return (
                    0,
                    Some(http(
                        code,
                        r#"{"code":-1001,"msg":"Internal error; unable to process your request. Please try again."}"#,
                    )),
                    format!("status {code}"),
                );
            }
            Some(Fault::Empty) => return (0, Some(http(200, "[]")), "rows 0 (fault empty)".into()),
            Some(Fault::Drop) => return (0, None, "dropped".into()),
            Some(Fault::Delay(ms)) => delay = ms,
            None => {}
        }
        let Some(symbol) = plan.symbols.get(&name) else {
            return (
                0,
                Some(http(400, r#"{"code":-1121,"msg":"Invalid symbol."}"#)),
                "400 invalid symbol".into(),
            );
        };
        let period = interval_ms(query.get("interval").map_or("1m", String::as_str));
        let end: i64 = query
            .get("endTime")
            .and_then(|s| s.parse().ok())
            .unwrap_or(now);
        let limit: usize = query
            .get("limit")
            .and_then(|s| s.parse().ok())
            .unwrap_or(500);
        let start: Option<i64> = query.get("startTime").and_then(|s| s.parse().ok());
        let first = start.unwrap_or(0).max(symbol.listed_from);
        let first = (first + period - 1).div_euclid(period) * period;
        let last = end.min(now).div_euclid(period) * period;
        let mut opens: Vec<i64> = if last < first {
            vec![]
        } else if start.is_some() {
            (0..)
                .map(|i| first + i * period)
                .take_while(|&o| o <= last)
                .filter(|o| !symbol.holes.contains(o))
                .take(limit)
                .collect()
        } else {
            let mut rows: Vec<i64> = (0..)
                .map(|i| last - i * period)
                .take_while(|&o| o >= first)
                .filter(|o| !symbol.holes.contains(o))
                .take(limit)
                .collect();
            rows.reverse();
            rows
        };
        opens.sort_unstable();
        let rows: Vec<Value> = opens
            .iter()
            .map(|&open| {
                let (f, trades, _) = bar_fields(symbol.seed, open, period, now);
                json!([
                    open,
                    f[0],
                    f[1],
                    f[2],
                    f[3],
                    f[4],
                    open + period - 1,
                    f[5],
                    trades,
                    f[6],
                    f[7],
                    "0"
                ])
            })
            .collect();
        let body = Value::from(rows).to_string();
        assert!(
            body.len() < 60 * 1024,
            "mock page {}B would use the auxiliary CPU pool",
            body.len()
        );
        let summary = match (opens.first(), opens.last()) {
            (Some(a), Some(b)) => format!("rows {} [{a}..{b}] delay {delay}", opens.len()),
            _ => format!("rows 0 delay {delay}"),
        };
        (delay, Some(http(200, &body)), summary)
    }
    /// Requests ordered by arrival virtual time, then canonically within one millisecond.
    pub fn trace_lines(&self) -> Vec<String> {
        let mut trace = self.trace.lock().unwrap().clone();
        trace.sort();
        trace.into_iter().map(|(_, _, line)| line).collect()
    }
}
fn http(status: u16, body: &str) -> Vec<u8> {
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        _ => "Status",
    };
    format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
    .into_bytes()
}

//! Closed-mode scenarios. Every input (configuration, exchange data, faults, frames, request
//! times) is fixed here; nothing depends on the wall clock.
use crate::{
    mock::{Fault, Plan, Symbol, bar_fields},
    sim::{DAY0, HOUR, MINUTE, Sim, write_v1_snapshot},
};
use anyhow::Result;
use serde_json::{Value, json};
use std::{future::Future, pin::Pin};

type Run = for<'a> fn(&'a mut Sim) -> Pin<Box<dyn Future<Output = Result<()>> + 'a>>;
/// (name, virtual start in ms since the epoch, body)
pub const ALL: &[(&str, i64, Run)] = &[
    ("s01_cold_start_1m", DAY0 + 10 * HOUR + 20_000, |s| {
        Box::pin(s01(s))
    }),
    ("s02_warm_start_1h", DAY0 + 10 * HOUR + 20 * MINUTE, |s| {
        Box::pin(s02(s))
    }),
    ("s03_rest_5xx_retry", DAY0 + 10 * HOUR + 20_000, |s| {
        Box::pin(s03(s))
    }),
    ("s04_rest_timeout_retry", DAY0 + 10 * HOUR + 20_000, |s| {
        Box::pin(s04(s))
    }),
    ("s05_empty_response", DAY0 + 10 * HOUR + 20_000, |s| {
        Box::pin(s05(s))
    }),
    ("s06_sync_in_guard", DAY0 + 10 * HOUR + 50_000, |s| {
        Box::pin(s06(s))
    }),
    (
        "s07_reconnect_across_boundary",
        DAY0 + 10 * HOUR + 20_000,
        |s| Box::pin(s07(s)),
    ),
    ("s08_delisting", DAY0 + 10 * HOUR + 20_000, |s| {
        Box::pin(s08(s))
    }),
    (
        "s09_persist_guard_restart",
        DAY0 + 10 * HOUR + 30_000,
        |s| Box::pin(s09(s)),
    ),
    ("s10_http_params", DAY0 + 10 * HOUR + 20_000, |s| {
        Box::pin(s10(s))
    }),
    ("s11_with_evidence_param", DAY0 + 10 * HOUR + 20_000, |s| {
        Box::pin(s11(s))
    }),
];

pub fn default_plan() -> Plan {
    let mut plan = Plan::default();
    for (seed, name) in ["BTCUSDT", "ETHUSDT", "SOLUSDT"].iter().enumerate() {
        plan.symbols.insert(
            (*name).into(),
            Symbol {
                seed: seed as i64 + 1,
                listed_from: DAY0 - 30 * 24 * HOUR,
                status: "TRADING".into(),
                listed: true,
                pair: (*name).into(),
                contract: "PERPETUAL".into(),
                holes: vec![],
            },
        );
    }
    plan
}

/// The kp-1m shape: continuous futures 1m, string numbers, persistence guard 2 s / 15 s.
fn kp1m(capacity: usize, patterns: &[&str], dump_seconds: u64) -> Value {
    json!({
        "listen": "127.0.0.1:0",
        "number_type": "string",
        "final_wait_ms": 8000,
        "history_capacity": capacity,
        "subscriptions": [{"market":"future","interval":"1m","history_capacity":capacity,
            "symbol_patterns":patterns,"continuous":true}],
        "rest": {"sync_clock": false},
        "market_api": {"enabled": false, "metadata_refresh_seconds": 86400},
        "persistence": {"directory": "", "load_on_startup": true, "dump_on_shutdown": true,
            "interval_seconds": dump_seconds, "boundary_guard_before_ms": 2000,
            "boundary_guard_after_ms": 15000}
    })
}
/// The live 1h shape (examples/full-market-persistent.json, futures part): double numbers,
/// kline (not continuous) topics, persistence guard 30 s / 30 s.
fn live1h(capacity: usize) -> Value {
    json!({
        "listen": "127.0.0.1:0",
        "final_wait_ms": 8000,
        "history_capacity": capacity,
        "subscriptions": [{"market":"future","interval":"1h","history_capacity":capacity,
            "symbol_patterns":[".*?USDT"],"continuous":false}],
        "rest": {"sync_clock": false},
        "market_api": {"enabled": false, "metadata_refresh_seconds": 86400},
        "persistence": {"directory": "", "load_on_startup": true, "dump_on_shutdown": true,
            "interval_seconds": 300, "boundary_guard_before_ms": 30000,
            "boundary_guard_after_ms": 30000}
    })
}
fn bulk(query: &str) -> String {
    format!("/fapi/v1/klines/bulk?{query}")
}
fn stream_all(sim: &mut Sim, symbols: &[&str]) {
    for s in symbols {
        sim.streaming.insert((*s).into());
    }
}
fn fault(sim: &Sim, symbol: &str, faults: &[Fault]) {
    sim.mock
        .plan
        .lock()
        .unwrap()
        .faults
        .entry(symbol.into())
        .or_default()
        .extend(faults.iter().cloned());
}
async fn probe(sim: &mut Sim, label: &str, symbols: &str) {
    sim.get(&format!("{label}/ready"), "/health/ready").await;
    sim.get(
        &format!("{label}/bulk_closed"),
        &bulk(&format!("interval=1m&limit=3&symbols={symbols}")),
    )
    .await;
}

async fn s01(sim: &mut Sim) -> Result<()> {
    let t0 = sim.now();
    let m0 = t0 - 20_000;
    sim.boot(kp1m(120, &[".*?USDT"], 300)).await?;
    stream_all(sim, &["BTCUSDT", "ETHUSDT", "SOLUSDT"]);
    sim.get("t0/ready", "/health/ready").await;
    sim.get("t0/bulk", &bulk("interval=1m&limit=2&symbols=BTCUSDT"))
        .await;
    sim.until(t0 + 300).await;
    sim.get("t0+300ms/ready", "/health/ready").await;
    sim.until(m0 + 30_500).await;
    sim.get("first_frame/ready", "/health/ready").await;
    sim.get("first_frame/serving", "/health/serving").await;
    sim.get(
        "first_frame/bulk_btc",
        &bulk("interval=1m&limit=3&symbols=BTCUSDT"),
    )
    .await;
    sim.get("first_frame/bulk_all", &bulk("interval=1m&limit=2"))
        .await;
    sim.get(
        "first_frame/bulk_forming",
        &bulk("interval=1m&limit=2&closed_only=false&symbols=BTCUSDT,ETHUSDT"),
    )
    .await;
    sim.http(
        "first_frame/post",
        "POST",
        "/fapi/v1/klines/bulk",
        Some(r#"{"interval":"1m","limit":2,"symbols":["BTCUSDT","SOLUSDT"]}"#),
    )
    .await;
    for minute in 1..=6 {
        sim.until(m0 + minute * MINUTE + 1_000).await;
        probe(sim, &format!("m{minute}+1s"), "BTCUSDT,ETHUSDT,SOLUSDT").await;
    }
    sim.snap("after_first_dump").await;
    sim.until(m0 + 7 * MINUTE + 40_000).await;
    sim.get(
        "end/bulk_limit100",
        &bulk("interval=1m&limit=100&symbols=ETHUSDT"),
    )
    .await;
    Ok(())
}

async fn s02(sim: &mut Sim) -> Result<()> {
    let t0 = sim.now();
    let h0 = t0 - 20 * MINUTE;
    let dir = sim.persist_dir();
    // BTCUSDT: 0.4.24-format snapshot with 199 finals up to 3 hours ago and one hole.
    let bars: Vec<_> = (0..200)
        .map(|i| h0 - (202 - i) * HOUR)
        .filter(|&open| open != h0 - 20 * HOUR)
        .map(|open| {
            let (f, trades, _) = bar_fields(1, open, HOUR, open + HOUR);
            let mut values = [0.; 8];
            for (v, s) in values.iter_mut().zip(&f) {
                *v = s.parse().unwrap();
            }
            (open, open + HOUR - 1, trades, values)
        })
        .collect();
    write_v1_snapshot(&dir, "future", "1h", "BTCUSDT", h0 - HOUR, &bars);
    // ETHUSDT: the same format with a broken checksum; restore skips it (cold recovery).
    write_v1_snapshot(&dir, "future", "1h", "ETHUSDT", h0 - HOUR, &bars[..10]);
    for entry in std::fs::read_dir(&dir)? {
        let path = entry?.path();
        let bytes = std::fs::read(&path)?;
        if bytes.len() < 1000 {
            let mut bytes = bytes;
            let n = bytes.len();
            bytes[n - 1] ^= 0xff;
            std::fs::write(&path, bytes)?;
        }
    }
    sim.snap("fixtures").await;
    sim.boot(live1h(200)).await?;
    stream_all(sim, &["BTCUSDT", "ETHUSDT", "SOLUSDT"]);
    sim.get("t0/ready", "/health/ready").await;
    sim.until(t0 + 1_000).await;
    sim.get("t0+1s/ready", "/health/ready").await;
    sim.until(h0 + 30 * MINUTE + 500).await;
    sim.get("h0+30m/ready", "/health/ready").await;
    for (label, query) in [
        ("h0+30m/bulk_btc", "interval=1h&limit=5&symbols=BTCUSDT"),
        ("h0+30m/bulk_all", "interval=1h&limit=3"),
        (
            "h0+30m/bulk_forming",
            "interval=1h&limit=2&closed_only=false",
        ),
        ("h0+30m/bulk_long", "interval=1h&limit=100&symbols=BTCUSDT"),
    ] {
        sim.get(label, &bulk(query)).await;
    }
    sim.snap("h0+30m").await;
    sim.until(h0 + HOUR + 20_500).await;
    sim.snap("h1+20s_guarded").await;
    sim.get("h1+20s/bulk_all", &bulk("interval=1h&limit=3"))
        .await;
    sim.until(h0 + HOUR + 31_500).await;
    sim.snap("h1+31s").await;
    Ok(())
}

async fn s03(sim: &mut Sim) -> Result<()> {
    let t0 = sim.now();
    let m0 = t0 - 20_000;
    fault(
        sim,
        "BTCUSDT",
        &[Fault::Status(503), Fault::Status(503), Fault::Status(503)],
    );
    sim.boot(kp1m(60, &["BTCUSDT"], 300)).await?;
    stream_all(sim, &["BTCUSDT"]);
    for at in [2_000, 4_000, 12_500, 13_500, 14_000] {
        sim.until(t0 + at).await;
        probe(sim, &format!("t0+{at}"), "BTCUSDT").await;
    }
    // A single 502 is retried inside the transport; a 429 starts the shared cooldown.
    sim.until(m0 + 2 * MINUTE + 20_000).await;
    fault(sim, "BTCUSDT", &[Fault::Status(502)]);
    sim.set_connected(false);
    sim.set_connected(true);
    sim.until(m0 + 2 * MINUTE + 25_000).await;
    probe(sim, "502", "BTCUSDT").await;
    sim.until(m0 + 3 * MINUTE + 20_000).await;
    fault(sim, "BTCUSDT", &[Fault::Status(429)]);
    sim.set_connected(false);
    sim.set_connected(true);
    for at in [1_000, 15_000, 61_000, 75_000, 90_000] {
        sim.until(m0 + 3 * MINUTE + 20_000 + at).await;
        probe(sim, &format!("429+{at}"), "BTCUSDT").await;
    }
    Ok(())
}

async fn s04(sim: &mut Sim) -> Result<()> {
    let t0 = sim.now();
    let m0 = t0 - 20_000;
    fault(
        sim,
        "ETHUSDT",
        &[
            Fault::Delay(20_000),
            Fault::Delay(20_000),
            Fault::Delay(20_000),
        ],
    );
    sim.boot(kp1m(60, &["ETHUSDT"], 300)).await?;
    stream_all(sim, &["ETHUSDT"]);
    for at in [10_000, 47_500, 49_000, 58_500, 59_500] {
        sim.until(t0 + at).await;
        probe(sim, &format!("t0+{at}"), "ETHUSDT").await;
    }
    // A dropped connection (transport retry after 1 s), then slow but within the 15 s timeout.
    sim.until(m0 + 3 * MINUTE + 20_000).await;
    fault(sim, "ETHUSDT", &[Fault::Drop, Fault::Delay(14_000)]);
    sim.set_connected(false);
    sim.set_connected(true);
    for at in [7_000, 14_500, 16_000] {
        sim.until(m0 + 3 * MINUTE + 20_000 + at).await;
        probe(sim, &format!("slow+{at}"), "ETHUSDT").await;
    }
    Ok(())
}

async fn s05(sim: &mut Sim) -> Result<()> {
    let t0 = sim.now();
    fault(sim, "SOLUSDT", &[Fault::Empty, Fault::Empty]);
    // A listing 5 minutes old has fewer rows than the capacity.
    sim.mock.plan.lock().unwrap().symbols.insert(
        "NEWUSDT".into(),
        Symbol {
            seed: 9,
            listed_from: t0 - 20_000 - 5 * MINUTE,
            status: "TRADING".into(),
            listed: true,
            pair: "NEWUSDT".into(),
            contract: "PERPETUAL".into(),
            holes: vec![],
        },
    );
    sim.boot(kp1m(60, &["SOLUSDT", "NEWUSDT"], 300)).await?;
    stream_all(sim, &["SOLUSDT", "NEWUSDT"]);
    for at in [1_000, 10_500, 11_000, 21_000, 22_000, 41_000] {
        sim.until(t0 + at).await;
        probe(sim, &format!("t0+{at}"), "SOLUSDT,NEWUSDT").await;
    }
    sim.get(
        "new/bulk_long",
        &bulk("interval=1m&limit=20&symbols=NEWUSDT"),
    )
    .await;
    Ok(())
}

async fn s06(sim: &mut Sim) -> Result<()> {
    let t0 = sim.now();
    let b1 = t0 + 10_000;
    sim.boot(kp1m(60, &["BTCUSDT"], 300)).await?;
    stream_all(sim, &["BTCUSDT"]);
    sim.until(t0 + 5_000).await;
    probe(sim, "t0+5s", "BTCUSDT").await;
    // Reconnect 1.5 s before a boundary: the epoch sync waits out the REST guard (3 s / 10 s).
    sim.until(b1 - 1_500).await;
    sim.set_connected(false);
    sim.set_connected(true);
    for at in [-1_000, 5_000, 9_000, 10_500, 11_000] {
        sim.until(b1 + at).await;
        probe(sim, &format!("b1{at:+}"), "BTCUSDT").await;
    }
    // Reconnect 2 s after the next boundary.
    let b2 = b1 + MINUTE;
    sim.until(b2 + 2_000).await;
    sim.set_connected(false);
    sim.set_connected(true);
    for at in [3_000, 9_500, 10_500] {
        sim.until(b2 + at).await;
        probe(sim, &format!("b2{at:+}"), "BTCUSDT").await;
    }
    Ok(())
}

async fn s07(sim: &mut Sim) -> Result<()> {
    let t0 = sim.now();
    let m0 = t0 - 20_000;
    sim.boot(kp1m(60, &["BTCUSDT", "ETHUSDT"], 300)).await?;
    stream_all(sim, &["BTCUSDT", "ETHUSDT"]);
    sim.until(m0 + 40_000).await;
    probe(sim, "m0+40s", "BTCUSDT,ETHUSDT").await;
    // Connection lost before the boundary; the final of m0 is never streamed.
    sim.until(m0 + 55_000).await;
    sim.set_connected(false);
    sim.until(m0 + MINUTE + 5_000).await;
    probe(sim, "m1+5s_down", "BTCUSDT,ETHUSDT").await;
    sim.until(m0 + MINUTE + 20_000).await;
    sim.set_connected(true);
    for at in [20_100, 20_500, 31_000] {
        sim.until(m0 + MINUTE + at).await;
        probe(sim, &format!("m1+{at}"), "BTCUSDT,ETHUSDT").await;
    }
    // A silent topic (connection up): the next frame fills the gap with synthetic bars.
    sim.until(m0 + 2 * MINUTE + 50_000).await;
    sim.streaming.remove("ETHUSDT");
    sim.until(m0 + 4 * MINUTE + 25_000).await;
    probe(sim, "m4+25s_silent", "BTCUSDT,ETHUSDT").await;
    sim.streaming.insert("ETHUSDT".into());
    for at in [30_500, 50_000] {
        sim.until(m0 + 4 * MINUTE + at).await;
        probe(sim, &format!("m4+{at}"), "BTCUSDT,ETHUSDT").await;
        sim.get(
            &format!("m4+{at}/eth_forming"),
            &bulk("interval=1m&limit=5&closed_only=false&symbols=ETHUSDT"),
        )
        .await;
    }
    sim.until(m0 + 6 * MINUTE + 30_000).await;
    probe(sim, "m6+30s_after_routine", "BTCUSDT,ETHUSDT").await;
    Ok(())
}

async fn s08(sim: &mut Sim) -> Result<()> {
    let t0 = sim.now();
    let m0 = t0 - 20_000;
    sim.boot(kp1m(60, &[".*?USDT"], 30)).await?;
    stream_all(sim, &["BTCUSDT", "ETHUSDT", "SOLUSDT"]);
    sim.until(m0 + MINUTE + 40_000).await;
    probe(sim, "listed", "BTCUSDT,ETHUSDT,SOLUSDT").await;
    sim.snap("listed").await;
    // SOLUSDT leaves TRADING (still in exchangeInfo).
    sim.until(m0 + MINUTE + 55_000).await;
    sim.mock
        .plan
        .lock()
        .unwrap()
        .symbols
        .get_mut("SOLUSDT")
        .unwrap()
        .status = "SETTLING".into();
    sim.refresh_directory().await?;
    sim.get(
        "sol_settling/bulk_sol",
        &bulk("interval=1m&limit=2&symbols=SOLUSDT"),
    )
    .await;
    for at in [500, 10_500] {
        sim.until(m0 + 2 * MINUTE + at).await;
        probe(
            sim,
            &format!("sol_settling+{at}"),
            "BTCUSDT,ETHUSDT,SOLUSDT",
        )
        .await;
    }
    sim.until(m0 + 2 * MINUTE + 51_500).await;
    sim.snap("sol_removed").await;
    // ETHUSDT disappears from exchangeInfo.
    sim.until(m0 + 3 * MINUTE + 25_000).await;
    sim.mock
        .plan
        .lock()
        .unwrap()
        .symbols
        .get_mut("ETHUSDT")
        .unwrap()
        .listed = false;
    sim.refresh_directory().await?;
    sim.until(m0 + 3 * MINUTE + 30_500).await;
    probe(sim, "eth_gone", "BTCUSDT,ETHUSDT,SOLUSDT").await;
    sim.get("eth_gone/bulk_all", &bulk("interval=1m&limit=2"))
        .await;
    sim.until(m0 + 4 * MINUTE + 21_500).await;
    sim.snap("eth_removed").await;
    // SOLUSDT trades again: a new series, recovered cold.
    sim.until(m0 + 4 * MINUTE + 25_000).await;
    sim.mock
        .plan
        .lock()
        .unwrap()
        .symbols
        .get_mut("SOLUSDT")
        .unwrap()
        .status = "TRADING".into();
    sim.refresh_directory().await?;
    for at in [25_500, 35_000] {
        sim.until(m0 + 4 * MINUTE + at).await;
        probe(sim, &format!("sol_back+{at}"), "BTCUSDT,ETHUSDT,SOLUSDT").await;
    }
    Ok(())
}

async fn s09(sim: &mut Sim) -> Result<()> {
    let t0 = sim.now();
    let m0 = t0 - 30_000;
    let config = kp1m(60, &["BTCUSDT"], 5);
    sim.boot(config.clone()).await?;
    stream_all(sim, &["BTCUSDT"]);
    for at in [36_000, 50_500, 57_500, 59_500] {
        sim.until(m0 + at).await;
        sim.snap(&format!("m0+{at}")).await;
    }
    let m1 = m0 + MINUTE;
    for at in [500, 1_500, 10_500, 14_500, 15_500, 20_500] {
        sim.until(m1 + at).await;
        sim.snap(&format!("m1+{at}")).await;
    }
    // Stop inside the next after-boundary guard: the shutdown dump ignores the guard.
    let m2 = m1 + MINUTE;
    sim.until(m2 + 5_000).await;
    probe(sim, "before_restart", "BTCUSDT").await;
    sim.shutdown().await;
    // Restart 10 s later from the snapshot just written (warm start).
    sim.until(m2 + 15_000).await;
    sim.boot(config).await?;
    stream_all(sim, &["BTCUSDT"]);
    for at in [15_000, 15_500, 30_500] {
        sim.until(m2 + at).await;
        probe(sim, &format!("restart+{at}"), "BTCUSDT").await;
    }
    sim.until(m2 + 40_500).await;
    sim.snap("after_restart").await;
    Ok(())
}

async fn s10(sim: &mut Sim) -> Result<()> {
    let t0 = sim.now();
    sim.boot(kp1m(60, &["BTCUSDT", "ETHUSDT"], 300)).await?;
    stream_all(sim, &["BTCUSDT", "ETHUSDT"]);
    sim.until(t0 + 15_000).await;
    let gets = [
        "interval=1m&limit=2&symbols=BTCUSDT",
        "interval=1m&limit=2&symbols=BTCUSDT&foo=bar",
        "interval=1m&limit=2&symbols=BTCUSDT&closed_only=true&unknown=1",
        "interval=1m&limit=2&symbols=BTCUSDT&evidence=1&contract=kp",
        "interval=1m&limit=0&symbols=BTCUSDT",
        "interval=1m&limit=101&symbols=BTCUSDT",
        "interval=1m&limit=-5&symbols=BTCUSDT",
        "interval=1m&limit=abc&symbols=BTCUSDT",
        "interval=1m&symbols=BTCUSDT",
        "limit=2&symbols=BTCUSDT",
        "interval=7m&limit=2&symbols=BTCUSDT",
        "interval=1h&limit=2&symbols=BTCUSDT",
        "interval=1m&limit=2&symbols=BTCUSDT,,ETHUSDT",
        "interval=1m&limit=2&symbols=%20BTCUSDT%20",
        "interval=1m&limit=2&symbols=XXXUSDT",
        "interval=1m&limit=2&symbols=BTCUSDT,XXXUSDT",
        "interval=1m&limit=2&closed_only=false&symbols=ETHUSDT",
        "interval=1m&limit=2&closed_only=0&symbols=ETHUSDT",
        "interval=1m&limit=2&closed_only=maybe&symbols=ETHUSDT",
    ];
    for (n, query) in gets.iter().enumerate() {
        sim.get(&format!("get{n:02}"), &bulk(query)).await;
    }
    let posts = [
        r#"{"interval":"1m","limit":2,"symbols":["BTCUSDT"]}"#,
        r#"{"interval":"1m","limit":2,"symbols":["BTCUSDT"],"extra":1}"#,
        r#"{"interval":"1m","limit":2,"symbols":["BTCUSDT"],"with_evidence":true}"#,
        r#"{"limit":2,"symbols":["BTCUSDT"]}"#,
        r#"{"interval":"1m","limit":"2","symbols":"BTCUSDT"}"#,
        r#"{"interval":"1m","closed_only":false,"symbols":["ETHUSDT",null,""]}"#,
        r#"not json"#,
    ];
    for (n, body) in posts.iter().enumerate() {
        sim.http(
            &format!("post{n:02}"),
            "POST",
            "/fapi/v1/klines/bulk",
            Some(body),
        )
        .await;
    }
    sim.http(
        "post_query_param",
        "POST",
        "/fapi/v1/klines/bulk?foo=bar",
        Some(posts[0]),
    )
    .await;
    for uri in [
        "/api/v3/klines/bulk?interval=1m&limit=2&symbols=BTCUSDT",
        "/health/live",
        "/health/serving",
        "/actuator/health",
        "/no/such/path",
    ] {
        sim.get(uri, uri).await;
    }
    sim.http("delete_bulk", "DELETE", &bulk("interval=1m"), None)
        .await;
    Ok(())
}

async fn s11(sim: &mut Sim) -> Result<()> {
    let t0 = sim.now();
    sim.boot(kp1m(60, &["BTCUSDT", "ETHUSDT"], 300)).await?;
    stream_all(sim, &["BTCUSDT", "ETHUSDT"]);
    sim.until(t0 + 15_000).await;
    // 0.4.24 ignores this parameter. The evidence build answers with_evidence=true with 400
    // when evidence mode is off (design §4/§7 "known exception"); those labels say so.
    let gets = [
        ("plain", "interval=1m&limit=2&symbols=BTCUSDT,ETHUSDT"),
        (
            "known_exception:get_true",
            "interval=1m&limit=2&symbols=BTCUSDT,ETHUSDT&with_evidence=true",
        ),
        (
            "known_exception:get_true_forming",
            "interval=1m&limit=2&closed_only=false&symbols=BTCUSDT&with_evidence=true",
        ),
        (
            "get_false",
            "interval=1m&limit=2&symbols=BTCUSDT,ETHUSDT&with_evidence=false",
        ),
        (
            "get_empty",
            "interval=1m&limit=2&symbols=BTCUSDT,ETHUSDT&with_evidence=",
        ),
    ];
    for (label, query) in gets {
        sim.get(label, &bulk(query)).await;
    }
    sim.http(
        "known_exception:post_true",
        "POST",
        "/fapi/v1/klines/bulk?with_evidence=true",
        Some(r#"{"interval":"1m","limit":2,"symbols":["BTCUSDT"]}"#),
    )
    .await;
    sim.get(
        "after/plain",
        &bulk("interval=1m&limit=2&symbols=BTCUSDT,ETHUSDT"),
    )
    .await;
    Ok(())
}

//! Real serve startup/shutdown with exclusively local HTTP and WS peers.
use axum::{Json, Router, routing::get};
use futures_util::{SinkExt, StreamExt};
use kline_core::{Bar, Interval, Market, NumberType, evidence::EvidenceUpdate};
use kline_runtime::{evidence_state::StateDirectory, storage::Store};
use kline_service::{
    Catalog, Clock, Engine, Instrument, Settings, SystemClock, evidence::EvidenceSettings,
};
use serde_json::{Value, json};
use std::{
    num::NonZeroUsize,
    process::{Child, Command, Stdio},
    sync::Arc,
    time::Duration,
};
struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
#[tokio::test]
async fn serve_loads_before_discover_and_http_and_holds_lock_through_final_persistence() {
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("state");
    std::fs::create_dir(&state).unwrap();
    let rest_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let rest_root = format!("http://{}", rest_listener.local_addr().unwrap());
    let app=Router::new().route("/fapi/v1/exchangeInfo",get(||async{Json(json!({"symbols":[{"symbol":"BTCUSDT","pair":"BTCUSDT","contractType":"PERPETUAL","status":"TRADING"}]}))})).route("/fapi/v1/klines",get(||async {std::future::pending::<Json<Value>>().await}));
    let rest_task = tokio::spawn(async move {
        axum::serve(rest_listener, app).await.unwrap();
    });
    let ws_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws_root = format!("ws://{}/stream", ws_listener.local_addr().unwrap());
    let (ws_commands, _) = tokio::sync::broadcast::channel::<String>(16);
    let ws_sender = ws_commands.clone();
    let ws_task = tokio::spawn(async move {
        while let Ok((socket, _)) = ws_listener.accept().await {
            let mut commands = ws_sender.subscribe();
            tokio::spawn(async move {
                if let Ok(mut ws) = tokio_tungstenite::accept_async(socket).await {
                    loop {
                        tokio::select! {
                            frame=ws.next()=>match frame {Some(Ok(tokio_tungstenite::tungstenite::Message::Ping(bytes)))=>{let _=ws.send(tokio_tungstenite::tungstenite::Message::Pong(bytes)).await;},Some(Ok(_))=>{},_=>break},
                            command=commands.recv()=>match command {Ok(command)=>{if ws.send(tokio_tungstenite::tungstenite::Message::Text(command.into())).await.is_err(){break;}},Err(_)=>break},
                        }
                    }
                }
            });
        }
    });
    let now = SystemClock { offset_ms: 0 }.now_ms();
    let since = now - 5000;
    std::fs::write(state.join("retained.json"),json!([{"symbol":"OLDUSDT","pair":"OLDUSDT","contract_type":"PERPETUAL","since":since,"expires_ms":now+14_460_000,"delivery_ms":since}]).to_string()).unwrap();
    let e = Engine::new(
        Catalog::new(vec![Instrument {
            market: Market::Future,
            symbol: "OLDUSDT".into(),
            interval: Interval::parse("1m").unwrap(),
            trading: false,
            continuous: Some(("OLDUSDT".into(), "PERPETUAL".into())),
            capacity: NonZeroUsize::new(241).unwrap(),
        }])
        .unwrap(),
        Arc::new(SystemClock { offset_ms: 0 }),
        Settings {
            number_type: NumberType::String,
            evidence: Some(EvidenceSettings::default()),
            ..Settings::default()
        },
    );
    let open = now.div_euclid(60000) * 60000 - 60000;
    let mut b = Bar {
        open_time: open,
        close_time: open + 59999,
        ..Bar::default()
    };
    b.set_numbers(NumberType::String, ["1"; 8]).unwrap();
    e.commit_evidence(0, EvidenceUpdate::Rest(b)).unwrap();
    let mut store = Store::new(&state, 1).unwrap();
    assert_eq!(store.dump(&e, || true).written, 1);
    let reserved = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let listen = reserved.local_addr().unwrap();
    drop(reserved);
    let path = dir.path().join("config.json");
    std::fs::write(&path,json!({"listen":listen.to_string(),"number_type":"string","history_capacity":241,"evidence":{"ws_trust_window_ms":60000},"final_wait_ms":1,"bulk_admission_wait_ms":100,"http_admission_wait_ms":100,"bulk_pre_boundary_wait_ms":0,"subscriptions":[{"market":"future","interval":"1m","continuous":true,"history_capacity":241}],"rest":{"future_url":rest_root,"spot_url":rest_root,"latest_repair_workers":2,"sync_clock":false},"websocket":{"future_url":ws_root,"spot_url":ws_root},"market_api":{"enabled":false},"persistence":{"directory":state,"boundary_guard_before_ms":2000,"boundary_guard_after_ms":15000,"interval_seconds":300}}).to_string()).unwrap();
    let mut child = ChildGuard(
        Command::new(env!("CARGO_BIN_EXE_kline-proxy"))
            .arg(&path)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let client = reqwest::Client::new();
    let uri = format!(
        "http://{listen}/fapi/v1/klines/bulk?interval=1m&limit=5&symbols=OLDUSDT,BTCUSDT,UNKNOWN&with_evidence=true"
    );
    let body = tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            assert!(
                child.0.try_wait().unwrap().is_none(),
                "serve exited before HTTP"
            );
            if let Ok(response) = client.get(&uri).send().await
                && response.status().is_success()
            {
                break serde_json::from_slice::<Value>(&response.bytes().await.unwrap()).unwrap();
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(body["contract"], "kp-evidence-v1");
    assert_eq!(body["status"]["OLDUSDT"]["state"], "not_trading");
    assert_eq!(body["status"]["OLDUSDT"]["since"], since);
    assert_eq!(body["status"]["BTCUSDT"]["state"], "trading");
    assert_eq!(body["status"]["UNKNOWN"]["state"], "unknown");
    assert_eq!(body["ws_final_trusted"], false);
    assert!(
        body["evidence"]["OLDUSDT"]
            .as_array()
            .unwrap()
            .iter()
            .all(|v| v == "pending")
    );
    assert!(!body["klines"]["OLDUSDT"].as_array().unwrap().is_empty());
    let reset = Command::new(env!("CARGO_BIN_EXE_kline-proxy"))
        .arg("--reset-breaker")
        .arg(&path)
        .output()
        .unwrap();
    assert!(!reset.status.success());
    assert!(String::from_utf8_lossy(&reset.stderr).contains("locked"));
    tokio::time::timeout(Duration::from_secs(5), async {
        while ws_commands.receiver_count() == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let frame_open = SystemClock { offset_ms: 0 }.now_ms().div_euclid(60000) * 60000 - 60000;
    for (event, value) in [(1, "1"), (2, "2")] {
        ws_commands.send(json!({"e":"continuous_kline","E":event,"ps":"BTCUSDT","ct":"PERPETUAL","k":{"t":frame_open,"T":frame_open+59999,"i":"1m","o":value,"h":value,"l":value,"c":value,"v":"1","q":"1","n":1,"V":"1","Q":"1","x":true}}).to_string()).unwrap();
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let bytes = client
                .get(&uri)
                .send()
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            let body: Value = serde_json::from_slice(&bytes).unwrap();
            if body["revised_total"] == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(
        Command::new("/bin/kill")
            .arg("-TERM")
            .arg(child.0.id().to_string())
            .status()
            .unwrap()
            .success()
    );
    let status = tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            if let Some(status) = child.0.try_wait().unwrap() {
                break status;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    assert!(status.success());
    let lock = StateDirectory::lock(&state).unwrap();
    assert!(state.join("evidence.lock").is_file());
    let breaker: Value =
        serde_json::from_slice(&std::fs::read(state.join("breaker.json")).unwrap()).unwrap();
    assert!(breaker["last_conflict_ms"].as_i64().unwrap() >= now);
    assert_eq!(breaker["revised_total"], 1);
    let retained: Value =
        serde_json::from_slice(&std::fs::read(state.join("retained.json")).unwrap()).unwrap();
    assert_eq!(retained[0]["since"], since);
    drop(lock);
    let reset = Command::new(env!("CARGO_BIN_EXE_kline-proxy"))
        .arg("--reset-breaker")
        .arg(&path)
        .output()
        .unwrap();
    assert!(reset.status.success());
    let breaker: Value =
        serde_json::from_slice(&std::fs::read(state.join("breaker.json")).unwrap()).unwrap();
    assert_eq!(breaker["last_conflict_ms"], 0);
    assert_eq!(breaker["revised_total"], 1);
    std::fs::write(state.join("retained.json"), b"unreadable").unwrap();
    let refusal = Command::new(env!("CARGO_BIN_EXE_kline-proxy"))
        .arg(&path)
        .output()
        .unwrap();
    assert!(!refusal.status.success());
    assert!(String::from_utf8_lossy(&refusal.stderr).contains("retained.json"));
    std::fs::write(state.join("breaker.json"), b"bad breaker").unwrap();
    let reset = Command::new(env!("CARGO_BIN_EXE_kline-proxy"))
        .arg("--reset-breaker")
        .arg(&path)
        .output()
        .unwrap();
    assert!(reset.status.success());
    assert!(String::from_utf8_lossy(&reset.stderr).contains("old revised_total unreadable"));
    rest_task.abort();
    ws_task.abort();
}

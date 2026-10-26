use futures_util::{SinkExt, StreamExt};
use kline_core::{Interval, Market};
use kline_service::{Catalog, Engine, Instrument, Settings, SystemClock};
use std::{num::NonZeroUsize, sync::Arc, time::Duration};
use tokio_tungstenite::{accept_async, tungstenite::Message};

fn engine() -> Arc<Engine> {
    Engine::new(
        Catalog::new(vec![Instrument {
            market: Market::Spot,
            symbol: "BTCUSDT".into(),
            interval: Interval::parse("1h").unwrap(),
            trading: true,
            continuous: None,
            capacity: NonZeroUsize::new(1000).unwrap(),
        }])
        .unwrap(),
        Arc::new(SystemClock { offset_ms: 0 }),
        Settings::default(),
    )
}

#[test]
fn tls_has_an_explicit_available_crypto_provider() {
    let _ = rustls::ClientConfig::builder()
        .with_root_certificates(rustls::RootCertStore::empty())
        .with_no_client_auth();
}

#[tokio::test]
async fn topic_resubscription_preserves_the_connection_epoch_and_other_data() {
    use kline_service::upstream::{StreamStatus, run_controlled};
    use std::sync::atomic::Ordering;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let engine = engine();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        let frame = |t| {
            serde_json::json!({"e":"kline","s":"BTCUSDT","E":1,"k":{"t":t,"T":t+3_599_999,"i":"1h","o":"1","h":"1","l":"1","c":"1","v":"1","q":"1","V":"1","Q":"1","n":1,"x":true}}).to_string()
        };
        socket.send(Message::Text(frame(0).into())).await.unwrap();
        for method in ["UNSUBSCRIBE", "SUBSCRIBE"] {
            let message = tokio::time::timeout(Duration::from_secs(3), socket.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let value: serde_json::Value =
                serde_json::from_str(message.to_text().unwrap()).unwrap();
            assert_eq!(value["method"], method);
            assert_eq!(value["params"], serde_json::json!(["ethusdt@kline_1h"]));
            socket
                .send(Message::Text(
                    serde_json::json!({"result":null,"id":value["id"]})
                        .to_string()
                        .into(),
                ))
                .await
                .unwrap();
        }
        socket
            .send(Message::Text(frame(3_600_000).into()))
            .await
            .unwrap();
        let _ = socket.next().await;
    });
    let status = Arc::new(StreamStatus::default());
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let (commands, receiver) = tokio::sync::mpsc::channel(2);
    let task = tokio::spawn(run_controlled(
        engine.clone(),
        Market::Spot,
        url,
        stop_rx,
        status.clone(),
        Some(receiver),
    ));
    tokio::time::timeout(Duration::from_secs(3), async {
        while status.mapped_epoch.load(Ordering::Acquire) != 1 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    commands
        .send(vec!["ethusdt@kline_1h".into()])
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while engine.catalog.slot(0).get(3_600_000).is_none() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    assert!(engine.catalog.slot(0).get(0).unwrap().1);
    assert!(status.connected.load(Ordering::Acquire));
    assert_eq!(status.epoch.load(Ordering::Acquire), 1);
    assert_eq!(status.mapped_epoch.load(Ordering::Acquire), 1);
    stop_tx.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn real_socket_ingestion_responds_to_ping_and_stops_cleanly() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let engine = engine();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        socket
            .send(Message::Ping(vec![1, 2, 3].into()))
            .await
            .unwrap();
        socket.send(Message::Text(r#"{"e":"kline","s":"BTCUSDT","E":1,"k":{"t":0,"T":3599999,"i":"1h","o":"1","h":"1","l":"1","c":"1","v":"1","q":"1","V":"1","Q":"1","n":1,"x":true}}"#.into())).await.unwrap();
        let message = tokio::time::timeout(Duration::from_secs(1), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(matches!(message, Message::Pong(_)));
        let _ = socket.next().await;
    });
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(kline_service::upstream::run(
        engine.clone(),
        Market::Spot,
        url,
        stop_rx,
    ));
    tokio::time::timeout(Duration::from_secs(1), async {
        while engine.catalog.slot(0).is_empty() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    assert!(engine.catalog.slot(0).get(0).unwrap().1);
    stop_tx.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn reconnect_requires_a_new_mapped_frame_for_its_connection_epoch() {
    use kline_service::upstream::{StreamStatus, run_monitored};
    use std::sync::atomic::Ordering;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let (control_tx, mut control_rx) = tokio::sync::mpsc::channel::<()>(2);
    let server = tokio::spawn(async move {
        for _ in 0..2 {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();
            // The test observes a connected socket before permitting a mapped frame.
            control_rx.recv().await.unwrap();
            socket.send(Message::Text(r#"{"e":"kline","s":"BTCUSDT","E":1,"k":{"t":0,"T":3599999,"i":"1h","o":"1","h":"1","l":"1","c":"1","v":"1","q":"1","V":"1","Q":"1","n":1,"x":true}}"#.into())).await.unwrap();
            control_rx.recv().await.unwrap();
            let _ = socket.close(None).await;
        }
    });
    let status = Arc::new(StreamStatus::default());
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(run_monitored(
        engine(),
        Market::Spot,
        url,
        stop_rx,
        status.clone(),
    ));
    for expected in [1, 3] {
        tokio::time::timeout(Duration::from_secs(3), async {
            while !status.connected.load(Ordering::Acquire)
                || status.epoch.load(Ordering::Acquire) != expected
            {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        assert_ne!(status.mapped_epoch.load(Ordering::Acquire), expected);
        control_tx.send(()).await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while status.mapped_epoch.load(Ordering::Acquire) != expected {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        control_tx.send(()).await.unwrap();
    }
    stop_tx.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();
    assert!(!status.connected.load(Ordering::Acquire));
}

#[tokio::test]
async fn resubscriptions_are_merged_and_paced_per_connection() {
    use kline_service::upstream::{RESUBSCRIBE_INTERVAL, StreamStatus, run_controlled};
    use std::sync::atomic::Ordering;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let (first_pair_tx, first_pair_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        socket.send(Message::Text(r#"{"e":"kline","s":"BTCUSDT","E":1,"k":{"t":0,"T":3599999,"i":"1h","o":"1","h":"1","l":"1","c":"1","v":"1","q":"1","V":"1","Q":"1","n":1,"x":true}}"#.into())).await.unwrap();
        // (method, sorted topics, when received) for every control message within 3 seconds.
        let mut received = vec![];
        let mut first_pair_tx = Some(first_pair_tx);
        let until = tokio::time::Instant::now() + Duration::from_secs(3);
        while let Ok(Some(Ok(message))) = tokio::time::timeout_at(until, socket.next()).await {
            let Message::Text(text) = message else {
                continue;
            };
            let value: serde_json::Value = serde_json::from_str(&text).unwrap();
            let mut topics: Vec<String> = serde_json::from_value(value["params"].clone()).unwrap();
            topics.sort();
            received.push((
                value["method"].as_str().unwrap().to_owned(),
                topics,
                std::time::Instant::now(),
            ));
            if received.len() == 2 {
                first_pair_tx.take().unwrap().send(()).unwrap();
            }
        }
        received
    });
    let engine = engine();
    let status = Arc::new(StreamStatus::default());
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let (commands, receiver) = tokio::sync::mpsc::channel(2);
    let task = tokio::spawn(run_controlled(
        engine,
        Market::Spot,
        url,
        stop_rx,
        status.clone(),
        Some(receiver),
    ));
    tokio::time::timeout(Duration::from_secs(3), async {
        while status.mapped_epoch.load(Ordering::Acquire) != 1 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    commands.send(vec!["a@kline_1h".into()]).await.unwrap();
    first_pair_rx.await.unwrap();
    // Two more commands right after the first pair: they wait out the interval as one pair.
    commands.send(vec!["b@kline_1h".into()]).await.unwrap();
    commands
        .send(vec![
            "a@kline_1h".into(),
            "c@kline_1h".into(),
            "b@kline_1h".into(),
        ])
        .await
        .unwrap();
    let received = server.await.unwrap();
    stop_tx.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap();
    let methods: Vec<_> = received.iter().map(|(m, _, _)| m.as_str()).collect();
    assert_eq!(
        methods,
        ["UNSUBSCRIBE", "SUBSCRIBE", "UNSUBSCRIBE", "SUBSCRIBE"]
    );
    let topics = |i: usize| received[i].1.join(",");
    assert_eq!(topics(0), "a@kline_1h");
    assert_eq!(topics(1), "a@kline_1h");
    assert_eq!(topics(2), "a@kline_1h,b@kline_1h,c@kline_1h");
    assert_eq!(topics(3), "a@kline_1h,b@kline_1h,c@kline_1h");
    let gap = received[2].2 - received[1].2;
    assert!(
        gap >= RESUBSCRIBE_INTERVAL - Duration::from_millis(100),
        "second resubscription after {gap:?}"
    );
}

#[tokio::test]
async fn connection_attempts_take_a_turn_on_the_engine_pacer() {
    use kline_service::{connect_pacer::SPACING, upstream::StreamStatus};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let engine = engine();
    // Another attempt has just started: this connection must wait one spacing for its turn.
    engine.connect_pacer().turn().await;
    let turn_taken = std::time::Instant::now();
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(kline_service::upstream::run_controlled(
        engine,
        Market::Spot,
        url,
        stop_rx,
        Arc::new(StreamStatus::default()),
        None,
    ));
    let (_stream, _) = tokio::time::timeout(Duration::from_secs(3), listener.accept())
        .await
        .unwrap()
        .unwrap();
    let waited = turn_taken.elapsed();
    assert!(waited >= SPACING - Duration::from_millis(5), "{waited:?}");
    stop_tx.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn the_engine_records_connection_attempts_in_its_journal() {
    let path = std::env::temp_dir().join(format!(
        "kline-engine-connect-journal-{}.json",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&path);
    let engine = Engine::new(
        Catalog::new(vec![]).unwrap(),
        Arc::new(SystemClock { offset_ms: 0 }),
        Settings {
            connect_journal: Some(path.clone()),
            ..Settings::default()
        },
    );
    engine.connect_pacer().turn().await;
    let recorded: Vec<u64> = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    assert_eq!(recorded.len(), 1);
    std::fs::remove_file(path).unwrap();
}

// Keep the executor runnable while paused: network readiness must be drained before
// deliberately advancing the idle deadline (rather than relying on auto-advance).
async fn pump_io() {
    for _ in 0..1000 {
        tokio::task::yield_now().await;
    }
}

async fn idle_deadline_case(send_frame: bool) {
    use kline_service::upstream::{StreamStatus, run_controlled};
    use std::sync::atomic::{AtomicUsize, Ordering};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let e = engine();
    let status = Arc::new(StreamStatus::default());
    let received = Arc::new(AtomicUsize::new(0));
    let server_received = received.clone();
    let (frame_tx, mut frame_rx) = tokio::sync::mpsc::channel::<()>(1);
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        loop {
            tokio::select! {
                Some(()) = frame_rx.recv() => {
                    let frame = r#"{"e":"kline","s":"BTCUSDT","E":1,"k":{"t":0,"T":3599999,"i":"1h","o":"1","h":"1","l":"1","c":"1","v":"1","q":"1","V":"1","Q":"1","n":1,"x":true}}"#;
                    if socket.send(Message::Text(frame.into())).await.is_err() { break; }
                }
                message = socket.next() => match message {
                    Some(Ok(Message::Text(_))) => { server_received.fetch_add(1, Ordering::Relaxed); }
                    _ => break,
                }
            }
        }
    });
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let (commands, receiver) = tokio::sync::mpsc::channel(2);
    let task = tokio::spawn(run_controlled(
        e.clone(),
        Market::Spot,
        url,
        stop_rx,
        status.clone(),
        Some(receiver),
    ));
    tokio::time::timeout(Duration::from_secs(3), async {
        while !status.connected.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::pause();
    for _ in 0..5 {
        tokio::time::advance(Duration::from_secs(30)).await;
        commands.try_send(vec!["btcusdt@kline_1h".into()]).unwrap();
        pump_io().await;
    }
    assert_eq!(received.load(Ordering::Relaxed), 10);
    if send_frame {
        frame_tx.try_send(()).unwrap();
        pump_io().await;
        assert!(e.catalog.slot(0).get(0).is_some());
    }
    tokio::time::advance(Duration::from_secs(31)).await;
    pump_io().await;
    if send_frame {
        assert!(
            status.connected.load(Ordering::Acquire),
            "a real frame renews the idle deadline"
        );
        assert_eq!(status.epoch.load(Ordering::Acquire), 1);
        tokio::time::advance(Duration::from_secs(150)).await;
        pump_io().await;
    }
    assert!(
        status.epoch.load(Ordering::Acquire) >= 2,
        "silent input must disconnect despite outbound commands"
    );
    tokio::time::resume();
    stop_tx.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap();
    server.abort();
}

#[tokio::test]
async fn outbound_subscription_commands_do_not_renew_inbound_idle_deadline() {
    idle_deadline_case(false).await;
}

#[tokio::test]
async fn inbound_frames_renew_the_idle_deadline() {
    idle_deadline_case(true).await;
}

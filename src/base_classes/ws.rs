#![allow(dead_code)]

use crate::base_classes::ring_buffer::{Consumer, Producer};
use crate::base_classes::types::Ts;
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::thread::JoinHandle;
use std::time::Instant;

// Exchange-specific handler trait. Implement this per venue.
#[derive(Clone, Debug, Default)]
pub struct FeedSignal {
    inner: Arc<(Mutex<bool>, Condvar)>,
}

impl FeedSignal {
    #[inline(always)]
    pub fn new() -> Self {
        Self {
            inner: Arc::new((Mutex::new(false), Condvar::new())),
        }
    }

    #[inline(always)]
    pub fn notify(&self) {
        let (mutex, condvar) = &*self.inner;
        let mut flag = mutex.lock().unwrap();
        *flag = true;
        condvar.notify_one();
    }

    #[inline(always)]
    pub fn wait(&self) {
        let (mutex, condvar) = &*self.inner;
        let mut flag = mutex.lock().unwrap();
        while !*flag {
            flag = condvar.wait(flag).unwrap();
        }
        *flag = false;
    }
}

pub trait ExchangeHandler: Send + Sync + 'static {
    type Out: Send + 'static;

    // Target websocket URL (e.g., wss://stream.bybit.com/v5/public/linear)
    fn url(&self) -> &str;

    // Initial subscription messages to send after connect.
    fn initial_subscriptions(&self) -> &[String];

    // Parse inbound frames to output messages pushed to the ring buffer.
    fn parse_text(&self, text: &str, ts: Ts, recv_instant: Instant) -> Option<Self::Out>;
    fn parse_binary(&self, data: &[u8], ts: Ts, recv_instant: Instant) -> Option<Self::Out>;

    // Optional: application-level heartbeat (e.g., Binance JSON PING)
    fn app_heartbeat(&self) -> Option<AppHeartbeat> {
        None
    }

    // Optional: dynamic app heartbeat where payload is built at send-time (e.g., Gate requires time).
    // If provided, this takes precedence over the static variant above.
    fn app_heartbeat_interval(&self) -> Option<u64> {
        None
    }
    fn build_app_heartbeat(&self) -> Option<HeartbeatPayload> {
        None
    }

    // Optional: provide monotonically non-decreasing sequence for gating
    // Return (stream_key, seq). Messages with seq < last_seq for a key are dropped.
    #[inline(always)]
    fn sequence_key_text(&self, _text: &str) -> Option<(u64, u64)> {
        None
    }
    #[inline(always)]
    fn sequence_key_binary(&self, _data: &[u8]) -> Option<(u64, u64)> {
        None
    }

    // Human-readable label for logging (e.g., "binance:BTCUSDT").
    fn label(&self) -> String {
        self.url().to_string()
    }
}

// Fixed-size linear-probing map for last seen sequence per stream key.
struct SequenceGate<const M: usize> {
    keys: [u64; M],
    vals: [u64; M],
    used: [bool; M],
}

impl<const M: usize> SequenceGate<M> {
    #[inline(always)]
    fn new() -> Self {
        Self {
            keys: [0; M],
            vals: [0; M],
            used: [false; M],
        }
    }

    #[inline(always)]
    fn accept(&mut self, key: u64, seq: u64) -> bool {
        let mut idx = (key as usize) & (M - 1);
        let start = idx;
        loop {
            if !self.used[idx] {
                self.used[idx] = true;
                self.keys[idx] = key;
                self.vals[idx] = seq;
                return true;
            } else if self.keys[idx] == key {
                // Strictly increasing sequence; drop duplicates/out-of-order
                if seq > self.vals[idx] {
                    self.vals[idx] = seq;
                    return true;
                } else {
                    return false;
                }
            } else {
                idx = (idx + 1) & (M - 1);
                if idx == start {
                    return true;
                } // table full, accept
            }
        }
    }
}

#[derive(Clone, Debug)]
pub enum HeartbeatPayload {
    Text(String),
    Binary(Vec<u8>),
}

#[derive(Clone, Debug)]
pub struct AppHeartbeat {
    pub interval_secs: u64,
    pub payload: HeartbeatPayload,
}

// Spawns a dedicated WS thread, optionally pinned to a core, returning a Consumer for messages.
pub fn spawn_ws_worker<E, const N: usize>(
    handler: E,
    core_id: Option<usize>,
    signal: Option<FeedSignal>,
) -> (Consumer<E::Out, N>, JoinHandle<()>)
where
    E: ExchangeHandler,
{
    let (producer, consumer) = Producer::<E::Out, N>::new_pair();
    let producer_signal = signal.clone();
    let handle = std::thread::Builder::new()
        .name("ws-worker".into())
        .spawn(move || {
            if let Some(core) = core_id {
                pin_to_core(core);
            }
            // Choose implementation depending on feature flags.
            #[cfg(feature = "ws_tungstenite")]
            {
                run_ws_tungstenite(handler, producer, producer_signal);
                return;
            }

            #[cfg(all(not(feature = "ws_tungstenite"), feature = "ws_fast"))]
            {
                run_ws_fast(handler, producer, producer_signal);
                return;
            }

            #[cfg(all(not(feature = "ws_tungstenite"), not(feature = "ws_fast")))]
            {
                // Fallback no-op when no WS backend is enabled.
                let _ = producer; // silence unused in non-feature builds
                let _ = handler;
                let _ = signal;
                eprintln!("No websocket feature enabled (enable 'ws_tungstenite' or 'ws_fast').");
            }
        })
        .expect("failed to spawn ws-worker thread");

    (consumer, handle)
}

#[inline(always)]
fn now_ts_ns() -> Ts {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    static CLOCK_BASE: OnceLock<(SystemTime, Instant)> = OnceLock::new();
    let (base_system, base_instant) =
        CLOCK_BASE.get_or_init(|| (SystemTime::now(), Instant::now()));

    let elapsed = Instant::now().saturating_duration_since(*base_instant);
    let monotonic_system = base_system.checked_add(elapsed).unwrap_or(*base_system);

    monotonic_system
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_nanos() as Ts
}

#[inline(always)]
fn now_secs() -> u64 {
    now_ts_ns() / 1_000_000_000
}

fn gate_ping_reply(text: &str) -> Option<String> {
    if !text.contains("\"channel\":\"futures.ping\"") {
        return None;
    }
    match find_json_string(text, "event") {
        Some(ev) if ev == "ping" => {}
        _ => return None,
    }
    let time = find_json_u64(text, "time").unwrap_or_else(now_secs);
    Some(format!(
        "{{\"time\":{time},\"channel\":\"futures.pong\",\"event\":\"pong\"}}"
    ))
}

fn find_json_string<'a>(s: &'a str, key: &str) -> Option<&'a str> {
    let k = format!("\"{}\"", key);
    let pos = s.find(&k)?;
    let rest = &s[pos + k.len()..];
    let colon = rest.find(':')?;
    let rest = &rest[colon + 1..];
    let q = rest.find('"')?;
    let rest2 = &rest[q + 1..];
    let end = rest2.find('"')?;
    Some(&rest2[..end])
}

fn find_json_u64(s: &str, key: &str) -> Option<u64> {
    let k = format!("\"{}\":", key);
    let pos = s.find(&k)?;
    let rest = &s[pos + k.len()..];
    let mut v: u64 = 0;
    let mut f = false;
    for ch in rest.bytes() {
        if ch.is_ascii_digit() {
            f = true;
            v = v.saturating_mul(10).saturating_add((ch - b'0') as u64);
        } else if f {
            break;
        } else if ch == b' ' {
            continue;
        } else {
            return None;
        }
    }
    if f { Some(v) } else { None }
}

// Linux-only core pin; no-op elsewhere.
#[allow(unused_variables)]
#[cfg(all(target_os = "linux", feature = "affinity"))]
fn pin_to_core(core_idx: usize) {
    unsafe {
        let pid = libc::pthread_self();
        let mut set: libc::cpu_set_t = core::mem::zeroed();
        libc::CPU_SET(core_idx, &mut set);
        let rc = libc::pthread_setaffinity_np(pid, core::mem::size_of::<libc::cpu_set_t>(), &set);
        if rc != 0 {
            eprintln!("pthread_setaffinity_np failed: {}", rc);
        }
    }
}

#[cfg(not(all(target_os = "linux", feature = "affinity")))]
fn pin_to_core(_core_idx: usize) {
    // no-op on non-Linux or when 'affinity' feature is disabled
}

// ---- tungstenite (sync) backend ----
#[cfg(feature = "ws_tungstenite")]
fn run_ws_tungstenite<E, const N: usize>(
    handler: E,
    producer: Producer<E::Out, N>,
    signal: Option<FeedSignal>,
) where
    E: ExchangeHandler,
{
    use std::time::{Duration, Instant};
    use tungstenite::{Message, connect};
    let url = handler.url().to_string();
    let label = handler.label();
    let initial_backoff = Duration::from_millis(250);
    let max_backoff = Duration::from_millis(3_000);
    let mut backoff = initial_backoff;
    loop {
        // connect
        let (mut socket, _response) = match connect(url::Url::parse(&url).unwrap()) {
            Ok(ok) => ok,
            Err(err) => {
                eprintln!(
                    "WS[{label}] connect error: {err}; reconnecting in {}ms",
                    backoff.as_millis()
                );
                std::thread::sleep(backoff);
                backoff = std::cmp::min(backoff * 2, max_backoff);
                continue;
            }
        };
        backoff = initial_backoff; // reset

        // Seq gate per-session
        const SEQ_SLOTS: usize = 256;
        let mut seq_gate: SequenceGate<SEQ_SLOTS> = SequenceGate::new();

        // initial subscriptions
        for sub in handler.initial_subscriptions() {
            if let Err(e) = socket.send(Message::Text(sub.clone())) {
                eprintln!("WS[{label}] send error: {e}");
            }
        }

        // Heartbeats
        let hb_interval = Duration::from_secs(20);
        let mut last_hb;
        let _ = socket.send(Message::Ping(Vec::new()));
        last_hb = Instant::now();

        let static_app_hb = handler.app_heartbeat();
        let dyn_app_hb_interval = handler.app_heartbeat_interval();
        let mut last_app_hb = Instant::now();
        if let Some(_) = dyn_app_hb_interval {
            if let Some(payload) = handler.build_app_heartbeat() {
                match payload {
                    HeartbeatPayload::Text(s) => {
                        let _ = socket.send(Message::Text(s));
                    }
                    HeartbeatPayload::Binary(b) => {
                        let _ = socket.send(Message::Binary(b));
                    }
                }
                last_app_hb = Instant::now();
            }
        } else if let Some(hb) = &static_app_hb {
            match &hb.payload {
                HeartbeatPayload::Text(s) => {
                    let _ = socket.send(Message::Text(s.clone()));
                }
                HeartbeatPayload::Binary(b) => {
                    let _ = socket.send(Message::Binary(b.clone()));
                }
            }
            last_app_hb = Instant::now();
        }

        // Session loop
        loop {
            if last_hb.elapsed() >= hb_interval {
                let _ = socket.send(Message::Ping(Vec::new()));
                last_hb = Instant::now();
            }
            if let Some(interval) = dyn_app_hb_interval {
                if last_app_hb.elapsed() >= Duration::from_secs(interval) {
                    if let Some(payload) = handler.build_app_heartbeat() {
                        match payload {
                            HeartbeatPayload::Text(s) => {
                                let _ = socket.send(Message::Text(s));
                            }
                            HeartbeatPayload::Binary(b) => {
                                let _ = socket.send(Message::Binary(b));
                            }
                        }
                    }
                    last_app_hb = Instant::now();
                }
            } else if let Some(hb) = &static_app_hb {
                if last_app_hb.elapsed() >= Duration::from_secs(hb.interval_secs) {
                    match &hb.payload {
                        HeartbeatPayload::Text(s) => {
                            let _ = socket.send(Message::Text(s.clone()));
                        }
                        HeartbeatPayload::Binary(b) => {
                            let _ = socket.send(Message::Binary(b.clone()));
                        }
                    }
                    last_app_hb = Instant::now();
                }
            }

            match socket.read() {
                Ok(msg) => {
                    let ts = now_ts_ns();
                    match msg {
                        Message::Text(txt) => {
                            if let Some(reply) = gate_ping_reply(&txt) {
                                let _ = socket.send(Message::Text(reply));
                                continue;
                            }
                            if let Some((k, s)) = handler.sequence_key_text(&txt) {
                                if !seq_gate.accept(k, s) {
                                    continue;
                                }
                            }
                            let recv_instant = Instant::now();
                            if let Some(out) = handler.parse_text(&txt, ts, recv_instant) {
                                producer.push_spin(out);
                                if let Some(ref signal) = signal {
                                    signal.notify();
                                }
                            }
                        }
                        Message::Binary(bin) => {
                            if let Some((k, s)) = handler.sequence_key_binary(&bin) {
                                if !seq_gate.accept(k, s) {
                                    continue;
                                }
                            }
                            let recv_instant = Instant::now();
                            if let Some(out) = handler.parse_binary(&bin, ts, recv_instant) {
                                producer.push_spin(out);
                                if let Some(ref signal) = signal {
                                    signal.notify();
                                }
                            }
                        }
                        Message::Ping(p) => {
                            let _ = socket.send(Message::Pong(p));
                        }
                        Message::Pong(_) => {
                            last_hb = Instant::now();
                        }
                        Message::Close(_) => break,
                        _ => {}
                    }
                }
                Err(e) => {
                    eprintln!(
                        "WS[{label}] read error: {e}; reconnecting in {}ms",
                        backoff.as_millis()
                    );
                    std::thread::sleep(backoff);
                    backoff = std::cmp::min(backoff * 2, max_backoff);
                    backoff += Duration::from_millis(25);
                    break; // reconnect
                }
            }
        }
    }
}

// ---- fastwebsockets (async) backend ----
#[cfg(all(feature = "ws_fast", not(feature = "ws_tungstenite")))]
fn run_ws_fast<E, const N: usize>(handler: E, producer: Producer<E::Out, N>)
where
    E: ExchangeHandler,
{
    use bytes::Bytes;
    use fastwebsockets::handshake::client::{Client, IntoClientRequest};
    use fastwebsockets::{FragmentCollector, OpCode, Role, WebSocket};
    use http::Request;
    use hyper::client::conn::http1;
    use hyper::rt::Executor;
    use hyper_util::client::legacy::Client as HyperClient;
    use hyper_util::rt::TokioExecutor;
    use tokio::net::TcpStream;
    use tokio::runtime::Builder as RtBuilder;
    use tokio::time::{Duration, Instant, interval};

    let url = handler.url().to_string();

    let rt = RtBuilder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio rt");
    rt.block_on(async move {
        let initial_backoff = Duration::from_millis(250);
        let max_backoff = Duration::from_millis(3_000);
        let mut backoff = initial_backoff;
        loop {
            const SEQ_SLOTS: usize = 256;
            let mut seq_gate: SequenceGate<SEQ_SLOTS> = SequenceGate::new();

            let req: Request<()> = url.clone().into_client_request().unwrap();
            let uri = req.uri().clone();
            let host = uri.host().unwrap().to_string();
            let port = uri.port_u16().unwrap_or(443);
            let addr = format!("{host}:{port}");

            let stream = match TcpStream::connect(addr).await {
                Ok(s) => s,
                Err(e) => {
                    eprintln!(
                        "tcp[{label}] connect: {e}; reconnecting in {}ms",
                        backoff.as_millis()
                    );
                    tokio::time::sleep(backoff).await;
            backoff = std::cmp::min(backoff * 2, max_backoff);
            backoff += Duration::from_millis(25);
                    continue;
                }
            };

            let (mut sender, conn) = match http1::handshake(stream).await {
                Ok(ok) => ok,
                Err(e) => {
                    eprintln!(
                        "http[{label}] handshake: {e}; reconnecting in {}ms",
                        backoff.as_millis()
                    );
                    tokio::time::sleep(backoff).await;
                    backoff = std::cmp::min(backoff * 2, max_backoff);
                    backoff += Duration::from_millis(25);
                    continue;
                }
            };
            tokio::spawn(async move { if let Err(e) = conn.await { eprintln!("hyper[{label}] conn error: {e}"); } });

            let (ws, _) = match Client::new(&uri).client_handshake(&mut sender).await {
                Ok(ok) => ok,
                Err(e) => {
                    eprintln!(
                        "ws[{label}] handshake: {e}; reconnecting in {}ms",
                        backoff.as_millis()
                    );
                    tokio::time::sleep(backoff).await;
                    backoff = std::cmp::min(backoff * 2, max_backoff);
                    backoff += Duration::from_millis(25);
                    continue;
                }
            };
            backoff = initial_backoff;
            let mut ws = FragmentCollector::new(ws);

            // send initial subscriptions
            for sub in handler.initial_subscriptions() {
                if let Err(e) = ws.write_text(sub).await { eprintln!("[{label}] send sub: {e}"); }
            }

            // Heartbeat timers
            let mut hb = interval(Duration::from_secs(20));
            let static_app_hb = handler.app_heartbeat();
            let dyn_app_hb_interval = handler.app_heartbeat_interval();
            let mut app_hb_timer = dyn_app_hb_interval.map(|secs| interval(Duration::from_secs(secs)))
                .or_else(|| static_app_hb.as_ref().map(|hb| interval(Duration::from_secs(hb.interval_secs))));

            // initial app heartbeat
            if dyn_app_hb_interval.is_some() {
                if let Some(payload) = handler.build_app_heartbeat() {
                    match payload {
                        HeartbeatPayload::Text(s) => { let _ = ws.write_text(s).await; }
                        HeartbeatPayload::Binary(b) => { let _ = ws.write_binary(Bytes::from(b)).await; }
                    }
                }
            } else if let Some(hb) = &static_app_hb {
                match &hb.payload {
                    HeartbeatPayload::Text(s) => { let _ = ws.write_text(s.clone()).await; }
                    HeartbeatPayload::Binary(b) => { let _ = ws.write_binary(Bytes::from(b.clone())).await; }
                }
            }

            // Session loop
            loop {
                tokio::select! {
                    res = ws.read_frame() => {
                        match res {
                            Ok(frame) => {
                                let ts = now_ts_ns();
                                match frame.opcode {
                                    OpCode::Text => {
                                        if let Ok(s) = std::str::from_utf8(&frame.payload) {
                                            if let Some(reply) = gate_ping_reply(s) {
                                                if ws.write_text(reply).await.is_err() {
                                                    break;
                                                }
                                                continue;
                                            }
                                            if let Some((k, seq)) = handler.sequence_key_text(s) { if !seq_gate.accept(k, seq) { continue; } }
                                            let recv_instant = Instant::now();
                                            if let Some(out) = handler.parse_text(s, ts, recv_instant) {
                                                producer.push_spin(out);
                                                if let Some(ref signal) = signal {
                                                    signal.notify();
                                                }
                                            }
                                        }
                                    }
                                    OpCode::Binary => {
                                        if let Some((k, seq)) = handler.sequence_key_binary(&frame.payload) { if !seq_gate.accept(k, seq) { continue; } }
                                        let recv_instant = Instant::now();
                                        if let Some(out) = handler.parse_binary(&frame.payload, ts, recv_instant) {
                                            producer.push_spin(out);
                                            if let Some(ref signal) = signal {
                                                signal.notify();
                                            }
                                        }
                                    }
                                    OpCode::Close => break,
                                    OpCode::Ping => { let _ = ws.write_pong(Bytes::new()).await; }
                                    _ => {}
                                }
                            }
                            Err(e) => {
                                eprintln!(
                                    "ws[{label}] read: {e}; reconnecting in {}ms",
                                    backoff.as_millis()
                                );
                                tokio::time::sleep(backoff).await;
                                backoff = std::cmp::min(backoff * 2, max_backoff);
                                backoff += Duration::from_millis(25);
                                break;
                            }
                        }
                    }
                    _ = hb.tick() => { let _ = ws.write_ping(Bytes::new()).await; }
                    _ = async { if let Some(t) = &mut app_hb_timer { t.tick().await; } }, if app_hb_timer.is_some() => {
                        if dyn_app_hb_interval.is_some() {
                            if let Some(payload) = handler.build_app_heartbeat() {
                                match payload {
                                    HeartbeatPayload::Text(s) => { let _ = ws.write_text(s).await; }
                                    HeartbeatPayload::Binary(b) => { let _ = ws.write_binary(Bytes::from(b)).await; }
                                }
                            }
                        } else if let Some(hb) = &static_app_hb {
                            match &hb.payload {
                                HeartbeatPayload::Text(s) => { let _ = ws.write_text(s.clone()).await; }
                                HeartbeatPayload::Binary(b) => { let _ = ws.write_binary(Bytes::from(b.clone())).await; }
                            }
                        }
                    }
                }
            }
        }
    });
}

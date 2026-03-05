#![allow(dead_code)]

use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::{Mutex, Notify, mpsc, oneshot};
use tokio::task::spawn_blocking;
use tokio::time::{Instant, MissedTickBehavior, interval_at, sleep};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async_with_config, tungstenite::Message,
};

use crate::base_classes::state::{TradeDirection, state};
use crate::base_classes::types::Side;
use crate::exchanges::gate::rest;
use crate::exchanges::{endpoints::GateioWs, gate::signing};
use crate::utils::math::format_price;
use crate::utils::parsing::{
    extract_user_id, log_parse_drop, value_to_f64, value_to_string, value_to_u64,
};
use crate::utils::time::{current_unix_ms, current_unix_ts};

use super::gateway::ExecutionGateway;
use super::types::{
    ClientOrderId, ExchangeOrderId, ExecutionReport, OrderAck, OrderStatus, QuoteIntent,
};

/// Configuration required to establish a Gate.io private websocket session.
#[derive(Debug, Clone)]
pub struct GateWsConfig {
    pub api_key: String,
    pub api_secret: String,
    pub symbol: String,
    pub settle: Option<String>,
    pub ws_url: Option<String>,
    pub contract_size: Option<f64>,
}

/// Primary websocket execution gateway used by the order manager.
pub struct GateWsGateway {
    tx: mpsc::Sender<GatewayCommand>,
    reports: Arc<Mutex<Vec<ExecutionReport>>>,
    client_to_exchange: Arc<Mutex<HashMap<ClientOrderId, ExchangeOrderId>>>,
    report_notify: Arc<Notify>,
}

impl GateWsGateway {
    /// Connect to Gate websocket, perform login, and spawn the background worker.
    pub async fn connect(config: GateWsConfig) -> Result<Self> {
        let GateWsConfig {
            api_key,
            api_secret,
            symbol,
            settle,
            ws_url,
            contract_size,
        } = config;

        let contract_size = if let Some(size) = contract_size {
            size
        } else {
            let meta = rest::fetch_contract_meta_async(&symbol)
                .await
                .ok_or_else(|| anyhow!("failed to fetch Gate contract metadata"))?;
            meta.quanto_multiplier
                .ok_or_else(|| anyhow!("contract metadata missing quanto_multiplier"))?
        };

        let settle = settle.ok_or_else(|| anyhow!("missing settle currency for Gate WS"))?;
        let ws_url = ws_url.unwrap_or_else(GateWsGateway::base_ws_url);

        let worker_config = WorkerConfig {
            api_key,
            api_secret,
            symbol,
            settle,
            ws_url,
            contract_size,
        };

        let (tx, rx) = mpsc::channel(128);
        let reports = Arc::new(Mutex::new(Vec::new()));
        let client_to_exchange = Arc::new(Mutex::new(HashMap::new()));

        let report_notify = Arc::new(Notify::new());

        let worker = GateWsWorker::new(
            worker_config,
            rx,
            reports.clone(),
            client_to_exchange.clone(),
            report_notify.clone(),
        );
        tokio::spawn(async move {
            if let Err(err) = worker.run().await {
                eprintln!("Gate WS worker terminated: {:#}", err);
            }
        });

        Ok(Self {
            tx,
            reports,
            client_to_exchange,
            report_notify,
        })
    }

    #[inline]
    fn base_ws_url() -> String {
        "wss://fx-ws.gateio.ws/v4/ws".to_string()
    }

    /// Gracefully request the worker to shut down.
    pub async fn shutdown(&self) -> Result<()> {
        self.tx
            .send(GatewayCommand::Shutdown)
            .await
            .context("failed to send shutdown command")
    }
}

#[async_trait::async_trait]
impl ExecutionGateway for GateWsGateway {
    async fn submit(&self, intents: &[QuoteIntent]) -> Result<Vec<OrderAck>> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .send(GatewayCommand::Submit {
                intents: intents.to_vec(),
                resp: resp_tx,
            })
            .await
            .context("failed to enqueue submit command")?;
        resp_rx.await.context("submit response channel closed")?
    }

    async fn cancel_batch(&self, ids: &[ClientOrderId]) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .send(GatewayCommand::Cancel {
                ids: ids.iter().cloned().collect(),
                resp: resp_tx,
            })
            .await
            .context("failed to enqueue cancel command")?;
        resp_rx.await.context("cancel response channel closed")?
    }

    async fn poll_reports(&self) -> Result<Vec<ExecutionReport>> {
        loop {
            let (resp_tx, resp_rx) = oneshot::channel();
            self.tx
                .send(GatewayCommand::Poll { resp: resp_tx })
                .await
                .context("failed to enqueue poll command")?;
            let reports = resp_rx.await.context("poll response channel closed")??;
            if !reports.is_empty() {
                return Ok(reports);
            }
            self.report_notify.notified().await;
        }
    }
}

#[derive(Debug)]
struct WorkerConfig {
    api_key: String,
    api_secret: String,
    symbol: String,
    settle: String,
    ws_url: String,
    contract_size: f64,
}

enum GatewayCommand {
    Submit {
        intents: Vec<QuoteIntent>,
        resp: oneshot::Sender<Result<Vec<OrderAck>>>,
    },
    Cancel {
        ids: Vec<ClientOrderId>,
        resp: oneshot::Sender<Result<()>>,
    },
    Poll {
        resp: oneshot::Sender<Result<Vec<ExecutionReport>>>,
    },
    Shutdown,
}

enum PendingRequest {
    Submit {
        intents: Vec<QuoteIntent>,
        resp_tx: oneshot::Sender<Result<Vec<OrderAck>>>,
    },
    Cancel {
        ids: Vec<ClientOrderId>,
        resp_tx: oneshot::Sender<Result<()>>,
    },
}

struct GateWsWorker {
    cfg: WorkerConfig,
    command_rx: mpsc::Receiver<GatewayCommand>,
    reports: Arc<Mutex<Vec<ExecutionReport>>>,
    client_to_exchange: Arc<Mutex<HashMap<ClientOrderId, ExchangeOrderId>>>,
    report_notify: Arc<Notify>,
    req_counter: u64,
    user_id: Option<String>,
}

impl GateWsWorker {
    fn new(
        cfg: WorkerConfig,
        command_rx: mpsc::Receiver<GatewayCommand>,
        reports: Arc<Mutex<Vec<ExecutionReport>>>,
        client_to_exchange: Arc<Mutex<HashMap<ClientOrderId, ExchangeOrderId>>>,
        report_notify: Arc<Notify>,
    ) -> Self {
        Self {
            cfg,
            command_rx,
            reports,
            client_to_exchange,
            report_notify,
            req_counter: 0,
            user_id: None,
        }
    }

    async fn handle_user_trades_message(&self, value: &Value) -> Result<()> {
        let event = value.get("event").and_then(|v| v.as_str()).unwrap_or("");
        if event != "update" {
            return Ok(());
        }

        if let Some(results) = value.get("result").and_then(|v| v.as_array()) {
            for trade in results {
                self.record_user_trade(trade).await?;
            }
        }
        Ok(())
    }

    async fn record_user_trade(&self, trade: &Value) -> Result<()> {
        let contract = trade.get("contract").and_then(|v| v.as_str()).unwrap_or("");
        if !contract.is_empty() && !contract.eq_ignore_ascii_case(&self.cfg.symbol) {
            return Ok(());
        }

        let price = trade.get("price").and_then(value_to_f64);
        let size_contracts = trade.get("size").and_then(value_to_f64).unwrap_or(0.0);
        let quantity = if size_contracts.abs() > f64::EPSILON {
            Some(size_contracts.abs() * self.cfg.contract_size)
        } else {
            None
        };
        let direction = if size_contracts > 0.0 {
            Some(TradeDirection::Buy)
        } else if size_contracts < 0.0 {
            Some(TradeDirection::Sell)
        } else {
            None
        };
        let fee = trade.get("fee").and_then(value_to_f64);
        let role = trade
            .get("role")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let text = trade
            .get("text")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let order_id = trade.get("order_id").and_then(value_to_string);
        let ts_ns = trade
            .get("create_time_ms")
            .and_then(value_to_u64)
            .map(|ms| ms.saturating_mul(1_000_000))
            .or_else(|| {
                trade
                    .get("create_time")
                    .and_then(value_to_u64)
                    .map(|secs| secs.saturating_mul(1_000_000_000))
            });

        let role_for_state = role.clone();
        let text_for_state = text.clone();
        let order_id_for_state = order_id.clone();
        spawn_blocking(move || {
            let mut st = match state().lock() {
                Ok(guard) => guard,
                Err(poisoned) => {
                    eprintln!(
                        "FATAL: State lock poisoned in Gate user trades handler: {}",
                        poisoned
                    );
                    eprintln!("This indicates a panic occurred while holding the state lock.");
                    eprintln!(
                        "User trade: price={:?}, contracts={}, order_id={:?}",
                        price, size_contracts, order_id_for_state
                    );
                    panic!("State lock poisoned - cannot process user trades safely");
                }
            };
            let snap = &mut st.gate.user_trade;
            snap.price = price;
            snap.contracts = Some(size_contracts);
            snap.quantity = quantity;
            snap.fee = fee;
            snap.role = role_for_state;
            snap.text = text_for_state;
            snap.order_id = order_id_for_state;
            snap.ts_ns = ts_ns;
            snap.direction = direction;
            snap.seq = snap.seq.wrapping_add(1);
        })
        .await
        .map_err(|e| anyhow!("user trade state update failed: {e}"))?;

        if let Some(ref text_id) = text {
            let client_id = ClientOrderId::new(text_id.clone());
            let known = {
                let guard = self.client_to_exchange.lock().await;
                guard.contains_key(&client_id)
            };

            if known {
                let fill_qty = quantity.unwrap_or(0.0);
                let mut guard = self.reports.lock().await;
                guard.push(ExecutionReport {
                    client_order_id: client_id,
                    exchange_order_id: order_id.map(ExchangeOrderId),
                    status: OrderStatus::PartiallyFilled,
                    filled_qty: fill_qty,
                    avg_fill_price: price,
                    ts: ts_ns,
                });
                drop(guard);
                self.report_notify.notify_one();
            }
        }

        Ok(())
    }

    async fn run(mut self) -> Result<()> {
        let initial_backoff = Duration::from_millis(250);
        let max_backoff = Duration::from_millis(3_000);
        let mut backoff = initial_backoff;
        loop {
            match self.establish().await {
                Ok((mut sink, mut stream)) => {
                    backoff = initial_backoff;
                    let mut pending: HashMap<String, PendingRequest> = HashMap::new();
                    let mut should_exit = false;
                    let heartbeat_period = Duration::from_secs(25);
                    let mut heartbeat =
                        interval_at(Instant::now() + heartbeat_period, heartbeat_period);
                    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
                    while !should_exit {
                        tokio::select! {
                            _ = heartbeat.tick() => {
                                if let Err(err) = self.send_heartbeat_ping(&mut sink).await {
                                    eprintln!("Gate WS heartbeat send failed: {:#}", err);
                                    self.fail_pending(&mut pending, anyhow!("heartbeat send failed: {err}"));
                                    break;
                                }
                            }
                            Some(cmd) = self.command_rx.recv() => {
                                should_exit = self.handle_command(cmd, &mut sink, &mut pending).await?;
                            }
                            maybe_msg = stream.next() => {
                                match maybe_msg {
                                    Some(Ok(msg)) => {
                                        if let Err(err) = self.handle_message(msg, &mut sink, &mut pending).await {
                                            eprintln!("Gate WS message handling error: {:#}", err);
                                        }
                                    }
                                    Some(Err(err)) => {
                                        eprintln!("Gate WS stream error: {:#}", err);
                                        self.fail_pending(&mut pending, anyhow!("connection error: {err}"));
                                        break;
                                    }
                                    None => {
                                        eprintln!("Gate WS stream closed by remote");
                                        self.fail_pending(&mut pending, anyhow!("connection closed"));
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    if should_exit {
                        return Ok(());
                    }
                }
                Err(err) => {
                    eprintln!("Gate WS connection failed: {:#}", err);
                }
            }
            sleep(backoff).await;
            backoff = std::cmp::min(backoff * 2, max_backoff);
            backoff += Duration::from_millis(25);
        }
    }

    async fn establish(
        &mut self,
    ) -> Result<(
        futures_util::stream::SplitSink<
            WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
            Message,
        >,
        futures_util::stream::SplitStream<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>>,
    )> {
        let url = format!(
            "{}/{}",
            self.cfg.ws_url.trim_end_matches('/'),
            self.cfg.settle
        );
        let (mut ws, _) = connect_async_with_config(&url, None, true)
            .await
            .with_context(|| format!("failed to connect to {}", url))?;
        ws.send(Message::Ping(Vec::new()))
            .await
            .context("failed to send initial ping")?;

        self.perform_login(&mut ws).await?;
        if let Err(err) = self.subscribe_user_trades(&mut ws).await {
            eprintln!("Gate WS user trade subscribe failed: {:#}", err);
        }
        let (sink, stream) = ws.split();
        Ok((sink, stream))
    }

    async fn perform_login(
        &mut self,
        ws: &mut WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
    ) -> Result<()> {
        let ts = current_unix_ts();
        let req_id = self.next_request_id("login");
        let request = build_api_request(&self.cfg, GateioWs::LOGIN, None, &req_id, ts)?;
        ws.send(Message::Text(request)).await?;

        loop {
            match ws.next().await {
                Some(Ok(Message::Text(text))) => {
                    let resp: WsResponse = serde_json::from_str(&text)
                        .with_context(|| format!("failed to parse login response: {}", text))?;
                    if resp.request_id.as_deref() == Some(&req_id) {
                        if resp.is_success() {
                            if self.user_id.is_none() {
                                match serde_json::from_str::<Value>(&text) {
                                    Ok(raw) => {
                                        if let Some(uid) = extract_user_id(&raw) {
                                            self.user_id = Some(uid);
                                        }
                                    }
                                    Err(err) => {
                                        log_parse_drop("gate_ws", "login_user_id", &err, &text);
                                    }
                                }
                            }
                            return Ok(());
                        }
                        let err_msg = resp
                            .error_message()
                            .unwrap_or_else(|| "login failed".to_string());
                        bail!(err_msg);
                    }
                }
                Some(Ok(Message::Ping(data))) => {
                    ws.send(Message::Pong(data))
                        .await
                        .context("failed to send pong during login")?;
                }
                Some(Ok(_)) => {}
                Some(Err(err)) => return Err(anyhow!("login stream error: {err}")),
                None => bail!("connection closed before login ack"),
            }
        }
    }

    async fn subscribe_user_trades(
        &mut self,
        ws: &mut WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
    ) -> Result<()> {
        let uid = if let Some(uid) = self.user_id.clone() {
            uid
        } else if let Ok(env_uid) = env::var("GATE_UID") {
            env_uid
        } else {
            return Err(anyhow!(
                "Gate login did not provide uid; set GATE_UID to enable user trade stream"
            ));
        };

        let ts = current_unix_ts();
        let sign = sign_subscribe(&self.cfg.api_secret, GateioWs::USER_TRADES, ts);
        let payload = json!({
            "time": ts,
            "channel": GateioWs::USER_TRADES,
            "event": "subscribe",
            "payload": [uid, "!all"],
            "auth": {
                "method": "api_key",
                "KEY": self.cfg.api_key,
                "SIGN": sign,
            }
        });

        ws.send(Message::Text(payload.to_string())).await?;
        Ok(())
    }

    async fn handle_command(
        &mut self,
        cmd: GatewayCommand,
        sink: &mut futures_util::stream::SplitSink<
            WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
            Message,
        >,
        pending: &mut HashMap<String, PendingRequest>,
    ) -> Result<bool> {
        match cmd {
            GatewayCommand::Submit { intents, resp } => {
                let req_param = match self.build_order_param(&intents) {
                    Ok(v) => v,
                    Err(err) => {
                        let _ = resp.send(Err(err));
                        return Ok(false);
                    }
                };
                let ts = current_unix_ts();
                let req_id = self.next_request_id("submit");
                let request = match build_api_request(
                    &self.cfg,
                    GateioWs::CREATE_BATCH_ORDER,
                    Some(req_param),
                    &req_id,
                    ts,
                ) {
                    Ok(req) => req,
                    Err(err) => {
                        let _ = resp.send(Err(err));
                        return Ok(false);
                    }
                };
                if let Err(err) = sink.send(Message::Text(request)).await {
                    let _ = resp.send(Err(anyhow!(err)));
                    return Ok(false);
                }
                pending.insert(
                    req_id,
                    PendingRequest::Submit {
                        intents,
                        resp_tx: resp,
                    },
                );
                Ok(false)
            }
            GatewayCommand::Cancel { ids, resp } => {
                if ids.is_empty() {
                    let _ = resp.send(Ok(()));
                } else {
                    self.send_cancel_request(ids, resp, sink, pending).await;
                }
                Ok(false)
            }
            GatewayCommand::Poll { resp } => {
                let reports = {
                    let mut guard = self.reports.lock().await;
                    guard.drain(..).collect::<Vec<_>>()
                };
                let _ = resp.send(Ok(reports));
                Ok(false)
            }
            GatewayCommand::Shutdown => {
                let _ = sink.send(Message::Close(None)).await;
                Ok(true)
            }
        }
    }

    async fn handle_message(
        &mut self,
        msg: Message,
        sink: &mut futures_util::stream::SplitSink<
            WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
            Message,
        >,
        pending: &mut HashMap<String, PendingRequest>,
    ) -> Result<()> {
        match msg {
            Message::Text(text) => {
                if text.contains(GateioWs::USER_TRADES) && text.contains("\"channel\"") {
                    if let Ok(raw) = serde_json::from_str::<Value>(&text) {
                        if raw
                            .get("channel")
                            .and_then(|v| v.as_str())
                            .map(|ch| ch == GateioWs::USER_TRADES)
                            .unwrap_or(false)
                        {
                            self.handle_user_trades_message(&raw).await?;
                            return Ok(());
                        }
                    }
                }

                let resp: WsResponse = match serde_json::from_str(&text) {
                    Ok(r) => r,
                    Err(err) => {
                        log_parse_drop("gate_ws", "ws_response", &err, &text);
                        return Ok(());
                    }
                };

                if resp.ack.unwrap_or(false) {
                    // ignore ack message; final response will follow
                    return Ok(());
                }

                if let Some(req_id) = resp.request_id.clone() {
                    if let Some(pending_req) = pending.remove(&req_id) {
                        match pending_req {
                            PendingRequest::Submit { intents, resp_tx } => {
                                match self.process_submit_response(intents, &resp).await {
                                    Ok((acks, reports)) => {
                                        {
                                            let mut guard = self.reports.lock().await;
                                            guard.extend(reports);
                                        }
                                        self.report_notify.notify_one();
                                        let _ = resp_tx.send(Ok(acks));
                                    }
                                    Err(err) => {
                                        let _ = resp_tx.send(Err(err));
                                    }
                                }
                            }
                            PendingRequest::Cancel { ids, resp_tx } => {
                                match self.process_cancel_response(ids, &resp).await {
                                    Ok(mut reports) => {
                                        if !reports.is_empty() {
                                            let mut guard = self.reports.lock().await;
                                            guard.append(&mut reports);
                                            drop(guard);
                                            self.report_notify.notify_one();
                                        }
                                        let _ = resp_tx.send(Ok(()));
                                    }
                                    Err(err) => {
                                        let _ = resp_tx.send(Err(err));
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Message::Ping(data) => {
                sink.send(Message::Pong(data))
                    .await
                    .context("failed to send pong")?;
            }
            Message::Pong(_) | Message::Binary(_) => {}
            Message::Close(_) => {
                bail!("websocket closed by remote");
            }
            Message::Frame(_) => {}
        }
        Ok(())
    }

    async fn process_submit_response(
        &mut self,
        intents: Vec<QuoteIntent>,
        resp: &WsResponse,
    ) -> Result<(Vec<OrderAck>, Vec<ExecutionReport>)> {
        if !resp.is_success() {
            let err = resp
                .error_message()
                .unwrap_or_else(|| "order batch rejected".to_string());
            return Err(anyhow!(err));
        }

        let payload = resp
            .data
            .as_ref()
            .and_then(|d| d.result.as_ref())
            .ok_or_else(|| anyhow!("missing result payload in order response"))?;

        let arr = payload
            .as_array()
            .ok_or_else(|| anyhow!("order batch result is not an array"))?;

        if arr.len() != intents.len() {
            return Err(anyhow!(
                "order batch result length mismatch: got {} entries, expected {}",
                arr.len(),
                intents.len()
            ));
        }

        let mut order_acks = Vec::with_capacity(arr.len());
        let mut reports = Vec::with_capacity(arr.len());
        for (entry, intent) in arr.iter().zip(intents.iter()) {
            let succeeded = entry
                .get("succeeded")
                .and_then(|v| v.as_bool())
                .ok_or_else(|| anyhow!("missing succeeded flag in order response"))?;

            if !succeeded {
                let message = entry
                    .get("message")
                    .and_then(|v| v.as_str())
                    .unwrap_or("order rejected");
                // Prefer to surface the raw API rejection so the caller can react.
                return Err(anyhow!(message.to_string()));
            }

            if let Some(text) = entry.get("text").and_then(|v| v.as_str()) {
                if text != intent.client_order_id.0 {
                    eprintln!(
                        "warning: order response text {} does not match client id {}",
                        text, intent.client_order_id
                    );
                }
            }

            let exchange_id = entry.get("id").and_then(value_to_string);
            match exchange_id {
                Some(ref eid) => {
                    let mut map_guard = self.client_to_exchange.lock().await;
                    map_guard.insert(intent.client_order_id.clone(), ExchangeOrderId(eid.clone()));
                }
                None => {}
            }

            order_acks.push(OrderAck {
                client_order_id: intent.client_order_id.clone(),
                exchange_order_id: exchange_id.clone().map(ExchangeOrderId),
            });

            let report = self.build_execution_report(entry, intent, exchange_id.clone());
            reports.push(report);
        }

        Ok((order_acks, reports))
    }
    async fn process_cancel_response(
        &mut self,
        ids: Vec<ClientOrderId>,
        resp: &WsResponse,
    ) -> Result<Vec<ExecutionReport>> {
        if !resp.is_success() {
            let err = resp
                .error_message()
                .unwrap_or_else(|| "cancel rejected".to_string());
            return Err(anyhow!(err));
        }

        let mut results_map: HashMap<String, String> = HashMap::new();
        if let Some(result) = resp
            .data
            .as_ref()
            .and_then(|d| d.result.as_ref())
            .and_then(|v| v.as_array())
        {
            for entry in result {
                if let Some(id_str) = entry.get("id").and_then(|v| v.as_str()) {
                    let msg = entry
                        .get("message")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    results_map.insert(id_str.to_string(), msg);
                }
            }
        }

        let mut map_guard = self.client_to_exchange.lock().await;
        let mut reports = Vec::with_capacity(ids.len());
        for cid in ids {
            map_guard.remove(&cid);
            let key = cid.to_string();
            let message = results_map.get(&key).cloned().unwrap_or_default();
            let status = if message.is_empty() || message.eq_ignore_ascii_case("success") {
                OrderStatus::Canceled
            } else if message.eq_ignore_ascii_case("order_not_exists")
                || message.eq_ignore_ascii_case("order_not_found")
                || message.eq_ignore_ascii_case("order_finished")
            {
                OrderStatus::Canceled
            } else {
                eprintln!(
                    "warning: cancel response for {} returned message '{}'",
                    key, message
                );
                OrderStatus::Unknown
            };

            reports.push(ExecutionReport {
                client_order_id: cid,
                exchange_order_id: None,
                status,
                filled_qty: 0.0,
                avg_fill_price: None,
                ts: None,
            });
        }
        drop(map_guard);

        Ok(reports)
    }

    async fn send_cancel_request(
        &mut self,
        ids: Vec<ClientOrderId>,
        resp_tx: oneshot::Sender<Result<()>>,
        sink: &mut futures_util::stream::SplitSink<
            WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
            Message,
        >,
        pending: &mut HashMap<String, PendingRequest>,
    ) {
        let texts: Vec<Value> = ids.iter().map(|id| Value::String(id.to_string())).collect();
        let req_param = Value::Array(texts);
        let ts = current_unix_ts();
        let req_id = self.next_request_id("cancel");
        let request = match build_api_request(
            &self.cfg,
            GateioWs::CANCEL_BATCH_ORDER_IDS,
            Some(req_param),
            &req_id,
            ts,
        ) {
            Ok(req) => req,
            Err(err) => {
                let _ = resp_tx.send(Err(err));
                return;
            }
        };

        if let Err(err) = sink.send(Message::Text(request)).await {
            let _ = resp_tx.send(Err(anyhow!(err)));
            return;
        }

        pending.insert(req_id, PendingRequest::Cancel { ids, resp_tx });
    }

    fn build_execution_report(
        &self,
        entry: &Value,
        intent: &QuoteIntent,
        exchange_id: Option<String>,
    ) -> ExecutionReport {
        let entry_sample = entry.to_string();
        let size_contracts = entry.get("size").and_then(value_to_f64).unwrap_or_else(|| {
            log_parse_drop("gate_ws", "missing_size", &"missing size", &entry_sample);
            0.0
        });
        let left_contracts = entry.get("left").and_then(value_to_f64).unwrap_or_else(|| {
            log_parse_drop("gate_ws", "missing_left", &"missing left", &entry_sample);
            0.0
        });
        let filled_contracts = (size_contracts - left_contracts).max(0.0);
        let filled_qty = filled_contracts * self.cfg.contract_size;

        let status_raw = entry
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or_else(|| {
                log_parse_drop(
                    "gate_ws",
                    "missing_status",
                    &"missing status",
                    &entry_sample,
                );
                "unknown"
            });
        let mut status = match status_raw {
            "finished" => match entry
                .get("finish_as")
                .and_then(|v| v.as_str())
                .unwrap_or("")
            {
                "filled" => OrderStatus::Filled,
                "cancelled" => OrderStatus::Canceled,
                _ => OrderStatus::Unknown,
            },
            "open" => {
                if filled_contracts > 0.0 {
                    OrderStatus::PartiallyFilled
                } else {
                    OrderStatus::New
                }
            }
            _ => OrderStatus::Unknown,
        };

        if matches!(status, OrderStatus::Unknown | OrderStatus::Canceled) {
            if let Some(label) = entry.get("label").and_then(|v| v.as_str()) {
                if matches!(label, "ORDER_POC_IMMEDIATE" | "ORDER_POST_ONLY_FAILED") {
                    status = OrderStatus::Rejected;
                }
            }
        }

        let avg_fill_price = entry
            .get("fill_price")
            .and_then(|v| v.as_str())
            .and_then(|s| match s.parse::<f64>() {
                Ok(v) if v.is_finite() => Some(v),
                Ok(_) => {
                    log_parse_drop(
                        "gate_ws",
                        "non_finite_fill_price",
                        &"non-finite fill_price",
                        s,
                    );
                    None
                }
                Err(err) => {
                    log_parse_drop("gate_ws", "fill_price", &err, s);
                    None
                }
            })
            .filter(|p| *p > 0.0);

        let ts = entry
            .get("update_time")
            .and_then(value_to_f64)
            .map(|secs| (secs * 1_000_000.0) as u64);

        ExecutionReport {
            client_order_id: intent.client_order_id.clone(),
            exchange_order_id: exchange_id.map(ExchangeOrderId),
            status,
            filled_qty,
            avg_fill_price,
            ts,
        }
    }

    fn build_order_param(&self, intents: &[QuoteIntent]) -> Result<Value> {
        let mut orders = Vec::with_capacity(intents.len());
        for intent in intents {
            let contracts = self.size_to_contracts(intent)?;
            let price = format_price(intent.price);
            orders.push(json!({
                "contract": self.cfg.symbol,
                "size": contracts,
                "price": price,
                "tif": intent.tif.to_string(),
                "text": intent.client_order_id.to_string(),
            }));
        }
        Ok(Value::Array(orders))
    }

    fn size_to_contracts(&self, intent: &QuoteIntent) -> Result<i64> {
        let raw = (intent.size.abs() / self.cfg.contract_size).round() as i64;
        if raw == 0 {
            bail!(
                "intent {} size {:.8} is below contract size {}",
                intent.client_order_id,
                intent.size,
                self.cfg.contract_size
            );
        }
        let signed = if intent.side == Side::Bid { raw } else { -raw };
        Ok(signed)
    }

    async fn send_heartbeat_ping(
        &self,
        sink: &mut futures_util::stream::SplitSink<
            WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
            Message,
        >,
    ) -> Result<()> {
        let ping = json!({
            "time": current_unix_ts(),
            "channel": GateioWs::PING,
            "event": "ping",
        });
        sink.send(Message::Text(ping.to_string())).await?;
        Ok(())
    }

    fn fail_pending(&mut self, pending: &mut HashMap<String, PendingRequest>, err: anyhow::Error) {
        for (_, pending_req) in pending.drain() {
            match pending_req {
                PendingRequest::Submit { resp_tx, .. } => {
                    let _ = resp_tx.send(Err(anyhow!(err.to_string())));
                }
                PendingRequest::Cancel { resp_tx, .. } => {
                    let _ = resp_tx.send(Err(anyhow!(err.to_string())));
                }
            }
        }
    }

    fn next_request_id(&mut self, prefix: &str) -> String {
        self.req_counter = self.req_counter.wrapping_add(1);
        format!("{}-{}-{}", prefix, current_unix_ms(), self.req_counter)
    }
}

#[derive(Debug, Deserialize)]
struct WsResponse {
    #[serde(rename = "request_id")]
    request_id: Option<String>,
    #[serde(default)]
    ack: Option<bool>,
    #[serde(default)]
    header: Option<ResponseHeader>,
    #[serde(default)]
    data: Option<ResponseData>,
}

impl WsResponse {
    fn is_success(&self) -> bool {
        let status_ok = self
            .header
            .as_ref()
            .and_then(|h| h.status.as_deref())
            .map(|s| s == "200")
            .unwrap_or(true);
        let err_empty = self.data.as_ref().map(|d| d.errs.is_none()).unwrap_or(true);
        status_ok && err_empty
    }

    fn error_message(&self) -> Option<String> {
        self.data.as_ref().and_then(|d| d.errs.as_ref()).map(|err| {
            let label = err.label.clone().unwrap_or_default();
            let message = err.message.clone().unwrap_or_default();
            format!(
                "{}{}{}",
                label,
                if label.is_empty() { "" } else { ": " },
                message
            )
        })
    }
}

#[derive(Debug, Deserialize)]
struct ResponseHeader {
    #[serde(default)]
    status: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ResponseData {
    #[serde(default)]
    result: Option<Value>,
    #[serde(default)]
    errs: Option<ResponseError>,
}

#[derive(Debug, Deserialize)]
struct ResponseError {
    #[serde(default)]
    label: Option<String>,
    #[serde(default)]
    message: Option<String>,
}

fn build_api_request(
    cfg: &WorkerConfig,
    channel: &str,
    req_param: Option<Value>,
    req_id: &str,
    ts: i64,
) -> Result<String> {
    let payload_str = req_param
        .as_ref()
        .map(|v| serde_json::to_string(v))
        .transpose()?
        .unwrap_or_else(String::new);

    let signature = sign_api(&cfg.api_secret, channel, &payload_str, ts);
    let mut payload = json!({
        "req_id": req_id,
        "timestamp": ts.to_string(),
        "api_key": cfg.api_key,
        "signature": signature,
    });

    if let Some(param) = req_param {
        payload["req_param"] = param;
    }

    let request = json!({
        "time": ts,
        "channel": channel,
        "event": "api",
        "payload": payload,
    });

    Ok(serde_json::to_string(&request)?)
}

fn sign_api(secret: &str, channel: &str, req_param: &str, ts: i64) -> String {
    let payload = format!("{}\n{}\n{}\n{}", "api", channel, req_param, ts);
    signing::hmac_sha512_hex(secret, &payload)
}

fn sign_subscribe(secret: &str, channel: &str, ts: i64) -> String {
    let payload = format!("channel={channel}&event=subscribe&time={ts}");
    signing::hmac_sha512_hex(secret, &payload)
}

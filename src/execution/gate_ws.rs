#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use futures_util::{SinkExt, StreamExt};
use hmac::{Hmac, Mac};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::Sha512;
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::time::sleep;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

use crate::base_classes::types::Side;
use crate::exchanges::endpoints::GateioWs;
use crate::exchanges::gate_rest;

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

impl GateWsConfig {
    fn settle(&self) -> &str {
        self.settle.as_deref().unwrap_or("usdt")
    }
}

/// Primary websocket execution gateway used by the order manager.
pub struct GateWsGateway {
    tx: mpsc::Sender<GatewayCommand>,
    reports: Arc<Mutex<Vec<ExecutionReport>>>,
    client_to_exchange: Arc<Mutex<HashMap<ClientOrderId, ExchangeOrderId>>>,
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
            let meta = gate_rest::fetch_contract_meta_async(&symbol)
                .await
                .ok_or_else(|| anyhow!("failed to fetch Gate contract metadata"))?;
            meta.quanto_multiplier
                .ok_or_else(|| anyhow!("contract metadata missing quanto_multiplier"))?
        };

        let settle = settle.unwrap_or_else(|| "usdt".to_string());
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

        let worker = GateWsWorker::new(
            worker_config,
            rx,
            reports.clone(),
            client_to_exchange.clone(),
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

    async fn cancel(&self, id: &ClientOrderId) -> Result<()> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .send(GatewayCommand::Cancel {
                id: id.clone(),
                resp: resp_tx,
            })
            .await
            .context("failed to enqueue cancel command")?;
        resp_rx.await.context("cancel response channel closed")?
    }

    async fn poll_reports(&self) -> Result<Vec<ExecutionReport>> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .send(GatewayCommand::Poll { resp: resp_tx })
            .await
            .context("failed to enqueue poll command")?;
        resp_rx.await.context("poll response channel closed")?
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
        id: ClientOrderId,
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
        id: ClientOrderId,
        resp_tx: oneshot::Sender<Result<()>>,
    },
}

struct GateWsWorker {
    cfg: WorkerConfig,
    command_rx: mpsc::Receiver<GatewayCommand>,
    reports: Arc<Mutex<Vec<ExecutionReport>>>,
    client_to_exchange: Arc<Mutex<HashMap<ClientOrderId, ExchangeOrderId>>>,
    req_counter: u64,
    deferred_cancels: HashMap<ClientOrderId, oneshot::Sender<Result<()>>>,
}

impl GateWsWorker {
    fn new(
        cfg: WorkerConfig,
        command_rx: mpsc::Receiver<GatewayCommand>,
        reports: Arc<Mutex<Vec<ExecutionReport>>>,
        client_to_exchange: Arc<Mutex<HashMap<ClientOrderId, ExchangeOrderId>>>,
    ) -> Self {
        Self {
            cfg,
            command_rx,
            reports,
            client_to_exchange,
            req_counter: 0,
            deferred_cancels: HashMap::new(),
        }
    }

    async fn run(mut self) -> Result<()> {
        let mut backoff = Duration::from_secs(1);
        loop {
            match self.establish().await {
                Ok((mut sink, mut stream)) => {
                    backoff = Duration::from_secs(1);
                    let mut pending: HashMap<String, PendingRequest> = HashMap::new();
                    let mut should_exit = false;
                    while !should_exit {
                        tokio::select! {
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
            backoff = (backoff * 2).min(Duration::from_secs(30));
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
        let (mut ws, _) = connect_async(&url)
            .await
            .with_context(|| format!("failed to connect to {}", url))?;
        ws.send(Message::Ping(Vec::new())).await.ok();

        self.perform_login(&mut ws).await?;
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
                            return Ok(());
                        }
                        let err_msg = resp
                            .data
                            .and_then(|d| d.errs)
                            .map(|e| {
                                format!(
                                    "{}: {}",
                                    e.label.unwrap_or_default(),
                                    e.message.unwrap_or_default()
                                )
                            })
                            .unwrap_or_else(|| "login failed".to_string());
                        bail!(err_msg);
                    }
                }
                Some(Ok(Message::Ping(data))) => {
                    ws.send(Message::Pong(data)).await.ok();
                }
                Some(Ok(_)) => {}
                Some(Err(err)) => return Err(anyhow!("login stream error: {err}")),
                None => bail!("connection closed before login ack"),
            }
        }
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
            GatewayCommand::Cancel { id, resp } => {
                let exchange_id = {
                    let guard = self.client_to_exchange.lock().await;
                    guard.get(&id).cloned()
                };

                if let Some(exch) = exchange_id {
                    self.deferred_cancels.remove(&id);
                    self.send_cancel_request(id, exch.0, resp, sink, pending)
                        .await;
                } else {
                    if let Some(old) = self.deferred_cancels.insert(id.clone(), resp) {
                        let _ = old.send(Err(anyhow!(
                            "duplicate cancel request for {}; retaining latest",
                            id
                        )));
                    }
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
                let resp: WsResponse = match serde_json::from_str(&text) {
                    Ok(r) => r,
                    Err(_) => return Ok(()),
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
                                    Ok((acks, reports, followup_cancels)) => {
                                        {
                                            let mut guard = self.reports.lock().await;
                                            guard.extend(reports);
                                        }
                                        let _ = resp_tx.send(Ok(acks));
                                        for (cid, eid, cancel_resp) in followup_cancels {
                                            self.send_cancel_request(
                                                cid,
                                                eid,
                                                cancel_resp,
                                                sink,
                                                pending,
                                            )
                                            .await;
                                        }
                                    }
                                    Err(err) => {
                                        let _ = resp_tx.send(Err(err));
                                    }
                                }
                            }
                            PendingRequest::Cancel { id, resp_tx } => {
                                match self.process_cancel_response(&id, &resp).await {
                                    Ok(report_opt) => {
                                        if let Some(report) = report_opt {
                                            let mut guard = self.reports.lock().await;
                                            guard.push(report);
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
                sink.send(Message::Pong(data)).await.ok();
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
    ) -> Result<(
        Vec<OrderAck>,
        Vec<ExecutionReport>,
        Vec<(ClientOrderId, String, oneshot::Sender<Result<()>>)>,
    )> {
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

        let intents_map: HashMap<String, QuoteIntent> = intents
            .into_iter()
            .map(|intent| (intent.client_order_id.to_string(), intent))
            .collect();

        let arr = payload
            .as_array()
            .ok_or_else(|| anyhow!("order batch result is not an array"))?;

        let mut order_acks = Vec::new();
        let mut reports = Vec::new();
        let mut followup_cancels = Vec::new();

        for entry in arr {
            let succeeded = entry
                .get("succeeded")
                .and_then(|v| v.as_bool())
                .unwrap_or(true);
            let text_opt = entry.get("text").and_then(|v| v.as_str());
            let text_owned = text_opt.map(|s| s.to_string());

            if !succeeded {
                let message = entry
                    .get("message")
                    .and_then(|v| v.as_str())
                    .unwrap_or("order rejected");
                if let Some(text) = text_owned.as_deref() {
                    if let Some(intent) = intents_map.get(text) {
                        if let Some(resp_tx) = self.deferred_cancels.remove(&intent.client_order_id)
                        {
                            let _ = resp_tx.send(Err(anyhow!(message.to_string())));
                        }
                    }
                }
                // Prefer to surface the raw API rejection so the caller can react.
                return Err(anyhow!(message.to_string()));
            }

            let text = text_owned
                .as_deref()
                .ok_or_else(|| anyhow!("order response missing text field: {}", entry))?;
            let client_intent = intents_map
                .get(text)
                .ok_or_else(|| anyhow!("order response text {} not found", text))?;

            let exchange_id = entry.get("id").and_then(value_to_string);
            match exchange_id {
                Some(ref eid) => {
                    let mut map_guard = self.client_to_exchange.lock().await;
                    map_guard.insert(
                        client_intent.client_order_id.clone(),
                        ExchangeOrderId(eid.clone()),
                    );
                    if let Some(resp_tx) =
                        self.deferred_cancels.remove(&client_intent.client_order_id)
                    {
                        followup_cancels.push((
                            client_intent.client_order_id.clone(),
                            eid.clone(),
                            resp_tx,
                        ));
                    }
                }
                None => {
                    if let Some(resp_tx) =
                        self.deferred_cancels.remove(&client_intent.client_order_id)
                    {
                        let _ = resp_tx.send(Err(anyhow!(
                            "order {} acknowledged without exchange id",
                            client_intent.client_order_id
                        )));
                    }
                }
            }

            order_acks.push(OrderAck {
                client_order_id: client_intent.client_order_id.clone(),
                exchange_order_id: exchange_id.clone().map(ExchangeOrderId),
            });

            let report = self.build_execution_report(entry, client_intent, exchange_id.clone());
            reports.push(report);
        }

        Ok((order_acks, reports, followup_cancels))
    }
    async fn process_cancel_response(
        &mut self,
        id: &ClientOrderId,
        resp: &WsResponse,
    ) -> Result<Option<ExecutionReport>> {
        if !resp.is_success() {
            let err = resp
                .error_message()
                .unwrap_or_else(|| "cancel rejected".to_string());
            return Err(anyhow!(err));
        }

        let exchange_id = {
            let mut map_guard = self.client_to_exchange.lock().await;
            map_guard.remove(id)
        };

        Ok(Some(ExecutionReport {
            client_order_id: id.clone(),
            exchange_order_id: exchange_id,
            status: OrderStatus::Canceled,
            filled_qty: 0.0,
            avg_fill_price: None,
            ts: None,
        }))
    }

    async fn send_cancel_request(
        &mut self,
        id: ClientOrderId,
        exchange_id: String,
        resp_tx: oneshot::Sender<Result<()>>,
        sink: &mut futures_util::stream::SplitSink<
            WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
            Message,
        >,
        pending: &mut HashMap<String, PendingRequest>,
    ) {
        let req_param = json!([exchange_id]);
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

        pending.insert(req_id, PendingRequest::Cancel { id, resp_tx });
    }

    fn build_execution_report(
        &self,
        entry: &Value,
        intent: &QuoteIntent,
        exchange_id: Option<String>,
    ) -> ExecutionReport {
        let size_contracts = entry.get("size").and_then(value_to_f64).unwrap_or(0.0);
        let left_contracts = entry.get("left").and_then(value_to_f64).unwrap_or(0.0);
        let filled_contracts = (size_contracts - left_contracts).max(0.0);
        let filled_qty = filled_contracts * self.cfg.contract_size;

        let status = match entry
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
        {
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

        let avg_fill_price = entry
            .get("fill_price")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
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
        for (_, resp_tx) in self.deferred_cancels.drain() {
            let _ = resp_tx.send(Err(anyhow!(err.to_string())));
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
    let mut mac =
        Hmac::<Sha512>::new_from_slice(secret.as_bytes()).expect("HMAC can take key of any size");
    let payload = format!("{}\n{}\n{}\n{}", "api", channel, req_param, ts);
    mac.update(payload.as_bytes());
    let bytes = mac.finalize().into_bytes();
    bytes
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect::<String>()
}

fn value_to_string(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

fn value_to_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.parse::<f64>().ok(),
        _ => None,
    }
}

fn current_unix_ts() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before epoch")
        .as_secs() as i64
}

fn current_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before epoch")
        .as_millis() as u64
}

fn format_price(price: f64) -> String {
    let formatted = format!("{:.8}", price);
    formatted
        .trim_end_matches('0')
        .trim_end_matches('.')
        .to_string()
}

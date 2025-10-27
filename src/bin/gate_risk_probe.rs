#![cfg(feature = "gate_exec")]

use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use hmac::{Hmac, Mac};
use reqwest::{Client, Method};
use rust_test::exchanges::endpoints::{GateioGet, GateioWs};
use rust_test::exchanges::gate::rest;
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha512};
use tokio::time::{Duration, MissedTickBehavior, interval};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

type HmacSha512 = Hmac<Sha512>;

#[derive(Parser, Debug)]
#[command(
    name = "gate-risk-probe",
    about = "Probe Gate positions and contract metadata"
)]
struct Cli {
    /// Path to the YAML runner config
    #[arg(long, default_value = "config/gate_mvp.yaml")]
    config: String,

    /// Override contract symbol (defaults to strategy.symbol)
    #[arg(long)]
    symbol: Option<String>,

    /// Refresh interval for REST position snapshots in seconds
    #[arg(long, default_value_t = 60)]
    refresh_secs: u64,

    /// Enable verbose printing of raw websocket frames
    #[arg(long)]
    verbose_ws: bool,
}

#[derive(Debug, Deserialize)]
struct StrategyConfig {
    symbol: String,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct CredentialsConfig {
    #[serde(default)]
    api_key_env: Option<String>,
    #[serde(default)]
    api_secret_env: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RunnerConfig {
    strategy: StrategyConfig,
    #[serde(default)]
    credentials: Option<CredentialsConfig>,
    #[serde(default)]
    settle: Option<String>,
}

#[derive(Debug, Clone)]
struct Credentials {
    api_key: String,
    api_secret: String,
}

#[derive(Debug, Clone)]
struct PositionSnapshot {
    contract: String,
    size_contracts: f64,
}

#[derive(Debug, Clone)]
struct PositionTracker {
    contract: String,
    contract_size: f64,
    net_contracts: f64,
}

impl PositionTracker {
    fn new(contract: String, contract_size: f64, net_contracts: f64) -> Self {
        Self {
            contract,
            contract_size,
            net_contracts,
        }
    }

    fn update_from_rest(&mut self, contracts: f64) {
        self.net_contracts = contracts;
        self.print_state("REST snapshot");
    }

    fn apply_trade(&mut self, delta_contracts: f64, trade_id: Option<&str>) {
        self.net_contracts += delta_contracts;
        let source = trade_id
            .map(|id| format!("user trade {id}"))
            .unwrap_or_else(|| "user trade".into());
        self.print_state(&source);
    }

    fn print_state(&self, source: &str) {
        let qty = self.net_contracts * self.contract_size;
        println!(
            "[{source}] net_contracts={:.6} qty={:.6} contract_size={} symbol={}",
            self.net_contracts, qty, self.contract_size, self.contract
        );
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let cli = Cli::parse();

    println!("Loading config from {}", cli.config);
    let runner_cfg = load_config(&cli.config)?;

    let settle = runner_cfg
        .settle
        .clone()
        .unwrap_or_else(|| "usdt".to_string());
    let contract = cli
        .symbol
        .clone()
        .unwrap_or_else(|| runner_cfg.strategy.symbol.clone());

    let credentials = resolve_credentials(&runner_cfg)?;
    println!("Using contract {contract} (settle: {settle})");

    println!("Fetching contract metadata...");
    let meta = rest::fetch_contract_meta_async(&contract)
        .await
        .ok_or_else(|| anyhow!("failed to fetch contract meta for {contract}"))?;
    let contract_size = meta
        .quanto_multiplier
        .filter(|v| v.is_finite() && *v > 0.0)
        .ok_or_else(|| anyhow!("invalid quanto_multiplier for {contract}"))?;
    println!(
        "Resolved contract_size (quanto multiplier) = {}",
        contract_size
    );

    let rest_client = GateRestClient::new(credentials.clone());

    println!("Fetching initial positions via REST...");
    let initial_snapshot = rest_client
        .fetch_position(&settle, &contract)
        .await
        .with_context(|| "failed to fetch positions".to_string())?;
    if let Some(snapshot) = &initial_snapshot {
        println!(
            "Initial REST position: contract={} size_contracts={:.6}",
            snapshot.contract, snapshot.size_contracts
        );
    } else {
        println!("Initial REST position: no open position reported");
    }
    let initial_contracts = initial_snapshot
        .as_ref()
        .map(|p| p.size_contracts)
        .unwrap_or(0.0);
    let mut tracker = PositionTracker::new(contract.clone(), contract_size, initial_contracts);
    tracker.print_state("startup");

    println!("Connecting to Gate private websocket...");
    let ws_url = format!("{}/{}", WS_BASE.trim_end_matches('/'), settle);
    let (mut ws_stream, _) = connect_async(&ws_url)
        .await
        .with_context(|| format!("failed to connect to {}", ws_url))?;
    ws_stream.send(Message::Ping(Vec::new())).await.ok();

    let mut ws_auth = WsAuth::new(credentials, contract.clone());
    let login_info = ws_auth
        .perform_login(&mut ws_stream)
        .await
        .context("login failed")?;
    println!("Login acknowledged (req_id={})", login_info.req_id);
    let user_id = login_info
        .user_id
        .or_else(|| std::env::var("GATE_UID").ok());
    let user_id = user_id.ok_or_else(|| {
        anyhow!("Gate login did not provide user id; set GATE_UID env or ensure API key exposes it")
    })?;
    println!("Resolved Gate user id: {user_id}");

    ws_auth
        .subscribe_user_trades(&mut ws_stream, &user_id)
        .await
        .context("failed to subscribe to futures.usertrades")?;
    println!("Subscribed to {}", GateioWs::USER_TRADES);

    let (mut write, mut read) = ws_stream.split();

    let mut heartbeat = interval(Duration::from_secs(20));
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut rest_refresh = interval(Duration::from_secs(cli.refresh_secs.max(5)));
    rest_refresh.set_missed_tick_behavior(MissedTickBehavior::Delay);

    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);

    loop {
        tokio::select! {
            _ = ctrl_c.as_mut() => {
                println!("Received Ctrl-C; shutting down.");
                break;
            }
            _ = heartbeat.tick() => {
                if let Err(err) = write.send(Message::Ping(Vec::new())).await {
                    eprintln!("failed to send heartbeat ping: {err}");
                    break;
                }
            }
            _ = rest_refresh.tick() => {
                match rest_client.fetch_position(&settle, &contract).await {
                    Ok(snapshot_opt) => {
                        if let Some(snapshot) = snapshot_opt {
                            println!(
                                "Periodic REST position: contract={} size_contracts={:.6}",
                                snapshot.contract, snapshot.size_contracts
                            );
                            tracker.update_from_rest(snapshot.size_contracts);
                        } else {
                            println!("Periodic REST position: no open position reported");
                            tracker.update_from_rest(0.0);
                        }
                    }
                    Err(err) => {
                        eprintln!("failed to refresh REST position: {:#}", err);
                    }
                }
            }
            msg = read.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        if cli.verbose_ws {
                            println!("WS <= {text}");
                        }
                        if let Err(err) = handle_ws_text(&text, &mut tracker, &contract) {
                            eprintln!("error handling ws frame: {:#}", err);
                        }
                    }
                    Some(Ok(Message::Binary(bin))) => {
                        println!("binary frame (len={}): {:x?}", bin.len(), &bin[..bin.len().min(16)]);
                    }
                    Some(Ok(Message::Frame(_))) => {}
                    Some(Ok(Message::Ping(data))) => {
                        write.send(Message::Pong(data)).await.ok();
                    }
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Close(frame))) => {
                        println!("ws close: {:?}", frame);
                        break;
                    }
                    Some(Err(err)) => {
                        eprintln!("ws error: {err}");
                        break;
                    }
                    None => {
                        println!("ws stream ended");
                        break;
                    }
                }
            }
        }
    }

    println!("Gate risk probe exiting.");
    Ok(())
}

fn load_config(path: &str) -> Result<RunnerConfig> {
    let contents = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read config at {}", path))?;
    let cfg: RunnerConfig = serde_yaml::from_str(&contents)
        .with_context(|| format!("failed to parse config at {}", path))?;
    Ok(cfg)
}

fn resolve_credentials(cfg: &RunnerConfig) -> Result<Credentials> {
    let creds = cfg.credentials.clone().unwrap_or_default();
    let key_env = creds
        .api_key_env
        .unwrap_or_else(|| "GATEIO_API_KEY".to_string());
    let secret_env = creds
        .api_secret_env
        .unwrap_or_else(|| "GATEIO_SECRET_KEY".to_string());

    let api_key = std::env::var(&key_env).with_context(|| format!("missing env var {key_env}"))?;
    let api_secret =
        std::env::var(&secret_env).with_context(|| format!("missing env var {secret_env}"))?;
    Ok(Credentials {
        api_key,
        api_secret,
    })
}

struct GateRestClient {
    client: Client,
    credentials: Credentials,
}

impl GateRestClient {
    fn new(credentials: Credentials) -> Self {
        Self {
            client: Client::builder()
                .user_agent("gate-risk-probe/0.1")
                .build()
                .expect("reqwest client"),
            credentials,
        }
    }

    async fn fetch_position(
        &self,
        settle: &str,
        contract: &str,
    ) -> Result<Option<PositionSnapshot>> {
        let path = format!("/api/v4/futures/{}/positions", settle);
        let query_string = format!("contract={}", contract);
        let value = self
            .signed_request(Method::GET, &path, &query_string, "")
            .await
            .with_context(|| format!("failed to GET positions for {contract}"))?;

        let mut matching_entry: Option<&Value> = None;
        match &value {
            Value::Array(array) => {
                matching_entry = array.iter().find(|entry| {
                    entry
                        .get("contract")
                        .and_then(|v| v.as_str())
                        .map(|s| s.eq_ignore_ascii_case(contract))
                        .unwrap_or(false)
                });
            }
            Value::Object(_) => {
                if value
                    .get("contract")
                    .and_then(|v| v.as_str())
                    .map(|s| s.eq_ignore_ascii_case(contract))
                    .unwrap_or(false)
                {
                    matching_entry = Some(&value);
                }
            }
            _ => {}
        }

        if let Some(entry) = matching_entry {
            if let Ok(pretty) = serde_json::to_string_pretty(entry) {
                println!("REST positions snapshot ({contract}): {}", pretty);
            }
            if let Some(snapshot) = parse_position(entry) {
                return Ok(Some(snapshot));
            }
            println!("REST positions snapshot ({contract}): unable to parse contract size field");
        } else {
            println!("REST positions snapshot ({contract}): no matching entry");
        }

        Ok(None)
    }

    async fn signed_request(
        &self,
        method: Method,
        path: &str,
        query: &str,
        body: &str,
    ) -> Result<Value> {
        let base_url = format!("{}{}", GateioGet::BASE, path);
        let ts = current_unix_seconds_string();
        let method_name = method.as_str().to_string();
        let hashed_payload = sha512_hex(body);
        let payload = format!(
            "{}\n{}\n{}\n{}\n{}",
            method_name, path, query, hashed_payload, ts
        );
        let mut mac = HmacSha512::new_from_slice(self.credentials.api_secret.as_bytes())
            .expect("HMAC can take key of any size");
        mac.update(payload.as_bytes());
        let signature = hex_bytes(mac.finalize().into_bytes());

        let url = if query.is_empty() {
            base_url
        } else {
            format!("{base_url}?{query}")
        };

        let mut request = self.client.request(method.clone(), &url);
        if !body.is_empty() {
            request = request.body(body.to_string());
        }
        let response = request
            .header("KEY", &self.credentials.api_key)
            .header("SIGN", signature)
            .header("Timestamp", &ts)
            .header("Content-Type", "application/json")
            .send()
            .await?;

        let status = response.status();
        let text = response.text().await?;
        if !status.is_success() {
            bail!("HTTP {} -> {}", status, text);
        }
        if text.trim().is_empty() {
            return Ok(Value::Null);
        }
        let value: Value = serde_json::from_str(&text)
            .with_context(|| format!("failed to parse JSON response: {}", text))?;
        Ok(value)
    }
}

fn parse_position(value: &Value) -> Option<PositionSnapshot> {
    let contract = value.get("contract")?.as_str()?.to_string();
    let fields = ["size", "current_size", "position", "contracts"];
    for key in fields {
        if let Some(size_value) = value.get(key) {
            if let Some(size_contracts) = value_to_f64(size_value) {
                return Some(PositionSnapshot {
                    contract,
                    size_contracts,
                });
            }
        }
    }
    None
}

fn handle_ws_text(text: &str, tracker: &mut PositionTracker, contract: &str) -> Result<()> {
    let value: Value =
        serde_json::from_str(text).with_context(|| format!("invalid JSON message: {}", text))?;
    let channel = value.get("channel").and_then(|v| v.as_str()).unwrap_or("");
    let event = value.get("event").and_then(|v| v.as_str()).unwrap_or("");

    if channel == GateioWs::USER_TRADES {
        match event {
            "update" => {
                if let Some(results) = value.get("result").and_then(|v| v.as_array()) {
                    for trade in results {
                        if !trade
                            .get("contract")
                            .and_then(|v| v.as_str())
                            .map(|c| c.eq_ignore_ascii_case(contract))
                            .unwrap_or(false)
                        {
                            continue;
                        }
                        let size_contracts =
                            trade.get("size").and_then(value_to_f64).unwrap_or(0.0);
                        if size_contracts.abs() < f64::EPSILON {
                            continue;
                        }
                        let trade_id = trade
                            .get("id")
                            .or_else(|| trade.get("trade_id"))
                            .and_then(|v| value_to_string(v));
                        println!(
                            "User trade: size_contracts={:.6} price={} role={} text={} id={:?}",
                            size_contracts,
                            trade
                                .get("price")
                                .and_then(value_to_string)
                                .unwrap_or_else(|| "?".into()),
                            trade.get("role").and_then(|v| v.as_str()).unwrap_or("?"),
                            trade.get("text").and_then(|v| v.as_str()).unwrap_or(""),
                            trade_id
                        );
                        tracker.apply_trade(size_contracts, trade_id.as_deref());
                    }
                }
            }
            "subscribe" => println!("Subscribed ack for {}", channel),
            _ => {}
        }
    } else if event == "ping" {
        println!("WS ping payload: {}", text);
    }
    Ok(())
}

fn value_to_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.parse::<f64>().ok(),
        _ => None,
    }
}

fn value_to_string(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

fn hex_bytes(bytes: impl AsRef<[u8]>) -> String {
    bytes
        .as_ref()
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect()
}

fn current_unix_seconds_string() -> String {
    let dur = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before epoch");
    let secs = dur.as_secs();
    let nanos = dur.subsec_nanos();
    format!("{}.{:09}", secs, nanos)
}

fn sha512_hex(input: &str) -> String {
    let mut hasher = Sha512::new();
    hasher.update(input.as_bytes());
    hex_bytes(hasher.finalize())
}

const WS_BASE: &str = "wss://fx-ws.gateio.ws/v4/ws";

struct WsAuth {
    credentials: Credentials,
    contract: String,
    req_counter: u64,
}

struct LoginInfo {
    req_id: String,
    user_id: Option<String>,
}

impl WsAuth {
    fn new(credentials: Credentials, contract: String) -> Self {
        Self {
            credentials,
            contract,
            req_counter: 0,
        }
    }

    async fn perform_login(
        &mut self,
        ws: &mut tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    ) -> Result<LoginInfo> {
        let ts = current_unix_ts();
        let req_id = self.next_request_id("login");
        let request = build_api_request(&self.credentials, GateioWs::LOGIN, None, &req_id, ts)?;
        ws.send(Message::Text(request)).await?;

        loop {
            match ws.next().await {
                Some(Ok(Message::Text(text))) => {
                    let resp: WsResponse = serde_json::from_str(&text)
                        .with_context(|| format!("failed to parse login response: {}", text))?;
                    if resp.request_id.as_deref() == Some(&req_id) {
                        if resp.is_success() {
                            let user_id = serde_json::from_str::<Value>(&text)
                                .ok()
                                .and_then(|v| extract_user_id_value(&v));
                            return Ok(LoginInfo { req_id, user_id });
                        }
                        let err_msg = resp
                            .error_message()
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

    async fn subscribe_user_trades(
        &mut self,
        ws: &mut tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        user_id: &str,
    ) -> Result<()> {
        let ts = current_unix_ts();
        let sign = sign_subscribe(&self.credentials.api_secret, GateioWs::USER_TRADES, ts);
        let payload = json!({
            "time": ts,
            "channel": GateioWs::USER_TRADES,
            "event": "subscribe",
            "payload": [user_id, &self.contract],
            "auth": {
                "method": "api_key",
                "KEY": self.credentials.api_key,
                "SIGN": sign,
            }
        });
        ws.send(Message::Text(payload.to_string())).await?;
        Ok(())
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
        self.data
            .as_ref()
            .and_then(|d| d.errs.as_ref())
            .and_then(|e| e.message.clone())
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
    errs: Option<ResponseError>,
}

#[derive(Debug, Deserialize)]
struct ResponseError {
    #[serde(default)]
    message: Option<String>,
}

fn build_api_request(
    credentials: &Credentials,
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
    let signature = sign_api(&credentials.api_secret, channel, &payload_str, ts);
    let mut payload = json!({
        "req_id": req_id,
        "timestamp": ts.to_string(),
        "api_key": credentials.api_key,
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
        HmacSha512::new_from_slice(secret.as_bytes()).expect("HMAC can take key of any size");
    let payload = format!("{}\n{}\n{}\n{}", "api", channel, req_param, ts);
    mac.update(payload.as_bytes());
    hex_bytes(mac.finalize().into_bytes())
}

fn sign_subscribe(secret: &str, channel: &str, ts: i64) -> String {
    let mut mac =
        HmacSha512::new_from_slice(secret.as_bytes()).expect("HMAC can take key of any size");
    let payload = format!("channel={channel}&event=subscribe&time={ts}");
    mac.update(payload.as_bytes());
    hex_bytes(mac.finalize().into_bytes())
}

fn extract_user_id_value(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Object(map) => {
            if let Some(uid) = map
                .get("user_id")
                .or_else(|| map.get("uid"))
                .or_else(|| map.get("userId"))
            {
                extract_user_id_value(uid)
            } else {
                map.values().find_map(extract_user_id_value)
            }
        }
        Value::Array(items) => items.iter().find_map(extract_user_id_value),
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

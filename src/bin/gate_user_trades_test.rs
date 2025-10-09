#[cfg(not(feature = "gate_exec"))]
fn main() {
    eprintln!(
        "gate_user_trades_test requires the `gate_exec` feature (tokio/ws/clap). \\nrun with `cargo run --bin gate_user_trades_test --features gate_exec -- ...`."
    );
}

#[cfg(feature = "gate_exec")]
mod runner {
    use std::env;
    use std::fs::File;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use anyhow::{Context, Result, anyhow};
    use clap::Parser;
    use futures_util::{SinkExt, StreamExt};
    use hmac::{Hmac, Mac};
    use rust_test::exchanges::endpoints::GateioWs;
    use rust_test::exchanges::gate::canonical_contract_symbol;
    use serde::Deserialize;
    use serde_json::{Value, json};
    use serde_yaml::Value as YamlValue;
    use sha2::Sha512;
    use tokio::time::{Duration, Instant, MissedTickBehavior, interval_at};
    use tokio_tungstenite::{connect_async, tungstenite::Message};

    type HmacSha512 = Hmac<Sha512>;

    /// Stream Gate.io user trade notifications for manual testing.
    #[derive(Debug, Parser)]
    #[command(name = "gate_user_trades_test")]
    #[command(about = "Connect to Gate.io futures WS user trades channel and print updates.")]
    struct Cli {
        /// Optional path to Gate MVP config for credentials fallback.
        #[arg(long)]
        config: Option<PathBuf>,
        /// Contracts to subscribe to (comma separated). Use !all to receive all contracts.
        #[arg(long, short = 'c', value_delimiter = ',', default_value = "BTC_USDT")]
        contracts: Vec<String>,
        /// Explicit Gate user ID (falls back to GATE_UID env var).
        #[arg(long)]
        user_id: Option<String>,
        /// API key (falls back to GATE_API_KEY env var).
        #[arg(long)]
        api_key: Option<String>,
        /// API secret (falls back to GATE_API_SECRET env var).
        #[arg(long)]
        api_secret: Option<String>,
        /// Settlement currency segment (default: usdt).
        #[arg(long, default_value = "usdt")]
        settle: String,
        /// Override websocket base URL (e.g. wss://fx-ws-testnet.gateio.ws/v4/ws).
        #[arg(long)]
        ws_url: Option<String>,
        /// Connect to Gate testnet (alias for --ws-url wss://fx-ws-testnet.gateio.ws/v4/ws).
        #[arg(long, default_value_t = false)]
        testnet: bool,
        /// Pretty-print JSON payloads instead of single-line output.
        #[arg(long, default_value_t = false)]
        pretty: bool,
    }

    #[derive(Debug, Deserialize)]
    struct Credentials {
        #[serde(default)]
        api_key: Option<String>,
        #[serde(default)]
        api_secret: Option<String>,
        #[serde(default)]
        api_key_env: Option<String>,
        #[serde(default)]
        api_secret_env: Option<String>,
        #[serde(default)]
        user_id: Option<String>,
        #[serde(default)]
        user_id_env: Option<String>,
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
            self.data.as_ref().and_then(|d| d.errs.as_ref()).map(|err| {
                let label = err.label.clone().unwrap_or_default();
                let message = err.message.clone().unwrap_or_default();
                if label.is_empty() {
                    message
                } else if message.is_empty() {
                    label
                } else {
                    format!("{label}: {message}")
                }
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

    pub(crate) async fn run() -> Result<()> {
        dotenvy::dotenv().ok();

        let cli = Cli::parse();

        let config_path = cli
            .config
            .clone()
            .unwrap_or_else(|| PathBuf::from("config/gate_mvp.yaml"));
        let credentials = match load_credentials(&config_path) {
            Ok(creds) => creds,
            Err(err) => {
                eprintln!("Failed to load credentials from {:?}: {err}", config_path);
                None
            }
        };

        let api_key = cli
            .api_key
            .or_else(|| env::var("GATE_API_KEY").ok())
            .or_else(|| credentials.as_ref().and_then(|c| c.api_key.clone()))
            .or_else(|| {
                credentials
                    .as_ref()
                    .and_then(|c| c.api_key_env.as_deref())
                    .and_then(|name| env::var(name).ok())
            })
            .context("missing Gate API key (set --api-key, GATE_API_KEY, or populate config credentials)")?;
        let api_secret = cli
            .api_secret
            .or_else(|| env::var("GATE_API_SECRET").ok())
            .or_else(|| credentials.as_ref().and_then(|c| c.api_secret.clone()))
            .or_else(|| {
                credentials
                    .as_ref()
                    .and_then(|c| c.api_secret_env.as_deref())
                    .and_then(|name| env::var(name).ok())
            })
            .context("missing Gate API secret (set --api-secret, GATE_API_SECRET, or populate config credentials)")?;
        let mut user_id = cli
            .user_id
            .or_else(|| env::var("GATE_UID").ok())
            .or_else(|| credentials.as_ref().and_then(|c| c.user_id.clone()))
            .or_else(|| {
                credentials
                    .as_ref()
                    .and_then(|c| c.user_id_env.as_deref())
                    .and_then(|name| env::var(name).ok())
            });

        let base_ws = if let Some(url) = cli.ws_url {
            url
        } else if cli.testnet {
            "wss://fx-ws-testnet.gateio.ws/v4/ws".to_string()
        } else {
            "wss://fx-ws.gateio.ws/v4/ws".to_string()
        };
        let settle = cli.settle.to_ascii_lowercase();
        let ws_url = format!("{}/{}", base_ws.trim_end_matches('/'), settle);

        let mut contracts: Vec<String> = cli
            .contracts
            .iter()
            .map(|c| c.trim())
            .filter(|c| !c.is_empty())
            .map(|c| {
                if c.eq_ignore_ascii_case("!all") {
                    "!all".to_string()
                } else {
                    canonical_contract_symbol(c)
                }
            })
            .collect();
        contracts.dedup();
        if contracts.is_empty() {
            contracts.push(canonical_contract_symbol("BTC_USDT"));
        }

        println!(
            "Connecting to {ws_url} for user {} ...",
            user_id.as_deref().unwrap_or("(pending)")
        );
        let (mut ws, _) = connect_async(&ws_url)
            .await
            .with_context(|| format!("failed to connect to {ws_url}"))?;

        // Send initial ping to avoid idle disconnects on some gateways.
        ws.send(Message::Ping(Vec::new())).await.ok();

        // Perform API login so subsequent subscriptions can access private channels.
        let login_req_id = make_request_id("login");
        let ts = current_unix_ts();
        let login_msg = build_api_request(
            &api_key,
            &api_secret,
            GateioWs::LOGIN,
            None,
            &login_req_id,
            ts,
        )?;
        ws.send(Message::Text(login_msg)).await?;

        let login_deadline = Instant::now() + Duration::from_secs(10);
        let mut login_ok = false;
        while let Some(msg) = ws.next().await {
            match msg {
                Ok(Message::Text(text)) => {
                    if let Ok(resp) = serde_json::from_str::<WsResponse>(&text) {
                        if resp.request_id.as_deref() == Some(&login_req_id) {
                            if cli.pretty {
                                match serde_json::from_str::<Value>(&text) {
                                    Ok(json) => {
                                        println!(
                                            "Login response:\n{}",
                                            serde_json::to_string_pretty(&json)
                                                .unwrap_or_else(|_| text.clone())
                                        );
                                    }
                                    Err(_) => println!("Login response: {text}"),
                                }
                            } else {
                                println!("Login response: {text}");
                            }
                            if resp.is_success() {
                                if user_id.is_none() {
                                    if let Some(data) = resp.data.as_ref() {
                                        if let Some(result) = data.result.as_ref() {
                                            user_id = extract_user_id(result);
                                        }
                                    }
                                }
                                login_ok = true;
                                println!("Login acknowledged.");
                                break;
                            }
                            let err = resp
                                .error_message()
                                .unwrap_or_else(|| "unknown login failure".to_string());
                            return Err(anyhow!("login failed: {err}"));
                        }
                    }
                }
                Ok(Message::Ping(data)) => {
                    ws.send(Message::Pong(data)).await.ok();
                }
                Ok(Message::Pong(_)) => {}
                Ok(_) => {}
                Err(err) => return Err(anyhow!("login stream error: {err}")),
            }
            if Instant::now() > login_deadline {
                return Err(anyhow!("did not receive login ack within timeout"));
            }
        }
        if !login_ok {
            return Err(anyhow!("connection closed before login ack"));
        }

        let user_id = user_id
            .or_else(|| {
                eprintln!(
                    "Login response did not expose user_id; supply --user-id or set GATE_UID."
                );
                None
            })
            .context("missing Gate user id (set --user-id or GATE_UID)")?;

        // Build subscription payload: first element is user id, subsequent are contracts.
        let mut payload = Vec::with_capacity(1 + contracts.len());
        payload.push(Value::String(user_id.clone()));
        for contract in &contracts {
            payload.push(Value::String(contract.clone()));
        }
        let sub_ts = current_unix_ts();
        let usertrades_sign = sign_subscribe(&api_secret, GateioWs::USER_TRADES, sub_ts);
        let subscribe_msg = json!({
            "time": sub_ts,
            "channel": GateioWs::USER_TRADES,
            "event": "subscribe",
            "payload": payload,
            "auth": {
                "method": "api_key",
                "KEY": api_key,
                "SIGN": usertrades_sign,
            }
        });
        ws.send(Message::Text(subscribe_msg.to_string())).await?;
        println!(
            "Subscribed to {} for contracts {:?}.",
            GateioWs::USER_TRADES,
            contracts
        );

        let mut heartbeat = interval_at(
            Instant::now() + Duration::from_secs(25),
            Duration::from_secs(25),
        );
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                biased;
                _ = heartbeat.tick() => {
                    let ping = json!({
                        "time": current_unix_ts(),
                        "channel": GateioWs::PING,
                        "event": "ping",
                    });
                    if let Err(err) = ws.send(Message::Text(ping.to_string())).await {
                        eprintln!("failed to send heartbeat ping: {err}");
                        break;
                    }
                }
                msg = ws.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            if cli.pretty {
                                match serde_json::from_str::<Value>(&text) {
                                    Ok(val) => {
                                        if let Some(ch) = val.get("channel").and_then(|c| c.as_str()) {
                                            if ch == GateioWs::USER_TRADES {
                                                println!("{}", serde_json::to_string_pretty(&val).unwrap_or(text.clone()));
                                            } else {
                                                println!("{}", serde_json::to_string_pretty(&val).unwrap_or(text.clone()));
                                            }
                                        } else {
                                            println!("{}", serde_json::to_string_pretty(&val).unwrap_or(text.clone()));
                                        }
                                    }
                                    Err(_) => println!("{text}"),
                                }
                            } else {
                                if let Ok(val) = serde_json::from_str::<Value>(&text) {
                                    handle_message(&val);
                                } else {
                                    println!("{text}");
                                }
                            }
                        }
                        Some(Ok(Message::Binary(bin))) => {
                            println!("binary frame (len={}): {:x?}", bin.len(), &bin[..bin.len().min(32)]);
                        }
                        Some(Ok(Message::Frame(_))) => {}
                        Some(Ok(Message::Ping(data))) => {
                            ws.send(Message::Pong(data)).await.ok();
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

        Ok(())
    }

    fn load_credentials(path: &PathBuf) -> anyhow::Result<Option<Credentials>> {
        if !path.exists() {
            return Ok(None);
        }
        let file =
            File::open(path).with_context(|| format!("failed to open config at {:?}", path))?;
        let root: YamlValue = serde_yaml::from_reader(file)
            .with_context(|| format!("failed to parse YAML config at {:?}", path))?;
        match root.get("credentials") {
            Some(creds_val) => {
                let creds: Credentials = serde_yaml::from_value(creds_val.clone())
                    .context("failed to deserialize credentials section")?;
                Ok(Some(creds))
            }
            None => Ok(None),
        }
    }

    fn handle_message(val: &Value) {
        if let Some(obj) = val.as_object() {
            let channel = obj
                .get("channel")
                .and_then(|v| v.as_str())
                .unwrap_or("(unknown)");
            let event = obj
                .get("event")
                .and_then(|v| v.as_str())
                .unwrap_or("(unknown)");
            if channel == GateioWs::USER_TRADES && event == "update" {
                if let Some(results) = obj.get("result").and_then(|r| r.as_array()) {
                    for trade in results {
                        if let Some(trade_obj) = trade.as_object() {
                            let id = trade_obj.get("id").and_then(|v| v.as_str()).unwrap_or("?");
                            let price = trade_obj
                                .get("price")
                                .and_then(|v| match v {
                                    Value::String(s) => Some(s.clone()),
                                    Value::Number(n) => Some(n.to_string()),
                                    _ => None,
                                })
                                .unwrap_or_else(|| "?".to_string());
                            let size = trade_obj.get("size").and_then(|v| v.as_i64()).unwrap_or(0);
                            let role = trade_obj
                                .get("role")
                                .and_then(|v| v.as_str())
                                .unwrap_or("?");
                            let contract = trade_obj
                                .get("contract")
                                .and_then(|v| v.as_str())
                                .unwrap_or("?");
                            let side = trade_obj.get("text").and_then(|v| v.as_str()).unwrap_or("");
                            println!(
                                "trade id={id} contract={contract} price={price} size={size} role={role} text={side}"
                            );
                        }
                    }
                } else {
                    println!("usertrades update: {val}");
                }
            } else if event == "subscribe" {
                println!("subscribed channel={channel}");
            } else {
                println!("{channel}:{event} -> {val}");
            }
        } else {
            println!("{val}");
        }
    }

    fn build_api_request(
        api_key: &str,
        api_secret: &str,
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

        let signature = sign_api(api_secret, channel, &payload_str, ts);
        let mut payload = json!({
            "req_id": req_id,
            "timestamp": ts.to_string(),
            "api_key": api_key,
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
        let bytes = mac.finalize().into_bytes();
        bytes.iter().map(|b| format!("{:02x}", b)).collect()
    }

    fn sign_subscribe(secret: &str, channel: &str, ts: i64) -> String {
        let message = format!("channel={channel}&event=subscribe&time={ts}");
        let mut mac =
            HmacSha512::new_from_slice(secret.as_bytes()).expect("HMAC can take key of any size");
        mac.update(message.as_bytes());
        mac.finalize()
            .into_bytes()
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect()
    }

    fn extract_user_id(val: &Value) -> Option<String> {
        match val {
            Value::String(s) => Some(s.clone()),
            Value::Number(n) => Some(n.to_string()),
            Value::Object(map) => {
                if let Some(user_id) = map
                    .get("user_id")
                    .or_else(|| map.get("uid"))
                    .or_else(|| map.get("userId"))
                {
                    extract_user_id(user_id)
                } else {
                    map.values().find_map(extract_user_id)
                }
            }
            Value::Array(items) => items.iter().find_map(extract_user_id),
            _ => None,
        }
    }

    fn make_request_id(prefix: &str) -> String {
        format!("{}-{}", prefix, current_unix_ms())
    }

    fn current_unix_ts() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before epoch")
            .as_secs() as i64
    }

    fn current_unix_ms() -> u128 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before epoch")
            .as_millis()
    }
}

#[cfg(feature = "gate_exec")]
fn main() -> anyhow::Result<()> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|err| anyhow::anyhow!(err))?
        .block_on(runner::run())
}

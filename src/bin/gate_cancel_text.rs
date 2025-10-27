#![cfg(feature = "gate_exec")]

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use rust_test::exchanges::endpoints::GateioWs;
use rust_test::exchanges::gate::rest;
use rust_test::execution::GateWsConfig;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

#[derive(Debug, Parser)]
#[command(
    name = "gate-cancel-text",
    about = "Place two test orders and cancel them by client text in a single request"
)]
struct Cli {
    #[arg(long, default_value = "BTC_USDT")]
    symbol: String,
    #[arg(long, default_value = "usdt")]
    settle: String,
    #[arg(long, default_value_t = 100_000.0)]
    bid_price: f64,
    #[arg(long, default_value_t = 130_000.0)]
    ask_price: f64,
    #[arg(long, default_value_t = 0.0001)]
    size: f64,
    #[arg(long, default_value = "post_only")]
    tif: String,
    #[arg(long, default_value = "gateio_api_key")]
    api_key_env: String,
    #[arg(long, default_value = "gateio_secret_key")]
    api_secret_env: String,
    #[arg(long, default_value = "wss://fx-ws.gateio.ws/v4/ws")]
    ws_url: String,
    #[arg(long, default_value_t = 5)]
    settle_wait_secs: u64,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let cli = Cli::parse();

    if cli.size <= 0.0 {
        bail!("size must be positive");
    }

    let api_key = std::env::var(&cli.api_key_env)
        .with_context(|| format!("missing env var {}", cli.api_key_env))?;
    let api_secret = std::env::var(&cli.api_secret_env)
        .with_context(|| format!("missing env var {}", cli.api_secret_env))?;

    let contract_meta = rest::fetch_contract_meta_async(&cli.symbol)
        .await
        .ok_or_else(|| anyhow!("failed to fetch contract metadata for {}", cli.symbol))?;
    let contract_size = contract_meta
        .quanto_multiplier
        .ok_or_else(|| anyhow!("contract metadata missing quanto_multiplier"))?;

    let tif = normalize_tif(&cli.tif)?;

    let settle = cli.settle.to_ascii_lowercase();

    let ws_config = GateWsConfig {
        api_key,
        api_secret,
        symbol: cli.symbol.clone(),
        settle: Some(settle.clone()),
        ws_url: Some(cli.ws_url.clone()),
        contract_size: Some(contract_size),
    };

    let ws_endpoint = format!(
        "{}/{}",
        ws_config
            .ws_url
            .as_deref()
            .unwrap_or("wss://fx-ws.gateio.ws/v4/ws")
            .trim_end_matches('/'),
        settle
    );

    println!(
        "Connecting to {} for symbol {} (contract_size={})",
        ws_endpoint, ws_config.symbol, contract_size
    );

    let (stream, _) = connect_async(&ws_endpoint).await?;
    let (mut write, mut read) = stream.split();
    let mut req_counter: u64 = 0;

    // Login first
    let login_req_id = next_request_id("login", &mut req_counter);
    let login_payload = build_api_request(
        &ws_config,
        GateioWs::LOGIN,
        None,
        &login_req_id,
        current_unix_ts(),
    )?;
    write
        .send(tokio_tungstenite::tungstenite::Message::Text(login_payload))
        .await?;
    wait_for_response(&mut read, &login_req_id).await?;
    println!("Login acknowledged.");

    let bid_text = format!("t-cancel-test-b-{}", current_unix_ms());
    let ask_text = format!("t-cancel-test-s-{}", current_unix_ms());

    let bid_contracts = size_to_contracts(cli.size, contract_size)?;
    let ask_contracts = -bid_contracts;

    let orders = Value::Array(vec![
        json!({
            "contract": ws_config.symbol,
            "size": bid_contracts,
            "price": format_price(cli.bid_price),
            "tif": tif,
            "text": bid_text,
        }),
        json!({
            "contract": ws_config.symbol,
            "size": ask_contracts,
            "price": format_price(cli.ask_price),
            "tif": tif,
            "text": ask_text,
        }),
    ]);

    let place_req_id = next_request_id("place", &mut req_counter);
    let place_payload = build_api_request(
        &ws_config,
        GateioWs::CREATE_BATCH_ORDER,
        Some(orders),
        &place_req_id,
        current_unix_ts(),
    )?;
    write
        .send(tokio_tungstenite::tungstenite::Message::Text(place_payload))
        .await?;
    let place_resp = wait_for_response(&mut read, &place_req_id).await?;
    println!(
        "Batch order response: status ok={}, data={:?}",
        place_resp.is_success(),
        place_resp.data
    );

    println!("Sleeping {}s before cancel...", cli.settle_wait_secs);
    tokio::time::sleep(Duration::from_secs(cli.settle_wait_secs)).await;

    let cancel_req_id = next_request_id("cancel", &mut req_counter);
    let cancel_param = json!([bid_text, ask_text]);
    let cancel_payload = build_api_request(
        &ws_config,
        GateioWs::CANCEL_BATCH_ORDER_IDS,
        Some(cancel_param),
        &cancel_req_id,
        current_unix_ts(),
    )?;
    write
        .send(tokio_tungstenite::tungstenite::Message::Text(
            cancel_payload,
        ))
        .await?;
    let cancel_resp = wait_for_response(&mut read, &cancel_req_id).await?;
    println!(
        "Cancel response: status ok={}, data={:?}",
        cancel_resp.is_success(),
        cancel_resp.data
    );

    let _ = write
        .send(tokio_tungstenite::tungstenite::Message::Close(None))
        .await;
    println!("Done. Inspect the cancel response to verify text-based cancellation.");
    Ok(())
}

fn normalize_tif(raw: &str) -> Result<String> {
    let lower = raw.to_ascii_lowercase();
    match lower.as_str() {
        "gtc" => Ok("gtc".into()),
        "ioc" => Ok("ioc".into()),
        "fok" => Ok("fok".into()),
        "post" | "post_only" | "poc" => Ok("poc".into()),
        other => bail!("unsupported tif {other}"),
    }
}

fn size_to_contracts(size: f64, contract_size: f64) -> Result<i64> {
    let raw = (size / contract_size).round() as i64;
    if raw == 0 {
        bail!(
            "size {} is too small for contract size {}",
            size,
            contract_size
        );
    }
    Ok(raw)
}

async fn wait_for_response(
    read: &mut futures_util::stream::SplitStream<
        WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
    >,
    req_id: &str,
) -> Result<WsResponse> {
    while let Some(msg) = read.next().await {
        match msg? {
            tokio_tungstenite::tungstenite::Message::Text(text) => {
                if let Ok(resp) = serde_json::from_str::<WsResponse>(&text) {
                    if resp.request_id.as_deref() == Some(req_id) {
                        if resp.ack.unwrap_or(false) {
                            continue;
                        }
                        return Ok(resp);
                    }
                }
            }
            tokio_tungstenite::tungstenite::Message::Ping(_) => {
                // Ignore; short-lived script does not need to reply.
            }
            _ => {}
        }
    }
    Err(anyhow!("websocket closed before response for {req_id}"))
}

fn build_api_request(
    cfg: &GateWsConfig,
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
    use hmac::{Hmac, Mac};
    use sha2::Sha512;

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

fn format_price(price: f64) -> String {
    let formatted = format!("{:.8}", price);
    formatted
        .trim_end_matches('0')
        .trim_end_matches('.')
        .to_string()
}

fn next_request_id(prefix: &str, counter: &mut u64) -> String {
    *counter = counter.wrapping_add(1);
    format!("{}-{}-{}", prefix, current_unix_ms(), counter)
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
}

#[derive(Debug, Deserialize)]
struct ResponseHeader {
    #[serde(default)]
    status: Option<String>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct ResponseData {
    #[serde(default)]
    result: Option<Value>,
    #[serde(default)]
    errs: Option<ResponseError>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct ResponseError {
    #[serde(default)]
    label: Option<String>,
    #[serde(default)]
    message: Option<String>,
}

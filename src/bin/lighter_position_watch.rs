use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

use anyhow::{Result, anyhow, bail};
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use reqwest::Url;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio_tungstenite::{connect_async_with_config, tungstenite::Message};

use rust_test::config::runner::{load_lighter_credentials, load_runner_config, log_runner_config};
use rust_test::execution::{LighterCredentials, lighter_auth_token};
use rust_test::exchanges::lighter::fetch_market_meta_async;
use rust_test::utils::parsing::log_parse_drop;

#[derive(Parser, Debug)]
#[command(name = "lighter-position-watch", about = "Print Lighter position updates + notional")]
struct Cli {
    /// Path to the YAML runner config
    #[arg(long, default_value = "config/lighter_mvp.yaml")]
    config: String,

    /// Override symbol (defaults to strategy.symbol)
    #[arg(long)]
    symbol: Option<String>,

    /// Enable verbose prints
    #[arg(long)]
    debug: bool,
}

#[derive(Debug, Deserialize)]
struct LighterAccountOrdersMsg {
    #[serde(default)]
    orders: HashMap<String, Vec<LighterOrderEntry>>,
    #[serde(default, rename = "type")]
    msg_type: Option<String>,
}

#[derive(Debug, Deserialize)]
struct LighterOrderEntry {
    #[serde(default)]
    order_index: Option<i64>,
    #[serde(default)]
    client_order_index: Option<i64>,
    #[serde(default)]
    filled_base_amount: Option<String>,
    #[serde(default)]
    is_ask: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct LighterMarketStatsMsg {
    #[serde(default)]
    market_stats: LighterMarketStats,
}

#[derive(Debug, Deserialize)]
struct LighterMarketStats {
    #[serde(default)]
    mark_price: Option<String>,
    #[serde(default)]
    last_trade_price: Option<String>,
    #[serde(default)]
    index_price: Option<String>,
}

#[derive(Debug, Clone, Copy)]
struct OrderState {
    is_ask: bool,
    filled_base: f64,
}

fn parse_price(value: &str) -> Option<f64> {
    match value.parse::<f64>() {
        Ok(v) if v.is_finite() && v > 0.0 => Some(v),
        Ok(_) => {
            log_parse_drop(
                "lighter_position_watch",
                "non_finite_price",
                &"non-finite price",
                value,
            );
            None
        }
        Err(err) => {
            log_parse_drop("lighter_position_watch", "price", &err, value);
            None
        }
    }
}

fn ws_url_from_base(base_url: &str) -> Result<String> {
    let url = Url::parse(base_url)?;
    let host = url
        .host_str()
        .ok_or_else(|| anyhow!("lighter base_url missing host"))?;
    let scheme = match url.scheme() {
        "http" => "ws",
        "https" => "wss",
        "ws" | "wss" => url.scheme(),
        _ => "wss",
    };
    let mut out = format!("{scheme}://{host}");
    if let Some(port) = url.port() {
        out.push(':');
        out.push_str(&port.to_string());
    }
    out.push_str("/stream");
    Ok(out)
}

fn resolve_signer_path(creds: &LighterCredentials) -> Result<String> {
    let raw = creds.signer_lib.as_str();
    if Path::new(raw).exists() {
        return Ok(raw.to_string());
    }

    let mut candidates: Vec<String> = vec![];
    if cfg!(target_os = "macos") {
        if raw.ends_with(".so") {
            candidates.push(raw.trim_end_matches(".so").to_string() + ".dylib");
        }
    } else {
        if raw.ends_with(".dylib") {
            candidates.push(raw.trim_end_matches(".dylib").to_string() + ".so");
        }
        if cfg!(target_arch = "aarch64") {
            candidates.push(raw.replace("amd64", "arm64").replace(".dylib", ".so"));
        } else if cfg!(target_arch = "x86_64") {
            candidates.push(raw.replace("arm64", "amd64").replace(".dylib", ".so"));
        }
    }

    if let Some(found) = candidates.iter().find(|p| Path::new(p.as_str()).exists()) {
        return Ok(found.clone());
    }

    bail!(
        "Lighter signer library not found at {} (candidates tried: {:?})",
        raw,
        candidates
    )
}

fn print_position(
    net_base: f64,
    contract_size: f64,
    last_price: Option<f64>,
    price_source: Option<&'static str>,
    orders: usize,
) {
    let net_contracts = if contract_size > 0.0 {
        net_base / contract_size
    } else {
        0.0
    };
    let direction = if net_base > 0.0 {
        "long"
    } else if net_base < 0.0 {
        "short"
    } else {
        "flat"
    };
    let (price_str, source_str, notional_str) = if let Some(px) = last_price {
        let notional = net_base.abs() * px;
        (
            format!("{:.6}", px),
            price_source.unwrap_or("unknown"),
            format!("{:.6}", notional),
        )
    } else {
        ("NA".to_string(), price_source.unwrap_or("unknown"), "NA".to_string())
    };
    println!(
        "[position] net_base={:.6} net_contracts={:.6} dir={} notional={} price={} source={} orders={}",
        net_base, net_contracts, direction, notional_str, price_str, source_str, orders
    );
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    if let Err(err) = dotenvy::dotenv() {
        eprintln!("WARN: failed to load .env: {}", err);
    }
    let cli = Cli::parse();
    let config = load_runner_config(&cli.config)?;
    log_runner_config(&config);

    let symbol = cli
        .symbol
        .clone()
        .unwrap_or_else(|| config.strategy.symbol.clone());
    let meta = fetch_market_meta_async(&symbol)
        .await
        .ok_or_else(|| anyhow!("failed to fetch Lighter market metadata for {}", symbol))?;
    let size_scale = 10_f64.powi(meta.size_decimals as i32);
    let contract_size = 10_f64.powi(-(meta.size_decimals as i32));

    let mut creds = load_lighter_credentials(&config)?;
    creds.signer_lib = resolve_signer_path(&creds)?;
    let ws_url = ws_url_from_base(&creds.base_url)?;
    let market_key = meta.market_id.to_string();

    println!(
        "Watching Lighter {} (market_id={}, account_idx={})",
        meta.symbol, meta.market_id, creds.account_index
    );

    let mut orders: HashMap<i64, OrderState> = HashMap::new();
    let mut last_price: Option<f64> = None;
    let mut price_source: Option<&'static str> = None;
    let mut backoff = Duration::from_secs(1);

    loop {
        let auth = match lighter_auth_token(&creds, cli.debug).await {
            Ok(token) => token,
            Err(err) => {
                eprintln!("[position] auth failed: {:#}", err);
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(30));
                continue;
            }
        };

        let ws = match connect_async_with_config(&ws_url, None, true).await {
            Ok((ws, _)) => ws,
            Err(err) => {
                eprintln!("[position] ws connect failed: {:#}", err);
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(30));
                continue;
            }
        };
        let (mut sink, mut stream) = ws.split();

        let stats_channel = format!("market_stats/{}", meta.market_id);
        let orders_channel = format!("account_orders/{}/{}", meta.market_id, creds.account_index);
        sink.send(Message::Text(json!({
            "type": "subscribe",
            "channel": stats_channel,
        }).to_string())).await?;
        sink.send(Message::Text(json!({
            "type": "subscribe",
            "channel": orders_channel,
            "auth": auth,
        }).to_string())).await?;

        backoff = Duration::from_secs(1);

        while let Some(msg) = stream.next().await {
            let msg = match msg {
                Ok(msg) => msg,
                Err(err) => {
                    eprintln!("[position] ws error: {:#}", err);
                    break;
                }
            };

            match msg {
                Message::Ping(payload) => {
                    let _ = sink.send(Message::Pong(payload)).await;
                }
                Message::Text(text) => {
                    if let Ok(value) = serde_json::from_str::<Value>(&text) {
                        if value
                            .get("type")
                            .and_then(|v| v.as_str())
                            .map_or(false, |t| t == "ping")
                        {
                            let _ = sink
                                .send(Message::Text(r#"{"type":"pong"}"#.to_string()))
                                .await;
                            continue;
                        }

                        if value
                            .get("channel")
                            .and_then(|v| v.as_str())
                            .map_or(false, |ch| ch.starts_with("market_stats"))
                        {
                            if let Ok(stats_msg) =
                                serde_json::from_value::<LighterMarketStatsMsg>(value)
                            {
                                if let Some(px) = stats_msg
                                    .market_stats
                                    .mark_price
                                    .as_deref()
                                    .and_then(parse_price)
                                {
                                    last_price = Some(px);
                                    price_source = Some("mark");
                                } else if let Some(px) = stats_msg
                                    .market_stats
                                    .last_trade_price
                                    .as_deref()
                                    .and_then(parse_price)
                                {
                                    last_price = Some(px);
                                    price_source = Some("last_trade");
                                } else if let Some(px) = stats_msg
                                    .market_stats
                                    .index_price
                                    .as_deref()
                                    .and_then(parse_price)
                                {
                                    last_price = Some(px);
                                    price_source = Some("index");
                                }
                            }
                            continue;
                        }

                        let msg_type = value.get("type").and_then(|v| v.as_str());
                        if matches!(
                            msg_type,
                            Some("update/account_orders" | "update/account_all_orders")
                        ) {
                            if let Ok(msg) =
                                serde_json::from_value::<LighterAccountOrdersMsg>(value)
                            {
                                if let Some(entries) = msg.orders.get(&market_key) {
                                    let is_full = matches!(
                                        msg.msg_type.as_deref(),
                                        Some("update/account_all_orders")
                                    );
                                    if is_full {
                                        orders.clear();
                                    }

                                    let mut changed = is_full;
                                    for entry in entries {
                                        let id = entry
                                            .client_order_index
                                            .or(entry.order_index);
                                        let Some(id) = id else {
                                            continue;
                                        };
                                        let is_ask = entry.is_ask;
                                        let state = orders.entry(id).or_insert_with(|| {
                                            changed = true;
                                            OrderState {
                                                is_ask: is_ask.unwrap_or(false),
                                                filled_base: 0.0,
                                            }
                                        });

                                        if let Some(flag) = is_ask {
                                            if flag != state.is_ask {
                                                state.is_ask = flag;
                                                changed = true;
                                            }
                                        }
                                        if let Some(filled_str) = entry.filled_base_amount.as_ref()
                                        {
                                            if let Ok(base) = filled_str.parse::<f64>() {
                                                let filled = base / size_scale;
                                                if (filled - state.filled_base).abs() > 1e-9 {
                                                    state.filled_base = filled;
                                                    changed = true;
                                                }
                                            }
                                        }
                                    }

                                    if changed {
                                        let net_base = orders.values().fold(0.0, |acc, st| {
                                            if st.is_ask {
                                                acc - st.filled_base
                                            } else {
                                                acc + st.filled_base
                                            }
                                        });
                                        print_position(
                                            net_base,
                                            contract_size,
                                            last_price,
                                            price_source,
                                            orders.len(),
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
                Message::Binary(bin) => {
                    if let Ok(text) = std::str::from_utf8(&bin) {
                        if let Ok(value) = serde_json::from_str::<Value>(text) {
                            if value
                                .get("type")
                                .and_then(|v| v.as_str())
                                .map_or(false, |t| t == "ping")
                            {
                                let _ = sink
                                    .send(Message::Text(r#"{"type":"pong"}"#.to_string()))
                                    .await;
                            }
                        }
                    }
                }
                Message::Close(_) => break,
                _ => {}
            }
        }

        eprintln!("[position] ws disconnected, reconnecting...");
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(30));
    }
}

#![cfg(feature = "gate_exec")]

use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant, SystemTime};

use anyhow::{Result, anyhow, bail};
use clap::Parser;
use parking_lot::Mutex;
use reqwest::Url;
use rust_test::base_classes::engine::{
    configure_demean_enabled, configure_feed_overrides, spawn_state_engine,
};
use rust_test::base_classes::reference::ReferenceEvent;
use rust_test::base_classes::state::state;
use rust_test::base_classes::types::Side;
use rust_test::config::runner::{
    RiskConfig, RunnerConfig, load_gate_credentials, load_lighter_credentials, load_runner_config,
    log_runner_config,
};
use rust_test::exchanges::gate::rest;
use rust_test::exchanges::lighter::rest as lighter_rest;
use rust_test::execution::{
    ClientOrderId, DryRunGateway, ExecutionGateway, ExecutionReport, GateClient, GateCredentials,
    GateWsConfig, GateWsGateway, InventoryReportOutcome, InventoryTracker, LighterAuthClient,
    LighterCredentials, LighterGateway, OrderAck, OrderManager, OrderStatus, QuoteIntent, Venue,
    resolve_lighter_signer_path,
};
use rust_test::logging::quote::{DebugLogger, QuoteLogHandle, format_f64};
use rust_test::strategy::{
    MomentumFadeStrategy, ReferenceMeta, SimpleQuoteStrategy, SizeSpec, StrategyEngine,
    StrategyKind,
};
use rust_test::utils::parsing::log_parse_drop;
use serde_json::{Value, json};
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{connect_async_with_config, tungstenite::Message};
use tokio::sync::{Semaphore, mpsc};
use tokio::time::{self, MissedTickBehavior, interval, sleep};

#[derive(Debug, Parser)]
#[command(name = "gate-runner", about = "Gate.io MVP dry-run executor")]
struct Cli {
    /// Path to YAML configuration
    #[arg(long, default_value = "config/gate_mvp.yaml")]
    config: String,
}

fn latency_debug_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("GATE_LATENCY_DEBUG")
            .map(|v| v != "0" && !v.eq_ignore_ascii_case("false"))
            .unwrap_or(false)
    })
}

fn dur_us(d: Duration) -> u128 {
    d.as_micros()
}

fn lighter_mid_price() -> Option<f64> {
    let guard = match state().lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            eprintln!("WARN: markout state lock poisoned: {}", poisoned);
            poisoned.into_inner()
        }
    };
    let snap = &guard.lighter.orderbook;
    let mid = snap.price.filter(|v| v.is_finite() && *v > 0.0)?;
    let last_recv = snap.received_at?;
    if last_recv.elapsed() > LIGHTER_MARKOUT_MAX_AGE {
        return None;
    }
    Some(mid)
}

const LIGHTER_POSITION_WS_CHANNEL: &str = "account_all_positions/{ACCOUNT_ID}";

fn parse_lighter_market_id(map: &serde_json::Map<String, Value>) -> Option<u32> {
    let candidates = ["market_id", "marketId", "market_index", "marketIndex"];
    for key in candidates {
        if let Some(value) = map.get(key) {
            if let Some(id) = value.as_u64() {
                return Some(id as u32);
            }
            if let Some(raw) = value.as_str() {
                match raw.parse::<u32>() {
                    Ok(id) => return Some(id),
                    Err(err) => {
                        log_parse_drop("gate_runner", "market_id", &err, raw);
                    }
                }
            }
        }
    }
    None
}

fn parse_lighter_position_sign(map: &serde_json::Map<String, Value>) -> Option<f64> {
    if let Some(sign_val) = map.get("sign") {
        let sign = sign_val
            .as_i64()
            .map(|v| v as f64)
            .or_else(|| sign_val.as_f64())
            .or_else(|| {
                sign_val.as_str().and_then(|s| match s.parse::<f64>() {
                    Ok(v) if v.is_finite() => Some(v),
                    Ok(_) => {
                        log_parse_drop(
                            "gate_runner",
                            "non_finite_sign",
                            &"non-finite sign",
                            s,
                        );
                        None
                    }
                    Err(err) => {
                        log_parse_drop("gate_runner", "sign", &err, s);
                        None
                    }
                })
            });
        if let Some(sign) = sign {
            if sign < 0.0 {
                return Some(-1.0);
            }
            if sign > 0.0 {
                return Some(1.0);
            }
        }
    }
    if let Some(flag) = map.get("is_long").and_then(|v| v.as_bool()) {
        return Some(if flag { 1.0 } else { -1.0 });
    }
    if let Some(flag) = map.get("is_short").and_then(|v| v.as_bool()) {
        return Some(if flag { -1.0 } else { 1.0 });
    }
    let side_keys = ["side", "position_side", "positionSide", "direction"];
    for key in side_keys {
        if let Some(side) = map.get(key).and_then(|v| v.as_str()) {
            let lower = side.to_ascii_lowercase();
            if lower.contains("short") || lower.contains("sell") || lower.contains("ask") {
                return Some(-1.0);
            }
            if lower.contains("long") || lower.contains("buy") || lower.contains("bid") {
                return Some(1.0);
            }
        }
    }
    None
}

fn parse_lighter_position_entry(
    map: &serde_json::Map<String, Value>,
    market_id: u32,
    contract_size: f64,
) -> Option<f64> {
    if let Some(id) = parse_lighter_market_id(map) {
        if id != market_id {
            return None;
        }
    }

    let size_keys = [
        "position",
        "position_size",
        "positionSize",
        "size",
        "base_amount",
        "baseAmount",
        "base_size",
        "baseSize",
        "net_base_amount",
        "netBaseAmount",
        "position_size",
        "positionSize",
        "qty",
        "quantity",
        "amount",
    ];
    let mut base = None;
    for key in size_keys {
        if let Some(value) = map.get(key) {
            base = value
                .as_str()
                .and_then(|s| match s.parse::<f64>() {
                    Ok(v) if v.is_finite() => Some(v),
                    Ok(_) => {
                        log_parse_drop(
                            "gate_runner",
                            "non_finite_position",
                            &"non-finite position size",
                            s,
                        );
                        None
                    }
                    Err(err) => {
                        log_parse_drop("gate_runner", "position", &err, s);
                        None
                    }
                })
                .or_else(|| value.as_f64());
            if base.is_some() {
                break;
            }
        }
    }
    let mut base = base?;

    if base >= 0.0 {
        if let Some(sign) = parse_lighter_position_sign(map) {
            base *= sign;
        }
    }

    if contract_size > 0.0 {
        Some(base / contract_size)
    } else {
        None
    }
}

fn lighter_ws_url_from_base(base_url: &str) -> Result<String> {
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

fn extract_lighter_position_contracts(
    value: &Value,
    market_id: u32,
    contract_size: f64,
) -> Option<f64> {
    match value {
        Value::Array(items) => {
            for item in items {
                if let Some(found) =
                    extract_lighter_position_contracts(item, market_id, contract_size)
                {
                    return Some(found);
                }
            }
        }
        Value::Object(map) => {
            if let Some(code) = map.get("code").and_then(|v| v.as_i64()) {
                if code != 200 {
                    return None;
                }
            }
            if let Some(positions) = map.get("positions") {
                return extract_lighter_position_contracts(positions, market_id, contract_size);
            }
            if let Some(found) = parse_lighter_position_entry(map, market_id, contract_size) {
                return Some(found);
            }
            let key = market_id.to_string();
            if let Some(entry) = map.get(&key) {
                if let Some(found) =
                    extract_lighter_position_contracts(entry, market_id, contract_size)
                {
                    return Some(found);
                }
            }
            for entry in map.values() {
                if let Some(found) =
                    extract_lighter_position_contracts(entry, market_id, contract_size)
                {
                    return Some(found);
                }
            }
        }
        _ => {}
    }
    None
}

async fn spawn_lighter_position_ws(
    auth_client: LighterAuthClient,
    creds: LighterCredentials,
    meta: lighter_rest::LighterMarketMeta,
    inventory: Arc<Mutex<InventoryTracker>>,
    strategy: Arc<Mutex<StrategyEngine>>,
    debug: DebugLogger,
    contract_size: f64,
) {
    let ws_url = match lighter_ws_url_from_base(&creds.base_url) {
        Ok(url) => url,
        Err(err) => {
            debug.error(|| format!("Lighter position ws url failed: {:#}", err));
            return;
        }
    };

    let mut backoff = Duration::from_secs(1);
    loop {
        let token = match auth_client.auth_token().await {
            Ok(token) => token,
            Err(err) => {
                debug.error(|| format!("Lighter position ws auth failed: {:#}", err));
                sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(30));
                continue;
            }
        };

        let ws = match connect_async_with_config(&ws_url, None, true).await {
            Ok((ws, _)) => ws,
            Err(err) => {
                debug.error(|| format!("Lighter position ws connect failed: {:#}", err));
                sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(30));
                continue;
            }
        };

        let (mut sink, mut stream) = ws.split();
        let channel = LIGHTER_POSITION_WS_CHANNEL.replace("{ACCOUNT_ID}", &creds.account_index.to_string());
        let sub = json!({
            "type": "subscribe",
            "channel": channel,
            "auth": token,
        })
        .to_string();
        if let Err(err) = sink.send(Message::Text(sub)).await {
            debug.error(|| format!("Lighter position ws subscribe failed: {:#}", err));
        }

        backoff = Duration::from_secs(1);

        while let Some(msg) = stream.next().await {
            let msg = match msg {
                Ok(msg) => msg,
                Err(err) => {
                    debug.error(|| format!("Lighter position ws error: {:#}", err));
                    break;
                }
            };

            match msg {
                Message::Ping(payload) => {
                    let _ = sink.send(Message::Pong(payload)).await;
                }
                Message::Text(text) => {
                    let value = match serde_json::from_str::<Value>(&text) {
                        Ok(value) => value,
                        Err(_) => continue,
                    };
                    let msg_type = value
                        .get("type")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    let channel = value
                        .get("channel")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    if msg_type == "ping" {
                        let _ = sink
                            .send(Message::Text(r#"{"type":"pong"}"#.to_string()))
                            .await;
                        continue;
                    }

                    let is_positions_update = msg_type == "update/account_all_positions"
                        || channel.starts_with("account_all_positions");
                    if is_positions_update && debug.is_enabled() {
                        // Raw payloads are noisy; keep this muted unless needed for debugging.
                    }
                    if is_positions_update {
                        if let Some(positions) = value.get("positions") {
                            if let Some(contracts) = extract_lighter_position_contracts(
                                positions,
                                meta.market_id,
                                contract_size,
                            ) {
                                let change = {
                                    let mut guard = inventory.lock();
                                    guard.replace_from_rest(contracts)
                                };
                                if let Some((prev, new)) = change {
                                    let latest_price = {
                                        let guard = strategy.lock();
                                        guard.latest_price()
                                    };
                                    let notional = format_inventory_notional(
                                        new,
                                        contract_size,
                                        latest_price,
                                    );
                                    debug.info(|| {
                                        format!(
                                            "inventory sync (Lighter ws) {:.4} -> {:.4} contracts {}",
                                            prev, new, notional
                                        )
                                    });
                                }
                            }
                        }
                    }
                }
                Message::Close(_) => break,
                _ => {}
            }
        }

        sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(30));
    }
}

const REF_WARN: Duration = Duration::from_millis(20);
const STAGE_WARN: Duration = Duration::from_millis(5);
const CANCEL_WARN: Duration = Duration::from_micros(500);
const LIGHTER_MARKOUT_MAX_AGE: Duration = Duration::from_secs(2);

#[derive(Clone, Debug)]
struct OrderMinima {
    base: f64,
    quote: f64,
    size_decimals: u32,
}

struct CancelMessage {
    reference: ReferenceEvent,
    dispatched_at: Instant,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    if let Err(err) = dotenvy::dotenv() {
        eprintln!("WARN: failed to load .env: {}", err);
    }
    let cli = Cli::parse();
    let mut config = load_runner_config(&cli.config)?;
    log_runner_config(&config);
    let venue = config.strategy.venue;
    configure_feed_overrides(config.feeds);
    configure_demean_enabled(config.mode.demean_prices);
    let debug = DebugLogger::new(config.mode.debug_prints);

    let settle = if venue == Venue::Gate {
        Some(
            config
                .settle
                .clone()
                .ok_or_else(|| anyhow!("missing settle currency in config for Gate venue"))?,
        )
    } else {
        None
    };

    let mut lighter_meta: Option<lighter_rest::LighterMarketMeta> = None;
    let mut lighter_mins: Option<OrderMinima> = None;
    let contract_size = match venue {
        Venue::Gate => {
            let contract_meta = rest::fetch_contract_meta_async(&config.strategy.symbol)
                .await
                .ok_or_else(|| {
                    anyhow!(
                        "failed to fetch Gate contract metadata for {}",
                        config.strategy.symbol
                    )
                })?;

            if contract_meta.in_delisting.unwrap_or(false) {
                bail!(
                    "{} is marked for delisting on Gate; aborting execution",
                    config.strategy.symbol
                );
            }

            if let Some(min_tick) = contract_meta
                .order_price_round
                .or(contract_meta.rounding_precision)
                .filter(|v| v.is_finite() && *v > 0.0)
            {
                if (config.strategy.min_tick - min_tick).abs() > f64::EPSILON {
                    debug.info(|| {
                        format!(
                            "overriding min_tick for {} from {:.8} to {:.8}",
                            config.strategy.symbol, config.strategy.min_tick, min_tick
                        )
                    });
                }
                config.strategy.min_tick = min_tick;
            }

            let size = contract_meta
                .quanto_multiplier
                .filter(|m| m.is_finite() && *m > 0.0)
                .ok_or_else(|| {
                    anyhow!(
                        "contract metadata missing valid quanto_multiplier for {}",
                        config.strategy.symbol
                    )
                })?;

            debug.info(|| {
                format!(
                    "resolved contract size for {} (gate): {}",
                    config.strategy.symbol, size
                )
            });
            size
        }
        Venue::Lighter => {
            let meta = lighter_rest::fetch_market_meta_async(&config.strategy.symbol)
                .await
                .ok_or_else(|| {
                    anyhow!(
                        "failed to fetch Lighter market metadata for {}",
                        config.strategy.symbol
                    )
                })?;

            let min_tick = 10f64.powi(-(meta.price_decimals as i32));
            if (config.strategy.min_tick - min_tick).abs() > f64::EPSILON {
                debug.info(|| {
                    format!(
                        "overriding min_tick for {} from {:.8} to {:.8} (lighter)",
                        config.strategy.symbol, config.strategy.min_tick, min_tick
                    )
                });
                config.strategy.min_tick = min_tick;
            }

            debug.info(|| {
                format!(
                    "resolved Lighter market {} (id {}) price_decimals={} size_decimals={} min_base={} min_quote={}",
                    meta.symbol,
                    meta.market_id,
                    meta.price_decimals,
                    meta.size_decimals,
                    meta.min_base_amount,
                    meta.min_quote_amount
                )
            });
            lighter_meta = Some(meta.clone());
            lighter_mins = Some(OrderMinima {
                base: meta.min_base_amount,
                quote: meta.min_quote_amount,
                size_decimals: meta.size_decimals,
            });
            // Smallest lot size unit is determined by size_decimals.
            10f64.powi(-(meta.size_decimals as i32))
        }
    };

    let config = Arc::new(config);

    let logger = if config.logging.is_enabled() {
        let handle = QuoteLogHandle::spawn(config.as_ref(), debug.clone())?;
        debug.info(|| format!("Activity logging enabled -> {}", handle.path().display()));
        Some(handle)
    } else {
        None
    };

    enum LiveCreds {
        Gate(GateCredentials),
        Lighter(LighterCredentials),
    }

    let credentials = if config.mode.dry_run {
        None
    } else {
        match venue {
            Venue::Gate => Some(LiveCreds::Gate(load_gate_credentials(config.as_ref())?)),
            Venue::Lighter => Some(LiveCreds::Lighter(load_lighter_credentials(
                config.as_ref(),
            )?)),
        }
    };

    let rest_client = match (venue, credentials.as_ref()) {
        (Venue::Gate, Some(LiveCreds::Gate(creds))) => {
            Some(Arc::new(GateClient::new(creds.clone())))
        }
        _ => None,
    };

    let mut lighter_auth_client: Option<LighterAuthClient> = None;
    let mut lighter_creds: Option<LighterCredentials> = None;

    if let (Venue::Lighter, Some(LiveCreds::Lighter(creds)), Some(meta)) =
        (venue, credentials.as_ref(), lighter_meta.as_ref())
    {
        let mut resolved = creds.clone();
        match resolve_lighter_signer_path(&resolved.signer_lib) {
            Ok(path) => resolved.signer_lib = path,
            Err(err) => debug.error(|| format!("failed to resolve Lighter signer: {:#}", err)),
        }
        lighter_creds = Some(resolved.clone());
        match LighterAuthClient::connect(resolved, config.mode.debug_prints).await {
            Ok(client) => lighter_auth_client = Some(client),
            Err(err) => debug.error(|| format!("failed to init Lighter auth client: {:#}", err)),
        }
        if debug.is_enabled() {
            debug.info(|| {
                format!(
                    "Lighter position ws enabled (market_id={}, account_idx={})",
                    meta.market_id, creds.account_index
                )
            });
        }
    }

    let initial_contracts = match (venue, rest_client.as_ref(), settle.as_deref()) {
        (Venue::Gate, Some(client), Some(settle)) => match client
            .fetch_position_contracts(settle, &config.strategy.symbol)
            .await
        {
            Ok(Some(contracts)) => {
                debug
                    .info(|| format!("Initial REST position: {} contracts", format_f64(contracts)));
                contracts
            }
            Ok(None) => {
                debug.info(|| "Initial REST position: none reported".to_string());
                0.0
            }
            Err(err) => {
                debug.error(|| format!("failed to fetch initial position: {:#}", err));
                0.0
            }
        },
        _ => 0.0,
    };

    let inventory = Arc::new(Mutex::new(InventoryTracker::new(
        contract_size,
        initial_contracts,
    )));
    if venue == Venue::Lighter && debug.is_enabled() {
        debug.info(|| "Initial Lighter position: awaiting account_all_positions".to_string());
    }

    if let (Venue::Gate, Some(client), Some(settle)) =
        (venue, rest_client.clone(), settle.clone())
    {
        let inventory_clone = inventory.clone();
        let settle_clone = settle;
        let symbol_clone = config.strategy.symbol.clone();
        let debug_clone = debug.clone();
        tokio::spawn(async move {
            let mut ticker = time::interval(Duration::from_secs(60));
            ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                match client
                    .fetch_position_contracts(&settle_clone, &symbol_clone)
                    .await
                {
                    Ok(Some(contracts)) => {
                        debug_clone
                            .info(|| format!("REST refresh: {} contracts", format_f64(contracts)));
                        let change = {
                            let mut guard = inventory_clone.lock();
                            guard.replace_from_rest(contracts)
                        };
                        if let Some((prev, new)) = change {
                            debug_clone.info(|| {
                                format!("inventory sync (REST) {:.4} -> {:.4} contracts", prev, new)
                            });
                        }
                    }
                    Ok(None) => {
                        debug_clone.info(|| "REST refresh: no position reported".to_string());
                        let change = {
                            let mut guard = inventory_clone.lock();
                            guard.replace_from_rest(0.0)
                        };
                        if let Some((prev, new)) = change {
                            debug_clone.info(|| {
                                format!("inventory sync (REST) {:.4} -> {:.4} contracts", prev, new)
                            });
                        }
                    }
                    Err(err) => {
                        debug_clone.error(|| format!("REST position refresh failed: {:#}", err));
                    }
                }
            }
        });
    }

    let (reference_tx, mut reference_rx) = mpsc::unbounded_channel();
    let (fast_ref_tx, mut fast_ref_rx) = mpsc::unbounded_channel();
    let _engine = spawn_state_engine(
        config.strategy.symbol.clone(),
        Some(reference_tx),
        Some(fast_ref_tx),
        config.pricing_model.clone(),
    );
    debug.info(|| {
        format!(
            "{} runner started for {} (dry_run: {})",
            venue.as_str(),
            config.strategy.symbol,
            config.mode.dry_run
        )
    });

    let gateway: Arc<dyn ExecutionGateway> = if config.mode.dry_run {
        Arc::new(DryRunGateway::new())
    } else {
        let creds = credentials
            .as_ref()
            .expect("credentials must exist for live mode");
        match (venue, creds, lighter_meta.as_ref()) {
            (Venue::Gate, LiveCreds::Gate(creds), _) => {
                let settle = settle
                    .as_deref()
                    .expect("settle required for Gate venue");
                Arc::new(setup_gate_gateway(config.as_ref(), contract_size, creds, settle).await?)
            }
            (Venue::Lighter, LiveCreds::Lighter(creds), Some(meta)) => {
                let resolved = lighter_creds.as_ref().unwrap_or(creds);
                Arc::new(setup_lighter_gateway(config.as_ref(), resolved, meta).await?)
            }
            _ => bail!("credential/venue mismatch"),
        }
    };
    let base_size = config
        .strategy
        .resolve_size(lighter_mins.as_ref().map(|m| m.base))?;
    let order_manager = Arc::new(OrderManager::new(gateway, Duration::from_secs(30)));
    let strategy = Arc::new(Mutex::new(match config.strategy_kind {
        StrategyKind::SimpleQuote => StrategyEngine::Simple(SimpleQuoteStrategy::new(
            config.strategy.clone(),
            base_size,
        )),
        StrategyKind::MomentumFade => {
            let momentum = config
                .momentum_fade
                .clone()
                .expect("momentum_fade config missing");
            StrategyEngine::Momentum(MomentumFadeStrategy::new(
                momentum,
                config.strategy.venue,
                config.strategy.symbol.clone(),
                config.strategy.min_tick,
                base_size,
            ))
        }
    }));
    debug.info(|| format!("using base size {:.6}", base_size));

    if let (Venue::Lighter, Some(auth_client), Some(creds), Some(meta)) = (
        venue,
        lighter_auth_client.clone(),
        lighter_creds.clone(),
        lighter_meta.clone(),
    ) {
        tokio::spawn(spawn_lighter_position_ws(
            auth_client,
            creds,
            meta,
            inventory.clone(),
            strategy.clone(),
            debug.clone(),
            contract_size,
        ));
    }

    {
        let reports_strategy = strategy.clone();
        let reports_config = config.clone();
        let reports_order_manager = order_manager.clone();
        let reports_logger = logger.clone();
        let reports_debug = debug.clone();
        let reports_inventory = inventory.clone();
        tokio::spawn(async move {
            loop {
                match reports_order_manager.poll_reports().await {
                    Ok(reports) => {
                        if reports.is_empty() {
                            continue;
                        }
                        if let Err(err) = process_reports(
                            reports,
                            reports_strategy.clone(),
                            reports_config.clone(),
                            reports_logger.clone(),
                            reports_debug.clone(),
                            reports_inventory.clone(),
                            contract_size,
                        )
                        .await
                        {
                            reports_debug.error(|| format!("error processing reports: {:#}", err));
                        }
                    }
                    Err(err) => {
                        reports_debug.error(|| format!("order report poll error: {:#}", err));
                        sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        });
    }

    let (cancel_tx, mut cancel_rx) = mpsc::unbounded_channel::<CancelMessage>();
    let cancel_strategy = strategy.clone();
    let cancel_order_manager = order_manager.clone();
    let cancel_logger = logger.clone();
    let cancel_debug = debug.clone();
    let cancel_config = config.clone();

    tokio::spawn(async move {
        while let Some(msg) = cancel_rx.recv().await {
            if let Err(err) = handle_market_update(
                msg,
                cancel_strategy.clone(),
                cancel_config.clone(),
                cancel_order_manager.clone(),
                cancel_logger.clone(),
                cancel_debug.clone(),
            )
            .await
            {
                cancel_debug.error(|| format!("error handling market update: {:#}", err));
            }
        }
    });

    let fast_cancel_tx = cancel_tx.clone();
    tokio::spawn(async move {
        while let Some(reference) = fast_ref_rx.recv().await {
            let msg = CancelMessage {
                reference,
                dispatched_at: Instant::now(),
            };
            if fast_cancel_tx.send(msg).is_err() {
                break;
            }
        }
    });

    let mut market_timer = interval(Duration::from_millis(20));
    // Skip missed ticks to avoid backlog delaying market updates
    market_timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let quote_interval_ms = match config.strategy_kind {
        StrategyKind::MomentumFade => config
            .momentum_fade
            .as_ref()
            .map(|cfg| cfg.min_interval_ms)
            .unwrap_or(config.strategy.quote_interval_ms),
        StrategyKind::SimpleQuote => config.strategy.quote_interval_ms,
    };
    let mut quote_timer = interval(Duration::from_millis(quote_interval_ms.max(1)));
    // Skip missed ticks so quoting never starves the cancel hot path
    quote_timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let warmup = Duration::from_secs(25);
    let start_time = Instant::now();
    let quote_gate = Arc::new(Semaphore::new(1));

    tokio::select! {
        _ = ctrl_c_notifier() => {
            debug.info(|| "Received shutdown signal; exiting.".to_string());
        }
        _ = async {
            use tokio::sync::mpsc::error::TryRecvError;
            loop {
                // Drain all queued reference updates first (hot path)
                loop {
                    match reference_rx.try_recv() {
                        Ok(reference) => {
                            if start_time.elapsed() >= warmup {
                                let msg = CancelMessage {
                                    reference,
                                    dispatched_at: Instant::now(),
                                };
                                if cancel_tx.send(msg).is_err() {
                                    debug.error(|| "cancel handler channel closed; exiting.".to_string());
                                    return;
                                }
                            }
                            continue;
                        }
                        Err(TryRecvError::Empty) => break,
                        Err(TryRecvError::Disconnected) => {
                            debug.error(|| "reference channel closed; exiting.".to_string());
                            return;
                        }
                    }
                }

                // Await the next event, preferring new references when they arrive
                tokio::select! {
                    reference = reference_rx.recv() => {
                        match reference {
                            Some(reference) => {
                                if start_time.elapsed() >= warmup {
                                    let msg = CancelMessage {
                                        reference,
                                        dispatched_at: Instant::now(),
                                    };
                                    if cancel_tx.send(msg).is_err() {
                                        debug.error(|| "cancel handler channel closed; exiting.".to_string());
                                        break;
                                    }
                                }
                                // loop back and drain more immediately
                                continue;
                            }
                            None => {
                                debug.error(|| "reference channel closed; exiting.".to_string());
                                break;
                            }
                        }
                    }
                    _ = market_timer.tick() => {
                        if let Some(logger) = logger.as_ref() {
                            logger.log_market_snapshot();
                        }

                        if start_time.elapsed() < warmup {
                            continue;
                        }
                    }
                    _ = quote_timer.tick() => {
                        if start_time.elapsed() < warmup {
                            continue;
                        }
                        let now = Instant::now();
                        let strategy_clone = strategy.clone();
                        let config_clone = config.clone();
                        let order_manager_clone = order_manager.clone();
                        let logger_clone = logger.clone();
                        let debug_clone = debug.clone();
                        let inventory_clone = inventory.clone();
                        let minima_clone = lighter_mins.clone();
                        let size_spec_clone = config_clone.strategy.size.clone();
                        let quote_gate_clone = quote_gate.clone();
                        if let Ok(permit) = quote_gate_clone.try_acquire_owned() {
                            tokio::spawn(async move {
                                let _permit = permit;
                                if let Err(err) = handle_quote_tick(
                                    now,
                                    strategy_clone,
                                    config_clone,
                                    contract_size,
                                    order_manager_clone,
                                    logger_clone,
                                    debug_clone.clone(),
                                    inventory_clone,
                                    minima_clone,
                                    size_spec_clone,
                                )
                                .await
                                {
                                    debug_clone
                                        .error(|| format!("error handling quote tick: {:#}", err));
                                }
                            });
                        }
                    }
                }
            }
        } => {}
    }

    Ok(())
}

struct FilteredIntents {
    allowed: Vec<QuoteIntent>,
    skipped: Vec<(ClientOrderId, String)>,
}
async fn handle_market_update(
    msg: CancelMessage,
    strategy: Arc<Mutex<StrategyEngine>>,
    config: Arc<RunnerConfig>,
    order_manager: Arc<OrderManager>,
    logger: Option<QuoteLogHandle>,
    debug: DebugLogger,
) -> Result<()> {
    let config_ref = config.as_ref();
    let CancelMessage {
        reference,
        dispatched_at,
    } = msg;
    let latency_debug = latency_debug_enabled();
    let now = Instant::now();
    let reference_age = now.saturating_duration_since(reference.received_at);
    let lock_start = Instant::now();
    let mut strategy_guard = strategy.lock();
    let lock_acquired = Instant::now();
    let lock_wait = lock_start.elapsed();
    let queue_delay = lock_start.saturating_duration_since(reference.received_at);
    let dispatch_delay = lock_start.saturating_duration_since(dispatched_at);
    let cancels = strategy_guard.on_market_update(&reference);
    let state_metrics = strategy_guard.state_metrics();
    let after_update = Instant::now();
    let strat_dur = lock_start.elapsed();
    let send_start_opt = if cancels.is_empty() {
        None
    } else {
        let send_start = Instant::now();
        Some(send_start)
    };
    let after_record = Instant::now();
    drop(strategy_guard);

    if latency_debug && lock_wait > Duration::from_micros(200) {
        debug.latency(|| {
            format!(
                "latency-debug::market strategy_lock_wait={}us cancels={}",
                dur_us(lock_wait),
                cancels.len()
            )
        });
    }

    if latency_debug && (reference_age > REF_WARN || strat_dur > STAGE_WARN) {
        debug.latency(|| {
            format!(
                "latency-debug::market ref_age={}us on_market={}us source={} cancels={}",
                dur_us(reference_age),
                dur_us(strat_dur),
                reference.source,
                cancels.len()
            )
        });
    }

    if cancels.is_empty() {
        return Ok(());
    }

    if config_ref.strategy.venue == Venue::Lighter {
        // On Lighter we batch cancels with the next quote submit via `sendTxBatch`
        // (cancels then orders), to avoid a cancel→submit gap.
        debug.info(|| {
            format!(
                "repricing {} on {} (ts={:?}); scheduling {} cancels for next batch",
                config_ref.strategy.symbol,
                reference.source,
                reference.ts_ns,
                cancels.len()
            )
        });
        return Ok(());
    }

    let send_start = send_start_opt.expect("send_start missing with cancels");
    let cancel_internal = send_start.saturating_duration_since(reference.received_at);
    let compute_delay = send_start.saturating_duration_since(lock_start);
    let sent_ts = SystemTime::now();

    let cancels_to_send = cancels.clone();
    let order_manager_clone = order_manager.clone();
    let debug_clone = debug.clone();
    tokio::spawn(async move {
        let call_start = Instant::now();
        if let Err(err) = order_manager_clone.cancel_many(&cancels_to_send).await {
            let ids = cancels_to_send.clone();
            let err_msg = format!("{:#}", err);
            debug_clone.error(move || format!("cancel {:?} failed: {}", ids, err_msg));
        }
        if latency_debug_enabled() {
            let call_elapsed = call_start.elapsed();
            let cancel_count = cancels_to_send.len();
            debug_clone.latency(|| {
                format!(
                    "latency-debug::cancel ref_age={}us call={}us ids={}",
                    dur_us(cancel_internal),
                    dur_us(call_elapsed),
                    cancel_count
                )
            });
        }
    });

    if latency_debug || cancel_internal > CANCEL_WARN {
        debug.latency(|| {
            format!(
                "latency-debug::cancel summary queue={}us dispatch={}us compute={}us total={}us cancels={} active_orders={} pending_cancels={} needs_requote={}",
                dur_us(queue_delay),
                dur_us(dispatch_delay),
                dur_us(compute_delay),
                dur_us(cancel_internal),
                cancels.len(),
                state_metrics.active_orders,
                state_metrics.pending_cancels,
                state_metrics.needs_requote
            )
        });
        if latency_debug {
            debug.latency(|| {
                format!(
                    "latency-debug::cancel breakdown lock_acquire={}us on_market={}us record={}us total_locked={}us",
                    dur_us(lock_wait),
                    dur_us(after_update.saturating_duration_since(lock_acquired)),
                    dur_us(after_record.saturating_duration_since(after_update)),
                    dur_us(after_record.saturating_duration_since(lock_acquired))
                )
            });
        }
    }

    // Logging after dispatch so we don't delay the cancel send
    debug.info(|| {
        format!(
            "repricing {} on {} (ts={:?}); cancelling {} orders",
            config_ref.strategy.symbol,
            reference.source,
            reference.ts_ns,
            cancels.len()
        )
    });
    if let Some(logger) = logger.as_ref() {
        for id in &cancels {
            logger.log_cancel(id, &reference, cancel_internal, send_start, sent_ts);
        }
    }

    Ok(())
}

async fn handle_quote_tick(
    now: Instant,
    strategy: Arc<Mutex<StrategyEngine>>,
    config: Arc<RunnerConfig>,
    contract_size: f64,
    order_manager: Arc<OrderManager>,
    logger: Option<QuoteLogHandle>,
    debug: DebugLogger,
    inventory: Arc<Mutex<InventoryTracker>>,
    order_minima: Option<OrderMinima>,
    size_spec: SizeSpec,
) -> Result<()> {
    let config_ref = config.as_ref();

    let plan_opt = match strategy.try_lock() {
        Some(mut guard) => {
            let plan = guard.plan_quotes(now);
            let metrics = guard.state_metrics();
            drop(guard);
            if latency_debug_enabled() {
                debug.latency(|| {
                    format!(
                        "latency-debug::strategy_state active_orders={} pending_cancels={} needs_requote={}",
                        metrics.active_orders, metrics.pending_cancels, metrics.needs_requote
                    )
                });
            }
            plan
        }
        None => {
            if latency_debug_enabled() {
                debug.latency(|| "latency-debug::quote skipped (strategy busy)".to_string());
            }
            return Ok(());
        }
    };

    if let Some(mut plan) = plan_opt {
        let reference_price = plan.reference_price;
        let net_contracts = {
            let guard = inventory.lock();
            guard.net_contracts()
        };
        let filter = filter_intents(
            &plan.intents,
            &config_ref.risk,
            contract_size,
            net_contracts,
            reference_price,
            order_minima.as_ref(),
            &size_spec,
        )?;
	        if !filter.skipped.is_empty() {
	            for (id, reason) in &filter.skipped {
	                debug.info(|| format!("skipping intent {} -> {}", id, reason));
	            }
	        }
	        plan.intents = filter.allowed.clone();
	        if plan.intents.is_empty() && plan.cancels.is_empty() {
	            return Ok(());
	        }
	        let latency_debug = latency_debug_enabled();

        let ref_meta = if let Some(meta) = plan.reference_meta.as_ref() {
            debug.info(|| {
                format!(
                    "quoting {} on {} (ts={:?}) latency={}µs",
                    config_ref.strategy.symbol,
                    meta.source,
                    meta.ts_ns,
                    now.checked_duration_since(plan.planned_at)
                        .unwrap_or_default()
                        .as_micros()
                )
            });
            Some(meta.clone())
        } else {
            debug.info(|| {
                format!(
                    "quoting {} with latest price {:.4}",
                    config_ref.strategy.symbol, plan.reference_price
                )
            });
            None
        };

        let intents = plan.intents.clone();
        let cancels = plan.cancels.clone();
        let send_start = Instant::now();
        let sent_ts = SystemTime::now();
        let debounce_budget_ms = match config_ref.strategy_kind {
            StrategyKind::MomentumFade => config_ref
                .momentum_fade
                .as_ref()
                .map(|cfg| cfg.min_interval_ms)
                .unwrap_or(config_ref.strategy.quote_interval_ms),
            StrategyKind::SimpleQuote => config_ref.strategy.quote_interval_ms,
        };
        let debounce_budget = Duration::from_millis(debounce_budget_ms.max(1));
        let (reference_instant, timer_wait) = if let Some(meta) = ref_meta.as_ref() {
            let age = plan.planned_at.saturating_duration_since(meta.received_at);
            if age <= debounce_budget {
                (meta.received_at, age)
            } else {
                (plan.planned_at, Duration::ZERO)
            }
        } else {
            (plan.planned_at, Duration::ZERO)
        };
        let raw_latency = send_start.saturating_duration_since(reference_instant);
        let quote_internal = raw_latency.saturating_sub(timer_wait);

        if latency_debug {
            if let Some(meta) = ref_meta.as_ref() {
                let meta_age = now.saturating_duration_since(meta.received_at);
                if meta_age > REF_WARN {
                    debug.latency(|| {
                        format!(
                            "latency-debug::quote ref_age={}us planned_age={}us intents={}",
                            dur_us(meta_age),
                            dur_us(plan.planned_at.saturating_duration_since(meta.received_at)),
                            intents.len()
                        )
                    });
                }
            }
        }

        if let Some(logger) = logger.as_ref() {
            if !cancels.is_empty() {
                let reference = ReferenceEvent {
                    price: plan.reference_price,
                    best_bid: plan.reference_best_bid,
                    best_ask: plan.reference_best_ask,
                    ts_ns: ref_meta.as_ref().and_then(|m| m.ts_ns),
                    source: ref_meta
                        .as_ref()
                        .map(|m| m.source.clone())
                        .unwrap_or_else(|| "unknown".to_string()),
                    received_at: ref_meta
                        .as_ref()
                        .map(|m| m.received_at)
                        .unwrap_or(plan.planned_at),
                };
                for id in &cancels {
                    logger.log_cancel(id, &reference, quote_internal, send_start, sent_ts);
                }
            }
            logger.log_quote_submission(
                &intents,
                ref_meta.as_ref(),
                plan.reference_price,
                Some(quote_internal),
                send_start,
                sent_ts,
            );
        }
        {
            let commit_lock_start = Instant::now();
            let mut strategy_guard = strategy.lock();
            if latency_debug_enabled() {
                let wait = commit_lock_start.elapsed();
                if wait > Duration::from_micros(200) {
                    debug
                        .latency(|| format!("latency-debug::quote commit_wait={}us", dur_us(wait)));
                }
            }
            strategy_guard.commit_plan(&plan);
        }
        {
            let added_ids = {
                let mut guard = inventory.lock();
                guard.record_orders(&intents)
            };
            if !added_ids.is_empty() {
                debug.info(|| {
                    format!(
                        "inventory register: {}",
                        added_ids
                            .iter()
                            .map(|id| id.0.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                });
            }
        }

        let intents_for_send = intents.clone();
        let cancels_for_send = cancels.clone();
        let ref_meta_for_send = ref_meta.clone();
        let config_clone = config.clone();
        let order_manager_clone = order_manager.clone();
        let quote_internal_for_send = quote_internal;
        let debug_clone = debug.clone();
        tokio::spawn(async move {
            let call_start = Instant::now();
            let res = if cancels_for_send.is_empty() {
                order_manager_clone.submit(intents_for_send.clone()).await
            } else {
                order_manager_clone
                    .cancel_and_submit(cancels_for_send.clone(), intents_for_send.clone())
                    .await
            };
            match res {
                Ok(acks) => {
                    if latency_debug_enabled() {
                        let call_elapsed = call_start.elapsed();
                        debug_clone.latency(|| {
                            format!(
                                "latency-debug::submit call={}us internal={}us intents={}",
                                dur_us(call_elapsed),
                                dur_us(quote_internal_for_send),
                                intents_for_send.len()
                            )
                        });
                    }
                    log_submission(
                        &intents_for_send,
                        &acks,
                        ref_meta_for_send.as_ref(),
                        reference_price,
                        Some(quote_internal_for_send),
                        config_clone.as_ref(),
                        &debug_clone,
                    );
                }
                Err(err) => {
                    let err_msg = format!("{:#}", err);
                    if cancels_for_send.is_empty() {
                        let intents_copy = intents_for_send.clone();
                        debug_clone.error(move || {
                            format!("submit {:?} failed: {}", intents_copy, err_msg)
                        });
                    } else {
                        let intents_copy = intents_for_send.clone();
                        let cancels_copy = cancels_for_send.clone();
                        debug_clone.error(move || {
                            format!(
                                "cancel+submit cancels={} intents={:?} failed: {}",
                                cancels_copy.len(),
                                intents_copy,
                                err_msg
                            )
                        });
                    }
                }
            }
        });
    }
    Ok(())
}

fn format_inventory_notional(
    net_contracts: f64,
    contract_size: f64,
    reference_price: Option<f64>,
) -> String {
    let base_qty = net_contracts * contract_size;
    if let Some(px) = reference_price.filter(|v| v.is_finite() && *v > 0.0) {
        let notional = base_qty.abs() * px;
        format!(
            "notional={:.6} base={:.6} ref_px={:.6}",
            notional, base_qty, px
        )
    } else {
        format!("notional=NA base={:.6} ref_px=NA", base_qty)
    }
}

async fn process_reports(
    reports: Vec<ExecutionReport>,
    strategy: Arc<Mutex<StrategyEngine>>,
    config: Arc<RunnerConfig>,
    logger: Option<QuoteLogHandle>,
    debug: DebugLogger,
    inventory: Arc<Mutex<InventoryTracker>>,
    contract_size: f64,
) -> Result<()> {
    if reports.is_empty() {
        return Ok(());
    }

    let markout_enabled = config.mode.markout_prints;
    let mut fill_contexts = Vec::with_capacity(reports.len());
    for report in &reports {
        let now = Instant::now();
        let (latest_price, fill_context) = {
            let mut strategy = strategy.lock();
            let context = strategy.fill_context(&report.client_order_id, now);
            strategy.handle_report(report);
            (strategy.latest_price(), context)
        };
        fill_contexts.push(fill_context);
        let outcome = {
            let mut guard = inventory.lock();
            guard.apply_report(report)
        };

        match outcome {
            InventoryReportOutcome::Applied(update) => {
                debug.info(|| {
                    let notional = format_inventory_notional(
                        update.new_contracts,
                        contract_size,
                        latest_price,
                    );
                    format!(
                        "inventory update ({:?}): {} {:?} status {:?} delta={:.4} contracts (fill_qty={:.4} @ {:?}) {prev:.4} -> {next:.4} {notional}",
                        update.source,
                        update.client_order_id,
                        update.side,
                        update.status,
                        update.delta_contracts,
                        update.fill_qty,
                        update.fill_price,
                        prev = update.prev_contracts,
                        next = update.new_contracts,
                        notional = notional
                    )
                });
            }
            InventoryReportOutcome::Missing {
                order_id,
                filled_qty,
                avg_price,
                status,
            } => {
                debug.error(|| {
                    let net_contracts = {
                        let guard = inventory.lock();
                        guard.net_contracts()
                    };
                    let notional =
                        format_inventory_notional(net_contracts, contract_size, latest_price);
                    format!(
                        "inventory warning: missing order {} for fill {:.4} (avg {:?}, status {:?}) {}",
                        order_id, filled_qty, avg_price, status, notional
                    )
                });
            }
            InventoryReportOutcome::None => {}
        }

        if markout_enabled && report.filled_qty > f64::EPSILON {
            if let Some(fill_price) = report
                .avg_fill_price
                .filter(|px| px.is_finite() && *px > 0.0)
            {
                let id = report.client_order_id.clone();
                tokio::spawn(async move {
                    sleep(Duration::from_secs(1)).await;
                    match lighter_mid_price() {
                        Some(mid) => {
                            let bps = (mid - fill_price) / fill_price * 10_000.0;
                            println!(
                                "[markout] id={} fill={:.6} mid_1s={:.6} bps={:.2}",
                                id.0, fill_price, mid, bps
                            );
                        }
                        None => {
                            println!("[markout] id={} fill={:.6} mid_1s=NA", id.0, fill_price);
                        }
                    }
                });
            }
        }
    }

    if let Some(logger) = logger.as_ref() {
        logger.log_reports_with_context(&reports, &fill_contexts);
    }

    log_reports(&reports, config.as_ref(), &debug);
    Ok(())
}

fn filter_intents(
    intents: &[QuoteIntent],
    risk: &RiskConfig,
    contract_size: f64,
    current_contracts: f64,
    reference_price: f64,
    order_minima: Option<&OrderMinima>,
    size_spec: &SizeSpec,
) -> Result<FilteredIntents> {
    if !contract_size.is_finite() || contract_size <= 0.0 {
        bail!("invalid contract size {contract_size}");
    }
    if !reference_price.is_finite() || reference_price <= 0.0 {
        bail!("invalid reference price {reference_price}");
    }
    if matches!(size_spec, SizeSpec::ExchangeMin) && order_minima.is_none() {
        bail!("size: min is only supported when exchange minima are known for this venue");
    }

    let base_contracts = current_contracts;
    let mut allowed = Vec::with_capacity(intents.len());
    let mut skipped = Vec::new();

    for intent in intents {
        let price = intent.price.abs();
        if !price.is_finite() || price <= 0.0 {
            bail!(
                "invalid price on intent {}: {}",
                intent.client_order_id,
                price
            );
        }

        let mut effective_size = intent.size.abs();
        if let Some(mins) = order_minima {
            let min_size = mins.base.max(mins.quote / price);
            let scale = 10f64.powi(mins.size_decimals as i32);
            let rounded_min = (min_size * scale).ceil() / scale;
            match size_spec {
                SizeSpec::ExchangeMin => {
                    if effective_size < rounded_min {
                        // lift to the required minimum for Lighter
                        effective_size = rounded_min;
                    }
                }
                SizeSpec::Fixed(_) => {
                    if effective_size + f64::EPSILON < rounded_min {
                        bail!(
                            "intent {} size {:.8} below Lighter minimum {:.8}",
                            intent.client_order_id,
                            intent.size,
                            rounded_min
                        );
                    }
                }
            }
        }

        let contracts = (effective_size / contract_size).round() as i64;
        if contracts == 0 {
            bail!(
                "intent {} size {:.8} is below contract size {}",
                intent.client_order_id,
                intent.size,
                contract_size
            );
        }

        let effective_size = contracts as f64 * contract_size;
        let notional = price * effective_size;
        if notional > risk.max_order_notional {
            bail!(
                "intent {} exceeds per-order notional limit: {:.2} > {:.2} (contracts={} size={:.8})",
                intent.client_order_id,
                notional,
                risk.max_order_notional,
                contracts,
                effective_size
            );
        }

        if risk.max_position_notional > 0.0 {
            let contracts_as_f64 = contracts as f64;
            let projected_contracts = match intent.side {
                Side::Bid => base_contracts + contracts_as_f64,
                Side::Ask => base_contracts - contracts_as_f64,
            };
            let basis_price = price.max(reference_price.abs());
            let projected_notional = projected_contracts.abs() * contract_size * basis_price;
            let current_notional = base_contracts.abs() * contract_size * basis_price;

            let breaches_limit = projected_notional > risk.max_position_notional;
            let increases_exposure = projected_notional > current_notional + 1e-9;

            if breaches_limit && increases_exposure {
                skipped.push((
                    intent.client_order_id.clone(),
                    format!(
                        "projected notional {:.2} exceeds {:.2}",
                        projected_notional, risk.max_position_notional
                    ),
                ));
                continue;
            }
        }

        let mut adjusted_intent = intent.clone();
        adjusted_intent.size = effective_size;
        allowed.push(adjusted_intent);
    }

    Ok(FilteredIntents { allowed, skipped })
}

fn log_submission(
    intents: &[QuoteIntent],
    acks: &[OrderAck],
    reference_meta: Option<&ReferenceMeta>,
    reference_price: f64,
    quote_internal: Option<Duration>,
    config: &RunnerConfig,
    debug: &DebugLogger,
) {
    let (source, ts_ns) = reference_meta
        .map(|meta| (meta.source.as_str(), meta.ts_ns))
        .unwrap_or(("unknown", None));
    let mode = if config.mode.dry_run {
        "dry-run"
    } else {
        "live"
    };
    for (intent, ack) in intents.iter().zip(acks.iter()) {
        debug.info(|| {
            format!(
                "ref {:.4} ({}) -> {:?} {:.4} @ {:.4} ({}) exch_id={:?} latency={}µs",
                reference_price,
                source,
                intent.side,
                intent.size,
                intent.price,
                mode,
                ack.exchange_order_id,
                quote_internal.map(|dur| dur.as_micros()).unwrap_or(0)
            )
        });
        if config.mode.log_fills {
            debug.info(|| {
                format!(
                    "  intent {} tif={} size={:.4} ts={:?}",
                    intent.client_order_id, intent.tif, intent.size, ts_ns
                )
            });
        }
    }
}

fn log_reports(reports: &[ExecutionReport], config: &RunnerConfig, debug: &DebugLogger) {
    for report in reports {
        let should_log = config.mode.log_fills
            || matches!(
                report.status,
                OrderStatus::Filled | OrderStatus::PartiallyFilled | OrderStatus::Rejected
            );
        if should_log {
            debug.info(|| {
                format!(
                    "report {} status {:?} filled {:.6} avg {:?} ts={:?}",
                    report.client_order_id,
                    report.status,
                    report.filled_qty,
                    report.avg_fill_price,
                    report.ts
                )
            });
        }
    }
}

async fn ctrl_c_notifier() {
    let _ = tokio::signal::ctrl_c().await;
}

async fn setup_gate_gateway(
    config: &RunnerConfig,
    contract_size: f64,
    creds: &GateCredentials,
    settle: &str,
) -> Result<GateWsGateway> {
    let ws_config = GateWsConfig {
        api_key: creds.api_key.clone(),
        api_secret: creds.api_secret.clone(),
        symbol: config.strategy.symbol.clone(),
        settle: Some(settle.to_string()),
        ws_url: None,
        contract_size: Some(contract_size),
    };

    GateWsGateway::connect(ws_config).await
}

async fn setup_lighter_gateway(
    config: &RunnerConfig,
    creds: &LighterCredentials,
    meta: &lighter_rest::LighterMarketMeta,
) -> Result<LighterGateway> {
    let mut creds = creds.clone();
    creds.signer_lib = resolve_lighter_signer_path(&creds.signer_lib)?;
    LighterGateway::connect(
        creds,
        meta.market_id,
        meta.price_decimals,
        meta.size_decimals,
        config.mode.debug_prints,
    )
    .await
    .map_err(Into::into)
}

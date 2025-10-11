#![cfg(feature = "gate_exec")]

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use clap::Parser;
use rust_test::base_classes::engine::spawn_state_engine;
use rust_test::base_classes::reference::ReferenceEvent;
use rust_test::base_classes::state::state as global_state;
use rust_test::base_classes::state::{ExchangeAdjustment, ExchangeSnap, FeedSnap, TradeDirection};
use rust_test::base_classes::types::Side;
use rust_test::exchanges::{endpoints::GateioGet, gate_rest};
use rust_test::execution::{
    ClientOrderId, DryRunGateway, ExecutionGateway, ExecutionReport, GateWsConfig, GateWsGateway,
    OrderAck, OrderManager, OrderStatus, QuoteIntent, Venue,
};
use rust_test::strategy::{QuoteConfig, ReferenceMeta, SimpleQuoteStrategy};
use serde_json::Value;
use tokio::sync::{Mutex, mpsc};
use tokio::time::{self, MissedTickBehavior, interval};

use hmac::{Hmac, Mac};
use reqwest::{Client, Method};
use sha2::{Digest, Sha512};

#[derive(Debug, Parser)]
#[command(name = "gate-runner", about = "Gate.io MVP dry-run executor")]
struct Cli {
    /// Path to YAML configuration
    #[arg(long, default_value = "config/gate_mvp.yaml")]
    config: String,
}

#[derive(Debug, serde::Deserialize, Clone)]
struct RiskConfig {
    max_order_notional: f64,
    #[serde(default)]
    max_position_notional: f64,
}

#[derive(Debug, serde::Deserialize, Clone)]
struct ModeConfig {
    #[serde(default = "default_true")]
    dry_run: bool,
    #[serde(default)]
    log_fills: bool,
    #[serde(default)]
    debug_prints: bool,
}

#[derive(Debug, serde::Deserialize, Clone, Default)]
#[serde(default)]
struct LoggingConfig {
    enabled: bool,
    path: Option<String>,
    #[serde(default = "default_flush_interval_ms")]
    flush_interval_ms: u64,
}

impl LoggingConfig {
    fn is_enabled(&self) -> bool {
        self.enabled
    }

    fn resolve_path(&self) -> PathBuf {
        self.path
            .as_ref()
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("logs/gate_activity.csv"))
    }

    fn flush_interval(&self) -> Duration {
        Duration::from_millis(self.flush_interval_ms.max(1))
    }
}

fn default_true() -> bool {
    true
}

fn default_flush_interval_ms() -> u64 {
    200
}

#[derive(Debug, Clone)]
struct GateCredentials {
    api_key: String,
    api_secret: String,
}

#[derive(Debug, Default)]
struct InventoryTracker {
    contract_size: f64,
    net_contracts: f64,
    orders: HashMap<ClientOrderId, OrderFillState>,
    dangling: HashMap<ClientOrderId, DanglingFill>,
}

#[derive(Debug)]
struct OrderFillState {
    side: Side,
    filled_qty: f64,
}

#[derive(Debug, Clone)]
struct InventoryUpdate {
    client_order_id: ClientOrderId,
    side: Side,
    prev_contracts: f64,
    new_contracts: f64,
    delta_contracts: f64,
    fill_qty: f64,
    fill_price: Option<f64>,
    status: OrderStatus,
    source: InventoryUpdateSource,
}

#[derive(Debug, Clone)]
struct DanglingFill {
    side: Side,
    filled_qty: f64,
}

#[derive(Debug, Clone, Copy)]
enum InventoryUpdateSource {
    Registered,
    Inferred,
}

enum InventoryReportOutcome {
    None,
    Applied(InventoryUpdate),
    Missing {
        order_id: ClientOrderId,
        filled_qty: f64,
        avg_price: Option<f64>,
        status: OrderStatus,
    },
}

impl InventoryTracker {
    fn new(contract_size: f64, initial_contracts: f64) -> Self {
        Self {
            contract_size,
            net_contracts: initial_contracts,
            orders: HashMap::new(),
            dangling: HashMap::new(),
        }
    }

    fn net_contracts(&self) -> f64 {
        self.net_contracts
    }

    fn record_orders(&mut self, intents: &[QuoteIntent]) -> Vec<ClientOrderId> {
        let mut added = Vec::with_capacity(intents.len());
        for intent in intents {
            let id = intent.client_order_id.clone();
            let mut state = OrderFillState {
                side: intent.side,
                filled_qty: 0.0,
            };
            if let Some(dangling) = self.dangling.remove(&id) {
                state.filled_qty = dangling.filled_qty;
            }
            self.orders.insert(id.clone(), state);
            added.push(id);
        }
        added
    }

    fn apply_report(&mut self, report: &ExecutionReport) -> InventoryReportOutcome {
        if let Some(entry) = self.orders.get_mut(&report.client_order_id) {
            let side = entry.side;
            let mut update: Option<InventoryUpdate> = None;

            if report.filled_qty > entry.filled_qty + f64::EPSILON {
                let delta_qty = report.filled_qty - entry.filled_qty;
                let delta_contracts = if self.contract_size > 0.0 {
                    delta_qty / self.contract_size
                } else {
                    0.0
                };

                if delta_contracts.abs() > f64::EPSILON {
                    let prev_contracts = self.net_contracts;
                    match side {
                        Side::Bid => self.net_contracts += delta_contracts,
                        Side::Ask => self.net_contracts -= delta_contracts,
                    }
                    entry.filled_qty = report.filled_qty;
                    update = Some(InventoryUpdate {
                        client_order_id: report.client_order_id.clone(),
                        side,
                        prev_contracts,
                        new_contracts: self.net_contracts,
                        delta_contracts: self.net_contracts - prev_contracts,
                        fill_qty: delta_qty,
                        fill_price: report.avg_fill_price,
                        status: report.status.clone(),
                        source: InventoryUpdateSource::Registered,
                    });
                }
            }

            if matches!(
                report.status,
                OrderStatus::Filled | OrderStatus::Canceled | OrderStatus::Rejected
            ) {
                self.orders.remove(&report.client_order_id);
                self.dangling.remove(&report.client_order_id);
            }

            if let Some(update) = update {
                InventoryReportOutcome::Applied(update)
            } else {
                InventoryReportOutcome::None
            }
        } else if report.filled_qty > f64::EPSILON {
            let (side, prev_filled) =
                if let Some(dangling) = self.dangling.get(&report.client_order_id) {
                    (dangling.side, dangling.filled_qty)
                } else if let Some(inferred) = infer_side_from_id(&report.client_order_id) {
                    (inferred, 0.0)
                } else {
                    return InventoryReportOutcome::Missing {
                        order_id: report.client_order_id.clone(),
                        filled_qty: report.filled_qty,
                        avg_price: report.avg_fill_price,
                        status: report.status.clone(),
                    };
                };

            let delta_qty = report.filled_qty - prev_filled;
            let delta_contracts = if self.contract_size > 0.0 {
                delta_qty / self.contract_size
            } else {
                0.0
            };
            if delta_contracts.abs() > f64::EPSILON {
                let prev = self.net_contracts;
                match side {
                    Side::Bid => self.net_contracts += delta_contracts,
                    Side::Ask => self.net_contracts -= delta_contracts,
                }
                let update = InventoryUpdate {
                    client_order_id: report.client_order_id.clone(),
                    side,
                    prev_contracts: prev,
                    new_contracts: self.net_contracts,
                    delta_contracts: self.net_contracts - prev,
                    fill_qty: delta_qty,
                    fill_price: report.avg_fill_price,
                    status: report.status.clone(),
                    source: InventoryUpdateSource::Inferred,
                };

                self.dangling.insert(
                    report.client_order_id.clone(),
                    DanglingFill {
                        side,
                        filled_qty: report.filled_qty,
                    },
                );

                if matches!(
                    report.status,
                    OrderStatus::Filled | OrderStatus::Canceled | OrderStatus::Rejected
                ) {
                    self.dangling.remove(&report.client_order_id);
                }

                InventoryReportOutcome::Applied(update)
            } else {
                self.dangling.insert(
                    report.client_order_id.clone(),
                    DanglingFill {
                        side,
                        filled_qty: report.filled_qty,
                    },
                );
                InventoryReportOutcome::None
            }
        } else {
            InventoryReportOutcome::None
        }
    }

    fn replace_from_rest(&mut self, contracts: f64) -> Option<(f64, f64)> {
        let prev = self.net_contracts;
        if (contracts - prev).abs() > 1e-9 {
            self.net_contracts = contracts;
            Some((prev, contracts))
        } else {
            None
        }
    }
}

#[derive(Clone)]
struct DebugLogger {
    tx: mpsc::UnboundedSender<DebugEvent>,
    debug_enabled: bool,
}

enum DebugEvent {
    Info(String),
    Warn(String),
    Error(String),
}

impl DebugLogger {
    fn new(debug_enabled: bool) -> Self {
        let (tx, mut rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                match event {
                    DebugEvent::Info(msg) => println!("{}", msg),
                    DebugEvent::Warn(msg) => eprintln!("{}", msg),
                    DebugEvent::Error(msg) => eprintln!("{}", msg),
                }
            }
        });
        Self { tx, debug_enabled }
    }

    fn info<F>(&self, msg: F)
    where
        F: FnOnce() -> String,
    {
        if !self.debug_enabled {
            return;
        }
        let _ = self.tx.send(DebugEvent::Info(msg()));
    }

    fn error<F>(&self, msg: F)
    where
        F: FnOnce() -> String,
    {
        let _ = self.tx.send(DebugEvent::Error(msg()));
    }

    fn latency<F>(&self, msg: F)
    where
        F: FnOnce() -> String,
    {
        let _ = self.tx.send(DebugEvent::Warn(msg()));
    }
}

#[derive(Debug, serde::Deserialize, Clone)]
struct RunnerConfig {
    strategy: QuoteConfig,
    risk: RiskConfig,
    mode: ModeConfig,
    #[serde(default)]
    logging: LoggingConfig,
    #[serde(default)]
    credentials: Option<CredentialsConfig>,
    #[serde(default)]
    settle: Option<String>,
}

#[derive(Debug, serde::Deserialize, Clone, Default)]
struct CredentialsConfig {
    #[serde(default)]
    api_key_env: Option<String>,
    #[serde(default)]
    api_secret_env: Option<String>,
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

const REF_WARN: Duration = Duration::from_millis(20);
const STAGE_WARN: Duration = Duration::from_millis(5);

fn apply_demean(price: f64, adj: Option<&ExchangeAdjustment>) -> f64 {
    if !price.is_finite() || price <= 0.0 {
        return price;
    }
    if let Some(adj) = adj {
        if adj.samples > 0 {
            return price - adj.offset.unwrap_or(0.0);
        }
    }
    price
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let cli = Cli::parse();
    let mut config = load_config(&cli.config)?;
    let debug = DebugLogger::new(config.mode.debug_prints);

    let contract_meta = gate_rest::fetch_contract_meta_async(&config.strategy.symbol)
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

    let contract_size = contract_meta
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
            "resolved contract size for {}: {}",
            config.strategy.symbol, contract_size
        )
    });

    let logger = if config.logging.is_enabled() {
        let handle = QuoteLogHandle::spawn(&config, debug.clone())?;
        debug.info(|| format!("Activity logging enabled -> {}", handle.path().display()));
        Some(handle)
    } else {
        None
    };

    let settle = config.settle.clone().unwrap_or_else(|| "usdt".to_string());

    let credentials = if config.mode.dry_run {
        None
    } else {
        Some(load_gate_credentials(&config)?)
    };

    let rest_client = credentials
        .as_ref()
        .map(|creds| GateRestClient::new(creds.clone()));

    let initial_contracts = if let Some(client) = rest_client.as_ref() {
        match client
            .fetch_position_contracts(&settle, &config.strategy.symbol)
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
        }
    } else {
        0.0
    };

    let inventory = Arc::new(Mutex::new(InventoryTracker::new(
        contract_size,
        initial_contracts,
    )));

    if let Some(client) = rest_client.clone() {
        let inventory_clone = inventory.clone();
        let settle_clone = settle.clone();
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
                            let mut guard = inventory_clone.lock().await;
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
                            let mut guard = inventory_clone.lock().await;
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
    let _engine = spawn_state_engine(config.strategy.symbol.clone(), Some(reference_tx));
    debug.info(|| {
        format!(
            "Gate runner started for {} (dry_run: {})",
            config.strategy.symbol, config.mode.dry_run
        )
    });

    let gateway: Arc<dyn ExecutionGateway> = if config.mode.dry_run {
        Arc::new(DryRunGateway::new())
    } else {
        let creds = credentials
            .as_ref()
            .expect("credentials must exist for live mode")
            .clone();
        Arc::new(setup_live_gateway(&config, contract_size, &creds).await?)
    };
    let order_manager = Arc::new(OrderManager::new(gateway, Duration::from_secs(30)));
    let mut strategy = SimpleQuoteStrategy::new(config.strategy.clone());

    let mut market_timer = interval(Duration::from_millis(20));
    market_timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut quote_timer = interval(Duration::from_millis(50));
    quote_timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let warmup = Duration::from_secs(25);
    let start_time = Instant::now();

    tokio::select! {
        _ = ctrl_c_notifier() => {
            debug.info(|| "Received shutdown signal; exiting.".to_string());
        }
        _ = async {
            loop {
                tokio::select! {
                    reference = reference_rx.recv() => {
                        match reference {
                            Some(reference) => {
                                if start_time.elapsed() < warmup {
                                    continue;
                                }
                                if let Err(err) = handle_market_update(
                                    reference,
                                    &mut strategy,
                                    &config,
                                    order_manager.clone(),
                                    logger.clone(),
                                    debug.clone(),
                                ).await {
                                    debug.error(|| format!("error handling market update: {:#}", err));
                                }
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

                        if let Err(err) =
                            drain_reports(
                                &mut strategy,
                                &config,
                                order_manager.clone(),
                                logger.clone(),
                                debug.clone(),
                                inventory.clone(),
                            ).await
                        {
                            debug.error(|| format!("error processing reports: {:#}", err));
                        }
                    }
                    _ = quote_timer.tick() => {
                        if start_time.elapsed() < warmup {
                            continue;
                        }
                        let now = Instant::now();
                        if let Err(err) = handle_quote_tick(
                            now,
                            &mut strategy,
                            &config,
                            contract_size,
                            order_manager.clone(),
                            logger.clone(),
                            debug.clone(),
                            inventory.clone(),
                        ).await {
                            debug.error(|| format!("error handling quote tick: {:#}", err));
                        }
                    }
                }
            }
        } => {}
    }

    Ok(())
}

fn load_config(path: &str) -> Result<RunnerConfig> {
    let contents = std::fs::read_to_string(Path::new(path))
        .with_context(|| format!("failed to read config at {}", path))?;
    let config: RunnerConfig = serde_yaml::from_str(&contents)
        .with_context(|| format!("failed to parse config at {}", path))?;
    Ok(config)
}

#[derive(Clone)]
struct GateRestClient {
    client: Client,
    credentials: GateCredentials,
}

struct FilteredIntents {
    allowed: Vec<QuoteIntent>,
    skipped: Vec<(ClientOrderId, String)>,
}

impl GateRestClient {
    fn new(credentials: GateCredentials) -> Self {
        let client = Client::builder()
            .user_agent("gate-runner/0.1")
            .build()
            .expect("reqwest client");
        Self {
            client,
            credentials,
        }
    }

    async fn fetch_position_contracts(&self, settle: &str, contract: &str) -> Result<Option<f64>> {
        let path = format!("/api/v4/futures/{}/positions", settle);
        let query = format!("contract={}", contract);
        let value = self
            .signed_request(Method::GET, &path, &query, "")
            .await
            .with_context(|| format!("failed to GET positions for {}", contract))?;

        let matching_entry = match &value {
            Value::Array(entries) => entries.iter().find(|entry| {
                entry
                    .get("contract")
                    .and_then(|v| v.as_str())
                    .map(|s| s.eq_ignore_ascii_case(contract))
                    .unwrap_or(false)
            }),
            Value::Object(_) => {
                if value
                    .get("contract")
                    .and_then(|v| v.as_str())
                    .map(|s| s.eq_ignore_ascii_case(contract))
                    .unwrap_or(false)
                {
                    Some(&value)
                } else {
                    None
                }
            }
            _ => None,
        };

        Ok(matching_entry.and_then(extract_position_contracts))
    }

    async fn signed_request(
        &self,
        method: Method,
        path: &str,
        query: &str,
        body: &str,
    ) -> Result<Value> {
        let method_name = method.as_str();
        let ts = current_unix_seconds_string();
        let payload_hash = sha512_hex(body);
        let sign_payload = format!(
            "{}\n{}\n{}\n{}\n{}",
            method_name, path, query, payload_hash, ts
        );

        let mut mac = Hmac::<Sha512>::new_from_slice(self.credentials.api_secret.as_bytes())
            .expect("HMAC accepts any key size");
        mac.update(sign_payload.as_bytes());
        let signature = hex_bytes(mac.finalize().into_bytes());

        let url = if query.is_empty() {
            format!("{}{}", GateioGet::BASE, path)
        } else {
            format!("{}{}?{}", GateioGet::BASE, path, query)
        };

        let mut request = self.client.request(method, &url);
        if !body.is_empty() {
            request = request.body(body.to_string());
        }
        let response = request
            .header("KEY", &self.credentials.api_key)
            .header("Timestamp", &ts)
            .header("SIGN", signature)
            .header("Accept", "application/json")
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

fn load_gate_credentials(config: &RunnerConfig) -> Result<GateCredentials> {
    let creds = config.credentials.clone().unwrap_or_default();
    let key_env = creds
        .api_key_env
        .unwrap_or_else(|| "GATEIO_API_KEY".to_string());
    let secret_env = creds
        .api_secret_env
        .unwrap_or_else(|| "GATEIO_SECRET_KEY".to_string());

    let api_key = std::env::var(&key_env).with_context(|| format!("missing env var {key_env}"))?;
    let api_secret =
        std::env::var(&secret_env).with_context(|| format!("missing env var {secret_env}"))?;
    Ok(GateCredentials {
        api_key,
        api_secret,
    })
}

async fn handle_market_update(
    reference: ReferenceEvent,
    strategy: &mut SimpleQuoteStrategy,
    config: &RunnerConfig,
    order_manager: Arc<OrderManager>,
    logger: Option<QuoteLogHandle>,
    debug: DebugLogger,
) -> Result<()> {
    let latency_debug = latency_debug_enabled();
    let now = Instant::now();
    let reference_age = now.saturating_duration_since(reference.received_at);
    let meta = ReferenceMeta {
        source: reference.source.clone(),
        ts_ns: reference.ts_ns,
        received_at: reference.received_at,
    };
    let strat_start = Instant::now();
    let cancels =
        strategy.on_market_update(reference.price, Some(meta.clone()), reference.received_at);
    let strat_dur = strat_start.elapsed();

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

    if !cancels.is_empty() {
        debug.info(|| {
            format!(
                "repricing {} on {} (ts={:?}); cancelling {} orders",
                config.strategy.symbol,
                reference.source,
                reference.ts_ns,
                cancels.len()
            )
        });
        let send_start = Instant::now();
        strategy.record_cancel_submission(send_start);
        let cancel_internal = send_start.saturating_duration_since(reference.received_at);
        let sent_ts = SystemTime::now();
        if let Some(logger) = logger.as_ref() {
            for id in &cancels {
                logger.log_cancel(id, &reference, cancel_internal, send_start, sent_ts);
            }
        }

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
    }

    Ok(())
}

async fn handle_quote_tick(
    now: Instant,
    strategy: &mut SimpleQuoteStrategy,
    config: &RunnerConfig,
    contract_size: f64,
    order_manager: Arc<OrderManager>,
    logger: Option<QuoteLogHandle>,
    debug: DebugLogger,
    inventory: Arc<Mutex<InventoryTracker>>,
) -> Result<()> {
    drain_reports(
        strategy,
        config,
        order_manager.clone(),
        logger.clone(),
        debug.clone(),
        inventory.clone(),
    )
    .await?;

    if let Some(mut plan) = strategy.plan_quotes(now) {
        let reference_price = plan.reference_price;
        let net_contracts = {
            let guard = inventory.lock().await;
            guard.net_contracts()
        };
        let filter = filter_intents(
            &plan.intents,
            &config.risk,
            contract_size,
            net_contracts,
            reference_price,
        )?;
        if !filter.skipped.is_empty() {
            for (id, reason) in &filter.skipped {
                debug.info(|| format!("skipping intent {} -> {}", id, reason));
            }
        }
        if filter.allowed.is_empty() {
            return Ok(());
        }
        plan.intents = filter.allowed.clone();
        let latency_debug = latency_debug_enabled();

        let ref_meta = if let Some(meta) = plan.reference_meta.as_ref() {
            debug.info(|| {
                format!(
                    "quoting {} on {} (ts={:?}) latency={}µs",
                    config.strategy.symbol,
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
                    config.strategy.symbol, plan.reference_price
                )
            });
            None
        };

        let intents = plan.intents.clone();
        let send_start = Instant::now();
        let sent_ts = SystemTime::now();
        let debounce_budget = Duration::from_millis(config.strategy.debounce_ms.max(1));
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
            logger.log_quote_submission(
                &intents,
                ref_meta.as_ref(),
                plan.reference_price,
                Some(quote_internal),
                send_start,
                sent_ts,
            );
        }
        strategy.commit_plan(&plan);
        {
            let added_ids = {
                let mut guard = inventory.lock().await;
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
        let ref_meta_for_send = ref_meta.clone();
        let config_clone = config.clone();
        let order_manager_clone = order_manager.clone();
        let quote_internal_for_send = quote_internal;
        let debug_clone = debug.clone();
        tokio::spawn(async move {
            let call_start = Instant::now();
            match order_manager_clone.submit(intents_for_send.clone()).await {
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
                        &config_clone,
                        &debug_clone,
                    );
                }
                Err(err) => {
                    let err_msg = format!("{:#}", err);
                    let intents_copy = intents_for_send.clone();
                    debug_clone
                        .error(move || format!("submit {:?} failed: {}", intents_copy, err_msg));
                }
            }
        });
    }
    Ok(())
}

async fn drain_reports(
    strategy: &mut SimpleQuoteStrategy,
    config: &RunnerConfig,
    order_manager: Arc<OrderManager>,
    logger: Option<QuoteLogHandle>,
    debug: DebugLogger,
    inventory: Arc<Mutex<InventoryTracker>>,
) -> Result<()> {
    let reports = order_manager.poll_reports().await?;
    if reports.is_empty() {
        return Ok(());
    }

    for report in &reports {
        strategy.handle_report(report);
        let outcome = {
            let mut guard = inventory.lock().await;
            guard.apply_report(report)
        };

        match outcome {
            InventoryReportOutcome::Applied(update) => {
                debug.info(|| {
                    format!(
                        "inventory update ({:?}): {} {:?} status {:?} delta={:.4} contracts (fill_qty={:.4} @ {:?}) {prev:.4} -> {next:.4}",
                        update.source,
                        update.client_order_id,
                        update.side,
                        update.status,
                        update.delta_contracts,
                        update.fill_qty,
                        update.fill_price,
                        prev = update.prev_contracts,
                        next = update.new_contracts
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
                    format!(
                        "inventory warning: missing order {} for fill {:.4} (avg {:?}, status {:?})",
                        order_id,
                        filled_qty,
                        avg_price,
                        status
                    )
                });
            }
            InventoryReportOutcome::None => {}
        }
    }

    if let Some(logger) = logger.as_ref() {
        logger.log_reports(&reports);
    }

    log_reports(&reports, config, &debug);
    Ok(())
}

fn filter_intents(
    intents: &[QuoteIntent],
    risk: &RiskConfig,
    contract_size: f64,
    current_contracts: f64,
    reference_price: f64,
) -> Result<FilteredIntents> {
    if !contract_size.is_finite() || contract_size <= 0.0 {
        bail!("invalid contract size {contract_size}");
    }
    if !reference_price.is_finite() || reference_price <= 0.0 {
        bail!("invalid reference price {reference_price}");
    }

    let mut running_contracts = current_contracts;
    let mut allowed = Vec::with_capacity(intents.len());
    let mut skipped = Vec::new();

    for intent in intents {
        let contracts = (intent.size.abs() / contract_size).round() as i64;
        if contracts == 0 {
            bail!(
                "intent {} size {:.8} is below contract size {}",
                intent.client_order_id,
                intent.size,
                contract_size
            );
        }

        let effective_size = contracts as f64 * contract_size;
        let price = intent.price.abs();
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
                Side::Bid => running_contracts + contracts_as_f64,
                Side::Ask => running_contracts - contracts_as_f64,
            };
            let basis_price = price.max(reference_price.abs());
            let projected_notional = projected_contracts.abs() * contract_size * basis_price;
            let current_notional = running_contracts.abs() * contract_size * basis_price;

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
            running_contracts = projected_contracts;
        }

        allowed.push(intent.clone());
    }

    Ok(FilteredIntents { allowed, skipped })
}

fn infer_side_from_id(id: &ClientOrderId) -> Option<Side> {
    let lower = id.0.to_ascii_lowercase();
    if lower.contains("-b-") {
        Some(Side::Bid)
    } else if lower.contains("-s-") {
        Some(Side::Ask)
    } else {
        None
    }
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

struct QuoteSnapshot {
    side: Side,
    price: f64,
    size: f64,
    filled_qty: f64,
    sent_ns: Option<u128>,
    send_instant: Option<Instant>,
}

struct CancelSnapshot {
    send_instant: Instant,
}

struct QuoteCsvLogger {
    writer: BufWriter<File>,
    path: PathBuf,
    venue: String,
    last_bybit: (u64, u64, u64),
    last_binance: (u64, u64, u64),
    last_gate: (u64, u64, u64),
    last_bitget: (u64, u64, u64),
    orders: HashMap<String, QuoteSnapshot>,
    pending_cancels: HashMap<String, CancelSnapshot>,
}

enum ExchangeId {
    Bybit,
    Binance,
    Gate,
    Bitget,
}

enum LogEvent {
    MarketSnapshot,
    QuoteSubmission {
        intents: Vec<QuoteIntent>,
        reference_meta: Option<ReferenceMeta>,
        reference_price: f64,
        quote_internal: Option<Duration>,
        send_instant: Instant,
        sent_ts: SystemTime,
    },
    Cancel {
        order_id: String,
        reference: ReferenceEvent,
        cancel_internal: Duration,
        send_instant: Instant,
        sent_ts: SystemTime,
    },
    Reports(Vec<ExecutionReport>),
}

#[derive(Clone)]
struct QuoteLogHandle {
    tx: mpsc::UnboundedSender<LogEvent>,
    path: PathBuf,
}

impl QuoteLogHandle {
    fn spawn(config: &RunnerConfig, debug: DebugLogger) -> Result<Self> {
        let logger = QuoteCsvLogger::new(config)?;
        let path = logger.path.clone();
        let flush_interval = config.logging.flush_interval();
        let (tx, mut rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            let mut logger = logger;
            let mut last_flush = Instant::now();
            while let Some(event) = rx.recv().await {
                let res = match event {
                    LogEvent::MarketSnapshot => logger.log_market_snapshot(),
                    LogEvent::QuoteSubmission {
                        intents,
                        reference_meta,
                        reference_price,
                        quote_internal,
                        send_instant,
                        sent_ts,
                    } => logger.log_quote_submission(
                        &intents,
                        reference_meta.as_ref(),
                        reference_price,
                        quote_internal,
                        send_instant,
                        sent_ts,
                    ),
                    LogEvent::Cancel {
                        order_id,
                        reference,
                        cancel_internal,
                        send_instant,
                        sent_ts,
                    } => logger.log_cancel(
                        &order_id,
                        &reference,
                        cancel_internal,
                        send_instant,
                        sent_ts,
                    ),
                    LogEvent::Reports(reports) => {
                        let mut result = Ok(());
                        for report in &reports {
                            if let Err(err) = logger.log_report(report) {
                                result = Err(err);
                                break;
                            }
                        }
                        result
                    }
                };

                if let Err(err) = res {
                    debug.error(|| format!("csv logger error: {:#}", err));
                }

                if last_flush.elapsed() >= flush_interval {
                    if let Err(err) = logger.flush() {
                        debug.error(|| format!("csv logger flush error: {:#}", err));
                    }
                    last_flush = Instant::now();
                }
            }

            if let Err(err) = logger.flush() {
                debug.error(|| format!("csv logger final flush error: {:#}", err));
            }
        });

        Ok(Self { tx, path })
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn log_market_snapshot(&self) {
        let _ = self.tx.send(LogEvent::MarketSnapshot);
    }

    fn log_quote_submission(
        &self,
        intents: &[QuoteIntent],
        reference_meta: Option<&ReferenceMeta>,
        reference_price: f64,
        quote_internal: Option<Duration>,
        send_instant: Instant,
        sent_ts: SystemTime,
    ) {
        if intents.is_empty() {
            return;
        }
        let _ = self.tx.send(LogEvent::QuoteSubmission {
            intents: intents.to_vec(),
            reference_meta: reference_meta.cloned(),
            reference_price,
            quote_internal,
            send_instant,
            sent_ts,
        });
    }

    fn log_cancel(
        &self,
        order_id: &ClientOrderId,
        reference: &ReferenceEvent,
        cancel_internal: Duration,
        send_instant: Instant,
        sent_ts: SystemTime,
    ) {
        let _ = self.tx.send(LogEvent::Cancel {
            order_id: order_id.0.clone(),
            reference: reference.clone(),
            cancel_internal,
            send_instant,
            sent_ts,
        });
    }

    fn log_reports(&self, reports: &[ExecutionReport]) {
        if reports.is_empty() {
            return;
        }
        let _ = self.tx.send(LogEvent::Reports(reports.to_vec()));
    }
}

impl QuoteCsvLogger {
    fn new(config: &RunnerConfig) -> Result<Self> {
        let path = config.logging.resolve_path();
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create log dir {}", parent.display()))?;
        }
        let file = File::create(&path)
            .with_context(|| format!("failed to create log file {}", path.display()))?;
        let mut writer = BufWriter::new(file);
        writeln!(
            writer,
            "ts_ns,exchange,feed,price,direction,event_type,client_order_id,side,size,reference_source,reference_ts_ns,reference_price,quote_internal_us,cancel_internal_us,quote_external_us,cancel_external_us,sent_ts_ns"
        )?;
        writer.flush()?;

        Ok(Self {
            writer,
            path,
            venue: venue_to_string(config.strategy.venue).to_string(),
            last_bybit: (0, 0, 0),
            last_binance: (0, 0, 0),
            last_gate: (0, 0, 0),
            last_bitget: (0, 0, 0),
            orders: HashMap::new(),
            pending_cancels: HashMap::new(),
        })
    }

    fn log_market_snapshot(&mut self) -> Result<()> {
        let st = global_state()
            .lock()
            .map_err(|_| anyhow!("global state poisoned"))?;

        self.write_exchange(
            ExchangeId::Bybit,
            "bybit",
            &st.bybit,
            Some(&st.demean.bybit),
        )?;
        self.write_exchange(
            ExchangeId::Binance,
            "binance",
            &st.binance,
            Some(&st.demean.binance),
        )?;
        self.write_exchange(ExchangeId::Gate, "gate", &st.gate, None)?;
        self.write_exchange(
            ExchangeId::Bitget,
            "bitget",
            &st.bitget,
            Some(&st.demean.bitget),
        )?;
        Ok(())
    }

    fn write_exchange(
        &mut self,
        id: ExchangeId,
        exchange: &str,
        snap: &ExchangeSnap,
        adj: Option<&ExchangeAdjustment>,
    ) -> Result<()> {
        let cache = match id {
            ExchangeId::Bybit => &mut self.last_bybit,
            ExchangeId::Binance => &mut self.last_binance,
            ExchangeId::Gate => &mut self.last_gate,
            ExchangeId::Bitget => &mut self.last_bitget,
        };
        let (orderbook_seq, bbo_seq, trade_seq) = cache;
        let writer = &mut self.writer;
        Self::write_feed_entry(
            writer,
            exchange,
            "orderbook",
            &snap.orderbook,
            orderbook_seq,
            adj,
        )?;
        Self::write_feed_entry(writer, exchange, "bbo", &snap.bbo, bbo_seq, adj)?;
        Self::write_feed_entry(writer, exchange, "trade", &snap.trade, trade_seq, adj)?;
        Ok(())
    }

    fn write_feed_entry(
        writer: &mut BufWriter<File>,
        exchange: &str,
        feed: &str,
        snap: &FeedSnap,
        last_seq: &mut u64,
        adj: Option<&ExchangeAdjustment>,
    ) -> Result<()> {
        if snap.seq == *last_seq {
            return Ok(());
        }
        if let Some(price) = snap.price.filter(|p| p.is_finite()) {
            *last_seq = snap.seq;
            let ts = snap.ts_ns.unwrap_or(0);
            let direction = trade_direction_str(snap.direction);
            let price_adj = apply_demean(price, adj);
            Self::write_row(
                writer,
                ts,
                exchange,
                feed,
                Some(price_adj),
                direction,
                "market",
                "",
                "",
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )?;
        }
        Ok(())
    }

    fn log_quote_submission(
        &mut self,
        intents: &[QuoteIntent],
        reference_meta: Option<&ReferenceMeta>,
        reference_price: f64,
        quote_internal: Option<Duration>,
        send_instant: Instant,
        sent_ts: SystemTime,
    ) -> Result<()> {
        let quote_internal_us = quote_internal.map(|d| d.as_micros() as i128);
        let sent_ns = system_time_to_ns(sent_ts);
        let venue = self.venue.clone();
        for intent in intents {
            self.orders.insert(
                intent.client_order_id.0.clone(),
                QuoteSnapshot {
                    side: intent.side,
                    price: intent.price,
                    size: intent.size,
                    filled_qty: 0.0,
                    sent_ns: Some(sent_ns),
                    send_instant: Some(send_instant),
                },
            );
            Self::write_row(
                &mut self.writer,
                clamp_u128_to_u64(sent_ns),
                venue.as_str(),
                "quote",
                Some(intent.price),
                side_to_direction(intent.side),
                "quote",
                &intent.client_order_id.0,
                side_to_str(intent.side),
                Some(intent.size),
                reference_meta.map(|m| m.source.as_str()),
                reference_meta.and_then(|m| m.ts_ns),
                Some(reference_price),
                quote_internal_us,
                None,
                None,
                None,
                Some(sent_ns),
            )?;
        }
        Ok(())
    }

    fn log_cancel(
        &mut self,
        order_id: &str,
        reference: &ReferenceEvent,
        cancel_internal: Duration,
        send_instant: Instant,
        sent_ts: SystemTime,
    ) -> Result<()> {
        let cancel_internal_us = cancel_internal.as_micros() as i128;
        let sent_ns = system_time_to_ns(sent_ts);
        self.pending_cancels
            .insert(order_id.to_string(), CancelSnapshot { send_instant });

        let snapshot = self.orders.get(order_id);
        let (side_str, direction, price, size) = if let Some(snapshot) = snapshot {
            (
                side_to_str(snapshot.side),
                side_to_direction(snapshot.side),
                Some(snapshot.price),
                Some(snapshot.size),
            )
        } else {
            ("", "", None, None)
        };
        let venue = self.venue.clone();

        Self::write_row(
            &mut self.writer,
            clamp_u128_to_u64(sent_ns),
            venue.as_str(),
            "cancel",
            price,
            direction,
            "cancel",
            order_id,
            side_str,
            size,
            Some(reference.source.as_str()),
            reference.ts_ns,
            Some(reference.price),
            None,
            Some(cancel_internal_us),
            None,
            None,
            Some(sent_ns),
        )
    }

    fn log_report(&mut self, report: &ExecutionReport) -> Result<()> {
        let now_instant = Instant::now();
        let id_str = report.client_order_id.0.clone();
        let mut cancel_external_us: Option<i128> = None;
        let mut quote_external_us: Option<i128> = None;

        if let Some(cancel_info) = self.pending_cancels.remove(&id_str) {
            let cancel_dur = now_instant
                .checked_duration_since(cancel_info.send_instant)
                .unwrap_or_default();
            cancel_external_us = Some(cancel_dur.as_micros() as i128);
        }

        let mut price_for_status = None;
        let mut direction = "unknown";
        let mut side_str = "";
        let mut sent_ns_for_status: Option<u128> = None;

        if let Some(snapshot) = self.orders.get_mut(&id_str) {
            price_for_status = Some(snapshot.price);
            direction = side_to_direction(snapshot.side);
            side_str = side_to_str(snapshot.side);
            sent_ns_for_status = snapshot.sent_ns;
            if let Some(send_inst) = snapshot.send_instant.take() {
                let ack_dur = now_instant
                    .checked_duration_since(send_inst)
                    .unwrap_or_default();
                quote_external_us = Some(ack_dur.as_micros() as i128);
            }
        }

        if report.filled_qty > 0.0 {
            let mut delta = report.filled_qty;
            let mut price = report.avg_fill_price;
            let mut direction = "unknown";
            let mut side_str = "";
            let mut sent_ns = None;

            if let Some(snapshot) = self.orders.get_mut(&report.client_order_id.0) {
                let fill_delta = report.filled_qty - snapshot.filled_qty;
                if fill_delta > f64::EPSILON {
                    delta = fill_delta;
                } else {
                    delta = 0.0;
                }
                snapshot.filled_qty = report.filled_qty;
                price = price.or(Some(snapshot.price));
                direction = side_to_direction(snapshot.side);
                side_str = side_to_str(snapshot.side);
                sent_ns = snapshot.sent_ns;
            }

            if delta > 0.0 {
                let ts_ns_u128 = report
                    .ts
                    .map(|ts| ts as u128)
                    .unwrap_or_else(|| system_time_to_ns(SystemTime::now()));
                Self::write_row(
                    &mut self.writer,
                    clamp_u128_to_u64(ts_ns_u128),
                    &self.venue,
                    "fill",
                    price,
                    direction,
                    "fill",
                    &report.client_order_id.0,
                    side_str,
                    Some(delta),
                    None,
                    None,
                    None,
                    None,
                    None,
                    quote_external_us,
                    cancel_external_us,
                    sent_ns,
                )?;
            }
        }

        let event_type = match report.status {
            OrderStatus::New => Some("quote_ack"),
            OrderStatus::PartiallyFilled => Some("quote_ack"),
            OrderStatus::Filled => Some("quote_ack"),
            OrderStatus::Canceled => Some("cancel_ack"),
            OrderStatus::Rejected => Some("quote_reject"),
            _ => None,
        };

        if let Some(event) = event_type {
            let ts_ns_u128 = report
                .ts
                .map(|ts| ts as u128)
                .unwrap_or_else(|| system_time_to_ns(SystemTime::now()));
            Self::write_row(
                &mut self.writer,
                clamp_u128_to_u64(ts_ns_u128),
                &self.venue,
                event,
                price_for_status,
                direction,
                "report",
                &report.client_order_id.0,
                side_str,
                None,
                None,
                None,
                None,
                None,
                None,
                quote_external_us,
                cancel_external_us,
                sent_ns_for_status,
            )?;
        }

        if matches!(
            report.status,
            OrderStatus::Filled | OrderStatus::Canceled | OrderStatus::Rejected
        ) {
            self.orders.remove(&report.client_order_id.0);
            self.pending_cancels.remove(&report.client_order_id.0);
        }

        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        Ok(())
    }

    fn write_row(
        writer: &mut BufWriter<File>,
        ts_ns: u64,
        exchange: &str,
        feed: &str,
        price: Option<f64>,
        direction: &str,
        event_type: &str,
        client_order_id: &str,
        side: &str,
        size: Option<f64>,
        reference_source: Option<&str>,
        reference_ts_ns: Option<u64>,
        reference_price: Option<f64>,
        quote_internal_us: Option<i128>,
        cancel_internal_us: Option<i128>,
        quote_external_us: Option<i128>,
        cancel_external_us: Option<i128>,
        sent_ts_ns: Option<u128>,
    ) -> Result<()> {
        writeln!(
            writer,
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            ts_ns,
            exchange,
            feed,
            format_option_f64(price),
            direction,
            event_type,
            client_order_id,
            side,
            format_option_f64(size),
            reference_source.unwrap_or(""),
            reference_ts_ns.map_or(String::new(), |v| v.to_string()),
            reference_price.map_or(String::new(), |v| format_f64(v)),
            quote_internal_us.map_or(String::new(), |v| v.to_string()),
            cancel_internal_us.map_or(String::new(), |v| v.to_string()),
            quote_external_us.map_or(String::new(), |v| v.to_string()),
            cancel_external_us.map_or(String::new(), |v| v.to_string()),
            sent_ts_ns.map_or(String::new(), |v| v.to_string()),
        )?;
        Ok(())
    }
}

fn venue_to_string(venue: Venue) -> &'static str {
    match venue {
        Venue::Gate => "gate",
    }
}

fn trade_direction_str(dir: Option<TradeDirection>) -> &'static str {
    dir.map(TradeDirection::as_str).unwrap_or("")
}

fn side_to_direction(side: Side) -> &'static str {
    match side {
        Side::Bid => "buy",
        Side::Ask => "sell",
    }
}

fn side_to_str(side: Side) -> &'static str {
    match side {
        Side::Bid => "bid",
        Side::Ask => "ask",
    }
}

fn format_option_f64(value: Option<f64>) -> String {
    value
        .filter(|v| v.is_finite())
        .map(format_f64)
        .unwrap_or_default()
}

fn format_f64(value: f64) -> String {
    format!("{:.8}", value)
}

fn system_time_to_ns(ts: SystemTime) -> u128 {
    ts.duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos()
}

fn clamp_u128_to_u64(value: u128) -> u64 {
    if value > u64::MAX as u128 {
        u64::MAX
    } else {
        value as u64
    }
}

async fn ctrl_c_notifier() {
    let _ = tokio::signal::ctrl_c().await;
}

async fn setup_live_gateway(
    config: &RunnerConfig,
    contract_size: f64,
    creds: &GateCredentials,
) -> Result<GateWsGateway> {
    let ws_config = GateWsConfig {
        api_key: creds.api_key.clone(),
        api_secret: creds.api_secret.clone(),
        symbol: config.strategy.symbol.clone(),
        settle: config.settle.clone(),
        ws_url: None,
        contract_size: Some(contract_size),
    };

    GateWsGateway::connect(ws_config).await
}

fn extract_position_contracts(value: &Value) -> Option<f64> {
    let fields = ["size", "current_size", "position", "contracts"];
    for key in fields {
        if let Some(entry) = value.get(key) {
            if let Some(val) = value_to_f64(entry) {
                return Some(val);
            }
        }
    }
    None
}

fn value_to_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(num) => num.as_f64(),
        Value::String(s) => s.parse::<f64>().ok(),
        _ => None,
    }
}

fn sha512_hex(input: &str) -> String {
    let mut hasher = Sha512::new();
    hasher.update(input.as_bytes());
    hex_bytes(hasher.finalize())
}

fn hex_bytes(bytes: impl AsRef<[u8]>) -> String {
    bytes
        .as_ref()
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect()
}

fn current_unix_seconds_string() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before epoch");
    let secs = now.as_secs();
    let nanos = now.subsec_nanos();
    format!("{}.{:09}", secs, nanos)
}

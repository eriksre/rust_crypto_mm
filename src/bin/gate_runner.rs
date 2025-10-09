#![cfg(feature = "gate_exec")]

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow};
use clap::Parser;
use rust_test::base_classes::engine::spawn_state_engine;
use rust_test::base_classes::reference::ReferenceEvent;
use rust_test::base_classes::state::state as global_state;
use rust_test::base_classes::state::{ExchangeAdjustment, ExchangeSnap, FeedSnap, TradeDirection};
use rust_test::base_classes::types::Side;
use rust_test::execution::{
    DryRunGateway, ExecutionGateway, ExecutionReport, GateWsConfig, GateWsGateway, OrderAck,
    OrderManager, OrderStatus, QuoteIntent, Venue,
};
use rust_test::strategy::{QuoteConfig, ReferenceMeta, SimpleQuoteStrategy};
use tokio::sync::{Mutex, mpsc};
use tokio::time::{MissedTickBehavior, interval};

#[derive(Debug, Parser)]
#[command(name = "gate-runner", about = "Gate.io MVP dry-run executor")]
struct Cli {
    /// Path to YAML configuration
    #[arg(long, default_value = "config/gate_mvp.yaml")]
    config: String,
}

#[derive(Debug, serde::Deserialize, Clone)]
struct RiskConfig {
    max_notional: f64,
    max_order_notional: f64,
}

#[derive(Debug, serde::Deserialize, Clone)]
struct ModeConfig {
    #[serde(default = "default_true")]
    dry_run: bool,
    #[serde(default)]
    log_fills: bool,
}

#[derive(Debug, serde::Deserialize, Clone, Default)]
#[serde(default)]
struct LoggingConfig {
    enabled: bool,
    path: Option<String>,
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
}

fn default_true() -> bool {
    true
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
    let config = load_config(&cli.config)?;

    let logger = if config.logging.is_enabled() {
        let logger = QuoteCsvLogger::new(&config)?;
        println!("Activity logging enabled -> {}", logger.path.display());
        Some(Arc::new(Mutex::new(logger)))
    } else {
        None
    };

    let (reference_tx, mut reference_rx) = mpsc::unbounded_channel();
    let _engine = spawn_state_engine(config.strategy.symbol.clone(), Some(reference_tx));
    println!(
        "Gate runner started for {} (dry_run: {})",
        config.strategy.symbol, config.mode.dry_run
    );

    let gateway: Arc<dyn ExecutionGateway> = if config.mode.dry_run {
        Arc::new(DryRunGateway::new())
    } else {
        Arc::new(setup_live_gateway(&config).await?)
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
            println!("Received shutdown signal; exiting.");
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
                                ).await {
                                    eprintln!("error handling market update: {:#}", err);
                                }
                            }
                            None => {
                                eprintln!("reference channel closed; exiting.");
                                break;
                            }
                        }
                    }
                    _ = market_timer.tick() => {
                        if let Some(logger) = logger.as_ref() {
                            if let Err(err) = {
                                let mut guard = logger.lock().await;
                                guard.log_market_snapshot()
                            } {
                                eprintln!("error logging market snapshot: {:#}", err);
                            }
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
                            ).await
                        {
                            eprintln!("error processing reports: {:#}", err);
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
                            order_manager.clone(),
                            logger.clone(),
                        ).await {
                            eprintln!("error handling quote tick: {:#}", err);
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

async fn handle_market_update(
    reference: ReferenceEvent,
    strategy: &mut SimpleQuoteStrategy,
    config: &RunnerConfig,
    order_manager: Arc<OrderManager>,
    logger: Option<LoggerHandle>,
) -> Result<()> {
    let debug = latency_debug_enabled();
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

    if debug && (reference_age > REF_WARN || strat_dur > STAGE_WARN) {
        eprintln!(
            "latency-debug::market ref_age={}us on_market={}us source={} cancels={}",
            dur_us(reference_age),
            dur_us(strat_dur),
            reference.source,
            cancels.len()
        );
    }

    if !cancels.is_empty() {
        println!(
            "repricing {} on {} (ts={:?}); cancelling {} orders",
            config.strategy.symbol,
            reference.source,
            reference.ts_ns,
            cancels.len()
        );
        let send_start = Instant::now();
        let cancel_internal = send_start.saturating_duration_since(reference.received_at);
        let sent_ts = SystemTime::now();
        if let Some(logger) = logger.as_ref() {
            for id in &cancels {
                if let Err(err) = {
                    let mut guard = logger.lock().await;
                    guard.log_cancel(
                        &id.0,
                        &reference,
                        cancel_internal,
                        send_start,
                        sent_ts,
                    )
                } {
                    eprintln!("error logging cancel {}: {:#}", id, err);
                }
            }
        }

        let cancels_to_send = cancels.clone();
        let order_manager_clone = order_manager.clone();
        tokio::spawn(async move {
            let call_start = Instant::now();
            if let Err(err) = order_manager_clone.cancel_many(&cancels_to_send).await {
                for id in &cancels_to_send {
                    eprintln!("cancel {} failed: {:#}", id, err);
                }
            }
            if latency_debug_enabled() {
                let call_elapsed = call_start.elapsed();
                eprintln!(
                    "latency-debug::cancel ref_age={}us call={}us ids={}",
                    dur_us(cancel_internal),
                    dur_us(call_elapsed),
                    cancels_to_send.len()
                );
            }
        });
    }

    Ok(())
}

async fn handle_quote_tick(
    now: Instant,
    strategy: &mut SimpleQuoteStrategy,
    config: &RunnerConfig,
    order_manager: Arc<OrderManager>,
    logger: Option<LoggerHandle>,
) -> Result<()> {
    if let Some(plan) = strategy.plan_quotes(now) {
        enforce_risk(&plan.intents, &config.risk)?;
        let debug = latency_debug_enabled();

        let ref_meta = if let Some(meta) = plan.reference_meta.as_ref() {
            println!(
                "quoting {} on {} (ts={:?}) latency={}µs",
                config.strategy.symbol,
                meta.source,
                meta.ts_ns,
                now.checked_duration_since(plan.planned_at)
                    .unwrap_or_default()
                    .as_micros()
            );
            Some(meta.clone())
        } else {
            println!(
                "quoting {} with latest price {:.4}",
                config.strategy.symbol, plan.reference_price
            );
            None
        };

        let intents = plan.intents.clone();
        let reference_price = plan.reference_price;
        let send_start = Instant::now();
        let sent_ts = SystemTime::now();
        let debounce_budget = Duration::from_millis(config.strategy.debounce_ms.max(1));
        let (reference_instant, timer_wait) = if let Some(meta) = ref_meta.as_ref() {
            let age = plan
                .planned_at
                .saturating_duration_since(meta.received_at);
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

        if debug {
            if let Some(meta) = ref_meta.as_ref() {
                let meta_age = now.saturating_duration_since(meta.received_at);
                if meta_age > REF_WARN {
                    eprintln!(
                        "latency-debug::quote ref_age={}us planned_age={}us intents={}",
                        dur_us(meta_age),
                        dur_us(plan
                            .planned_at
                            .saturating_duration_since(meta.received_at)),
                        intents.len()
                    );
                }
            }
        }

        if let Some(logger) = logger.as_ref() {
            if let Err(err) = {
                let mut guard = logger.lock().await;
                guard.log_quote_submission(
                    &intents,
                    ref_meta.as_ref(),
                    plan.reference_price,
                    Some(quote_internal),
                    send_start,
                    sent_ts,
                )
            } {
                eprintln!("error logging quote submission: {:#}", err);
            }
        }
        strategy.commit_plan(&plan);

        let intents_for_send = intents.clone();
        let ref_meta_for_send = ref_meta.clone();
        let config_clone = config.clone();
        let order_manager_clone = order_manager.clone();
        let quote_internal_for_send = quote_internal;
        tokio::spawn(async move {
            let call_start = Instant::now();
            match order_manager_clone.submit(intents_for_send.clone()).await {
                Ok(acks) => {
                    if latency_debug_enabled() {
                        let call_elapsed = call_start.elapsed();
                        eprintln!(
                            "latency-debug::submit call={}us internal={}us intents={}",
                            dur_us(call_elapsed),
                            dur_us(quote_internal_for_send),
                            intents_for_send.len()
                        );
                    }
                    log_submission(
                        &intents_for_send,
                        &acks,
                        ref_meta_for_send.as_ref(),
                        reference_price,
                        Some(quote_internal_for_send),
                        &config_clone,
                    );
                }
                Err(err) => {
                    eprintln!("submit {:?} failed: {:#}", intents_for_send, err);
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
    logger: Option<LoggerHandle>,
) -> Result<()> {
    let reports = order_manager.poll_reports().await?;
    if reports.is_empty() {
        return Ok(());
    }

    for report in &reports {
        strategy.handle_report(report);
    }

    if let Some(logger) = logger.as_ref() {
        let mut guard = logger.lock().await;
        for report in &reports {
            if let Err(err) = guard.log_report(report) {
                eprintln!("error logging report {}: {:#}", report.client_order_id, err);
            }
        }
    }

    log_reports(&reports, config);
    Ok(())
}

fn enforce_risk(intents: &[QuoteIntent], risk: &RiskConfig) -> Result<()> {
    let mut batch_notional = 0.0;
    for intent in intents {
        let notional = intent.price.abs() * intent.size.abs();
        if notional > risk.max_order_notional {
            anyhow::bail!(
                "intent {} exceeds per-order notional limit: {:.2} > {:.2}",
                intent.client_order_id,
                notional,
                risk.max_order_notional
            );
        }
        batch_notional += notional;
    }
    if batch_notional > risk.max_notional {
        anyhow::bail!(
            "batch exceeds max notional limit: {:.2} > {:.2}",
            batch_notional,
            risk.max_notional
        );
    }
    Ok(())
}

fn log_submission(
    intents: &[QuoteIntent],
    acks: &[OrderAck],
    reference_meta: Option<&ReferenceMeta>,
    reference_price: f64,
    quote_internal: Option<Duration>,
    config: &RunnerConfig,
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
        println!(
            "ref {:.4} ({}) -> {:?} {:.4} @ {:.4} ({}) exch_id={:?} latency={}µs",
            reference_price,
            source,
            intent.side,
            intent.size,
            intent.price,
            mode,
            ack.exchange_order_id,
            quote_internal.map(|dur| dur.as_micros()).unwrap_or(0)
        );
        if config.mode.log_fills {
            println!(
                "  intent {} tif={} size={:.4} ts={:?}",
                intent.client_order_id, intent.tif, intent.size, ts_ns
            );
        }
    }
}

fn log_reports(reports: &[ExecutionReport], config: &RunnerConfig) {
    for report in reports {
        let should_log = config.mode.log_fills
            || matches!(
                report.status,
                OrderStatus::Filled | OrderStatus::PartiallyFilled | OrderStatus::Rejected
            );
        if should_log {
            println!(
                "report {} status {:?} filled {:.6} avg {:?} ts={:?}",
                report.client_order_id,
                report.status,
                report.filled_qty,
                report.avg_fill_price,
                report.ts
            );
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

type LoggerHandle = Arc<Mutex<QuoteCsvLogger>>;

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

        self.write_exchange(ExchangeId::Bybit, "bybit", &st.bybit, Some(&st.demean.bybit))?;
        self.write_exchange(ExchangeId::Binance, "binance", &st.binance, Some(&st.demean.binance))?;
        self.write_exchange(ExchangeId::Gate, "gate", &st.gate, None)?;
        self.write_exchange(ExchangeId::Bitget, "bitget", &st.bitget, Some(&st.demean.bitget))?;
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
        writer.flush()?;
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

async fn setup_live_gateway(config: &RunnerConfig) -> Result<GateWsGateway> {
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

    let ws_config = GateWsConfig {
        api_key,
        api_secret,
        symbol: config.strategy.symbol.clone(),
        settle: config.settle.clone(),
        ws_url: None,
        contract_size: None,
    };

    GateWsGateway::connect(ws_config).await
}

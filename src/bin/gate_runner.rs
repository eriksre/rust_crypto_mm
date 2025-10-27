#![cfg(feature = "gate_exec")]

use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant, SystemTime};

use anyhow::{Result, anyhow, bail};
use clap::Parser;
use rust_test::base_classes::engine::{configure_feed_overrides, spawn_state_engine};
use rust_test::base_classes::reference::ReferenceEvent;
use rust_test::base_classes::types::Side;
use rust_test::config::runner::{
    RiskConfig, RunnerConfig, load_gate_credentials, load_runner_config,
};
use rust_test::exchanges::gate::rest;
use rust_test::execution::{
    ClientOrderId, DryRunGateway, ExecutionGateway, ExecutionReport, GateClient, GateCredentials,
    GateWsConfig, GateWsGateway, InventoryReportOutcome, InventoryTracker, OrderAck, OrderManager,
    OrderStatus, QuoteIntent,
};
use rust_test::logging::quote::{DebugLogger, QuoteLogHandle, format_f64};
use rust_test::strategy::{ReferenceMeta, SimpleQuoteStrategy};
use tokio::sync::{Mutex, Semaphore, mpsc};
use tokio::time::{self, MissedTickBehavior, interval};

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

const REF_WARN: Duration = Duration::from_millis(20);
const STAGE_WARN: Duration = Duration::from_millis(5);
const CANCEL_WARN: Duration = Duration::from_micros(500);

struct CancelMessage {
    reference: ReferenceEvent,
    dispatched_at: Instant,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let cli = Cli::parse();
    let mut config = load_runner_config(&cli.config)?;
    configure_feed_overrides(config.feeds);
    let debug = DebugLogger::new(config.mode.debug_prints);

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

    let config = Arc::new(config);

    let logger = if config.logging.is_enabled() {
        let handle = QuoteLogHandle::spawn(config.as_ref(), debug.clone())?;
        debug.info(|| format!("Activity logging enabled -> {}", handle.path().display()));
        Some(handle)
    } else {
        None
    };

    let settle = config.settle.clone().unwrap_or_else(|| "usdt".to_string());

    let credentials = if config.mode.dry_run {
        None
    } else {
        Some(load_gate_credentials(config.as_ref())?)
    };

    let rest_client = credentials
        .as_ref()
        .map(|creds| Arc::new(GateClient::new(creds.clone())));

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
    let (fast_ref_tx, mut fast_ref_rx) = mpsc::unbounded_channel();
    let _engine = spawn_state_engine(
        config.strategy.symbol.clone(),
        Some(reference_tx),
        Some(fast_ref_tx),
    );
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
        Arc::new(setup_live_gateway(config.as_ref(), contract_size, &creds).await?)
    };
    let order_manager = Arc::new(OrderManager::new(gateway, Duration::from_secs(30)));
    let strategy = Arc::new(Mutex::new(SimpleQuoteStrategy::new(
        config.strategy.clone(),
    )));

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
    let mut quote_timer = interval(Duration::from_millis(50));
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

                        if let Err(err) =
                            drain_reports(
                                strategy.clone(),
                                config.clone(),
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
                        let strategy_clone = strategy.clone();
                        let config_clone = config.clone();
                        let order_manager_clone = order_manager.clone();
                        let logger_clone = logger.clone();
                        let debug_clone = debug.clone();
                        let inventory_clone = inventory.clone();
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
    strategy: Arc<Mutex<SimpleQuoteStrategy>>,
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
    let meta = ReferenceMeta {
        source: reference.source.clone(),
        ts_ns: reference.ts_ns,
        received_at: reference.received_at,
    };
    let lock_start = Instant::now();
    let mut strategy_guard = strategy.lock().await;
    let lock_wait = lock_start.elapsed();
    let queue_delay = lock_start.saturating_duration_since(reference.received_at);
    let dispatch_delay = lock_start.saturating_duration_since(dispatched_at);
    let cancels =
        strategy_guard.on_market_update(reference.price, Some(meta.clone()), reference.received_at);
    let strat_dur = lock_start.elapsed();
    let send_start_opt = if cancels.is_empty() {
        None
    } else {
        let send_start = Instant::now();
        strategy_guard.record_cancel_submission(send_start);
        Some(send_start)
    };
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
                "latency-debug::cancel summary queue={}us dispatch={}us compute={}us total={}us cancels={}",
                dur_us(queue_delay),
                dur_us(dispatch_delay),
                dur_us(compute_delay),
                dur_us(cancel_internal),
                cancels.len()
            )
        });
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
    strategy: Arc<Mutex<SimpleQuoteStrategy>>,
    config: Arc<RunnerConfig>,
    contract_size: f64,
    order_manager: Arc<OrderManager>,
    logger: Option<QuoteLogHandle>,
    debug: DebugLogger,
    inventory: Arc<Mutex<InventoryTracker>>,
) -> Result<()> {
    drain_reports(
        strategy.clone(),
        config.clone(),
        order_manager.clone(),
        logger.clone(),
        debug.clone(),
        inventory.clone(),
    )
    .await?;

    let config_ref = config.as_ref();

    let plan_opt = match strategy.try_lock() {
        Ok(mut guard) => guard.plan_quotes(now),
        Err(_) => {
            if latency_debug_enabled() {
                debug.latency(|| "latency-debug::quote skipped (strategy busy)".to_string());
            }
            return Ok(());
        }
    };

    if let Some(mut plan) = plan_opt {
        let reference_price = plan.reference_price;
        let net_contracts = {
            let guard = inventory.lock().await;
            guard.net_contracts()
        };
        let filter = filter_intents(
            &plan.intents,
            &config_ref.risk,
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
        let send_start = Instant::now();
        let sent_ts = SystemTime::now();
        let debounce_budget = Duration::from_millis(config_ref.strategy.debounce_ms.max(1));
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
        {
            let commit_lock_start = Instant::now();
            let mut strategy_guard = strategy.lock().await;
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
                        config_clone.as_ref(),
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
    strategy: Arc<Mutex<SimpleQuoteStrategy>>,
    config: Arc<RunnerConfig>,
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
        {
            let mut strategy = strategy.lock().await;
            strategy.handle_report(report);
        }
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

    log_reports(&reports, config.as_ref(), &debug);
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

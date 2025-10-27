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
    let mut config = load_runner_config(&cli.config)?;
    crate::base_classes::engine::configure_feed_overrides(config.feeds);
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
    let _engine = spawn_state_engine(config.strategy.symbol.clone(), Some(reference_tx), None);
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

struct FilteredIntents {
    allowed: Vec<QuoteIntent>,
    skipped: Vec<(ClientOrderId, String)>,
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
    }

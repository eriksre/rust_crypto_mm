#![cfg(feature = "gate_exec")]

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use clap::Parser;
use rust_test::base_classes::engine::spawn_state_engine;
use rust_test::base_classes::state::ExchangeAdjustment;
use rust_test::base_classes::state::state as global_state;
use rust_test::execution::{
    DryRunGateway, ExecutionGateway, ExecutionReport, GateWsConfig, GateWsGateway, OrderAck,
    OrderManager, OrderStatus, QuoteIntent,
};
use rust_test::strategy::{QuoteConfig, ReferenceMeta, SimpleQuoteStrategy};
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

fn default_true() -> bool {
    true
}

#[derive(Debug, serde::Deserialize, Clone)]
struct RunnerConfig {
    strategy: QuoteConfig,
    risk: RiskConfig,
    mode: ModeConfig,
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

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let cli = Cli::parse();
    let config = load_config(&cli.config)?;

    let _engine = spawn_state_engine(config.strategy.symbol.clone());
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
    let mut last_key: Option<RevisionKey> = None;

    let mut market_timer = interval(Duration::from_millis(20));
    market_timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut quote_timer = interval(Duration::from_millis(50));
    quote_timer.set_missed_tick_behavior(MissedTickBehavior::Delay);

    tokio::select! {
        _ = ctrl_c_notifier() => {
            println!("Received shutdown signal; exiting.");
        }
        _ = async {
            loop {
                tokio::select! {
                    _ = market_timer.tick() => {
                        while let Some(reference) = latest_reference(&mut last_key) {
                            let now = Instant::now();
                            if let Err(err) = handle_market_update(
                                reference,
                                now,
                                &mut strategy,
                                &config,
                                order_manager.clone()
                            ).await {
                                eprintln!("error handling market update: {:#}", err);
                            }
                        }

                        if let Err(err) =
                            drain_reports(&mut strategy, &config, order_manager.clone()).await
                        {
                            eprintln!("error processing reports: {:#}", err);
                        }
                    }
                    _ = quote_timer.tick() => {
                        let now = Instant::now();
                        if let Err(err) = handle_quote_tick(
                            now,
                            &mut strategy,
                            &config,
                            order_manager.clone()
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

fn latest_reference(last_key: &mut Option<RevisionKey>) -> Option<ReferencePrice> {
    let st = global_state().lock().ok()?;

    struct Candidate {
        price: f64,
        seq: u64,
        ts_ns: Option<u64>,
        source_idx: u8,
        source: String,
    }

    fn is_newer(candidate: &Candidate, current: &Candidate) -> bool {
        let cand_ts = candidate.ts_ns.unwrap_or(0);
        let cur_ts = current.ts_ns.unwrap_or(0);
        if cand_ts != cur_ts {
            return cand_ts > cur_ts;
        }
        if candidate.seq != current.seq {
            return candidate.seq > current.seq;
        }
        candidate.source_idx > current.source_idx
    }

    let adjust_price = |price: Option<f64>, adj: &ExchangeAdjustment| -> Option<f64> {
        match price {
            Some(px) if px.is_finite() && px > 0.0 => {
                if adj.samples > 0 {
                    Some(px - adj.offset.unwrap_or(0.0))
                } else {
                    Some(px)
                }
            }
            Some(_) => None,
            None => None,
        }
    };

    let label = |base: &str, adj: &ExchangeAdjustment| -> String {
        if adj.samples > 0 {
            format!("{}_adj", base)
        } else {
            base.to_string()
        }
    };

    let mut best: Option<Candidate> = None;
    let mut consider =
        |price: Option<f64>, seq: u64, ts: Option<u64>, source_idx: u8, source: String| {
            if seq == 0 {
                return;
            }
            if let Some(price) = price {
                let cand = Candidate {
                    price,
                    seq,
                    ts_ns: ts,
                    source_idx,
                    source,
                };
                if let Some(current) = &best {
                    if is_newer(&cand, current) {
                        best = Some(cand);
                    }
                } else {
                    best = Some(cand);
                }
            }
        };

    consider(
        st.gate.bbo.price,
        st.gate.bbo.seq,
        st.gate.bbo.ts_ns,
        0,
        "gate_bbo".to_string(),
    );
    consider(
        st.gate.orderbook.price,
        st.gate.orderbook.seq,
        st.gate.orderbook.ts_ns,
        1,
        "gate_ob".to_string(),
    );
    consider(
        st.gate.trade.price,
        st.gate.trade.seq,
        st.gate.trade.ts_ns,
        2,
        "gate_trade".to_string(),
    );
    consider(
        adjust_price(st.bybit.bbo.price, &st.demean.bybit),
        st.bybit.bbo.seq,
        st.bybit.bbo.ts_ns,
        3,
        label("bybit_bbo", &st.demean.bybit),
    );
    consider(
        adjust_price(st.bybit.trade.price, &st.demean.bybit),
        st.bybit.trade.seq,
        st.bybit.trade.ts_ns,
        4,
        label("bybit_trade", &st.demean.bybit),
    );
    consider(
        adjust_price(st.binance.bbo.price, &st.demean.binance),
        st.binance.bbo.seq,
        st.binance.bbo.ts_ns,
        5,
        label("binance_bbo", &st.demean.binance),
    );
    consider(
        adjust_price(st.binance.trade.price, &st.demean.binance),
        st.binance.trade.seq,
        st.binance.trade.ts_ns,
        6,
        label("binance_trade", &st.demean.binance),
    );
    consider(
        adjust_price(st.bitget.bbo.price, &st.demean.bitget),
        st.bitget.bbo.seq,
        st.bitget.bbo.ts_ns,
        7,
        label("bitget_bbo", &st.demean.bitget),
    );
    consider(
        adjust_price(st.bitget.trade.price, &st.demean.bitget),
        st.bitget.trade.seq,
        st.bitget.trade.ts_ns,
        8,
        label("bitget_trade", &st.demean.bitget),
    );

    let candidate = best?;
    let key = RevisionKey {
        source_idx: candidate.source_idx,
        seq: candidate.seq,
        ts_ns: candidate.ts_ns,
    };

    if last_key.as_ref() == Some(&key) {
        return None;
    }
    *last_key = Some(key);
    Some(ReferencePrice {
        price: candidate.price,
        ts_ns: candidate.ts_ns,
        source: candidate.source,
    })
}

async fn handle_market_update(
    reference: ReferencePrice,
    now: Instant,
    strategy: &mut SimpleQuoteStrategy,
    config: &RunnerConfig,
    order_manager: Arc<OrderManager>,
) -> Result<()> {
    let meta = ReferenceMeta {
        source: reference.source.clone(),
        ts_ns: reference.ts_ns,
    };
    let cancels = strategy.on_market_update(reference.price, Some(meta), now);

    if !cancels.is_empty() {
        println!(
            "repricing {} on {} (ts={:?}); cancelling {} orders",
            config.strategy.symbol,
            reference.source,
            reference.ts_ns,
            cancels.len()
        );
        for id in cancels {
            if let Err(err) = order_manager.cancel(&id).await {
                eprintln!("cancel {} failed: {:#}", id, err);
            }
        }
    }

    Ok(())
}

async fn handle_quote_tick(
    now: Instant,
    strategy: &mut SimpleQuoteStrategy,
    config: &RunnerConfig,
    order_manager: Arc<OrderManager>,
) -> Result<()> {
    if let Some(plan) = strategy.plan_quotes(now) {
        enforce_risk(&plan.intents, &config.risk)?;

        if let Some(meta) = plan.reference_meta.as_ref() {
            println!(
                "quoting {} on {} (ts={:?})",
                config.strategy.symbol, meta.source, meta.ts_ns
            );
        } else {
            println!(
                "quoting {} with latest price {:.4}",
                config.strategy.symbol, plan.reference_price
            );
        }

        let intents = plan.intents.clone();
        let acks = order_manager.submit(intents.clone()).await?;
        strategy.commit_plan(&plan);
        log_submission(
            &intents,
            &acks,
            plan.reference_meta.as_ref(),
            plan.reference_price,
            config,
        );
    }
    Ok(())
}

async fn drain_reports(
    strategy: &mut SimpleQuoteStrategy,
    config: &RunnerConfig,
    order_manager: Arc<OrderManager>,
) -> Result<()> {
    let reports = order_manager.poll_reports().await?;
    if reports.is_empty() {
        return Ok(());
    }

    for report in &reports {
        strategy.handle_report(report);
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
            "ref {:.4} ({}) -> {:?} {:.4} @ {:.4} ({}) exch_id={:?}",
            reference_price,
            source,
            intent.side,
            intent.size,
            intent.price,
            mode,
            ack.exchange_order_id
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RevisionKey {
    source_idx: u8,
    seq: u64,
    ts_ns: Option<u64>,
}

#[derive(Clone, Debug)]
struct ReferencePrice {
    price: f64,
    ts_ns: Option<u64>,
    source: String,
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

#![cfg(feature = "gate_exec")]

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use clap::Parser;
use rust_test::base_classes::engine::spawn_state_engine;
use rust_test::base_classes::state::state as global_state;
use rust_test::execution::{
    DryRunGateway, ExecutionGateway, ExecutionReport, GateWsConfig, GateWsGateway, OrderAck,
    OrderManager, OrderStatus, QuoteIntent,
};
use rust_test::strategy::{QuoteConfig, SimpleQuoteStrategy};
use tokio::time::sleep;

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

    let poll_interval = Duration::from_millis(20);

    tokio::select! {
        _ = ctrl_c_notifier() => {
            println!("Received shutdown signal; exiting.");
        }
        _ = async {
            loop {
                if let Some(reference) = latest_reference(&mut last_key) {
                    let now = Instant::now();
                    if let Err(err) = handle_reference(
                        &reference,
                        now,
                        &mut strategy,
                        &config,
                        order_manager.clone()
                    ).await {
                        eprintln!(
                            "error handling reference {} {:.4}: {:#}",
                            reference.source,
                            reference.price,
                            err
                        );
                    }
                }

                if let Err(err) = drain_reports(&mut strategy, &config, order_manager.clone()).await {
                    eprintln!("error processing reports: {:#}", err);
                }

                sleep(poll_interval).await;
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
        source: &'static str,
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

    let mut best: Option<Candidate> = None;
    let mut consider =
        |price: Option<f64>, seq: u64, ts: Option<u64>, source_idx: u8, source: &'static str| {
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
        "gate_bbo",
    );
    consider(
        st.gate.orderbook.price,
        st.gate.orderbook.seq,
        st.gate.orderbook.ts_ns,
        1,
        "gate_ob",
    );
    consider(
        st.gate.trade.price,
        st.gate.trade.seq,
        st.gate.trade.ts_ns,
        2,
        "gate_trade",
    );
    consider(
        st.bybit.bbo.price,
        st.bybit.bbo.seq,
        st.bybit.bbo.ts_ns,
        3,
        "bybit_bbo",
    );
    consider(
        st.bybit.trade.price,
        st.bybit.trade.seq,
        st.bybit.trade.ts_ns,
        4,
        "bybit_trade",
    );
    consider(
        st.binance.bbo.price,
        st.binance.bbo.seq,
        st.binance.bbo.ts_ns,
        5,
        "binance_bbo",
    );
    consider(
        st.binance.trade.price,
        st.binance.trade.seq,
        st.binance.trade.ts_ns,
        6,
        "binance_trade",
    );
    consider(
        st.bitget.bbo.price,
        st.bitget.bbo.seq,
        st.bitget.bbo.ts_ns,
        7,
        "bitget_bbo",
    );
    consider(
        st.bitget.trade.price,
        st.bitget.trade.seq,
        st.bitget.trade.ts_ns,
        8,
        "bitget_trade",
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

async fn handle_reference(
    reference: &ReferencePrice,
    now: Instant,
    strategy: &mut SimpleQuoteStrategy,
    config: &RunnerConfig,
    order_manager: Arc<OrderManager>,
) -> Result<()> {
    if let Some(plan) = strategy.on_reference_price(reference.price, now) {
        enforce_risk(&plan.intents, &config.risk)?;

        if !plan.cancels.is_empty() {
            println!(
                "repricing {} on {} (ts={:?}); cancelling {} orders",
                config.strategy.symbol,
                reference.source,
                reference.ts_ns,
                plan.cancels.len()
            );
            for id in &plan.cancels {
                if let Err(err) = order_manager.cancel(id).await {
                    eprintln!("cancel {} failed: {:#}", id, err);
                }
            }
        } else {
            println!(
                "quoting {} on {} (ts={:?})",
                config.strategy.symbol, reference.source, reference.ts_ns
            );
        }

        let intents = plan.intents.clone();
        let acks = order_manager.submit(intents.clone()).await?;
        strategy.commit_plan(&plan);
        log_submission(&intents, &acks, reference, config);
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
    reference: &ReferencePrice,
    config: &RunnerConfig,
) {
    let mode = if config.mode.dry_run {
        "dry-run"
    } else {
        "live"
    };
    for (intent, ack) in intents.iter().zip(acks.iter()) {
        println!(
            "ref {:.4} ({}) -> {:?} {:.4} @ {:.4} ({}) exch_id={:?}",
            reference.price,
            reference.source,
            intent.side,
            intent.size,
            intent.price,
            mode,
            ack.exchange_order_id
        );
        if config.mode.log_fills {
            println!(
                "  intent {} tif={} size={:.4} ts={:?}",
                intent.client_order_id, intent.tif, intent.size, reference.ts_ns
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
    source: &'static str,
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

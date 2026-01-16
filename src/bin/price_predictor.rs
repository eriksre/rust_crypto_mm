use anyhow::Result;
use clap::Parser;
use tokio::sync::mpsc;

use rust_test::base_classes::engine::{
    configure_demean_enabled, configure_feed_overrides, spawn_state_engine,
};
use rust_test::base_classes::reference::ReferenceEvent;
use rust_test::config::runner::{load_runner_config, log_runner_config};

#[derive(Debug, Parser)]
#[command(name = "price-predictor", about = "Print model-predicted price/bid/ask/spread")]
struct Cli {
    /// Path to YAML configuration
    #[arg(long, default_value = "config/lighter_mvp.yaml")]
    config: String,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    if let Err(err) = dotenvy::dotenv() {
        eprintln!("WARN: failed to load .env: {}", err);
    }
    let cli = Cli::parse();
    let config = load_runner_config(&cli.config)?;
    log_runner_config(&config);

    configure_feed_overrides(config.feeds);
    configure_demean_enabled(config.mode.demean_prices);

    if config
        .pricing_model
        .as_ref()
        .map(|cfg| !cfg.enabled)
        .unwrap_or(true)
    {
        eprintln!("WARNING: pricing_model disabled; output will be raw reference prices.");
    }

    let (tx, mut rx) = mpsc::unbounded_channel();
    let _engine = spawn_state_engine(
        config.strategy.symbol.clone(),
        Some(tx),
        None,
        config.pricing_model.clone(),
    );

    println!(
        "Watching predicted prices for {} (Ctrl-C to exit).",
        config.strategy.symbol
    );
    println!(
        "{:<20} {:>16} {:>16} {:>16} {:>16} {:>10} {:>20}",
        "source", "price", "bid", "ask", "spread", "bps", "ts_ns"
    );

    while let Some(event) = rx.recv().await {
        print_event(&event);
    }

    Ok(())
}

fn print_event(event: &ReferenceEvent) {
    let (spread_abs, spread_bps) = spread_metrics(event.price, event.best_bid, event.best_ask);

    let ts_display = event
        .ts_ns
        .map(|v| v.to_string())
        .unwrap_or_else(|| "missing".to_string());
    println!(
        "{:<20} {:>16} {:>16} {:>16} {:>16} {:>10} {:>20}",
        event.source,
        fmt_px(event.price),
        fmt_opt_px(event.best_bid),
        fmt_opt_px(event.best_ask),
        fmt_opt_px(spread_abs),
        fmt_opt_bps(spread_bps),
        ts_display
    );
}

const PX_WIDTH: usize = 16;
const PX_PRECISION: usize = 8;
const BPS_WIDTH: usize = 10;
const BPS_PRECISION: usize = 2;

fn fmt_px(value: f64) -> String {
    format!("{:>width$.prec$}", value, width = PX_WIDTH, prec = PX_PRECISION)
}

fn fmt_opt_px(value: Option<f64>) -> String {
    match value.filter(|v| v.is_finite()) {
        Some(v) => fmt_px(v),
        None => format!("{:>width$}", "NA", width = PX_WIDTH),
    }
}

fn fmt_opt_bps(value: Option<f64>) -> String {
    match value.filter(|v| v.is_finite()) {
        Some(v) => format!("{:>width$.prec$}", v, width = BPS_WIDTH, prec = BPS_PRECISION),
        None => format!("{:>width$}", "NA", width = BPS_WIDTH),
    }
}

fn spread_metrics(price: f64, bid: Option<f64>, ask: Option<f64>) -> (Option<f64>, Option<f64>) {
    let (Some(bid), Some(ask)) = (bid, ask) else {
        return (None, None);
    };
    if !(bid.is_finite() && ask.is_finite() && bid > 0.0 && ask > 0.0) {
        return (None, None);
    }
    let spread = (ask - bid).abs();
    let bps = if price.is_finite() && price > 0.0 {
        (spread / price) * 10_000.0
    } else {
        f64::NAN
    };
    (Some(spread), Some(bps))
}

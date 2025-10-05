#![cfg(feature = "gate_exec")]

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::Parser;
use rust_test::base_classes::types::Side;
use rust_test::execution::{
    ClientOrderId, ExecutionGateway, GateWsConfig, GateWsGateway, OrderManager, QuoteIntent,
    TimeInForce, Venue,
};
use tokio::time::sleep;

#[derive(Debug, Parser)]
#[command(
    name = "gate-test-buy",
    about = "Place a single Gate.io futures limit order via WS"
)]
struct Cli {
    #[arg(long, default_value = "BTC_USDT")]
    symbol: String,
    #[arg(long, default_value_t = 100_000.0)]
    price: f64,
    #[arg(long, default_value_t = 0.0001)]
    size: f64,
    #[arg(long, default_value = "gtc")]
    tif: String,
    #[arg(long, default_value = "gateio_api_key")]
    api_key_env: String,
    #[arg(long, default_value = "gateio_secret_key")]
    api_secret_env: String,
    #[arg(long)]
    cancel: bool,
    #[arg(long, default_value_t = 5)]
    poll_secs: u64,
    #[arg(long, default_value_t = 0)]
    cancel_after_secs: u64,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let cli = Cli::parse();

    let api_key = std::env::var(&cli.api_key_env)
        .with_context(|| format!("missing env var {}", cli.api_key_env))?;
    let api_secret = std::env::var(&cli.api_secret_env)
        .with_context(|| format!("missing env var {}", cli.api_secret_env))?;

    let tif = parse_tif(&cli.tif)?;

    let gateway = Arc::new(
        GateWsGateway::connect(GateWsConfig {
            api_key,
            api_secret,
            symbol: cli.symbol.clone(),
            settle: None,
            ws_url: None,
            contract_size: None,
        })
        .await?,
    );

    let exec_gateway: Arc<dyn ExecutionGateway> = gateway.clone();
    let order_manager = Arc::new(OrderManager::new(exec_gateway, Duration::from_secs(30)));

    let intent = QuoteIntent::new(
        Venue::Gate,
        cli.symbol.clone(),
        Side::Bid,
        cli.price,
        cli.size,
        tif,
        ClientOrderId::new(format!("t-test-buy-{}", current_epoch_ms())),
    );

    println!(
        "Submitting {} {:.6} @ {:.2} ({:?})",
        intent.symbol, intent.size, intent.price, intent.tif
    );
    let acks = order_manager.submit(vec![intent.clone()]).await?;
    for ack in &acks {
        println!(
            "Ack: {} -> {:?}",
            ack.client_order_id, ack.exchange_order_id
        );
    }

    if cli.cancel {
        if cli.cancel_after_secs > 0 {
            sleep(Duration::from_secs(cli.cancel_after_secs)).await;
        }
        println!("Cancelling {}", intent.client_order_id);
        if let Err(err) = order_manager.cancel(&intent.client_order_id).await {
            eprintln!("Cancel error: {:#}", err);
        }
    }

    sleep(Duration::from_secs(cli.poll_secs)).await;
    let reports = order_manager.poll_reports().await?;
    if reports.is_empty() {
        println!("No execution reports yet.");
    } else {
        for report in &reports {
            println!(
                "Report {} status {:?} filled {:.6} avg {:?}",
                report.client_order_id, report.status, report.filled_qty, report.avg_fill_price
            );
        }
    }

    gateway.shutdown().await.ok();
    Ok(())
}

fn parse_tif(value: &str) -> Result<TimeInForce> {
    match value.to_ascii_lowercase().as_str() {
        "gtc" => Ok(TimeInForce::Gtc),
        "ioc" => Ok(TimeInForce::Ioc),
        "fok" => Ok(TimeInForce::Fok),
        "post_only" | "post" => Ok(TimeInForce::PostOnly),
        other => bail!("unsupported tif {other}"),
    }
}

fn current_epoch_ms() -> u128 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before epoch")
        .as_millis()
}

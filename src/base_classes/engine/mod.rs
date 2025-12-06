#![allow(dead_code)]

mod binance;
mod bitget;
mod bybit;
mod config;
mod demean_controller;
mod gate;
mod helpers;
mod lighter;
mod mexc;
mod okx;

use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use tokio::runtime::Runtime;
use tokio::sync::mpsc::UnboundedSender;

pub use config::{configure_demean_enabled, configure_feed_overrides};
use config::{current_feeds, demean_enabled};
use demean_controller::DemeanController;
use helpers::format_okx_inst_id;

use crate::base_classes::feed_gate::FeedTimestampGate;
use crate::base_classes::reference::ReferenceEvent;
use crate::base_classes::reference_publisher::ReferencePublisher;
use crate::base_classes::ws::{FeedSignal, spawn_ws_worker};
use crate::exchanges::binance::BinanceHandler;
use crate::exchanges::bitget::BitgetHandler;
use crate::exchanges::bybit::BybitHandler;
use crate::exchanges::gate::{GateHandler, canonical_contract_symbol};
use crate::exchanges::lighter::{LighterHandler, LighterMarketMeta, fetch_market_meta_async};
use crate::exchanges::mexc::{
    MexcContractMeta, MexcHandler, fetch_contract_meta as fetch_mexc_contract_meta,
};
use crate::exchanges::okx::{OkxHandler, OkxInstrumentMeta, fetch_instrument_meta};

#[cfg(feature = "gate_exec")]
use crate::execution::{GateWsConfig, GateWsGateway};
#[cfg(feature = "gate_exec")]
use futures_util::future::pending;
#[cfg(feature = "gate_exec")]
use std::env;

struct FastEventSender {
    tx: Option<UnboundedSender<ReferenceEvent>>,
}

impl FastEventSender {
    fn new(tx: Option<UnboundedSender<ReferenceEvent>>) -> Self {
        Self { tx }
    }

    fn send(&self, price: f64, source: &'static str, ts: Option<u64>, recv_at: Instant) {
        if let Some(tx) = self.tx.as_ref() {
            if price.is_finite() && price > 0.0 {
                let _ = tx.send(ReferenceEvent {
                    price,
                    ts_ns: ts,
                    source: source.to_string(),
                    received_at: recv_at,
                });
            }
        }
    }
}

async fn bybit_symbol_supported(symbol: &str) -> bool {
    let url = format!(
        "https://api.bybit.com/v5/market/instruments-info?category=linear&symbol={}",
        symbol
    );
    let client = reqwest::Client::new();
    let resp = match client.get(url).send().await {
        Ok(resp) => resp,
        Err(_) => return true,
    };
    if !resp.status().is_success() {
        return false;
    }
    let value: serde_json::Value = match resp.json().await {
        Ok(json) => json,
        Err(_) => return true,
    };
    if value
        .get("retCode")
        .and_then(|c| c.as_i64())
        .unwrap_or_default()
        != 0
    {
        return false;
    }
    value
        .get("result")
        .and_then(|res| res.get("list"))
        .and_then(|list| list.as_array())
        .map(|list| !list.is_empty())
        .unwrap_or(false)
}

async fn bitget_symbol_supported(symbol: &str) -> bool {
    let inst_id = symbol.replace('_', "").to_ascii_uppercase();
    // Bitget deprecated v1; v2 uses productType=USDT-FUTURES for linear swaps.
    let url = "https://api.bitget.com/api/v2/mix/market/contracts?productType=USDT-FUTURES";
    let client = reqwest::Client::new();
    let resp = match client.get(url).send().await {
        Ok(resp) => resp,
        Err(err) => {
            eprintln!("Bitget symbol check request failed: {err}; keeping Bitget enabled");
            return true;
        }
    };
    if !resp.status().is_success() {
        eprintln!(
            "Bitget symbol check HTTP {} for {}; keeping Bitget enabled",
            resp.status(),
            inst_id
        );
        return true;
    }
    let value: serde_json::Value = match resp.json().await {
        Ok(json) => json,
        Err(err) => {
            eprintln!("Bitget symbol check JSON decode failed: {err}; keeping Bitget enabled");
            return true;
        }
    };
    if value
        .get("code")
        .and_then(|code| code.as_str())
        .unwrap_or("")
        != "00000"
    {
        let msg = value
            .get("msg")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        eprintln!(
            "Bitget symbol check returned code {:?} msg {:?}; keeping Bitget enabled",
            value.get("code"),
            msg
        );
        return true;
    }
    let Some(entries) = value.get("data").and_then(|data| data.as_array()) else {
        eprintln!("Bitget symbol check: missing data array; keeping Bitget enabled");
        return true;
    };
    let found = entries.iter().any(|entry| {
        entry
            .get("symbol")
            .and_then(|v| v.as_str())
            .map(|sym| sym.eq_ignore_ascii_case(&inst_id))
            .unwrap_or(false)
    });
    if !found {
        eprintln!(
            "Bitget symbol {} not found in USDT-FUTURES list; disabling Bitget feeds (auto mode)",
            inst_id
        );
    }
    found
}

#[cfg(feature = "gate_exec")]
fn spawn_gate_user_trades_listener(
    api_key: String,
    api_secret: String,
    contract: String,
    settle: String,
    contract_size: f64,
) {
    let _ = thread::Builder::new()
        .name("gate-user-trades".into())
        .spawn(move || {
            let rt = match tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(err) => {
                    eprintln!("Failed to create tokio runtime for Gate user trades: {err}");
                    return;
                }
            };

            let cfg = GateWsConfig {
                api_key,
                api_secret,
                symbol: contract,
                settle: Some(settle),
                ws_url: None,
                contract_size: Some(contract_size),
            };

            match rt.block_on(GateWsGateway::connect(cfg)) {
                Ok(gateway) => {
                    let _keepalive = gateway;
                    let _ = rt.block_on(async { pending::<()>().await });
                }
                Err(err) => {
                    eprintln!("Failed to connect Gate user trades listener: {:#}", err);
                }
            }
        });
}

pub fn spawn_state_engine(
    symbol: String,
    reference_tx: Option<UnboundedSender<ReferenceEvent>>,
    fast_tx: Option<UnboundedSender<ReferenceEvent>>,
) -> JoinHandle<()> {
    thread::spawn(move || {
        let feeds = current_feeds();
        let mut publisher = ReferencePublisher::new(reference_tx);
        let fast_sender = FastEventSender::new(fast_tx);
        const N: usize = 1 << 15;
        let wake_signal = FeedSignal::new();
        let bybit_auto = feeds.bybit.is_auto();
        let binance_auto = feeds.binance.is_auto();
        let gate_auto = feeds.gate.is_auto();
        let bitget_auto = feeds.bitget.is_auto();
        let okx_auto = feeds.okx.is_auto();
        let mexc_auto = feeds.mexc.is_auto();
        let lighter_auto = feeds.lighter.is_auto();

        let symbol_uc = symbol.to_uppercase();
        let cross_venue_symbol = symbol_uc.replace('_', "");
        let bybit_symbol = cross_venue_symbol.clone();
        let binance_symbol = cross_venue_symbol.clone();
        let bitget_symbol = cross_venue_symbol.clone();
        let okx_inst_id = format_okx_inst_id(&symbol);
        let mexc_symbol = symbol_uc.replace('/', "_");
        let gate_contract = canonical_contract_symbol(&symbol);
        let gate_symbol = gate_contract.clone();
        let lighter_symbol = crate::exchanges::lighter::rest::normalize_symbol(&symbol);
        let rt = Runtime::new().expect("tokio rt");
        let (
            gate_contract_meta,
            bybit_supported,
            bitget_supported,
            okx_meta_result,
            mexc_meta_result,
            lighter_meta,
        ): (
            Option<crate::exchanges::gate::GateContractMeta>,
            bool,
            bool,
            Result<Option<OkxInstrumentMeta>, reqwest::Error>,
            Result<Option<MexcContractMeta>, reqwest::Error>,
            Option<LighterMarketMeta>,
        ) = rt.block_on(async {
            let gate_meta = crate::exchanges::gate::fetch_contract_meta_async(&gate_contract);
            let bybit_fut = async {
                if bybit_auto {
                    bybit_symbol_supported(&bybit_symbol).await
                } else {
                    true
                }
            };
            let bitget_fut = async {
                if bitget_auto {
                    bitget_symbol_supported(&bitget_symbol).await
                } else {
                    true
                }
            };
            let okx_fut = async {
                if okx_auto {
                    fetch_instrument_meta(&okx_inst_id).await
                } else {
                    Ok(None)
                }
            };
            let mexc_fut = async {
                if feeds.mexc.initial_enabled() || mexc_auto {
                    fetch_mexc_contract_meta(&mexc_symbol).await
                } else {
                    Ok(None)
                }
            };
            let lighter_fut = async {
                if feeds.lighter.initial_enabled() || lighter_auto {
                    fetch_market_meta_async(&lighter_symbol).await
                } else {
                    None
                }
            };
            tokio::join!(
                gate_meta,
                bybit_fut,
                bitget_fut,
                okx_fut,
                mexc_fut,
                lighter_fut
            )
        });

        let bybit_supported = if bybit_auto {
            if !bybit_supported {
                eprintln!(
                    "Bybit symbol {} not found; disabling Bybit feeds (auto mode)",
                    bybit_symbol
                );
            }
            bybit_supported
        } else {
            true
        };
        let gate_supported = if gate_auto {
            if gate_contract_meta.is_some() {
                true
            } else {
                eprintln!(
                    "Gate contract {} not found; disabling Gate feeds (auto mode)",
                    gate_contract
                );
                false
            }
        } else {
            true
        };
        let bitget_supported = if bitget_auto {
            if !bitget_supported {
                eprintln!(
                    "Bitget contract {} not found; disabling Bitget feeds (auto mode)",
                    bitget_symbol
                );
            }
            bitget_supported
        } else {
            true
        };
        let (okx_supported, okx_meta) = if okx_auto {
            match okx_meta_result {
                Ok(Some(meta)) => (true, Some(meta)),
                Ok(None) => {
                    eprintln!(
                        "OKX instrument {} not found; disabling OKX feeds (auto mode)",
                        okx_inst_id
                    );
                    (false, None)
                }
                Err(err) => {
                    eprintln!(
                        "OKX instrument lookup failed for {}: {}; keeping OKX enabled with default sizing",
                        okx_inst_id, err
                    );
                    (true, None)
                }
            }
        } else {
            (true, None)
        };

        let (mexc_supported, mexc_meta) = match mexc_meta_result {
            Ok(meta) => (true, meta),
            Err(err) => {
                eprintln!(
                    "MEXC contract lookup failed for {}: {}; keeping MEXC enabled with default sizing",
                    mexc_symbol, err
                );
                (true, None)
            }
        };

        let lighter_supported = if lighter_auto {
            if lighter_meta.is_some() {
                true
            } else {
                eprintln!(
                    "Lighter market {} not found; disabling Lighter feeds (auto mode)",
                    lighter_symbol
                );
                false
            }
        } else if feeds.lighter.initial_enabled() {
            if lighter_meta.is_some() {
                true
            } else {
                eprintln!(
                    "Lighter market {} not found; disabling Lighter feeds",
                    lighter_symbol
                );
                false
            }
        } else {
            false
        };

        let mut bybit_c = if feeds.bybit.initial_enabled() && bybit_supported {
            let (consumer, _jh) = spawn_ws_worker::<BybitHandler, N>(
                BybitHandler::new(symbol.clone()),
                None,
                Some(wake_signal.clone()),
            );
            Some(consumer)
        } else {
            None
        };
        let mut binance_c = if feeds.binance.initial_enabled() {
            let (consumer, _jh) = spawn_ws_worker::<BinanceHandler, N>(
                BinanceHandler::new(symbol.clone()),
                None,
                Some(wake_signal.clone()),
            );
            Some(consumer)
        } else {
            None
        };
        let mut gate_c = if feeds.gate.initial_enabled() && gate_supported {
            let (consumer, _jh) = spawn_ws_worker::<GateHandler, N>(
                GateHandler::new(symbol.clone()),
                None,
                Some(wake_signal.clone()),
            );
            Some(consumer)
        } else {
            None
        };
        let mut bitget_c = if feeds.bitget.initial_enabled() && bitget_supported {
            let (consumer, _jh) = spawn_ws_worker::<BitgetHandler, N>(
                BitgetHandler::new(symbol.clone()),
                None,
                Some(wake_signal.clone()),
            );
            Some(consumer)
        } else {
            None
        };
        let mut okx_c = if feeds.okx.initial_enabled() && okx_supported {
            let (consumer, _jh) = spawn_ws_worker::<OkxHandler, N>(
                OkxHandler::new(symbol.clone()),
                None,
                Some(wake_signal.clone()),
            );
            Some(consumer)
        } else {
            None
        };
        let mut mexc_c = if feeds.mexc.initial_enabled() && mexc_supported {
            let (consumer, _jh) = spawn_ws_worker::<MexcHandler, N>(
                MexcHandler::new(mexc_symbol.clone()),
                None,
                Some(wake_signal.clone()),
            );
            Some(consumer)
        } else {
            None
        };
        let mut lighter_c = if feeds.lighter.initial_enabled() && lighter_supported {
            if let Some(meta) = lighter_meta.clone() {
                let (consumer, _jh) = spawn_ws_worker::<LighterHandler, N>(
                    LighterHandler::new(meta.clone()),
                    None,
                    Some(wake_signal.clone()),
                );
                Some((consumer, meta))
            } else {
                None
            }
        } else {
            None
        };
        #[cfg(feature = "gate_exec")]
        {
            if gate_c.is_some() {
                let api_key = env::var("gateio_api_key").or_else(|_| env::var("GATE_API_KEY"));
                let api_secret =
                    env::var("gateio_secret_key").or_else(|_| env::var("GATE_API_SECRET"));
                if let (Ok(api_key), Ok(api_secret)) = (api_key, api_secret) {
                    let settle = env::var("GATE_SETTLE").unwrap_or_else(|_| "usdt".to_string());
                    let contract_size = gate_contract_meta
                        .as_ref()
                        .and_then(|meta| meta.quanto_multiplier)
                        .unwrap_or(1.0);
                    spawn_gate_user_trades_listener(
                        api_key,
                        api_secret,
                        gate_contract.clone(),
                        settle,
                        contract_size,
                    );
                }
            }
        }

        let mut feed_gate = FeedTimestampGate::new();
        let mut demean = DemeanController::new(demean_enabled(), Duration::from_secs(8));

        let mut bybit_engine = bybit_c
            .take()
            .map(|consumer| bybit::BybitEngine::new(bybit_symbol.clone(), consumer));
        let mut binance_engine = binance_c.take().and_then(|consumer| {
            binance::BinanceEngine::try_new(binance_symbol.clone(), consumer, binance_auto)
        });
        let mut gate_engine = gate_c.take().map(|consumer| {
            gate::GateEngine::new(gate_symbol.clone(), consumer, gate_contract_meta.clone())
        });
        let mut bitget_engine = bitget_c
            .take()
            .map(|consumer| bitget::BitgetEngine::new(bitget_symbol.clone(), consumer));
        let mut okx_engine = okx_c
            .take()
            .map(|consumer| okx::OkxEngine::new(okx_inst_id.clone(), consumer, okx_meta.clone()));
        let mut mexc_engine = mexc_c.take().map(|consumer| {
            mexc::MexcEngine::new(mexc_symbol.clone(), consumer, mexc_meta.clone())
        });
        let mut lighter_engine = lighter_c.take().map(|(consumer, meta)| {
            lighter::LighterEngine::new(lighter_symbol.clone(), meta, consumer)
        });

        loop {
            let mut progressed = false;

            if let Some(engine) = bybit_engine.as_mut() {
                progressed |= engine.process(&mut feed_gate, &mut publisher, &mut demean);
            }
            if let Some(engine) = binance_engine.as_mut() {
                progressed |= engine.process(&mut feed_gate, &mut publisher, &mut demean);
            }
            if let Some(engine) = gate_engine.as_mut() {
                progressed |=
                    engine.process(&mut feed_gate, &mut publisher, &mut demean, &fast_sender);
            }
            if let Some(engine) = bitget_engine.as_mut() {
                progressed |= engine.process(&mut feed_gate, &mut publisher, &mut demean);
            }
            if let Some(engine) = okx_engine.as_mut() {
                progressed |= engine.process(&mut feed_gate, &mut publisher, &mut demean);
            }
            if let Some(engine) = mexc_engine.as_mut() {
                progressed |= engine.process(&mut feed_gate, &mut publisher, &mut demean);
            }
            if let Some(engine) = lighter_engine.as_mut() {
                progressed |= engine.process(&mut feed_gate, &mut publisher, &mut demean);
            }

            if progressed {
                publisher.publish();
            } else {
                wake_signal.wait();
            }
        }
    })
}

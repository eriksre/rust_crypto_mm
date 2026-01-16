use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use reqwest::Url;
use serde::Deserialize;

use crate::base_classes::feed_config::FeedToggles;
use crate::execution::types::Venue;
use crate::execution::{GateCredentials, LighterCredentials};
use crate::pricing::PricingModelConfig;
use crate::strategy::{MomentumFadeConfig, QuoteConfig, StrategyKind};

fn default_true() -> bool {
    true
}

fn default_flush_interval_ms() -> u64 {
    200
}

#[derive(Debug, Deserialize, Clone)]
pub struct RiskConfig {
    pub max_order_notional: f64,
    #[serde(default)]
    pub max_position_notional: f64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ModeConfig {
    #[serde(default = "default_true")]
    pub dry_run: bool,
    #[serde(default)]
    pub log_fills: bool,
    #[serde(default)]
    pub debug_prints: bool,
    #[serde(default)]
    pub markout_prints: bool,
    #[serde(default = "default_true")]
    pub demean_prices: bool,
}

#[derive(Debug, Deserialize, Clone, Default)]
#[serde(default)]
pub struct LoggingConfig {
    pub enabled: bool,
    pub path: Option<String>,
    #[serde(default = "default_flush_interval_ms")]
    pub flush_interval_ms: u64,
}

impl LoggingConfig {
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub fn resolve_path(&self, venue: Venue) -> std::path::PathBuf {
        if let Some(path) = &self.path {
            return std::path::PathBuf::from(path);
        }
        std::path::PathBuf::from(format!("logs/{}_activity.csv", venue.as_str()))
    }

    pub fn flush_interval(&self) -> Duration {
        Duration::from_millis(self.flush_interval_ms)
    }
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct CredentialsConfig {
    #[serde(default)]
    pub api_key_env: Option<String>,
    #[serde(default)]
    pub api_secret_env: Option<String>,
    #[serde(default)]
    pub api_key: Option<String>,
    #[serde(default)]
    pub api_secret: Option<String>,
    #[serde(default)]
    pub account_index: Option<i64>,
    #[serde(default)]
    pub api_key_index: Option<i32>,
    #[serde(default)]
    pub base_url: Option<String>,
    #[serde(default)]
    pub signer_lib: Option<String>,
    #[serde(default)]
    pub chain_id: Option<u32>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RunnerConfig {
    #[serde(default)]
    pub strategy_kind: StrategyKind,
    pub strategy: QuoteConfig,
    #[serde(default)]
    pub momentum_fade: Option<MomentumFadeConfig>,
    pub risk: RiskConfig,
    pub mode: ModeConfig,
    #[serde(default)]
    pub logging: LoggingConfig,
    #[serde(default)]
    pub credentials: Option<CredentialsConfig>,
    #[serde(default)]
    pub settle: Option<String>,
    #[serde(default)]
    pub feeds: FeedToggles,
    #[serde(default)]
    pub pricing_model: Option<PricingModelConfig>,
}

pub fn load_runner_config(path: &str) -> Result<RunnerConfig> {
    let contents = std::fs::read_to_string(Path::new(path))
        .with_context(|| format!("failed to read config at {}", path))?;
    let config: RunnerConfig = serde_yaml::from_str(&contents)
        .with_context(|| format!("failed to parse config at {}", path))?;
    validate_runner_config(&config)?;
    Ok(config)
}

pub fn load_gate_credentials(config: &RunnerConfig) -> Result<GateCredentials> {
    let creds = config.credentials.clone().unwrap_or_default();
    let key_env = creds
        .api_key_env
        .unwrap_or_else(|| "GATEIO_API_KEY".to_string());
    let secret_env = creds
        .api_secret_env
        .unwrap_or_else(|| "GATEIO_SECRET_KEY".to_string());

    let (api_key, api_key_source) = match creds.api_key.clone() {
        Some(v) => (v, "config.api_key".to_string()),
        None => (
            std::env::var(&key_env).with_context(|| format!("missing env var {key_env}"))?,
            format!("env:{key_env}"),
        ),
    };
    let (api_secret, api_secret_source) = match creds.api_secret.clone() {
        Some(v) => (v, "config.api_secret".to_string()),
        None => (
            std::env::var(&secret_env).with_context(|| format!("missing env var {secret_env}"))?,
            format!("env:{secret_env}"),
        ),
    };
    eprintln!(
        "Resolved Gate credentials: api_key_source={}, api_secret_source={}",
        api_key_source, api_secret_source
    );
    Ok(GateCredentials {
        api_key,
        api_secret,
    })
}

fn default_signer_lib_path() -> String {
    if cfg!(target_os = "macos") {
        "libs/lighter/signer-arm64.dylib".to_string()
    } else if cfg!(target_arch = "aarch64") {
        "libs/lighter/signer-arm64.so".to_string()
    } else {
        "libs/lighter/signer-amd64.so".to_string()
    }
}

pub fn load_lighter_credentials(config: &RunnerConfig) -> Result<LighterCredentials> {
    let creds = config.credentials.clone().unwrap_or_default();
    let key_env = creds
        .api_key_env
        .unwrap_or_else(|| "LIGHTER_API_KEY".to_string());
    let (api_key_hex, api_key_source) = match creds.api_key.clone() {
        Some(v) => (v, "config.api_key".to_string()),
        None => (
            std::env::var(&key_env).with_context(|| format!("missing env var {key_env}"))?,
            format!("env:{key_env}"),
        ),
    };

    let (account_index, account_index_source) = match std::env::var("LIGHTER_ACCOUNT_INDEX") {
        Ok(v) => (
            v.parse::<i64>()
                .with_context(|| "invalid LIGHTER_ACCOUNT_INDEX")?,
            "env:LIGHTER_ACCOUNT_INDEX".to_string(),
        ),
        Err(_) => match creds.account_index {
            Some(v) => (v, "config.account_index".to_string()),
            None => bail!("missing LIGHTER_ACCOUNT_INDEX (env) or credentials.account_index"),
        },
    };
    let (api_key_index, api_key_index_source) = match std::env::var("LIGHTER_API_KEY_INDEX") {
        Ok(v) => (
            v.parse::<i32>()
                .with_context(|| "invalid LIGHTER_API_KEY_INDEX")?,
            "env:LIGHTER_API_KEY_INDEX".to_string(),
        ),
        Err(_) => match creds.api_key_index {
            Some(v) => (v, "config.api_key_index".to_string()),
            None => bail!("missing LIGHTER_API_KEY_INDEX (env) or credentials.api_key_index"),
        },
    };
    let (base_url, base_url_source) = match std::env::var("LIGHTER_BASE_URL") {
        Ok(v) => (v, "env:LIGHTER_BASE_URL".to_string()),
        Err(_) => match creds.base_url {
            Some(v) => (v, "config.base_url".to_string()),
            None => bail!("missing LIGHTER_BASE_URL (env) or credentials.base_url"),
        },
    };
    if Url::parse(&base_url).is_err() {
        bail!("invalid LIGHTER_BASE_URL {}", base_url);
    }

    let signer_lib_raw = match creds.signer_lib {
        Some(v) => v,
        None => bail!("missing credentials.signer_lib (set to 'auto' or explicit path)"),
    };
    let signer_lib = if signer_lib_raw.eq_ignore_ascii_case("auto") {
        default_signer_lib_path()
    } else {
        signer_lib_raw
    };
    eprintln!(
        "Resolved Lighter credentials: api_key_source={}, account_index_source={}, api_key_index_source={}, base_url_source={}, signer_lib={}",
        api_key_source,
        account_index_source,
        api_key_index_source,
        base_url_source,
        signer_lib
    );

    Ok(LighterCredentials {
        api_key_hex,
        account_index,
        api_key_index,
        base_url,
        signer_lib,
        chain_id: creds.chain_id,
    })
}

pub fn validate_runner_config(config: &RunnerConfig) -> Result<()> {
    if config.strategy.quote_interval_ms == 0 {
        bail!("strategy.quote_interval_ms must be > 0");
    }
    if config.strategy.min_tick <= 0.0 || !config.strategy.min_tick.is_finite() {
        bail!("strategy.min_tick must be finite and > 0");
    }
    if config.logging.enabled && config.logging.flush_interval_ms == 0 {
        bail!("logging.flush_interval_ms must be > 0 when logging is enabled");
    }
    if config.risk.max_order_notional <= 0.0 || !config.risk.max_order_notional.is_finite() {
        bail!("risk.max_order_notional must be finite and > 0");
    }
    if config.risk.max_position_notional < 0.0 || !config.risk.max_position_notional.is_finite() {
        bail!("risk.max_position_notional must be finite and >= 0");
    }
    if config.strategy.symbol.trim().is_empty() {
        bail!("strategy.symbol must be set");
    }

    match config.strategy_kind {
        StrategyKind::SimpleQuote => {
            if config.momentum_fade.is_some() {
                bail!("momentum_fade config present but strategy_kind is simple_quote");
            }
        }
        StrategyKind::MomentumFade => {
            let Some(momentum) = config.momentum_fade.as_ref() else {
                bail!("strategy_kind=momentum_fade requires momentum_fade config");
            };
            if config.strategy.venue != Venue::Lighter {
                bail!("momentum_fade strategy requires venue=lighter");
            }
            if !config.feeds.lighter.initial_enabled() {
                bail!("momentum_fade strategy requires feeds.lighter to be enabled");
            }
            if config
                .pricing_model
                .as_ref()
                .map(|cfg| cfg.enabled)
                .unwrap_or(false)
                == false
            {
                bail!("momentum_fade strategy requires pricing_model.enabled=true");
            }
            if momentum.lookback_ms == 0 {
                bail!("momentum_fade.lookback_ms must be > 0");
            }
            if !momentum.entry_threshold_bps.is_finite() || momentum.entry_threshold_bps < 0.0 {
                bail!("momentum_fade.entry_threshold_bps must be finite and >= 0");
            }
            if !momentum.adverse_threshold_bps.is_finite()
                || momentum.adverse_threshold_bps < 0.0
            {
                bail!("momentum_fade.adverse_threshold_bps must be finite and >= 0");
            }
            if momentum.min_interval_ms == 0 {
                bail!("momentum_fade.min_interval_ms must be > 0");
            }
            if let Some(symbol) = &momentum.symbol {
                if symbol.trim().is_empty() {
                    bail!("momentum_fade.symbol must be non-empty when set");
                }
                if symbol != &config.strategy.symbol {
                    bail!(
                        "momentum_fade.symbol {} does not match strategy.symbol {}",
                        symbol,
                        config.strategy.symbol
                    );
                }
            }
            if let Some(min_tick) = momentum.min_tick {
                if !min_tick.is_finite() || min_tick <= 0.0 {
                    bail!("momentum_fade.min_tick must be finite and > 0 when set");
                }
                if (min_tick - config.strategy.min_tick).abs() > f64::EPSILON {
                    bail!(
                        "momentum_fade.min_tick {:.8} does not match strategy.min_tick {:.8}",
                        min_tick,
                        config.strategy.min_tick
                    );
                }
            }
            if let Some(max_order) = momentum.max_order_notional {
                if !max_order.is_finite() || max_order <= 0.0 {
                    bail!("momentum_fade.max_order_notional must be finite and > 0 when set");
                }
                if (max_order - config.risk.max_order_notional).abs() > f64::EPSILON {
                    bail!(
                        "momentum_fade.max_order_notional {:.6} does not match risk.max_order_notional {:.6}",
                        max_order,
                        config.risk.max_order_notional
                    );
                }
            }
            if let Some(max_position) = momentum.max_position_notional {
                if !max_position.is_finite() || max_position < 0.0 {
                    bail!("momentum_fade.max_position_notional must be finite and >= 0 when set");
                }
                if (max_position - config.risk.max_position_notional).abs() > f64::EPSILON {
                    bail!(
                        "momentum_fade.max_position_notional {:.6} does not match risk.max_position_notional {:.6}",
                        max_position,
                        config.risk.max_position_notional
                    );
                }
            }
        }
    }
    Ok(())
}

pub fn log_runner_config(config: &RunnerConfig) {
    eprintln!(
        "Runner config: strategy_kind={}, venue={}, symbol={}, dry_run={}, log_fills={}, debug_prints={}, markout_prints={}, demean_prices={}",
        config.strategy_kind.as_str(),
        config.strategy.venue.as_str(),
        config.strategy.symbol,
        config.mode.dry_run,
        config.mode.log_fills,
        config.mode.debug_prints,
        config.mode.markout_prints,
        config.mode.demean_prices
    );
    if config.logging.enabled {
        eprintln!(
            "Logging enabled: path={} flush_interval_ms={}",
            config.logging.resolve_path(config.strategy.venue).display(),
            config.logging.flush_interval_ms
        );
    } else {
        eprintln!("Logging disabled");
    }
    if let Some(settle) = &config.settle {
        eprintln!("Settle currency: {settle}");
    } else {
        eprintln!("Settle currency: <unset>");
    }

    if config.strategy_kind == StrategyKind::MomentumFade {
        if let Some(momentum) = config.momentum_fade.as_ref() {
            eprintln!(
                "Momentum fade: entry_source={}, lookback_ms={}, entry_threshold_bps={}, tick_offset={}, adverse_threshold_bps={}, max_age_ms={}, min_interval_ms={}",
                momentum.entry_price_source.as_str(),
                momentum.lookback_ms,
                momentum.entry_threshold_bps,
                momentum.tick_offset,
                momentum.adverse_threshold_bps,
                momentum.max_age_ms,
                momentum.min_interval_ms
            );
            if let Some(symbol) = &momentum.symbol {
                eprintln!("Momentum fade symbol override: {}", symbol);
            }
            if let Some(min_tick) = momentum.min_tick {
                eprintln!("Momentum fade min_tick override: {:.8}", min_tick);
            }
            if let Some(max_order) = momentum.max_order_notional {
                eprintln!("Momentum fade max_order_notional override: {:.6}", max_order);
            }
            if let Some(max_pos) = momentum.max_position_notional {
                eprintln!(
                    "Momentum fade max_position_notional override: {:.6}",
                    max_pos
                );
            }
        }
    }
}

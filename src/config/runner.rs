use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use reqwest::Url;
use serde::Deserialize;

use crate::base_classes::feed_config::FeedToggles;
use crate::execution::types::Venue;
use crate::execution::{GateCredentials, LighterCredentials};
use crate::pricing::PricingModelConfig;
use crate::strategy::QuoteConfig;

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
    pub strategy: QuoteConfig,
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
    Ok(())
}

pub fn log_runner_config(config: &RunnerConfig) {
    eprintln!(
        "Runner config: venue={}, symbol={}, dry_run={}, log_fills={}, debug_prints={}, markout_prints={}, demean_prices={}",
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
}

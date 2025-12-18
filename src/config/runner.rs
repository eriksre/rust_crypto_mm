use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};
use serde::Deserialize;

use crate::base_classes::feed_config::FeedToggles;
use crate::execution::types::Venue;
use crate::execution::{GateCredentials, LighterCredentials};
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
        Duration::from_millis(self.flush_interval_ms.max(1))
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
}

pub fn load_runner_config(path: &str) -> Result<RunnerConfig> {
    let contents = std::fs::read_to_string(Path::new(path))
        .with_context(|| format!("failed to read config at {}", path))?;
    let config: RunnerConfig = serde_yaml::from_str(&contents)
        .with_context(|| format!("failed to parse config at {}", path))?;
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

    let api_key = match creds.api_key.clone() {
        Some(v) => v,
        None => std::env::var(&key_env).with_context(|| format!("missing env var {key_env}"))?,
    };
    let api_secret = match creds.api_secret.clone() {
        Some(v) => v,
        None => {
            std::env::var(&secret_env).with_context(|| format!("missing env var {secret_env}"))?
        }
    };
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
    let api_key_hex = match creds.api_key.clone() {
        Some(v) => v,
        None => std::env::var(&key_env).with_context(|| format!("missing env var {key_env}"))?,
    };

    let account_index = std::env::var("LIGHTER_ACCOUNT_INDEX")
        .ok()
        .and_then(|v| v.parse::<i64>().ok())
        .or(creds.account_index)
        .unwrap_or(0);
    let api_key_index = std::env::var("LIGHTER_API_KEY_INDEX")
        .ok()
        .and_then(|v| v.parse::<i32>().ok())
        .or(creds.api_key_index)
        .unwrap_or(0);
    let base_url = std::env::var("LIGHTER_BASE_URL")
        .ok()
        .or(creds.base_url)
        .unwrap_or_else(|| "https://mainnet.zklighter.elliot.ai/".to_string());

    let signer_lib_raw = creds.signer_lib.unwrap_or_else(default_signer_lib_path);
    let signer_lib = if signer_lib_raw.eq_ignore_ascii_case("auto") {
        default_signer_lib_path()
    } else {
        signer_lib_raw
    };

    Ok(LighterCredentials {
        api_key_hex,
        account_index,
        api_key_index,
        base_url,
        signer_lib,
        chain_id: creds.chain_id,
    })
}

use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};
use serde::Deserialize;

use crate::base_classes::feed_config::FeedToggles;
use crate::execution::GateCredentials;
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

    pub fn resolve_path(&self) -> std::path::PathBuf {
        self.path
            .as_ref()
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| std::path::PathBuf::from("logs/gate_activity.csv"))
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

    let api_key = std::env::var(&key_env).with_context(|| format!("missing env var {key_env}"))?;
    let api_secret =
        std::env::var(&secret_env).with_context(|| format!("missing env var {secret_env}"))?;
    Ok(GateCredentials {
        api_key,
        api_secret,
    })
}

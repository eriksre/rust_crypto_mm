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
#[serde(deny_unknown_fields)]
pub struct RiskConfig {
    pub max_order_notional: f64,
    #[serde(default)]
    pub max_position_notional: f64,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
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
    #[serde(default)]
    pub suppress_quote_loop_idle_logs: bool,
    #[serde(default)]
    pub suppress_inventory_rollback_warnings: bool,
    #[serde(default)]
    pub suppress_lighter_sendtx_quota_logs: bool,
}

#[derive(Debug, Deserialize, Clone, Default)]
#[serde(default)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
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
        api_key_source, account_index_source, api_key_index_source, base_url_source, signer_lib
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
    if let Some(model_cfg) = config.pricing_model.as_ref().filter(|cfg| cfg.enabled) {
        validate_pricing_model_config(model_cfg)?;
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
            if let Some(v) = momentum.entry_threshold_bps_bid {
                if !v.is_finite() || v < 0.0 {
                    bail!("momentum_fade.entry_threshold_bps_bid must be finite and >= 0");
                }
            }
            if let Some(v) = momentum.entry_threshold_bps_ask {
                if !v.is_finite() || v < 0.0 {
                    bail!("momentum_fade.entry_threshold_bps_ask must be finite and >= 0");
                }
            }
            if !momentum.adverse_threshold_bps.is_finite() || momentum.adverse_threshold_bps < 0.0 {
                bail!("momentum_fade.adverse_threshold_bps must be finite and >= 0");
            }
            if let Some(v) = momentum.adverse_threshold_bps_bid {
                if !v.is_finite() || v < 0.0 {
                    bail!("momentum_fade.adverse_threshold_bps_bid must be finite and >= 0");
                }
            }
            if let Some(v) = momentum.adverse_threshold_bps_ask {
                if !v.is_finite() || v < 0.0 {
                    bail!("momentum_fade.adverse_threshold_bps_ask must be finite and >= 0");
                }
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

fn validate_pricing_model_config(cfg: &PricingModelConfig) -> Result<()> {
    let kalman = &cfg.kalman;
    let tuning = &cfg.tuning;

    if kalman.ref_stream.trim().is_empty() || !kalman.ref_stream.contains(':') {
        bail!(
            "pricing_model.kalman.ref_stream must be in '<exchange>:<feed>' format, got '{}'",
            kalman.ref_stream
        );
    }
    ensure_finite("pricing_model.kalman.k_per_sec", kalman.k_per_sec)?;
    ensure_finite_ge("pricing_model.kalman.q_per_sec", kalman.q_per_sec, 0.0)?;
    ensure_finite("pricing_model.kalman.gamma_imb", kalman.gamma_imb)?;

    validate_f64_map(
        "pricing_model.kalman.bias_by_stream",
        &kalman.bias_by_stream,
        |v| v.is_finite(),
        "finite",
    )?;
    ensure_finite_gt("pricing_model.kalman.stats_alpha", kalman.stats_alpha, 0.0)?;
    if kalman.stats_alpha > 1.0 {
        bail!("pricing_model.kalman.stats_alpha must be <= 1.0");
    }
    if kalman.stats_warmup_obs == 0 {
        bail!("pricing_model.kalman.stats_warmup_obs must be > 0");
    }
    ensure_finite_gt(
        "pricing_model.kalman.r_learn_alpha",
        kalman.r_learn_alpha,
        0.0,
    )?;
    if kalman.r_learn_alpha > 1.0 {
        bail!("pricing_model.kalman.r_learn_alpha must be <= 1.0");
    }
    if kalman.r_learn_warmup_obs == 0 {
        bail!("pricing_model.kalman.r_learn_warmup_obs must be > 0");
    }
    ensure_finite_gt("pricing_model.kalman.r_floor", kalman.r_floor, 0.0)?;
    ensure_finite_gt("pricing_model.kalman.r_ceiling", kalman.r_ceiling, 0.0)?;
    if kalman.r_ceiling < kalman.r_floor {
        bail!("pricing_model.kalman.r_ceiling must be >= pricing_model.kalman.r_floor");
    }
    ensure_finite_ge("pricing_model.kalman.r_clip_mult", kalman.r_clip_mult, 1.0)?;
    if let Some(path) = kalman.r_state_path.as_ref() {
        if path.trim().is_empty() {
            bail!("pricing_model.kalman.r_state_path must be non-empty when set");
        }
    }
    if kalman.r_state_flush_interval_s == 0 {
        bail!("pricing_model.kalman.r_state_flush_interval_s must be > 0");
    }
    if kalman.r_state_flush_min_updates == 0 {
        bail!("pricing_model.kalman.r_state_flush_min_updates must be > 0");
    }

    ensure_finite("pricing_model.tuning.trade_dir_bps", tuning.trade_dir_bps)?;
    ensure_finite_ge(
        "pricing_model.tuning.trade_r_mult",
        tuning.trade_r_mult,
        0.0,
    )?;
    ensure_finite_ge(
        "pricing_model.tuning.trade_size_beta",
        tuning.trade_size_beta,
        0.0,
    )?;
    ensure_finite_ge("pricing_model.tuning.book_w_mid", tuning.book_w_mid, 0.0)?;
    ensure_finite_ge(
        "pricing_model.tuning.book_w_micro",
        tuning.book_w_micro,
        0.0,
    )?;
    ensure_finite_ge(
        "pricing_model.tuning.book_w_vwap5",
        tuning.book_w_vwap5,
        0.0,
    )?;
    ensure_finite_ge(
        "pricing_model.tuning.lighter_r_mult",
        tuning.lighter_r_mult,
        0.0,
    )?;
    ensure_finite_ge(
        "pricing_model.tuning.latency_alpha",
        tuning.latency_alpha,
        0.0,
    )?;
    ensure_finite_ge("pricing_model.tuning.stale_alpha", tuning.stale_alpha, 0.0)?;
    ensure_finite_ge("pricing_model.tuning.vol_alpha", tuning.vol_alpha, 0.0)?;
    ensure_finite_ge(
        "pricing_model.tuning.vol_halflife_s",
        tuning.vol_halflife_s,
        0.0,
    )?;
    ensure_finite_ge("pricing_model.tuning.robust_z", tuning.robust_z, 0.0)?;
    ensure_finite_ge("pricing_model.tuning.jump_z", tuning.jump_z, 0.0)?;
    ensure_finite_ge("pricing_model.tuning.jump_beta", tuning.jump_beta, 0.0)?;
    ensure_finite_ge(
        "pricing_model.tuning.bias_ewma_halflife_s",
        tuning.bias_ewma_halflife_s,
        0.0,
    )?;
    ensure_finite_ge("pricing_model.tuning.horizon_ms", tuning.horizon_ms, 0.0)?;
    ensure_finite_ge(
        "pricing_model.tuning.q_floor_mult",
        tuning.q_floor_mult,
        0.0,
    )?;
    ensure_finite_ge(
        "pricing_model.tuning.q_dt_floor_ms",
        tuning.q_dt_floor_ms,
        0.0,
    )?;
    ensure_finite_ge(
        "pricing_model.tuning.eval_move_bps",
        tuning.eval_move_bps,
        0.0,
    )?;
    ensure_finite_ge(
        "pricing_model.tuning.blend_age0_ms",
        tuning.blend_age0_ms,
        0.0,
    )?;
    ensure_finite_ge(
        "pricing_model.tuning.blend_age_scale_ms",
        tuning.blend_age_scale_ms,
        0.0,
    )?;
    ensure_finite_ge(
        "pricing_model.tuning.blend_diff0_bps",
        tuning.blend_diff0_bps,
        0.0,
    )?;
    ensure_finite_ge(
        "pricing_model.tuning.blend_diff_scale_bps",
        tuning.blend_diff_scale_bps,
        0.0,
    )?;
    ensure_finite_ge("pricing_model.tuning.blend_max_w", tuning.blend_max_w, 0.0)?;
    ensure_finite_ge(
        "pricing_model.tuning.vel_halflife_s",
        tuning.vel_halflife_s,
        0.0,
    )?;
    ensure_finite_ge(
        "pricing_model.tuning.vel_cap_bps_per_s",
        tuning.vel_cap_bps_per_s,
        0.0,
    )?;
    ensure_finite_ge("pricing_model.tuning.ecm_tau_ms", tuning.ecm_tau_ms, 0.0)?;
    ensure_finite_ge(
        "pricing_model.tuning.common_disp0_bps",
        tuning.common_disp0_bps,
        0.0,
    )?;
    ensure_finite_ge(
        "pricing_model.tuning.common_max_age_ms",
        tuning.common_max_age_ms,
        0.0,
    )?;
    ensure_finite_ge(
        "pricing_model.tuning.snap_diff0_bps",
        tuning.snap_diff0_bps,
        0.0,
    )?;
    ensure_finite_ge(
        "pricing_model.tuning.snap_diff_scale_bps",
        tuning.snap_diff_scale_bps,
        0.0,
    )?;
    ensure_finite_ge(
        "pricing_model.tuning.snap_disp_max_bps",
        tuning.snap_disp_max_bps,
        0.0,
    )?;
    ensure_finite_ge(
        "pricing_model.tuning.snap_disp_scale_bps",
        tuning.snap_disp_scale_bps,
        0.0,
    )?;
    ensure_finite_ge("pricing_model.tuning.snap_max_w", tuning.snap_max_w, 0.0)?;
    ensure_finite_ge(
        "pricing_model.tuning.snap_age0_ms",
        tuning.snap_age0_ms,
        0.0,
    )?;
    ensure_finite_ge(
        "pricing_model.tuning.snap_age_scale_ms",
        tuning.snap_age_scale_ms,
        0.0,
    )?;
    if tuning.snap_min_n <= 0 {
        bail!("pricing_model.tuning.snap_min_n must be > 0");
    }
    ensure_finite_ge(
        "pricing_model.tuning.side_age_tau_ms",
        tuning.side_age_tau_ms,
        0.0,
    )?;
    ensure_finite_ge(
        "pricing_model.tuning.dir_vscale_bps_per_s",
        tuning.dir_vscale_bps_per_s,
        0.0,
    )?;
    ensure_finite_ge(
        "pricing_model.tuning.quote_half_spread_floor_bps",
        tuning.quote_half_spread_floor_bps,
        0.0,
    )?;
    ensure_finite_ge(
        "pricing_model.tuning.quote_half_spread_cap_bps",
        tuning.quote_half_spread_cap_bps,
        0.0,
    )?;
    if tuning.quote_half_spread_cap_bps < tuning.quote_half_spread_floor_bps {
        bail!(
            "pricing_model.tuning.quote_half_spread_cap_bps must be >= quote_half_spread_floor_bps"
        );
    }
    ensure_finite_ge(
        "pricing_model.tuning.quote_disp0_bps",
        tuning.quote_disp0_bps,
        0.0,
    )?;
    ensure_finite_ge(
        "pricing_model.tuning.quote_disp_mult",
        tuning.quote_disp_mult,
        0.0,
    )?;
    ensure_finite_ge(
        "pricing_model.tuning.quote_age0_ms",
        tuning.quote_age0_ms,
        0.0,
    )?;
    ensure_finite_ge(
        "pricing_model.tuning.quote_age_bps_per_100ms",
        tuning.quote_age_bps_per_100ms,
        0.0,
    )?;
    ensure_finite_ge(
        "pricing_model.tuning.quote_unc_mult",
        tuning.quote_unc_mult,
        0.0,
    )?;
    ensure_finite_ge(
        "pricing_model.tuning.lighter_bias_halflife_s",
        tuning.lighter_bias_halflife_s,
        0.0,
    )?;
    ensure_finite_ge(
        "pricing_model.tuning.lighter_bias_cap_bps",
        tuning.lighter_bias_cap_bps,
        0.0,
    )?;

    if tuning.common_source.trim().is_empty() {
        bail!("pricing_model.tuning.common_source must be non-empty");
    }

    Ok(())
}

fn ensure_finite(name: &str, value: f64) -> Result<()> {
    if !value.is_finite() {
        bail!("{name} must be finite");
    }
    Ok(())
}

fn ensure_finite_ge(name: &str, value: f64, min: f64) -> Result<()> {
    if !value.is_finite() || value < min {
        bail!("{name} must be finite and >= {min}");
    }
    Ok(())
}

fn ensure_finite_gt(name: &str, value: f64, min: f64) -> Result<()> {
    if !value.is_finite() || value <= min {
        bail!("{name} must be finite and > {min}");
    }
    Ok(())
}

fn validate_f64_map<F>(
    map_name: &str,
    map: &std::collections::HashMap<String, f64>,
    is_valid: F,
    expectation: &str,
) -> Result<()>
where
    F: Fn(f64) -> bool,
{
    for (key, value) in map {
        if key.trim().is_empty() {
            bail!("{map_name} contains an empty key");
        }
        if !is_valid(*value) {
            bail!("{map_name}['{key}'] must be {expectation}, got {value}");
        }
    }
    Ok(())
}

pub fn log_runner_config(config: &RunnerConfig) {
    eprintln!(
        "Runner config: strategy_kind={}, venue={}, symbol={}, dry_run={}, log_fills={}, debug_prints={}, markout_prints={}, demean_prices={}, suppress_quote_loop_idle_logs={}, suppress_inventory_rollback_warnings={}, suppress_lighter_sendtx_quota_logs={}",
        config.strategy_kind.as_str(),
        config.strategy.venue.as_str(),
        config.strategy.symbol,
        config.mode.dry_run,
        config.mode.log_fills,
        config.mode.debug_prints,
        config.mode.markout_prints,
        config.mode.demean_prices,
        config.mode.suppress_quote_loop_idle_logs,
        config.mode.suppress_inventory_rollback_warnings,
        config.mode.suppress_lighter_sendtx_quota_logs
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
            if momentum.entry_threshold_bps_bid.is_some()
                || momentum.entry_threshold_bps_ask.is_some()
            {
                eprintln!(
                    "Momentum fade side entry thresholds: bid={:?} ask={:?}",
                    momentum.entry_threshold_bps_bid, momentum.entry_threshold_bps_ask
                );
            }
            if momentum.tick_offset_bid.is_some() || momentum.tick_offset_ask.is_some() {
                eprintln!(
                    "Momentum fade side tick offsets: bid={:?} ask={:?}",
                    momentum.tick_offset_bid, momentum.tick_offset_ask
                );
            }
            if momentum.adverse_threshold_bps_bid.is_some()
                || momentum.adverse_threshold_bps_ask.is_some()
            {
                eprintln!(
                    "Momentum fade side adverse thresholds: bid={:?} ask={:?}",
                    momentum.adverse_threshold_bps_bid, momentum.adverse_threshold_bps_ask
                );
            }
            if momentum.max_age_ms_bid.is_some() || momentum.max_age_ms_ask.is_some() {
                eprintln!(
                    "Momentum fade side max_age_ms: bid={:?} ask={:?}",
                    momentum.max_age_ms_bid, momentum.max_age_ms_ask
                );
            }
            if let Some(symbol) = &momentum.symbol {
                eprintln!("Momentum fade symbol override: {}", symbol);
            }
            if let Some(min_tick) = momentum.min_tick {
                eprintln!("Momentum fade min_tick override: {:.8}", min_tick);
            }
            if let Some(max_order) = momentum.max_order_notional {
                eprintln!(
                    "Momentum fade max_order_notional override: {:.6}",
                    max_order
                );
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_strategy_field_is_rejected() {
        let yaml = r#"
strategy:
  venue: gate
  symbol: BTC_USDT
  size: 1
  spread_bps: 40
risk:
  max_order_notional: 10
  max_position_notional: 20
mode:
  dry_run: true
"#;
        let err = serde_yaml::from_str::<RunnerConfig>(&yaml).expect_err("expected parse failure");
        let err_text = err.to_string();
        assert!(
            err_text.contains("spread_bps"),
            "expected unknown field error to mention spread_bps, got: {err_text}"
        );
    }

    #[test]
    fn pricing_model_validation_accepts_positive_k_per_sec() {
        let yaml = r#"
strategy:
  venue: gate
  symbol: BTC_USDT
  size: 1
risk:
  max_order_notional: 10
  max_position_notional: 20
mode:
  dry_run: true
pricing_model:
  enabled: true
  kalman:
    ref_stream: gate:orderbook
    k_per_sec: 0.1
    q_per_sec: 0.000001
"#;
        let cfg = serde_yaml::from_str::<RunnerConfig>(yaml).expect("valid config");
        validate_runner_config(&cfg).expect("validation should pass");
    }

    #[test]
    fn pricing_model_validation_rejects_invalid_stats_alpha() {
        let yaml = r#"
strategy:
  venue: gate
  symbol: BTC_USDT
  size: 1
risk:
  max_order_notional: 10
  max_position_notional: 20
mode:
  dry_run: true
pricing_model:
  enabled: true
  kalman:
    ref_stream: gate:orderbook
    stats_alpha: 0.0
"#;
        let cfg = serde_yaml::from_str::<RunnerConfig>(yaml).expect("yaml parse");
        let err = validate_runner_config(&cfg).expect_err("validation should fail");
        assert!(
            err.to_string().contains("pricing_model.kalman.stats_alpha"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn mode_config_parses_lighter_sendtx_quota_log_suppression_flag() {
        let yaml = r#"
strategy:
  venue: lighter
  symbol: SOL_USDT
  size: 1
risk:
  max_order_notional: 10
  max_position_notional: 20
mode:
  dry_run: true
  suppress_quote_loop_idle_logs: true
  suppress_inventory_rollback_warnings: true
  suppress_lighter_sendtx_quota_logs: true
"#;
        let cfg = serde_yaml::from_str::<RunnerConfig>(yaml).expect("yaml parse");
        assert!(cfg.mode.suppress_quote_loop_idle_logs);
        assert!(cfg.mode.suppress_inventory_rollback_warnings);
        assert!(cfg.mode.suppress_lighter_sendtx_quota_logs);
    }
}

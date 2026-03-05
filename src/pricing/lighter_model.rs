use std::collections::{HashMap, VecDeque};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::Deserialize;

use crate::base_classes::state::{SNAPSHOT_DEPTH, TradeDirection};

const TARGET_EXCHANGE: &str = "lighter";
const TARGET_FEED: &str = "orderbook";

fn default_true() -> bool {
    true
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TimeBasis {
    Wire,
    Engine,
}

fn default_time_basis() -> TimeBasis {
    TimeBasis::Wire
}

fn default_ref_stream() -> String {
    format!("{}:{}", TARGET_EXCHANGE, TARGET_FEED)
}

fn default_k_per_sec() -> f64 {
    0.0
}

fn default_q_per_sec() -> f64 {
    1e-6
}

fn default_gamma_imb() -> f64 {
    0.0
}

fn default_stats_alpha() -> f64 {
    0.02
}

fn default_stats_warmup_obs() -> u64 {
    50
}

fn default_r_learn_alpha() -> f64 {
    0.02
}

fn default_r_learn_warmup_obs() -> u64 {
    200
}

fn default_r_floor() -> f64 {
    1e-10
}

fn default_r_ceiling() -> f64 {
    1e-2
}

fn default_r_clip_mult() -> f64 {
    25.0
}

fn default_r_state_flush_interval_s() -> u64 {
    30
}

fn default_r_state_flush_min_updates() -> u64 {
    200
}

fn default_trade_dir_bps() -> f64 {
    0.5
}

/// Online EWMA estimator for a single metric (mean and MAD).
#[derive(Debug, Clone)]
struct EwmaStats {
    mean: f64,
    mad: f64,
    n: u64,
}

impl Default for EwmaStats {
    fn default() -> Self {
        Self {
            mean: 0.0,
            mad: 1.0,
            n: 0,
        }
    }
}

impl EwmaStats {
    fn update(&mut self, value: f64, alpha: f64) {
        let abs_dev = (value - self.mean).abs();
        self.mean = (1.0 - alpha) * self.mean + alpha * value;
        self.mad = ((1.0 - alpha) * self.mad + alpha * abs_dev).max(1e-10);
        self.n += 1;
    }

    /// z = (value - mean) / mad, clamped to [0, 10]. Returns 0 during warmup.
    fn z_above(&self, value: f64, warmup: u64) -> f64 {
        if self.n < warmup {
            return 0.0;
        }
        ((value - self.mean) / self.mad.max(1e-10))
            .max(0.0)
            .min(10.0)
    }

    /// z = (mean - value) / mad, clamped to [0, 10]. Used for top_ratio where
    /// below-normal is suspicious (thin book). Returns 0 during warmup.
    fn z_below(&self, value: f64, warmup: u64) -> f64 {
        if self.n < warmup {
            return 0.0;
        }
        ((self.mean - value) / self.mad.max(1e-10))
            .max(0.0)
            .min(10.0)
    }
}

/// Per-stream online stats for all three adaptive-noise signals.
#[derive(Debug, Default, Clone)]
struct StreamOnlineStats {
    latency: EwmaStats,
    spread: EwmaStats,
    top_ratio: EwmaStats,
}

#[derive(Debug, Clone)]
struct StreamNoiseStats {
    r: f64,
    n: u64,
}

impl StreamNoiseStats {
    fn new(r: f64) -> Self {
        Self { r, n: 0 }
    }
}

const LEARNED_R_STATE_HEADER: &str = "learned_r_state_v1";

fn default_trade_r_mult() -> f64 {
    5.0
}

fn default_trade_size_beta() -> f64 {
    0.0
}

fn default_book_w_mid() -> f64 {
    0.6
}

fn default_book_w_micro() -> f64 {
    0.25
}

fn default_book_w_vwap5() -> f64 {
    0.15
}

fn default_lighter_r_mult() -> f64 {
    0.05
}

fn default_latency_alpha() -> f64 {
    0.5
}

fn default_stale_alpha() -> f64 {
    0.5
}

fn default_vol_alpha() -> f64 {
    0.0
}

fn default_vol_halflife_s() -> f64 {
    1.0
}

fn default_robust_z() -> f64 {
    6.0
}

fn default_jump_z() -> f64 {
    4.0
}

fn default_jump_beta() -> f64 {
    0.3
}

fn default_bias_ewma_halflife_s() -> f64 {
    2.0
}

fn default_horizon_ms() -> f64 {
    500.0
}

fn default_q_floor_mult() -> f64 {
    20.0
}

fn default_q_dt_floor_ms() -> f64 {
    1.0
}

fn default_eval_move_bps() -> f64 {
    1.0
}

fn default_blend_age0_ms() -> f64 {
    50.0
}

fn default_blend_age_scale_ms() -> f64 {
    25.0
}

fn default_blend_diff0_bps() -> f64 {
    5.0
}

fn default_blend_diff_scale_bps() -> f64 {
    2.0
}

fn default_blend_max_w() -> f64 {
    0.8
}

fn default_vel_halflife_s() -> f64 {
    0.2
}

fn default_vel_cap_bps_per_s() -> f64 {
    200.0
}

fn default_ecm_tau_ms() -> f64 {
    0.0
}

fn default_common_source() -> String {
    "median".to_string()
}

fn default_common_disp0_bps() -> f64 {
    3.0
}

fn default_common_max_age_ms() -> f64 {
    150.0
}

fn default_snap_diff0_bps() -> f64 {
    5.0
}

fn default_snap_diff_scale_bps() -> f64 {
    1.0
}

fn default_snap_disp_max_bps() -> f64 {
    1.5
}

fn default_snap_disp_scale_bps() -> f64 {
    1.0
}

fn default_snap_max_w() -> f64 {
    0.9
}

fn default_snap_age0_ms() -> f64 {
    20.0
}

fn default_snap_age_scale_ms() -> f64 {
    10.0
}

fn default_snap_min_n() -> i32 {
    3
}

fn default_side_age_tau_ms() -> f64 {
    75.0
}

fn default_dir_vscale_bps_per_s() -> f64 {
    50.0
}

fn default_quote_half_spread_floor_bps() -> f64 {
    0.5
}

fn default_quote_half_spread_cap_bps() -> f64 {
    50.0
}

fn default_quote_disp0_bps() -> f64 {
    1.0
}

fn default_quote_disp_mult() -> f64 {
    1.0
}

fn default_quote_age0_ms() -> f64 {
    20.0
}

fn default_quote_age_bps_per_100ms() -> f64 {
    0.5
}

fn default_quote_unc_mult() -> f64 {
    1.0
}

fn default_lighter_bias_halflife_s() -> f64 {
    1.0
}

fn default_lighter_bias_cap_bps() -> f64 {
    10.0
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct PricingModelConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_time_basis")]
    pub time_basis: TimeBasis,
    #[serde(default)]
    pub kalman: KalmanParamsConfig,
    #[serde(default)]
    pub tuning: FilterTuningConfig,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(default)]
#[serde(deny_unknown_fields)]
pub struct KalmanParamsConfig {
    pub mu_log: Option<f64>,
    #[serde(default = "default_k_per_sec")]
    pub k_per_sec: f64,
    #[serde(default = "default_q_per_sec")]
    pub q_per_sec: f64,
    #[serde(default = "default_gamma_imb")]
    pub gamma_imb: f64,
    #[serde(default = "default_ref_stream")]
    pub ref_stream: String,
    #[serde(default)]
    pub bias_by_stream: HashMap<String, f64>,
    /// EWMA alpha for the online per-stream stats estimator.
    /// ~0.02 gives a ~50-observation window. Tune slower (0.005) for stable
    /// environments, faster (0.05) for rapidly-changing network conditions.
    #[serde(default = "default_stats_alpha")]
    pub stats_alpha: f64,
    /// Minimum observations per stream before z-scores become active.
    /// During warmup the adaptive adjustments are silently disabled (z = 0).
    #[serde(default = "default_stats_warmup_obs")]
    pub stats_warmup_obs: u64,
    #[serde(default = "default_r_learn_alpha")]
    pub r_learn_alpha: f64,
    #[serde(default = "default_r_learn_warmup_obs")]
    pub r_learn_warmup_obs: u64,
    #[serde(default = "default_r_floor")]
    pub r_floor: f64,
    #[serde(default = "default_r_ceiling")]
    pub r_ceiling: f64,
    #[serde(default = "default_r_clip_mult")]
    pub r_clip_mult: f64,
    #[serde(default)]
    pub r_state_path: Option<String>,
    #[serde(default = "default_r_state_flush_interval_s")]
    pub r_state_flush_interval_s: u64,
    #[serde(default = "default_r_state_flush_min_updates")]
    pub r_state_flush_min_updates: u64,
}

impl Default for KalmanParamsConfig {
    fn default() -> Self {
        Self {
            mu_log: None,
            k_per_sec: default_k_per_sec(),
            q_per_sec: default_q_per_sec(),
            gamma_imb: default_gamma_imb(),
            ref_stream: default_ref_stream(),
            bias_by_stream: HashMap::new(),
            stats_alpha: default_stats_alpha(),
            stats_warmup_obs: default_stats_warmup_obs(),
            r_learn_alpha: default_r_learn_alpha(),
            r_learn_warmup_obs: default_r_learn_warmup_obs(),
            r_floor: default_r_floor(),
            r_ceiling: default_r_ceiling(),
            r_clip_mult: default_r_clip_mult(),
            r_state_path: None,
            r_state_flush_interval_s: default_r_state_flush_interval_s(),
            r_state_flush_min_updates: default_r_state_flush_min_updates(),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct FilterTuningConfig {
    #[serde(default = "default_trade_dir_bps")]
    pub trade_dir_bps: f64,
    #[serde(default = "default_trade_r_mult")]
    pub trade_r_mult: f64,
    #[serde(default = "default_trade_size_beta")]
    pub trade_size_beta: f64,
    #[serde(default = "default_book_w_mid")]
    pub book_w_mid: f64,
    #[serde(default = "default_book_w_micro")]
    pub book_w_micro: f64,
    #[serde(default = "default_book_w_vwap5")]
    pub book_w_vwap5: f64,
    #[serde(default = "default_lighter_r_mult")]
    pub lighter_r_mult: f64,
    #[serde(default = "default_latency_alpha")]
    pub latency_alpha: f64,
    #[serde(default = "default_stale_alpha")]
    pub stale_alpha: f64,
    #[serde(default = "default_vol_alpha")]
    pub vol_alpha: f64,
    #[serde(default = "default_vol_halflife_s")]
    pub vol_halflife_s: f64,
    #[serde(default = "default_robust_z")]
    pub robust_z: f64,
    #[serde(default = "default_jump_z")]
    pub jump_z: f64,
    #[serde(default = "default_jump_beta")]
    pub jump_beta: f64,
    #[serde(default = "default_bias_ewma_halflife_s")]
    pub bias_ewma_halflife_s: f64,
    #[serde(default = "default_horizon_ms")]
    pub horizon_ms: f64,
    #[serde(default = "default_q_floor_mult")]
    pub q_floor_mult: f64,
    #[serde(default = "default_q_dt_floor_ms")]
    pub q_dt_floor_ms: f64,
    #[serde(default = "default_eval_move_bps")]
    pub eval_move_bps: f64,
    #[serde(default = "default_blend_age0_ms")]
    pub blend_age0_ms: f64,
    #[serde(default = "default_blend_age_scale_ms")]
    pub blend_age_scale_ms: f64,
    #[serde(default = "default_blend_diff0_bps")]
    pub blend_diff0_bps: f64,
    #[serde(default = "default_blend_diff_scale_bps")]
    pub blend_diff_scale_bps: f64,
    #[serde(default = "default_blend_max_w")]
    pub blend_max_w: f64,
    #[serde(default = "default_vel_halflife_s")]
    pub vel_halflife_s: f64,
    #[serde(default = "default_vel_cap_bps_per_s")]
    pub vel_cap_bps_per_s: f64,
    #[serde(default = "default_ecm_tau_ms")]
    pub ecm_tau_ms: f64,
    #[serde(default = "default_common_source")]
    pub common_source: String,
    #[serde(default = "default_common_disp0_bps")]
    pub common_disp0_bps: f64,
    #[serde(default = "default_common_max_age_ms")]
    pub common_max_age_ms: f64,
    #[serde(default = "default_snap_diff0_bps")]
    pub snap_diff0_bps: f64,
    #[serde(default = "default_snap_diff_scale_bps")]
    pub snap_diff_scale_bps: f64,
    #[serde(default = "default_snap_disp_max_bps")]
    pub snap_disp_max_bps: f64,
    #[serde(default = "default_snap_disp_scale_bps")]
    pub snap_disp_scale_bps: f64,
    #[serde(default = "default_snap_max_w")]
    pub snap_max_w: f64,
    #[serde(default = "default_snap_age0_ms")]
    pub snap_age0_ms: f64,
    #[serde(default = "default_snap_age_scale_ms")]
    pub snap_age_scale_ms: f64,
    #[serde(default = "default_snap_min_n")]
    pub snap_min_n: i32,
    #[serde(default = "default_side_age_tau_ms")]
    pub side_age_tau_ms: f64,
    #[serde(default = "default_dir_vscale_bps_per_s")]
    pub dir_vscale_bps_per_s: f64,
    #[serde(default = "default_quote_half_spread_floor_bps")]
    pub quote_half_spread_floor_bps: f64,
    #[serde(default = "default_quote_half_spread_cap_bps")]
    pub quote_half_spread_cap_bps: f64,
    #[serde(default = "default_quote_disp0_bps")]
    pub quote_disp0_bps: f64,
    #[serde(default = "default_quote_disp_mult")]
    pub quote_disp_mult: f64,
    #[serde(default = "default_quote_age0_ms")]
    pub quote_age0_ms: f64,
    #[serde(default = "default_quote_age_bps_per_100ms")]
    pub quote_age_bps_per_100ms: f64,
    #[serde(default = "default_quote_unc_mult")]
    pub quote_unc_mult: f64,
    #[serde(default = "default_lighter_bias_halflife_s")]
    pub lighter_bias_halflife_s: f64,
    #[serde(default = "default_lighter_bias_cap_bps")]
    pub lighter_bias_cap_bps: f64,
}

impl Default for FilterTuningConfig {
    fn default() -> Self {
        Self {
            trade_dir_bps: default_trade_dir_bps(),
            trade_r_mult: default_trade_r_mult(),
            trade_size_beta: default_trade_size_beta(),
            book_w_mid: default_book_w_mid(),
            book_w_micro: default_book_w_micro(),
            book_w_vwap5: default_book_w_vwap5(),
            lighter_r_mult: default_lighter_r_mult(),
            latency_alpha: default_latency_alpha(),
            stale_alpha: default_stale_alpha(),
            vol_alpha: default_vol_alpha(),
            vol_halflife_s: default_vol_halflife_s(),
            robust_z: default_robust_z(),
            jump_z: default_jump_z(),
            jump_beta: default_jump_beta(),
            bias_ewma_halflife_s: default_bias_ewma_halflife_s(),
            horizon_ms: default_horizon_ms(),
            q_floor_mult: default_q_floor_mult(),
            q_dt_floor_ms: default_q_dt_floor_ms(),
            eval_move_bps: default_eval_move_bps(),
            blend_age0_ms: default_blend_age0_ms(),
            blend_age_scale_ms: default_blend_age_scale_ms(),
            blend_diff0_bps: default_blend_diff0_bps(),
            blend_diff_scale_bps: default_blend_diff_scale_bps(),
            blend_max_w: default_blend_max_w(),
            vel_halflife_s: default_vel_halflife_s(),
            vel_cap_bps_per_s: default_vel_cap_bps_per_s(),
            ecm_tau_ms: default_ecm_tau_ms(),
            common_source: default_common_source(),
            common_disp0_bps: default_common_disp0_bps(),
            common_max_age_ms: default_common_max_age_ms(),
            snap_diff0_bps: default_snap_diff0_bps(),
            snap_diff_scale_bps: default_snap_diff_scale_bps(),
            snap_disp_max_bps: default_snap_disp_max_bps(),
            snap_disp_scale_bps: default_snap_disp_scale_bps(),
            snap_max_w: default_snap_max_w(),
            snap_age0_ms: default_snap_age0_ms(),
            snap_age_scale_ms: default_snap_age_scale_ms(),
            snap_min_n: default_snap_min_n(),
            side_age_tau_ms: default_side_age_tau_ms(),
            dir_vscale_bps_per_s: default_dir_vscale_bps_per_s(),
            quote_half_spread_floor_bps: default_quote_half_spread_floor_bps(),
            quote_half_spread_cap_bps: default_quote_half_spread_cap_bps(),
            quote_disp0_bps: default_quote_disp0_bps(),
            quote_disp_mult: default_quote_disp_mult(),
            quote_age0_ms: default_quote_age0_ms(),
            quote_age_bps_per_100ms: default_quote_age_bps_per_100ms(),
            quote_unc_mult: default_quote_unc_mult(),
            lighter_bias_halflife_s: default_lighter_bias_halflife_s(),
            lighter_bias_cap_bps: default_lighter_bias_cap_bps(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PricingObservation {
    pub exchange: String,
    pub feed: String,
    pub price: f64,
    pub bid_levels: [Option<(f64, f64)>; SNAPSHOT_DEPTH],
    pub ask_levels: [Option<(f64, f64)>; SNAPSHOT_DEPTH],
    pub wire_ts_ns: Option<u64>,
    pub source_engine_ts_ns: Option<u64>,
    pub source_system_ts_ns: Option<u64>,
    pub direction: Option<TradeDirection>,
    pub size: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct ModelOutput {
    pub fair_mid: f64,
    pub quote_bid: Option<f64>,
    pub quote_ask: Option<f64>,
    pub source_stream: String,
}

#[derive(Debug)]
pub struct LighterPricingModel {
    cfg: PricingModelConfig,
    mu_log: Option<f64>,
    biases: HashMap<String, f64>,
    ref_stream: String,
    default_r: f64,
    learned_r_by_stream: HashMap<String, StreamNoiseStats>,
    r_state_path: Option<PathBuf>,
    r_state_flush_interval: Duration,
    r_state_flush_min_updates: u64,
    r_state_last_flush: Instant,
    r_updates_since_flush: u64,
    dropped_metric_samples: u64,
    start_ns: Option<i64>,
    last_t: f64,
    last_z: f64,
    vol2: f64,
    x: f64,
    p: f64,
    x_c: f64,
    p_c: f64,
    v_c: f64,
    last_common_t: f64,
    last_common_x: f64,
    last_lighter_px: f64,
    last_lighter_t: f64,
    last_lighter_bid: f64,
    last_lighter_ask: f64,
    last_lighter_spread_bps: f64,
    last_lighter_bid_change_t: f64,
    last_lighter_ask_change_t: f64,
    lighter_basis_log: f64,
    last_basis_t: f64,
    last_px_by_exch: HashMap<String, f64>,
    last_t_by_exch: HashMap<String, f64>,
    spread_samples: VecDeque<f64>,
    base_half_spread_bps: Option<f64>,
    online_stats: HashMap<String, StreamOnlineStats>,
}

impl LighterPricingModel {
    pub fn new(cfg: PricingModelConfig) -> Self {
        let r_floor = cfg.kalman.r_floor;
        let r_ceiling = cfg.kalman.r_ceiling;
        let default_r = (10.0 * cfg.kalman.q_per_sec.max(r_floor)).clamp(r_floor, r_ceiling);
        let r_state_path = cfg.kalman.r_state_path.as_ref().map(PathBuf::from);
        let r_state_flush_interval = Duration::from_secs(cfg.kalman.r_state_flush_interval_s.max(1));
        let mut model = Self {
            mu_log: cfg.kalman.mu_log,
            biases: cfg.kalman.bias_by_stream.clone(),
            ref_stream: cfg.kalman.ref_stream.clone(),
            default_r,
            learned_r_by_stream: HashMap::new(),
            r_state_path,
            r_state_flush_interval,
            r_state_flush_min_updates: cfg.kalman.r_state_flush_min_updates.max(1),
            r_state_last_flush: Instant::now(),
            r_updates_since_flush: 0,
            dropped_metric_samples: 0,
            cfg,
            start_ns: None,
            last_t: 0.0,
            last_z: 0.0,
            vol2: 1e-8,
            x: 0.0,
            p: 1.0,
            x_c: 0.0,
            p_c: 1.0,
            v_c: 0.0,
            last_common_t: 0.0,
            last_common_x: 0.0,
            last_lighter_px: f64::NAN,
            last_lighter_t: f64::NAN,
            last_lighter_bid: f64::NAN,
            last_lighter_ask: f64::NAN,
            last_lighter_spread_bps: f64::NAN,
            last_lighter_bid_change_t: f64::NAN,
            last_lighter_ask_change_t: f64::NAN,
            lighter_basis_log: 0.0,
            last_basis_t: 0.0,
            last_px_by_exch: HashMap::new(),
            last_t_by_exch: HashMap::new(),
            spread_samples: VecDeque::new(),
            base_half_spread_bps: None,
            online_stats: HashMap::new(),
        };
        model.load_learned_r_state();
        model
    }

    pub fn update(&mut self, obs: &PricingObservation) -> Option<ModelOutput> {
        let stream = format!("{}:{}", obs.exchange, obs.feed);
        let is_trade = obs.feed == "trade";
        let is_book = obs.feed == "orderbook" || obs.feed == "bbo";
        let is_target_obs = obs.exchange == TARGET_EXCHANGE && obs.feed == TARGET_FEED;

        let (wire_ts, engine_ts) = self.resolve_timestamps(obs);
        let t_ns = match self.cfg.time_basis {
            TimeBasis::Wire => wire_ts.or(engine_ts),
            TimeBasis::Engine => engine_ts.or(wire_ts),
        }?;

        let t_ns_i64 = match i64::try_from(t_ns) {
            Ok(v) => v,
            Err(_) => {
                eprintln!("ERROR: timestamp overflow in pricing model: {}", t_ns);
                return None;
            }
        };
        let t_sec = self.to_seconds(t_ns_i64);

        let (mid, micro, vwap5) = self.book_prices(obs);
        let book_obs = self.combine_book_prices(mid, micro, vwap5);
        let trade_obs = self.trade_observation(obs);
        let obs_price = if is_trade { trade_obs } else { book_obs };
        if !obs_price.is_finite() || obs_price <= 0.0 {
            return None;
        }

        if self.mu_log.is_none() {
            self.mu_log = Some(obs_price.ln());
            self.x = 0.0;
            self.p = 1.0;
            self.x_c = 0.0;
            self.p_c = 1.0;
            self.last_z = 0.0;
            self.vol2 = 1e-8;
            self.last_t = t_sec;
            self.last_common_t = t_sec;
            self.last_common_x = 0.0;
            self.last_basis_t = t_sec;
        }

        let mu_log = self.mu_log.unwrap_or(0.0);
        let z_obs = (obs_price.ln() - mu_log) as f64;
        let (bid_px, ask_px, _bid_sz, _ask_sz) = self.top_of_book(obs);
        let latency_us = self.latency_us(wire_ts, engine_ts);
        let spread_bps = self.spread_bps(mid, bid_px, ask_px);
        let (_bid_depth, _ask_depth, top_ratio, imbalance) = self.depth_metrics(obs);

        self.update_online_stats(&stream, latency_us, spread_bps, top_ratio, is_book);

        let dt = (t_sec - self.last_t).max(0.0);
        // Use configured drift/reversion directly; validation happens at config load.
        let k = self.cfg.kalman.k_per_sec;
        let q_per_sec = self.cfg.kalman.q_per_sec.max(0.0);
        let phi = if dt > 0.0 { (k * dt).exp() } else { 1.0 };
        let q = if dt > 0.0 { q_per_sec * dt } else { 0.0 };

        self.x = phi * self.x;
        self.p = (phi * phi) * self.p + q;
        self.x_c = phi * self.x_c;
        self.p_c = (phi * phi) * self.p_c + q;
        let x_prior = self.x;
        let p_prior = self.p.max(0.0);

        if z_obs.is_finite() && dt > 0.0 {
            let dz = z_obs - self.last_z;
            let dt_floor = (self.cfg.tuning.q_dt_floor_ms / 1000.0).max(1e-6);
            let dt_eff = dt.max(dt_floor);
            let dz_dt = dz / dt_eff;
            if self.cfg.tuning.vol_halflife_s > 0.0 {
                let lam = 1.0 - (-dt * (2.0f64.ln()) / self.cfg.tuning.vol_halflife_s).exp();
                self.vol2 = (1.0 - lam) * self.vol2 + lam * (dz_dt * dz_dt);
            }
            self.last_z = z_obs;
        }

        let gamma_imb = self.cfg.kalman.gamma_imb;
        let imb_term = if is_book { gamma_imb * imbalance } else { 0.0 };
        let mut y_raw = z_obs + imb_term;
        if !y_raw.is_finite() {
            y_raw = self.last_z;
        }

        let mut b = *self.biases.get(&stream).unwrap_or(&0.0);
        if self.cfg.tuning.bias_ewma_halflife_s > 0.0 && stream != self.ref_stream && dt > 0.0 {
            let lam = 1.0 - (-dt * (2.0f64.ln()) / self.cfg.tuning.bias_ewma_halflife_s).exp();
            b = b + lam * ((y_raw - self.x) - b);
            self.biases.insert(stream.clone(), b);
        }

        let mut r_eff = self.current_base_r(&stream);
        let is_ref = stream == self.ref_stream;
        if is_ref {
            r_eff = (r_eff * self.cfg.tuning.lighter_r_mult.max(1e-6)).max(1e-12);
        }
        if is_trade {
            r_eff *= self.cfg.tuning.trade_r_mult.max(1.0);
            let sz = obs.size.unwrap_or(0.0);
            if sz > 0.0 && self.cfg.tuning.trade_size_beta > 0.0 {
                r_eff /= 1.0 + self.cfg.tuning.trade_size_beta * sz;
            }
        }

        let mut z_lat = 0.0;
        if !is_ref && self.cfg.tuning.latency_alpha > 0.0 {
            if let Some(lat) = latency_us {
                z_lat = self
                    .online_stats
                    .get(&stream)
                    .map(|s| s.latency.z_above(lat, self.cfg.kalman.stats_warmup_obs))
                    .unwrap_or(0.0);
                r_eff *= 1.0 + self.cfg.tuning.latency_alpha * z_lat;
            }
        }

        let mut stale_score = 0.0;
        if !is_ref && self.cfg.tuning.stale_alpha > 0.0 && is_book {
            let warmup = self.cfg.kalman.stats_warmup_obs;
            let stream_stats = self.online_stats.get(&stream);
            let z_sp = if spread_bps.is_finite() {
                stream_stats
                    .map(|s| s.spread.z_above(spread_bps, warmup))
                    .unwrap_or(0.0)
            } else {
                0.0
            };
            let z_top = if top_ratio.is_finite() {
                stream_stats
                    .map(|s| s.top_ratio.z_below(top_ratio, warmup))
                    .unwrap_or(0.0)
            } else {
                0.0
            };
            stale_score = z_sp + z_top;
            r_eff *= 1.0 + self.cfg.tuning.stale_alpha * stale_score;
        }

        if !is_ref && self.cfg.tuning.vol_alpha > 0.0 {
            let vol_bps_per_s = 1e4 * self.vol2.max(0.0).sqrt();
            let mut vol_scale = 1.0 + self.cfg.tuning.vol_alpha * (vol_bps_per_s / 100.0);
            if vol_scale > 10.0 {
                vol_scale = 10.0;
            }
            r_eff *= vol_scale * vol_scale;
        }

        let y = y_raw - b;
        let innovation = y - x_prior;
        let mut s_var = self.p + r_eff;
        let mut std = s_var.max(1e-12).sqrt();
        let mut _nu = (y - self.x) / std;
        let suspicious = !is_ref && (is_trade || z_lat >= 2.0 || stale_score >= 2.0);
        if suspicious && self.cfg.tuning.robust_z > 0.0 && _nu.abs() > self.cfg.tuning.robust_z {
            let scale = (_nu.abs() / self.cfg.tuning.robust_z).powi(2);
            r_eff *= scale;
            s_var = self.p + r_eff;
            std = s_var.max(1e-12).sqrt();
            _nu = (y - self.x) / std;
        } else if !suspicious
            && self.cfg.tuning.jump_z > 0.0
            && _nu.abs() > self.cfg.tuning.jump_z
            && self.cfg.tuning.jump_beta > 0.0
        {
            let jump = self.cfg.tuning.jump_beta * (y - self.x) * (y - self.x);
            self.p += jump;
            s_var = self.p + r_eff;
            std = s_var.max(1e-12).sqrt();
            _nu = (y - self.x) / std;
        }

        let k_gain = self.p / (self.p + r_eff);
        self.x = self.x + k_gain * (y - self.x);
        self.p = (1.0 - k_gain) * self.p;

        if stream != self.ref_stream {
            let k_gain_c = self.p_c / (self.p_c + r_eff);
            self.x_c = self.x_c + k_gain_c * (y - self.x_c);
            self.p_c = (1.0 - k_gain_c) * self.p_c;
            let dt_c = (t_sec - self.last_common_t).max(0.0);
            if dt_c > 1e-6 && self.cfg.tuning.vel_halflife_s > 0.0 {
                let inst_v = (self.x_c - self.last_common_x) / dt_c;
                let lam_v = 1.0 - (-dt_c * (2.0f64.ln()) / self.cfg.tuning.vel_halflife_s).exp();
                self.v_c = (1.0 - lam_v) * self.v_c + lam_v * inst_v;
                self.last_common_x = self.x_c;
                self.last_common_t = t_sec;
            }
        }
        self.update_stream_r(&stream, innovation, p_prior);
        self.maybe_persist_learned_r_state();

        if obs.exchange != TARGET_EXCHANGE && is_book && obs_price.is_finite() && obs_price > 0.0 {
            self.last_px_by_exch.insert(obs.exchange.clone(), obs_price);
            self.last_t_by_exch.insert(obs.exchange.clone(), t_sec);
        }

        let (common_median, common_mad_bps, common_n) = self.common_median(t_sec);
        let common_now = (mu_log + self.x_c).exp();
        let v_eff = self.v_c.clamp(
            -self.cfg.tuning.vel_cap_bps_per_s * 1e-4,
            self.cfg.tuning.vel_cap_bps_per_s * 1e-4,
        );

        let quote_age_ms = if self.last_lighter_t.is_finite() {
            ((t_sec - self.last_lighter_t).max(0.0)) * 1000.0
        } else {
            f64::INFINITY
        };

        let pred_mid_post = (mu_log + self.x).exp();

        let mut anchor_common = common_now;
        if common_median.is_finite()
            && common_median > 0.0
            && common_n >= self.cfg.tuning.snap_min_n
            && common_mad_bps.is_finite()
        {
            let w_disp = sigmoid(
                (self.cfg.tuning.snap_disp_max_bps - common_mad_bps)
                    / self.cfg.tuning.snap_disp_scale_bps.max(1e-6),
            );
            anchor_common =
                (anchor_common.ln() + w_disp * (common_median.ln() - anchor_common.ln())).exp();
        }

        let mut _nowcast_mid_pre_target = None;
        let mut lighter_w_lighter = 0.0;
        if is_target_obs {
            let (age_bid_pre, age_ask_pre) = self.side_ages(t_sec);
            let (fair_pre, _w_bid_pre, _w_ask_pre, _p_up) =
                self.compute_lighter_fair(v_eff, age_bid_pre, age_ask_pre);
            if fair_pre.is_finite()
                && fair_pre > 0.0
                && anchor_common.is_finite()
                && anchor_common > 0.0
            {
                let spread_now = self.last_lighter_spread_bps;
                let (z_sp, ok_basis) =
                    self.basis_health(spread_now, age_bid_pre, age_ask_pre, common_n);
                let dt_b = (t_sec - self.last_basis_t).max(0.0);
                if ok_basis && self.cfg.tuning.lighter_bias_halflife_s > 0.0 && dt_b > 0.0 {
                    let lam = 1.0
                        - (-dt_b * (2.0f64.ln()) / self.cfg.tuning.lighter_bias_halflife_s).exp();
                    let target = fair_pre.ln() - anchor_common.ln();
                    let cap_bias = self.cfg.tuning.lighter_bias_cap_bps * 1e-4;
                    self.lighter_basis_log = ((1.0 - lam) * self.lighter_basis_log + lam * target)
                        .clamp(-cap_bias, cap_bias);
                    self.last_basis_t = t_sec;
                }

                let anchor_on_lighter = (anchor_common.ln() + self.lighter_basis_log).exp();
                let w_q = sigmoid(
                    (self.cfg.tuning.snap_age0_ms - quote_age_ms)
                        / self.cfg.tuning.snap_age_scale_ms.max(1e-6),
                );
                let g_sp = (-0.5 * z_sp.max(0.0)).exp();
                let f_side = side_fresh(age_bid_pre, self.cfg.tuning.side_age_tau_ms)
                    .max(side_fresh(age_ask_pre, self.cfg.tuning.side_age_tau_ms));
                let w_l = (self.cfg.tuning.snap_max_w * w_q * g_sp * f_side).clamp(0.0, 1.0);
                _nowcast_mid_pre_target = Some(
                    (anchor_on_lighter.ln() + w_l * (fair_pre.ln() - anchor_on_lighter.ln())).exp(),
                );
                lighter_w_lighter = w_l;
            }
        }

        let (bid_px_now, ask_px_now, age_bid_now, age_ask_now) =
            self.current_lighter_book(is_target_obs, bid_px, ask_px, t_sec);
        let (fair_now, _w_bid_now, _w_ask_now, _p_up_now) =
            self.compute_lighter_fair_with(bid_px_now, ask_px_now, v_eff, age_bid_now, age_ask_now);
        let mut nowcast_mid = pred_mid_post;
        if fair_now.is_finite()
            && fair_now > 0.0
            && anchor_common.is_finite()
            && anchor_common > 0.0
        {
            let spread_now = if is_target_obs && spread_bps.is_finite() {
                spread_bps
            } else {
                self.last_lighter_spread_bps
            };
            let (z_sp, _ok_basis) =
                self.basis_health(spread_now, age_bid_now, age_ask_now, common_n);
            let anchor_on_lighter = (anchor_common.ln() + self.lighter_basis_log).exp();
            let w_q = sigmoid(
                (self.cfg.tuning.snap_age0_ms - quote_age_ms)
                    / self.cfg.tuning.snap_age_scale_ms.max(1e-6),
            );
            let g_sp = (-0.5 * z_sp.max(0.0)).exp();
            let f_side = side_fresh(age_bid_now, self.cfg.tuning.side_age_tau_ms)
                .max(side_fresh(age_ask_now, self.cfg.tuning.side_age_tau_ms));
            let w_l = (self.cfg.tuning.snap_max_w * w_q * g_sp * f_side).clamp(0.0, 1.0);
            nowcast_mid =
                (anchor_on_lighter.ln() + w_l * (fair_now.ln() - anchor_on_lighter.ln())).exp();
            if lighter_w_lighter <= 0.0 {
                lighter_w_lighter = w_l;
            }
        }

        if is_target_obs && mid.is_finite() && mid > 0.0 {
            self.last_lighter_px = mid;
            self.last_lighter_t = t_sec;
            if bid_px.is_finite() && bid_px > 0.0 {
                if !self.last_lighter_bid.is_finite() || bid_px != self.last_lighter_bid {
                    self.last_lighter_bid_change_t = t_sec;
                }
                self.last_lighter_bid = bid_px;
            }
            if ask_px.is_finite() && ask_px > 0.0 {
                if !self.last_lighter_ask.is_finite() || ask_px != self.last_lighter_ask {
                    self.last_lighter_ask_change_t = t_sec;
                }
                self.last_lighter_ask = ask_px;
            }
            if spread_bps.is_finite() {
                self.last_lighter_spread_bps = spread_bps;
            }
            self.update_base_spread_sample(spread_bps, age_bid_now, age_ask_now);
        }

        self.last_t = t_sec;

        let (quote_bid, quote_ask) =
            self.quote_from_fair(nowcast_mid, lighter_w_lighter, common_mad_bps, quote_age_ms);

        Some(ModelOutput {
            fair_mid: nowcast_mid,
            quote_bid,
            quote_ask,
            source_stream: stream,
        })
    }

    fn resolve_timestamps(&self, obs: &PricingObservation) -> (Option<u64>, Option<u64>) {
        let wire_ts = obs.wire_ts_ns;
        let engine_ts = obs
            .source_engine_ts_ns
            .or(obs.source_system_ts_ns)
            .or(obs.wire_ts_ns);
        (wire_ts, engine_ts)
    }

    fn to_seconds(&mut self, t_ns: i64) -> f64 {
        if self.start_ns.is_none() {
            self.start_ns = Some(t_ns);
        }
        let start = self.start_ns.unwrap_or(t_ns);
        (t_ns - start) as f64 / 1e9
    }

    fn book_prices(&self, obs: &PricingObservation) -> (f64, f64, f64) {
        let (bid_px, ask_px, bid_sz, ask_sz) = self.top_of_book(obs);
        let mid = if obs.price.is_finite() && obs.price > 0.0 {
            obs.price
        } else if bid_px.is_finite() && ask_px.is_finite() && bid_px > 0.0 && ask_px > 0.0 {
            0.5 * (bid_px + ask_px)
        } else {
            f64::NAN
        };
        let micro = if bid_px.is_finite()
            && ask_px.is_finite()
            && bid_sz.is_finite()
            && ask_sz.is_finite()
            && (bid_sz + ask_sz) > 0.0
        {
            (bid_px * ask_sz + ask_px * bid_sz) / (bid_sz + ask_sz)
        } else {
            f64::NAN
        };
        let vwap5 = self.vwap_mid_top5(obs);
        (mid, micro, vwap5)
    }

    fn combine_book_prices(&self, mid: f64, micro: f64, vwap5: f64) -> f64 {
        let mut sum = 0.0;
        let mut w_sum = 0.0;
        let add = |sum: &mut f64, w_sum: &mut f64, v: f64, w: f64| {
            if v.is_finite() && v > 0.0 && w > 0.0 {
                *sum += v * w;
                *w_sum += w;
            }
        };
        add(&mut sum, &mut w_sum, mid, self.cfg.tuning.book_w_mid);
        add(&mut sum, &mut w_sum, micro, self.cfg.tuning.book_w_micro);
        add(&mut sum, &mut w_sum, vwap5, self.cfg.tuning.book_w_vwap5);
        if w_sum > 0.0 { sum / w_sum } else { f64::NAN }
    }

    fn trade_observation(&self, obs: &PricingObservation) -> f64 {
        if !obs.price.is_finite() || obs.price <= 0.0 {
            return f64::NAN;
        }
        let dir_sign = match obs.direction {
            Some(TradeDirection::Buy) => 1.0,
            Some(TradeDirection::Sell) => -1.0,
            None => 0.0,
        };
        let offset_log = dir_sign * (self.cfg.tuning.trade_dir_bps * 1e-4);
        obs.price * offset_log.exp()
    }

    fn top_of_book(&self, obs: &PricingObservation) -> (f64, f64, f64, f64) {
        let bid = obs.bid_levels[0].map(|lvl| lvl.0).unwrap_or(f64::NAN);
        let ask = obs.ask_levels[0].map(|lvl| lvl.0).unwrap_or(f64::NAN);
        let bid_sz = obs.bid_levels[0].map(|lvl| lvl.1).unwrap_or(f64::NAN);
        let ask_sz = obs.ask_levels[0].map(|lvl| lvl.1).unwrap_or(f64::NAN);
        (bid, ask, bid_sz, ask_sz)
    }

    fn vwap_mid_top5(&self, obs: &PricingObservation) -> f64 {
        let mut bid_px = Vec::new();
        let mut bid_sz = Vec::new();
        let mut ask_px = Vec::new();
        let mut ask_sz = Vec::new();
        for level in 0..SNAPSHOT_DEPTH {
            if let Some((px, sz)) = obs.bid_levels[level] {
                if px.is_finite() && sz.is_finite() && sz > 0.0 {
                    bid_px.push(px);
                    bid_sz.push(sz);
                }
            }
            if let Some((px, sz)) = obs.ask_levels[level] {
                if px.is_finite() && sz.is_finite() && sz > 0.0 {
                    ask_px.push(px);
                    ask_sz.push(sz);
                }
            }
        }
        let bid_sum: f64 = bid_sz.iter().sum();
        let ask_sum: f64 = ask_sz.iter().sum();
        if bid_sum <= 0.0 || ask_sum <= 0.0 {
            return f64::NAN;
        }
        let bid_vwap = bid_px
            .iter()
            .zip(bid_sz.iter())
            .map(|(p, s)| p * s)
            .sum::<f64>()
            / bid_sum;
        let ask_vwap = ask_px
            .iter()
            .zip(ask_sz.iter())
            .map(|(p, s)| p * s)
            .sum::<f64>()
            / ask_sum;
        if bid_vwap.is_finite() && ask_vwap.is_finite() {
            0.5 * (bid_vwap + ask_vwap)
        } else {
            f64::NAN
        }
    }

    fn latency_us(&self, wire_ts: Option<u64>, engine_ts: Option<u64>) -> Option<f64> {
        let wire = wire_ts? as f64;
        let engine = engine_ts? as f64;
        Some((wire - engine) / 1e3)
    }

    fn spread_bps(&self, mid: f64, bid: f64, ask: f64) -> f64 {
        if mid.is_finite() && bid.is_finite() && ask.is_finite() && mid > 0.0 {
            ((ask - bid) / mid) * 1e4
        } else {
            f64::NAN
        }
    }

    fn depth_metrics(&self, obs: &PricingObservation) -> (f64, f64, f64, f64) {
        let mut bid_depth = 0.0;
        let mut ask_depth = 0.0;
        for level in 0..SNAPSHOT_DEPTH {
            if let Some((_px, sz)) = obs.bid_levels[level] {
                if sz.is_finite() && sz > 0.0 {
                    bid_depth += sz;
                }
            }
            if let Some((_px, sz)) = obs.ask_levels[level] {
                if sz.is_finite() && sz > 0.0 {
                    ask_depth += sz;
                }
            }
        }
        let depth = bid_depth + ask_depth;
        let top_sz = obs.bid_levels[0].map(|lvl| lvl.1).unwrap_or(0.0)
            + obs.ask_levels[0].map(|lvl| lvl.1).unwrap_or(0.0);
        let imbalance = if depth > 0.0 {
            (bid_depth - ask_depth) / depth
        } else {
            f64::NAN
        };
        let top_ratio = if depth > 0.0 {
            top_sz / depth
        } else {
            f64::NAN
        };
        (bid_depth, ask_depth, top_ratio, imbalance)
    }

    fn common_median(&self, t_sec: f64) -> (f64, f64, i32) {
        let max_age_ms = self.cfg.tuning.common_max_age_ms;
        let mut vals = Vec::new();
        for (ex, px) in &self.last_px_by_exch {
            if !px.is_finite() || *px <= 0.0 {
                continue;
            }
            if let Some(t_last) = self.last_t_by_exch.get(ex) {
                if t_last.is_finite() {
                    if max_age_ms > 0.0 {
                        let age_ms = (t_sec - t_last) * 1000.0;
                        if age_ms > max_age_ms {
                            continue;
                        }
                    }
                    vals.push(*px);
                }
            }
        }
        if vals.is_empty() {
            return (f64::NAN, f64::NAN, 0);
        }
        vals.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let med = vals[vals.len() / 2];
        let mut mad_bps = f64::NAN;
        if vals.len() >= 3 && med > 0.0 {
            let mut devs: Vec<f64> = vals.iter().map(|v| (v - med).abs()).collect();
            devs.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            let mad = devs[devs.len() / 2];
            mad_bps = (mad / med) * 1e4;
        }
        (med, mad_bps, vals.len() as i32)
    }

    fn side_ages(&self, t_sec: f64) -> (f64, f64) {
        let age_bid = if self.last_lighter_bid_change_t.is_finite() {
            ((t_sec - self.last_lighter_bid_change_t).max(0.0)) * 1000.0
        } else {
            f64::INFINITY
        };
        let age_ask = if self.last_lighter_ask_change_t.is_finite() {
            ((t_sec - self.last_lighter_ask_change_t).max(0.0)) * 1000.0
        } else {
            f64::INFINITY
        };
        (age_bid, age_ask)
    }

    fn compute_lighter_fair(
        &self,
        v_log_per_s: f64,
        age_bid_ms: f64,
        age_ask_ms: f64,
    ) -> (f64, f64, f64, f64) {
        self.compute_lighter_fair_with(
            self.last_lighter_bid,
            self.last_lighter_ask,
            v_log_per_s,
            age_bid_ms,
            age_ask_ms,
        )
    }

    fn compute_lighter_fair_with(
        &self,
        bid_px: f64,
        ask_px: f64,
        v_log_per_s: f64,
        age_bid_ms: f64,
        age_ask_ms: f64,
    ) -> (f64, f64, f64, f64) {
        if !(bid_px.is_finite() && bid_px > 0.0) && !(ask_px.is_finite() && ask_px > 0.0) {
            return (f64::NAN, f64::NAN, f64::NAN, 0.5);
        }

        let v_bps_per_s = v_log_per_s * 1e4;
        let p_up = sigmoid(v_bps_per_s / self.cfg.tuning.dir_vscale_bps_per_s.max(1e-6));
        let f_bid = if bid_px.is_finite() && bid_px > 0.0 {
            side_fresh(age_bid_ms, self.cfg.tuning.side_age_tau_ms)
        } else {
            0.0
        };
        let f_ask = if ask_px.is_finite() && ask_px > 0.0 {
            side_fresh(age_ask_ms, self.cfg.tuning.side_age_tau_ms)
        } else {
            0.0
        };
        let w_bid_raw = (1.0 - p_up) * f_bid;
        let w_ask_raw = p_up * f_ask;
        let w_sum = w_bid_raw + w_ask_raw;
        let (w_bid, w_ask) = if w_sum > 0.0 {
            (w_bid_raw / w_sum, w_ask_raw / w_sum)
        } else {
            let mut wb = if bid_px.is_finite() && bid_px > 0.0 {
                1.0
            } else {
                0.0
            };
            let mut wa = if ask_px.is_finite() && ask_px > 0.0 {
                1.0
            } else {
                0.0
            };
            let wsum2 = wb + wa;
            if wsum2 > 0.0 {
                wb /= wsum2;
                wa /= wsum2;
                (wb, wa)
            } else {
                (f64::NAN, f64::NAN)
            }
        };

        let spread_med = self
            .online_stats
            .get(&self.ref_stream)
            .filter(|s| s.spread.n >= self.cfg.kalman.stats_warmup_obs)
            .map(|s| s.spread.mean)
            .unwrap_or(0.0)
            .max(0.0);
        let delta = 0.5 * spread_med * 1e-4;
        let log_bid_fair = if bid_px.is_finite() && bid_px > 0.0 {
            bid_px.ln() + delta
        } else {
            f64::NAN
        };
        let log_ask_fair = if ask_px.is_finite() && ask_px > 0.0 {
            ask_px.ln() - delta
        } else {
            f64::NAN
        };
        if log_bid_fair.is_finite() && log_ask_fair.is_finite() {
            (
                (w_bid * log_bid_fair + w_ask * log_ask_fair).exp(),
                w_bid,
                w_ask,
                p_up,
            )
        } else if log_bid_fair.is_finite() {
            (log_bid_fair.exp(), 1.0, 0.0, p_up)
        } else if log_ask_fair.is_finite() {
            (log_ask_fair.exp(), 0.0, 1.0, p_up)
        } else {
            (f64::NAN, f64::NAN, f64::NAN, p_up)
        }
    }

    fn basis_health(
        &self,
        spread_now: f64,
        age_bid: f64,
        age_ask: f64,
        common_n: i32,
    ) -> (f64, bool) {
        let warmup = self.cfg.kalman.stats_warmup_obs;
        let ref_stats = self.online_stats.get(&self.ref_stream);
        let spread_med = ref_stats
            .filter(|s| s.spread.n >= warmup)
            .map(|s| s.spread.mean)
            .unwrap_or(0.0);
        let spread_mad = ref_stats
            .filter(|s| s.spread.n >= warmup)
            .map(|s| s.spread.mad.max(1e-6))
            .unwrap_or(1.0);
        let z_sp = if spread_now.is_finite() {
            ((spread_now - spread_med) / spread_mad).max(0.0)
        } else {
            0.0
        };
        let ok_basis = common_n >= self.cfg.tuning.snap_min_n
            && z_sp <= 2.0
            && age_bid <= 2.0 * self.cfg.tuning.side_age_tau_ms
            && age_ask <= 2.0 * self.cfg.tuning.side_age_tau_ms;
        (z_sp, ok_basis)
    }

    fn current_lighter_book(
        &self,
        is_target_obs: bool,
        bid_px: f64,
        ask_px: f64,
        t_sec: f64,
    ) -> (f64, f64, f64, f64) {
        let mut bid_px_now = self.last_lighter_bid;
        let mut ask_px_now = self.last_lighter_ask;
        let mut age_bid_now = if self.last_lighter_bid_change_t.is_finite() {
            ((t_sec - self.last_lighter_bid_change_t).max(0.0)) * 1000.0
        } else {
            f64::INFINITY
        };
        let mut age_ask_now = if self.last_lighter_ask_change_t.is_finite() {
            ((t_sec - self.last_lighter_ask_change_t).max(0.0)) * 1000.0
        } else {
            f64::INFINITY
        };
        if is_target_obs {
            if bid_px.is_finite() && bid_px > 0.0 {
                bid_px_now = bid_px;
                if !self.last_lighter_bid.is_finite() || bid_px != self.last_lighter_bid {
                    age_bid_now = 0.0;
                }
            }
            if ask_px.is_finite() && ask_px > 0.0 {
                ask_px_now = ask_px;
                if !self.last_lighter_ask.is_finite() || ask_px != self.last_lighter_ask {
                    age_ask_now = 0.0;
                }
            }
        }
        (bid_px_now, ask_px_now, age_bid_now, age_ask_now)
    }

    fn update_base_spread_sample(&mut self, spread_bps: f64, age_bid_ms: f64, age_ask_ms: f64) {
        if !spread_bps.is_finite() || spread_bps <= 0.0 {
            return;
        }
        if age_bid_ms > 50.0 || age_ask_ms > 50.0 {
            return;
        }
        self.spread_samples.push_back(spread_bps);
        while self.spread_samples.len() > 200 {
            self.spread_samples.pop_front();
        }
        if self.spread_samples.len() >= 50 {
            let mut vals: Vec<f64> = self.spread_samples.iter().copied().collect();
            vals.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            let med = vals[vals.len() / 2];
            if med.is_finite() && med > 0.0 {
                self.base_half_spread_bps = Some(0.5 * med);
            }
        }
    }

    fn quote_from_fair(
        &self,
        fair: f64,
        w_lighter: f64,
        disp_bps: f64,
        age_ms: f64,
    ) -> (Option<f64>, Option<f64>) {
        if !fair.is_finite() || fair <= 0.0 {
            return (None, None);
        }
        let base = self.base_half_spread_bps.unwrap_or(0.5);
        let floor = self.cfg.tuning.quote_half_spread_floor_bps.max(0.0);
        let cap = self.cfg.tuning.quote_half_spread_cap_bps.max(floor);
        let base_hs = base.max(floor);
        // Treat missing dispersion/age as "no extra widening" rather than forcing max spread.
        // This prevents quote blowouts when common dispersion can't be computed (e.g., too few fresh venues).
        let disp0 = self.cfg.tuning.quote_disp0_bps.max(0.0);
        let disp = if disp_bps.is_finite() {
            disp_bps.max(0.0)
        } else {
            disp0
        };
        let age0 = self.cfg.tuning.quote_age0_ms.max(0.0);
        let age = if age_ms.is_finite() {
            age_ms.max(0.0)
        } else {
            age0
        };
        let w = w_lighter.clamp(0.0, 1.0);
        let disp_mult = self.cfg.tuning.quote_disp_mult.max(0.0);
        let age_mult = self.cfg.tuning.quote_age_bps_per_100ms.max(0.0);
        let unc_mult = self.cfg.tuning.quote_unc_mult.max(0.0);
        let widen_disp = disp_mult * (disp - disp0).max(0.0);
        let widen_age = age_mult * ((age - age0).max(0.0) / 100.0);
        let mut hs = base_hs * (1.0 + unc_mult * (1.0 - w)) + widen_disp + widen_age;
        if hs < floor {
            hs = floor;
        }
        if hs > cap {
            hs = cap;
        }
        let log_f = fair.ln();
        let d = hs * 1e-4;
        let bid = (log_f - d).exp();
        let ask = (log_f + d).exp();
        (Some(bid), Some(ask))
    }

    fn update_online_stats(
        &mut self,
        stream: &str,
        latency_us: Option<f64>,
        spread_bps: f64,
        top_ratio: f64,
        is_book: bool,
    ) {
        let alpha = self.cfg.kalman.stats_alpha;
        if let Some(lat) = latency_us {
            if lat.is_finite() && lat >= 0.0 {
                self.online_stats
                    .entry(stream.to_string())
                    .or_default()
                    .latency
                    .update(lat, alpha);
            } else {
                self.warn_invalid_metric_sample(stream, "latency_us", lat);
            }
        }
        if is_book {
            if spread_bps.is_finite() && spread_bps >= 0.0 {
                self.online_stats
                    .entry(stream.to_string())
                    .or_default()
                    .spread
                    .update(spread_bps, alpha);
            } else {
                self.warn_invalid_metric_sample(stream, "spread_bps", spread_bps);
            }
            if top_ratio.is_finite() && top_ratio >= 0.0 {
                self.online_stats
                    .entry(stream.to_string())
                    .or_default()
                    .top_ratio
                    .update(top_ratio, alpha);
            } else {
                self.warn_invalid_metric_sample(stream, "top_ratio", top_ratio);
            }
        }
    }

    fn current_base_r(&self, stream: &str) -> f64 {
        self.learned_r_by_stream
            .get(stream)
            .map(|stats| stats.r)
            .unwrap_or(self.default_r)
            .clamp(self.cfg.kalman.r_floor, self.cfg.kalman.r_ceiling)
    }

    fn update_stream_r(&mut self, stream: &str, innovation: f64, p_prior: f64) {
        if !innovation.is_finite() || !p_prior.is_finite() || p_prior < 0.0 {
            self.warn_invalid_metric_sample(stream, "innovation_or_p_prior", innovation);
            return;
        }
        let raw_sample = innovation * innovation - p_prior;
        if !raw_sample.is_finite() {
            self.warn_invalid_metric_sample(stream, "r_sample", raw_sample);
            return;
        }
        let r_floor = self.cfg.kalman.r_floor;
        let r_ceiling = self.cfg.kalman.r_ceiling;
        let r_clip_mult = self.cfg.kalman.r_clip_mult;
        let clipped_sample = raw_sample.clamp(r_floor, r_ceiling);
        let stats = self
            .learned_r_by_stream
            .entry(stream.to_string())
            .or_insert_with(|| StreamNoiseStats::new(self.default_r));
        let low_clip = (stats.r / r_clip_mult).max(r_floor);
        let high_clip = (stats.r * r_clip_mult).min(r_ceiling);
        let bounded_sample = clipped_sample.clamp(low_clip, high_clip);
        stats.n += 1;
        let alpha = if stats.n <= self.cfg.kalman.r_learn_warmup_obs {
            self.cfg.kalman.r_learn_alpha.max(1.0 / stats.n as f64)
        } else {
            self.cfg.kalman.r_learn_alpha
        };
        stats.r = ((1.0 - alpha) * stats.r + alpha * bounded_sample).clamp(r_floor, r_ceiling);
        self.r_updates_since_flush = self.r_updates_since_flush.saturating_add(1);
    }

    fn load_learned_r_state(&mut self) {
        let Some(path) = self.r_state_path.as_ref() else {
            return;
        };
        if path.as_os_str().is_empty() {
            panic!("pricing_model.kalman.r_state_path resolved to an empty path");
        }
        if !path.exists() {
            eprintln!(
                "INFO: pricing model learned-r state file not found at {}; starting cold",
                path.display()
            );
            return;
        }
        if path.is_dir() {
            panic!(
                "pricing_model.kalman.r_state_path points to a directory, expected a file: {}",
                path.display()
            );
        }
        let file = File::open(path).unwrap_or_else(|err| {
            panic!(
                "failed to open pricing model learned-r state file {}: {}",
                path.display(),
                err
            )
        });
        let mut lines = BufReader::new(file).lines();
        let header_line = match lines.next() {
            Some(Ok(line)) => line,
            Some(Err(err)) => {
                panic!(
                    "failed reading pricing model learned-r state header from {}: {}",
                    path.display(),
                    err
                )
            }
            None => {
                panic!(
                    "pricing model learned-r state file is empty: {}",
                    path.display()
                )
            }
        };
        if !header_line.starts_with(&format!("# {}", LEARNED_R_STATE_HEADER)) {
            panic!(
                "invalid learned-r state header in {}: '{}'",
                path.display(),
                header_line
            );
        }
        let mut loaded = HashMap::new();
        for (idx, line_result) in lines.enumerate() {
            let line_no = idx + 2;
            let line = line_result.unwrap_or_else(|err| {
                panic!(
                    "failed reading learned-r state line {} from {}: {}",
                    line_no,
                    path.display(),
                    err
                )
            });
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            let (stream, raw_r) = trimmed.split_once(',').unwrap_or_else(|| {
                panic!(
                    "invalid learned-r line {} in {} (expected '<stream>,<r>'): {}",
                    line_no,
                    path.display(),
                    trimmed
                )
            });
            let stream = stream.trim();
            if stream.is_empty() || !stream.contains(':') {
                panic!(
                    "invalid stream key at line {} in {}: '{}'",
                    line_no,
                    path.display(),
                    stream
                );
            }
            let parsed_r: f64 = raw_r.trim().parse().unwrap_or_else(|err| {
                panic!(
                    "invalid r value at line {} in {}: {} ({})",
                    line_no,
                    path.display(),
                    raw_r.trim(),
                    err
                )
            });
            if !parsed_r.is_finite() || parsed_r <= 0.0 {
                panic!(
                    "invalid non-positive r at line {} in {}: {}",
                    line_no,
                    path.display(),
                    parsed_r
                );
            }
            loaded.insert(
                stream.to_string(),
                StreamNoiseStats::new(parsed_r.clamp(self.cfg.kalman.r_floor, self.cfg.kalman.r_ceiling)),
            );
        }
        eprintln!(
            "INFO: loaded {} learned-r stream values from {} (warm-start only; live relearning enabled)",
            loaded.len(),
            path.display()
        );
        self.learned_r_by_stream = loaded;
    }

    fn maybe_persist_learned_r_state(&mut self) {
        if self.r_state_path.is_none() || self.r_updates_since_flush == 0 {
            return;
        }
        let enough_updates = self.r_updates_since_flush >= self.r_state_flush_min_updates;
        let enough_time = self.r_state_last_flush.elapsed() >= self.r_state_flush_interval;
        if !enough_updates && !enough_time {
            return;
        }
        if let Err(err) = self.persist_learned_r_state() {
            let path = self
                .r_state_path
                .as_ref()
                .map(|p| p.display().to_string())
                .unwrap_or_else(|| "<unset>".to_string());
            panic!(
                "failed to persist pricing model learned-r state to {}: {}",
                path, err
            );
        }
        self.r_updates_since_flush = 0;
        self.r_state_last_flush = Instant::now();
    }

    fn persist_learned_r_state(&self) -> std::io::Result<()> {
        let Some(path) = self.r_state_path.as_ref() else {
            return Ok(());
        };
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("."));
        fs::create_dir_all(&parent)?;
        let tmp_path = path.with_extension("tmp");
        let file = File::create(&tmp_path)?;
        let mut writer = BufWriter::new(file);
        writeln!(
            writer,
            "# {} saved_unix_ms={}",
            LEARNED_R_STATE_HEADER,
            now_unix_ms()
        )?;
        let mut streams: Vec<_> = self.learned_r_by_stream.iter().collect();
        streams.sort_by(|(a, _), (b, _)| a.cmp(b));
        for (stream, stats) in streams {
            let r = stats.r;
            if !r.is_finite() || r <= 0.0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("stream {} has invalid learned r {}", stream, r),
                ));
            }
            writeln!(writer, "{},{}", stream, r)?;
        }
        writer.flush()?;
        writer.get_ref().sync_all()?;
        fs::rename(&tmp_path, path)?;
        Ok(())
    }

    fn warn_invalid_metric_sample(&mut self, stream: &str, metric: &str, value: f64) {
        self.dropped_metric_samples = self.dropped_metric_samples.saturating_add(1);
        let count = self.dropped_metric_samples;
        if count <= 20 || count % 1000 == 0 {
            eprintln!(
                "WARNING: dropped invalid pricing metric sample stream={} metric={} value={} dropped_count={}",
                stream, metric, value, count
            );
        }
    }
}

fn now_unix_ms() -> u128 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(dur) => dur.as_millis(),
        Err(err) => panic!("system clock is before UNIX_EPOCH: {}", err),
    }
}

fn side_fresh(age_ms: f64, tau_ms: f64) -> f64 {
    if !age_ms.is_finite() {
        return 0.0;
    }
    if age_ms <= 0.0 {
        return 1.0;
    }
    (-age_ms / tau_ms.max(1e-6)).exp()
}

fn sigmoid(x: f64) -> f64 {
    if x.is_finite() {
        1.0 / (1.0 + (-x).exp())
    } else if x.is_sign_positive() {
        1.0
    } else {
        0.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_cfg() -> PricingModelConfig {
        PricingModelConfig {
            enabled: true,
            time_basis: TimeBasis::Wire,
            kalman: KalmanParamsConfig::default(),
            tuning: FilterTuningConfig::default(),
        }
    }

    #[test]
    fn learned_r_updates_and_stays_bounded() {
        let mut cfg = base_cfg();
        cfg.kalman.r_floor = 1e-8;
        cfg.kalman.r_ceiling = 1e-4;
        cfg.kalman.r_clip_mult = 10.0;
        let mut model = LighterPricingModel::new(cfg);
        model.update_stream_r("binance:bbo", 10.0, 0.0);
        let stats = model
            .learned_r_by_stream
            .get("binance:bbo")
            .expect("stream noise stats missing");
        assert!(stats.r.is_finite());
        assert!(stats.r >= 1e-8);
        assert!(stats.r <= 1e-4);
        assert_eq!(stats.n, 1);
    }

    #[test]
    fn learned_r_state_load_is_warm_start_only() {
        let tmp_path = std::env::temp_dir().join(format!(
            "pricing_r_state_{}_{}.csv",
            std::process::id(),
            now_unix_ms()
        ));
        std::fs::write(
            &tmp_path,
            format!("# {} saved_unix_ms=1\nbinance:bbo,0.0002\n", LEARNED_R_STATE_HEADER),
        )
        .expect("failed writing temp state file");

        let mut cfg = base_cfg();
        cfg.kalman.r_floor = 1e-8;
        cfg.kalman.r_ceiling = 1e-2;
        cfg.kalman.r_state_path = Some(tmp_path.to_string_lossy().to_string());
        let mut model = LighterPricingModel::new(cfg);
        let loaded = model
            .learned_r_by_stream
            .get("binance:bbo")
            .expect("expected loaded stream");
        assert_eq!(loaded.n, 0, "loaded values must not skip relearning warmup");

        model.update_stream_r("binance:bbo", 0.001, 0.0);
        let updated = model
            .learned_r_by_stream
            .get("binance:bbo")
            .expect("stream missing after update");
        assert_eq!(updated.n, 1);
        std::fs::remove_file(&tmp_path).expect("failed removing temp state file");
    }

    #[test]
    fn invalid_online_stats_are_counted() {
        let mut model = LighterPricingModel::new(base_cfg());
        model.update_online_stats("gate:bbo", Some(f64::NAN), -1.0, f64::INFINITY, true);
        assert_eq!(model.dropped_metric_samples, 3);
    }

    #[test]
    fn trade_online_stats_do_not_warn_on_book_only_metrics() {
        let mut model = LighterPricingModel::new(base_cfg());
        model.update_online_stats("binance:trade", None, f64::NAN, f64::NAN, false);
        assert_eq!(model.dropped_metric_samples, 0);
    }
}

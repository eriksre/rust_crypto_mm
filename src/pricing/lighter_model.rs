use std::collections::{HashMap, VecDeque};

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

fn default_trade_dir_bps() -> f64 {
    0.5
}

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

#[derive(Debug, Deserialize, Clone, Default)]
#[serde(default)]
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
    #[serde(default)]
    pub r_by_stream: HashMap<String, f64>,
    #[serde(default)]
    pub latency_median_us: HashMap<String, f64>,
    #[serde(default)]
    pub latency_mad_us: HashMap<String, f64>,
    #[serde(default)]
    pub spread_median_bps: HashMap<String, f64>,
    #[serde(default)]
    pub spread_mad_bps: HashMap<String, f64>,
    #[serde(default)]
    pub top_ratio_median: HashMap<String, f64>,
    #[serde(default)]
    pub top_ratio_mad: HashMap<String, f64>,
}

#[derive(Debug, Deserialize, Clone)]
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
}

impl LighterPricingModel {
    pub fn new(cfg: PricingModelConfig) -> Self {
        let default_r = if cfg.kalman.r_by_stream.is_empty() {
            1e-4
        } else {
            let mut vals: Vec<f64> = cfg
                .kalman
                .r_by_stream
                .values()
                .copied()
                .filter(|v| v.is_finite() && *v > 0.0)
                .collect();
            if vals.is_empty() {
                1e-4
            } else {
                vals.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                let med = vals[vals.len() / 2];
                (10.0 * med).max(1e-10)
            }
        };

        Self {
            mu_log: cfg.kalman.mu_log,
            biases: cfg.kalman.bias_by_stream.clone(),
            ref_stream: cfg.kalman.ref_stream.clone(),
            default_r,
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
        }
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

        let t_ns_i64 = i64::try_from(t_ns).ok()?;
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

        let dt = (t_sec - self.last_t).max(0.0);
        let k = self.cfg.kalman.k_per_sec.min(0.0);
        let q_per_sec = self.cfg.kalman.q_per_sec.max(0.0);
        let phi = if dt > 0.0 { (k * dt).exp() } else { 1.0 };
        let q = if dt > 0.0 { q_per_sec * dt } else { 0.0 };

        self.x = phi * self.x;
        self.p = (phi * phi) * self.p + q;
        self.x_c = phi * self.x_c;
        self.p_c = (phi * phi) * self.p_c + q;

        if z_obs.is_finite() && dt > 0.0 {
            let dz = z_obs - self.last_z;
            let dt_floor = (self.cfg.tuning.q_dt_floor_ms / 1000.0).max(1e-6);
            let dt_eff = dt.max(dt_floor);
            let dz_dt = dz / dt_eff;
            if self.cfg.tuning.vol_halflife_s > 0.0 {
                let lam = 1.0
                    - (-dt * (2.0f64.ln()) / self.cfg.tuning.vol_halflife_s).exp();
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
            let lam = 1.0
                - (-dt * (2.0f64.ln()) / self.cfg.tuning.bias_ewma_halflife_s).exp();
            b = b + lam * ((y_raw - self.x) - b);
            self.biases.insert(stream.clone(), b);
        }

        let mut r_eff = self
            .cfg
            .kalman
            .r_by_stream
            .get(&stream)
            .copied()
            .unwrap_or(self.default_r)
            .max(1e-12);
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
                let med = *self.cfg.kalman.latency_median_us.get(&stream).unwrap_or(&0.0);
                let mad = *self.cfg.kalman.latency_mad_us.get(&stream).unwrap_or(&1.0);
                z_lat = ((lat - med) / mad).max(0.0).min(10.0);
                r_eff *= 1.0 + self.cfg.tuning.latency_alpha * z_lat;
            }
        }

        let mut stale_score = 0.0;
        if !is_ref && self.cfg.tuning.stale_alpha > 0.0 && is_book {
            let z_sp = if spread_bps.is_finite() {
                let med = *self.cfg.kalman.spread_median_bps.get(&stream).unwrap_or(&0.0);
                let mad = *self.cfg.kalman.spread_mad_bps.get(&stream).unwrap_or(&1.0);
                ((spread_bps - med) / mad).max(0.0).min(10.0)
            } else {
                0.0
            };
            let z_top = if top_ratio.is_finite() {
                let med = *self.cfg.kalman.top_ratio_median.get(&stream).unwrap_or(&0.0);
                let mad = *self.cfg.kalman.top_ratio_mad.get(&stream).unwrap_or(&1.0);
                ((med - top_ratio) / mad).max(0.0).min(10.0)
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
        let mut s_var = self.p + r_eff;
        let mut std = s_var.max(1e-12).sqrt();
        let mut _nu = (y - self.x) / std;
        let suspicious = !is_ref
            && (is_trade || z_lat >= 2.0 || stale_score >= 2.0);
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
                let lam_v = 1.0
                    - (-dt_c * (2.0f64.ln()) / self.cfg.tuning.vel_halflife_s).exp();
                self.v_c = (1.0 - lam_v) * self.v_c + lam_v * inst_v;
                self.last_common_x = self.x_c;
                self.last_common_t = t_sec;
            }
        }

        if obs.exchange != TARGET_EXCHANGE && is_book && obs_price.is_finite() && obs_price > 0.0 {
            self.last_px_by_exch.insert(obs.exchange.clone(), obs_price);
            self.last_t_by_exch.insert(obs.exchange.clone(), t_sec);
        }

        let (common_median, common_mad_bps, common_n) = self.common_median(t_sec);
        let common_now = (mu_log + self.x_c).exp();
        let v_eff = self
            .v_c
            .clamp(
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
            anchor_common = (anchor_common.ln() + w_disp * (common_median.ln() - anchor_common.ln())).exp();
        }

        let mut _nowcast_mid_pre_target = None;
        let mut lighter_w_lighter = 0.0;
        if is_target_obs {
            let (age_bid_pre, age_ask_pre) = self.side_ages(t_sec);
            let (fair_pre, _w_bid_pre, _w_ask_pre, _p_up) =
                self.compute_lighter_fair(v_eff, age_bid_pre, age_ask_pre);
            if fair_pre.is_finite() && fair_pre > 0.0 && anchor_common.is_finite() && anchor_common > 0.0 {
                let spread_now = self.last_lighter_spread_bps;
                let (z_sp, ok_basis) = self.basis_health(
                    spread_now,
                    age_bid_pre,
                    age_ask_pre,
                    common_n,
                );
                let dt_b = (t_sec - self.last_basis_t).max(0.0);
                if ok_basis && self.cfg.tuning.lighter_bias_halflife_s > 0.0 && dt_b > 0.0 {
                    let lam = 1.0
                        - (-dt_b * (2.0f64.ln()) / self.cfg.tuning.lighter_bias_halflife_s).exp();
                    let target = fair_pre.ln() - anchor_common.ln();
                    let cap_bias = self.cfg.tuning.lighter_bias_cap_bps * 1e-4;
                    self.lighter_basis_log =
                        ((1.0 - lam) * self.lighter_basis_log + lam * target).clamp(-cap_bias, cap_bias);
                    self.last_basis_t = t_sec;
                }

                let anchor_on_lighter =
                    (anchor_common.ln() + self.lighter_basis_log).exp();
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
            let (z_sp, _ok_basis) = self.basis_health(spread_now, age_bid_now, age_ask_now, common_n);
            let anchor_on_lighter =
                (anchor_common.ln() + self.lighter_basis_log).exp();
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
        if w_sum > 0.0 {
            sum / w_sum
        } else {
            f64::NAN
        }
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

    fn top_of_book(
        &self,
        obs: &PricingObservation,
    ) -> (f64, f64, f64, f64) {
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
            let mut wb = if bid_px.is_finite() && bid_px > 0.0 { 1.0 } else { 0.0 };
            let mut wa = if ask_px.is_finite() && ask_px > 0.0 { 1.0 } else { 0.0 };
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
            .cfg
            .kalman
            .spread_median_bps
            .get(&self.ref_stream)
            .copied()
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

    fn basis_health(&self, spread_now: f64, age_bid: f64, age_ask: f64, common_n: i32) -> (f64, bool) {
        let spread_med = self
            .cfg
            .kalman
            .spread_median_bps
            .get(&self.ref_stream)
            .copied()
            .unwrap_or(0.0);
        let spread_mad = self
            .cfg
            .kalman
            .spread_mad_bps
            .get(&self.ref_stream)
            .copied()
            .unwrap_or(1.0)
            .max(1e-6);
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
        let disp = if disp_bps.is_finite() { disp_bps } else { f64::INFINITY };
        let age = if age_ms.is_finite() { age_ms } else { 1e9 };
        let w = w_lighter.clamp(0.0, 1.0);
        let disp0 = self.cfg.tuning.quote_disp0_bps.max(0.0);
        let disp_mult = self.cfg.tuning.quote_disp_mult.max(0.0);
        let age0 = self.cfg.tuning.quote_age0_ms.max(0.0);
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

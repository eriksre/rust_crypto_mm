#![allow(dead_code)]

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow};
use tokio::sync::mpsc;
use tokio::time::MissedTickBehavior;

use crate::base_classes::reference::ReferenceEvent;
use crate::base_classes::state::{
    ExchangeAdjustment, ExchangeSnap, FeedSnap, TradeDirection, state as global_state,
};
use crate::base_classes::types::Side;
use crate::config::runner::RunnerConfig;
use crate::execution::{ClientOrderId, ExecutionReport, QuoteIntent, Venue};
use crate::strategy::ReferenceMeta;

// Re-export shared debug logger
pub use super::debug_logger::DebugLogger;

#[derive(Clone)]
pub struct QuoteLogHandle {
    tx: mpsc::UnboundedSender<LogEvent>,
    path: PathBuf,
}

enum LogEvent {
    MarketSnapshot,
    QuoteSubmission {
        intents: Vec<QuoteIntent>,
        reference_meta: Option<ReferenceMeta>,
        reference_price: f64,
        quote_internal: Option<Duration>,
        send_instant: Instant,
        sent_ts: SystemTime,
    },
    Cancel {
        order_id: String,
        reference: ReferenceEvent,
        cancel_internal: Duration,
        send_instant: Instant,
        sent_ts: SystemTime,
    },
    Reports(Vec<ExecutionReport>),
}

impl QuoteLogHandle {
    pub fn spawn(config: &RunnerConfig, debug: DebugLogger) -> Result<Self> {
        if !config.logging.is_enabled() {
            anyhow::bail!("logging disabled");
        }

        let logger = QuoteCsvLogger::new(config)?;
        let path = logger.path.clone();
        let flush_interval = config.logging.flush_interval();
        let (tx, mut rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            let mut logger = logger;
            let mut ticker = tokio::time::interval(flush_interval);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
            let mut last_flush = Instant::now();
            loop {
                tokio::select! {
                    Some(event) = rx.recv() => {
                        if let Err(err) = handle_event(&mut logger, event, &debug) {
                            debug.error(|| format!("csv logger error: {:#}", err));
                        }
                    }
                    _ = ticker.tick() => {
                        if last_flush.elapsed() >= flush_interval {
                            if let Err(err) = logger.flush() {
                                debug.error(|| format!("csv logger flush error: {:#}", err));
                            }
                            last_flush = Instant::now();
                        }
                    }
                    else => break,
                }
            }

            if let Err(err) = logger.flush() {
                debug.error(|| format!("csv logger final flush error: {:#}", err));
            }
        });

        Ok(Self { tx, path })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn log_market_snapshot(&self) {
        if let Err(err) = self.tx.send(LogEvent::MarketSnapshot) {
            eprintln!(
                "ERROR: Failed to log market snapshot (logger channel closed): {}",
                err
            );
        }
    }

    pub fn log_quote_submission(
        &self,
        intents: &[QuoteIntent],
        reference_meta: Option<&ReferenceMeta>,
        reference_price: f64,
        quote_internal: Option<Duration>,
        send_instant: Instant,
        sent_ts: SystemTime,
    ) {
        if intents.is_empty() {
            return;
        }
        if let Err(err) = self.tx.send(LogEvent::QuoteSubmission {
            intents: intents.to_vec(),
            reference_meta: reference_meta.cloned(),
            reference_price,
            quote_internal,
            send_instant,
            sent_ts,
        }) {
            eprintln!(
                "ERROR: Failed to log quote submission (logger channel closed): {}",
                err
            );
        }
    }

    pub fn log_cancel(
        &self,
        order_id: &ClientOrderId,
        reference: &ReferenceEvent,
        cancel_internal: Duration,
        send_instant: Instant,
        sent_ts: SystemTime,
    ) {
        if let Err(err) = self.tx.send(LogEvent::Cancel {
            order_id: order_id.0.clone(),
            reference: reference.clone(),
            cancel_internal,
            send_instant,
            sent_ts,
        }) {
            eprintln!(
                "ERROR: Failed to log cancel (logger channel closed): {}",
                err
            );
        }
    }

    pub fn log_reports(&self, reports: &[ExecutionReport]) {
        if reports.is_empty() {
            return;
        }
        if let Err(err) = self.tx.send(LogEvent::Reports(reports.to_vec())) {
            eprintln!(
                "ERROR: Failed to log reports (logger channel closed): {}",
                err
            );
        }
    }
}

fn handle_event(logger: &mut QuoteCsvLogger, event: LogEvent, debug: &DebugLogger) -> Result<()> {
    match event {
        LogEvent::MarketSnapshot => logger.log_market_snapshot(),
        LogEvent::QuoteSubmission {
            intents,
            reference_meta,
            reference_price,
            quote_internal,
            send_instant,
            sent_ts,
        } => logger.log_quote_submission(
            &intents,
            reference_meta.as_ref(),
            reference_price,
            quote_internal,
            send_instant,
            sent_ts,
        ),
        LogEvent::Cancel {
            order_id,
            reference,
            cancel_internal,
            send_instant,
            sent_ts,
        } => logger.log_cancel(
            &order_id,
            &reference,
            cancel_internal,
            send_instant,
            sent_ts,
        ),
        LogEvent::Reports(reports) => {
            for report in &reports {
                if let Err(err) = logger.log_report(report) {
                    debug.error(|| format!("csv logger report error: {:#}", err));
                    return Err(err);
                }
            }
            Ok(())
        }
    }
}

struct QuoteSnapshot {
    side: Side,
    price: f64,
    size: f64,
    filled_qty: f64,
    sent_ns: Option<u128>,
    send_instant: Option<Instant>,
}

struct CancelSnapshot {
    send_instant: Instant,
}

struct QuoteCsvLogger {
    writer: BufWriter<File>,
    path: PathBuf,
    venue: String,
    last_bybit: (u64, u64, u64),
    last_binance: (u64, u64, u64),
    last_gate: (u64, u64, u64),
    last_bitget: (u64, u64, u64),
    last_okx: (u64, u64, u64),
    orders: HashMap<String, QuoteSnapshot>,
    pending_cancels: HashMap<String, CancelSnapshot>,
}

enum ExchangeId {
    Bybit,
    Binance,
    Gate,
    Bitget,
    Okx,
}

impl QuoteCsvLogger {
    fn new(config: &RunnerConfig) -> Result<Self> {
        let path = config.logging.resolve_path();
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create log dir {}", parent.display()))?;
        }
        let file = File::create(&path)
            .with_context(|| format!("failed to create log file {}", path.display()))?;
        let mut writer = BufWriter::new(file);
        writeln!(
            writer,
            "ts_ns,exchange,feed,source_engine_ts_ns,source_system_ts_ns,price,direction,event_type,client_order_id,side,size,reference_source,reference_ts_ns,reference_price,quote_internal_us,cancel_internal_us,quote_external_us,cancel_external_us,sent_ts_ns"
        )?;
        writer.flush()?;

        Ok(Self {
            writer,
            path,
            venue: venue_to_string(config.strategy.venue).to_string(),
            last_bybit: (0, 0, 0),
            last_binance: (0, 0, 0),
            last_gate: (0, 0, 0),
            last_bitget: (0, 0, 0),
            last_okx: (0, 0, 0),
            orders: HashMap::new(),
            pending_cancels: HashMap::new(),
        })
    }

    fn log_market_snapshot(&mut self) -> Result<()> {
        let st = global_state()
            .lock()
            .map_err(|_| anyhow!("global state poisoned"))?;

        self.write_exchange(
            ExchangeId::Bybit,
            "bybit",
            &st.bybit,
            Some(&st.demean.bybit),
        )?;
        self.write_exchange(
            ExchangeId::Binance,
            "binance",
            &st.binance,
            Some(&st.demean.binance),
        )?;
        self.write_exchange(ExchangeId::Gate, "gate", &st.gate, None)?;
        self.write_exchange(
            ExchangeId::Bitget,
            "bitget",
            &st.bitget,
            Some(&st.demean.bitget),
        )?;
        self.write_exchange(ExchangeId::Okx, "okx", &st.okx, Some(&st.demean.okx))?;
        Ok(())
    }

    fn write_exchange(
        &mut self,
        id: ExchangeId,
        exchange: &str,
        snap: &ExchangeSnap,
        adj: Option<&ExchangeAdjustment>,
    ) -> Result<()> {
        let cache = match id {
            ExchangeId::Bybit => &mut self.last_bybit,
            ExchangeId::Binance => &mut self.last_binance,
            ExchangeId::Gate => &mut self.last_gate,
            ExchangeId::Bitget => &mut self.last_bitget,
            ExchangeId::Okx => &mut self.last_okx,
        };
        let (orderbook_seq, bbo_seq, trade_seq) = cache;
        Self::write_feed_entry(
            &mut self.writer,
            exchange,
            "orderbook",
            &snap.orderbook,
            orderbook_seq,
            adj,
        )?;
        Self::write_feed_entry(&mut self.writer, exchange, "bbo", &snap.bbo, bbo_seq, adj)?;
        Self::write_feed_entry(
            &mut self.writer,
            exchange,
            "trade",
            &snap.trade,
            trade_seq,
            adj,
        )?;
        Ok(())
    }

    fn write_feed_entry(
        writer: &mut BufWriter<File>,
        exchange: &str,
        feed: &str,
        snap: &FeedSnap,
        last_seq: &mut u64,
        adj: Option<&ExchangeAdjustment>,
    ) -> Result<()> {
        if snap.seq == *last_seq {
            return Ok(());
        }
        let price_opt = snap
            .price
            .filter(|p| p.is_finite() && *p > 0.0)
            .or_else(|| mid_from_levels(snap));
        if let Some(price) = price_opt {
            *last_seq = snap.seq;
            let ts = snap.ts_ns.unwrap_or(0);
            let direction = trade_direction_str(snap.direction);
            let price_adj = apply_demean(price, adj);
            write_row(
                writer,
                ts,
                exchange,
                feed,
                snap.source_engine_ts_ns,
                snap.source_system_ts_ns,
                Some(price_adj),
                direction,
                "market",
                "",
                "",
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )?;
        }
        Ok(())
    }

    fn log_quote_submission(
        &mut self,
        intents: &[QuoteIntent],
        reference_meta: Option<&ReferenceMeta>,
        reference_price: f64,
        quote_internal: Option<Duration>,
        send_instant: Instant,
        sent_ts: SystemTime,
    ) -> Result<()> {
        let quote_internal_us = quote_internal.map(|d| d.as_micros() as i128);
        let sent_ns = system_time_to_ns(sent_ts);
        let venue = self.venue.clone();
        for intent in intents {
            self.orders.insert(
                intent.client_order_id.0.clone(),
                QuoteSnapshot {
                    side: intent.side,
                    price: intent.price,
                    size: intent.size,
                    filled_qty: 0.0,
                    sent_ns: Some(sent_ns),
                    send_instant: Some(send_instant),
                },
            );
            write_row(
                &mut self.writer,
                clamp_u128_to_u64(sent_ns),
                venue.as_str(),
                "quote",
                None,
                None,
                Some(intent.price),
                side_to_direction(intent.side),
                "quote",
                &intent.client_order_id.0,
                side_to_str(intent.side),
                Some(intent.size),
                reference_meta.map(|m| m.source.as_str()),
                reference_meta.and_then(|m| m.ts_ns),
                Some(reference_price),
                quote_internal_us,
                None,
                None,
                None,
                Some(sent_ns),
            )?;
        }
        Ok(())
    }

    fn log_cancel(
        &mut self,
        order_id: &str,
        reference: &ReferenceEvent,
        cancel_internal: Duration,
        send_instant: Instant,
        sent_ts: SystemTime,
    ) -> Result<()> {
        let cancel_internal_us = cancel_internal.as_micros() as i128;
        let sent_ns = system_time_to_ns(sent_ts);
        self.pending_cancels
            .insert(order_id.to_string(), CancelSnapshot { send_instant });

        let snapshot = self.orders.get(order_id);
        let (side_str, direction, price, size) = if let Some(snapshot) = snapshot {
            (
                side_to_str(snapshot.side),
                side_to_direction(snapshot.side),
                Some(snapshot.price),
                Some(snapshot.size),
            )
        } else {
            ("", "", None, None)
        };
        let venue = self.venue.clone();

        write_row(
            &mut self.writer,
            clamp_u128_to_u64(sent_ns),
            venue.as_str(),
            "cancel",
            None,
            None,
            price,
            direction,
            "cancel",
            order_id,
            side_str,
            size,
            Some(reference.source.as_str()),
            reference.ts_ns,
            Some(reference.price),
            None,
            Some(cancel_internal_us),
            None,
            None,
            Some(sent_ns),
        )
    }

    fn log_report(&mut self, report: &ExecutionReport) -> Result<()> {
        let now_instant = Instant::now();
        let id_str = report.client_order_id.0.clone();
        let mut cancel_external_us: Option<i128> = None;
        let mut quote_external_us: Option<i128> = None;

        if let Some(cancel_info) = self.pending_cancels.remove(&id_str) {
            let cancel_dur = now_instant
                .checked_duration_since(cancel_info.send_instant)
                .unwrap_or_default();
            cancel_external_us = Some(cancel_dur.as_micros() as i128);
        }

        let mut price_for_status = None;
        let mut direction = "unknown";
        let mut side_str = "";
        let mut sent_ns_for_status: Option<u128> = None;

        if let Some(snapshot) = self.orders.get_mut(&id_str) {
            price_for_status = Some(snapshot.price);
            direction = side_to_direction(snapshot.side);
            side_str = side_to_str(snapshot.side);
            sent_ns_for_status = snapshot.sent_ns;
            if let Some(send_inst) = snapshot.send_instant.take() {
                let ack_dur = now_instant
                    .checked_duration_since(send_inst)
                    .unwrap_or_default();
                quote_external_us = Some(ack_dur.as_micros() as i128);
            }
        }

        if report.filled_qty > 0.0 {
            let mut delta = report.filled_qty;
            let mut price = report.avg_fill_price;
            let mut direction = "unknown";
            let mut side_str = "";
            let mut sent_ns = None;

            if let Some(snapshot) = self.orders.get_mut(&report.client_order_id.0) {
                let fill_delta = report.filled_qty - snapshot.filled_qty;
                if fill_delta > f64::EPSILON {
                    delta = fill_delta;
                } else {
                    delta = 0.0;
                }
                snapshot.filled_qty = report.filled_qty;
                price = price.or(Some(snapshot.price));
                direction = side_to_direction(snapshot.side);
                side_str = side_to_str(snapshot.side);
                sent_ns = snapshot.sent_ns;
            }

            if delta > 0.0 {
                let ts_ns_u128 = report
                    .ts
                    .map(|ts| ts as u128)
                    .unwrap_or_else(|| system_time_to_ns(SystemTime::now()));
                write_row(
                    &mut self.writer,
                    clamp_u128_to_u64(ts_ns_u128),
                    &self.venue,
                    "fill",
                    None,
                    None,
                    price,
                    direction,
                    "fill",
                    &report.client_order_id.0,
                    side_str,
                    Some(delta),
                    None,
                    None,
                    None,
                    None,
                    None,
                    quote_external_us,
                    cancel_external_us,
                    sent_ns,
                )?;
            }
        }

        let event_type = match report.status {
            crate::execution::OrderStatus::New => Some("quote_ack"),
            crate::execution::OrderStatus::PartiallyFilled => Some("quote_ack"),
            crate::execution::OrderStatus::Filled => Some("quote_ack"),
            crate::execution::OrderStatus::Canceled => Some("cancel_ack"),
            crate::execution::OrderStatus::Rejected => Some("quote_reject"),
            _ => None,
        };

        if let Some(event) = event_type {
            let ts_ns_u128 = report
                .ts
                .map(|ts| ts as u128)
                .unwrap_or_else(|| system_time_to_ns(SystemTime::now()));
            write_row(
                &mut self.writer,
                clamp_u128_to_u64(ts_ns_u128),
                &self.venue,
                event,
                None,
                None,
                price_for_status,
                direction,
                "report",
                &report.client_order_id.0,
                side_str,
                None,
                None,
                None,
                None,
                None,
                None,
                quote_external_us,
                cancel_external_us,
                sent_ns_for_status,
            )?;
        }

        if matches!(
            report.status,
            crate::execution::OrderStatus::Filled
                | crate::execution::OrderStatus::Canceled
                | crate::execution::OrderStatus::Rejected
        ) {
            self.orders.remove(&report.client_order_id.0);
            self.pending_cancels.remove(&report.client_order_id.0);
        }

        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        Ok(())
    }
}

fn venue_to_string(venue: Venue) -> &'static str {
    match venue {
        Venue::Gate => "gate",
    }
}

fn trade_direction_str(dir: Option<TradeDirection>) -> &'static str {
    dir.map(TradeDirection::as_str).unwrap_or("")
}

fn side_to_direction(side: Side) -> &'static str {
    match side {
        Side::Bid => "buy",
        Side::Ask => "sell",
    }
}

fn side_to_str(side: Side) -> &'static str {
    match side {
        Side::Bid => "bid",
        Side::Ask => "ask",
    }
}

fn write_row(
    writer: &mut BufWriter<File>,
    ts_ns: u64,
    exchange: &str,
    feed: &str,
    source_engine_ts_ns: Option<u64>,
    source_system_ts_ns: Option<u64>,
    price: Option<f64>,
    direction: &str,
    event_type: &str,
    client_order_id: &str,
    side: &str,
    size: Option<f64>,
    reference_source: Option<&str>,
    reference_ts_ns: Option<u64>,
    reference_price: Option<f64>,
    quote_internal_us: Option<i128>,
    cancel_internal_us: Option<i128>,
    quote_external_us: Option<i128>,
    cancel_external_us: Option<i128>,
    sent_ts_ns: Option<u128>,
) -> Result<()> {
    writeln!(
        writer,
        "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
        ts_ns,
        exchange,
        feed,
        source_engine_ts_ns.map_or(String::new(), |v| v.to_string()),
        source_system_ts_ns.map_or(String::new(), |v| v.to_string()),
        format_option_f64(price),
        direction,
        event_type,
        client_order_id,
        side,
        format_option_f64(size),
        reference_source.unwrap_or(""),
        reference_ts_ns.map_or(String::new(), |v| v.to_string()),
        reference_price.map_or(String::new(), |v| format_f64(v)),
        quote_internal_us.map_or(String::new(), |v| v.to_string()),
        cancel_internal_us.map_or(String::new(), |v| v.to_string()),
        quote_external_us.map_or(String::new(), |v| v.to_string()),
        cancel_external_us.map_or(String::new(), |v| v.to_string()),
        sent_ts_ns.map_or(String::new(), |v| v.to_string()),
    )?;
    Ok(())
}

fn format_option_f64(value: Option<f64>) -> String {
    value
        .filter(|v| v.is_finite())
        .map(format_f64)
        .unwrap_or_default()
}

pub fn format_f64(value: f64) -> String {
    format!("{:.8}", value)
}

fn system_time_to_ns(ts: SystemTime) -> u128 {
    ts.duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos()
}

fn clamp_u128_to_u64(value: u128) -> u64 {
    if value > u64::MAX as u128 {
        u64::MAX
    } else {
        value as u64
    }
}

fn apply_demean(price: f64, adj: Option<&ExchangeAdjustment>) -> f64 {
    if !price.is_finite() || price <= 0.0 {
        return price;
    }
    if let Some(adj) = adj {
        if adj.samples > 0 {
            return price - adj.offset.unwrap_or(0.0);
        }
    }
    price
}

fn mid_from_levels(snap: &FeedSnap) -> Option<f64> {
    let bid = snap
        .bid_levels
        .get(0)
        .and_then(|lvl| lvl.and_then(|(px, _)| (px.is_finite() && px > 0.0).then_some(px)));
    let ask = snap
        .ask_levels
        .get(0)
        .and_then(|lvl| lvl.and_then(|(px, _)| (px.is_finite() && px > 0.0).then_some(px)));
    match (bid, ask) {
        (Some(b), Some(a)) if a > 0.0 && b > 0.0 => Some(0.5 * (a + b)),
        _ => None,
    }
}

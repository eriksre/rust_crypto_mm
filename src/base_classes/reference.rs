use std::time::Instant;

#[derive(Clone, Debug)]
pub struct ReferenceEvent {
    pub price: f64,
    pub ts_ns: Option<u64>,
    pub source: String,
    pub received_at: Instant,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReferenceSource {
    GateBbo = 0,
    GateOrderbook = 1,
    GateTrade = 2,
    BybitBbo = 3,
    BybitTrade = 4,
    BinanceBbo = 5,
    BinanceTrade = 6,
    BitgetBbo = 7,
    BitgetTrade = 8,
    OkxBbo = 9,
    OkxTrade = 10,
}

impl ReferenceSource {
    pub fn idx(self) -> u8 {
        self as u8
    }

    pub fn as_str(self) -> &'static str {
        match self {
            ReferenceSource::GateBbo => "gate_bbo",
            ReferenceSource::GateOrderbook => "gate_ob",
            ReferenceSource::GateTrade => "gate_trade",
            ReferenceSource::BybitBbo => "bybit_bbo",
            ReferenceSource::BybitTrade => "bybit_trade",
            ReferenceSource::BinanceBbo => "binance_bbo",
            ReferenceSource::BinanceTrade => "binance_trade",
            ReferenceSource::BitgetBbo => "bitget_bbo",
            ReferenceSource::BitgetTrade => "bitget_trade",
            ReferenceSource::OkxBbo => "okx_bbo",
            ReferenceSource::OkxTrade => "okx_trade",
        }
    }
}

#[derive(Clone, Debug)]
pub struct ReferenceSample {
    pub source: ReferenceSource,
    pub seq: u64,
    pub ts_ns: Option<u64>,
    pub received_at: Instant,
    pub raw_price: f64,
    pub adjusted_price: f64,
}

impl ReferenceSample {
    pub fn new(
        source: ReferenceSource,
        seq: u64,
        ts_ns: Option<u64>,
        received_at: Instant,
        raw_price: f64,
        adjusted_price: f64,
    ) -> Self {
        Self {
            source,
            seq,
            ts_ns,
            received_at,
            raw_price,
            adjusted_price,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RevisionKey {
    pub source_idx: u8,
    pub seq: u64,
    pub ts_ns: Option<u64>,
}

impl RevisionKey {
    pub fn from_sample(sample: &ReferenceSample) -> Self {
        Self {
            source_idx: sample.source.idx(),
            seq: sample.seq,
            ts_ns: sample.ts_ns,
        }
    }

    pub fn is_newer_than(&self, other: &Self) -> bool {
        let cand_ts = self.ts_ns.unwrap_or(0);
        let cur_ts = other.ts_ns.unwrap_or(0);
        if cand_ts != cur_ts {
            return cand_ts > cur_ts;
        }
        if self.seq != other.seq {
            return self.seq > other.seq;
        }
        self.source_idx > other.source_idx
    }
}

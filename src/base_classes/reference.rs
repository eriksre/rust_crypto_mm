use std::time::Instant;

#[derive(Clone, Debug)]
pub struct ReferenceEvent {
    pub price: f64,
    pub ts_ns: Option<u64>,
    pub source: String,
    pub received_at: Instant,
}

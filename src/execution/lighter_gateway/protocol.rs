use super::*;

#[derive(Debug, Clone, Deserialize)]
pub(super) struct SendTxBatchResponse {
    pub(super) code: i32,
    pub(super) message: Option<String>,
    pub(super) tx_hash: Vec<String>,
}

#[derive(Debug, Deserialize, Default)]
pub(super) struct OrdersEnvelope {
    #[serde(default)]
    pub(super) code: i32,
    #[serde(default)]
    pub(super) message: Option<String>,
    #[serde(default)]
    pub(super) orders: Vec<LighterOrderEntry>,
}

#[derive(Debug, Deserialize)]
pub(super) struct NextNonceResponse {
    #[serde(default)]
    pub(super) code: i32,
    #[serde(default)]
    pub(super) message: Option<String>,
    #[serde(default)]
    pub(super) nonce: Option<i64>,
}

#[derive(Debug, Deserialize, Clone)]
pub(super) struct LighterOrderEntry {
    #[serde(default)]
    pub(super) order_index: Option<i64>,
    #[serde(default)]
    pub(super) client_order_index: Option<i64>,
    #[serde(default)]
    pub(super) market_index: Option<u32>,
    #[serde(default)]
    pub(super) price: Option<String>,
    #[serde(default)]
    pub(super) initial_base_amount: Option<String>,
    #[serde(default)]
    pub(super) remaining_base_amount: Option<String>,
    #[serde(default)]
    pub(super) filled_base_amount: Option<String>,
    #[serde(default)]
    pub(super) is_ask: Option<bool>,
    #[serde(default)]
    pub(super) status: Option<String>,
    #[serde(default)]
    pub(super) timestamp: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub(super) struct LighterAccountOrdersMsg {
    #[serde(default)]
    pub(super) channel: Option<String>,
    #[serde(default)]
    pub(super) orders: HashMap<String, Vec<LighterOrderEntry>>,
    #[serde(default, rename = "type")]
    pub(super) msg_type: Option<String>,
    #[serde(default)]
    pub(super) account: Option<i64>,
    #[serde(default)]
    pub(super) nonce: Option<i64>,
}

#[derive(Debug, Deserialize, Clone)]
pub(super) struct LighterTxEntry {
    #[serde(default)]
    pub(super) hash: Option<String>,
    #[serde(default, rename = "type")]
    pub(super) tx_type: Option<u8>,
    #[serde(default)]
    pub(super) status: Option<i64>,
    #[serde(default)]
    pub(super) message: Option<String>,
    #[serde(default)]
    pub(super) event_info: Option<String>,
    #[serde(default)]
    pub(super) executed_at: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub(super) struct LighterAccountTxMsg {
    #[serde(default)]
    pub(super) channel: Option<String>,
    #[serde(default)]
    pub(super) txs: Vec<LighterTxEntry>,
    #[serde(default, rename = "type")]
    pub(super) msg_type: Option<String>,
    #[serde(default)]
    pub(super) account: Option<i64>,
}

#[derive(Debug, Deserialize, Clone)]
pub(super) struct LighterTxStatusResponse {
    pub(super) code: i32,
    #[serde(default)]
    pub(super) message: Option<String>,
    pub(super) hash: String,
    #[serde(rename = "type")]
    pub(super) tx_type: u8,
    #[serde(default)]
    pub(super) status: i64,
    #[serde(default)]
    pub(super) nonce: i64,
    #[serde(default)]
    pub(super) queued_at: i64,
    #[serde(default)]
    pub(super) executed_at: i64,
    #[serde(default)]
    pub(super) event_info: String,
}

pub(super) const LIGHTER_TX_TYPE_CREATE_ORDER: u8 = 14;
pub(super) const LIGHTER_TX_TYPE_CANCEL_ORDER: u8 = 15;

pub(super) fn is_lighter_rate_limited(msg: &str) -> bool {
    msg.contains("Too Many Requests")
        || msg.contains("\"code\":23000")
        || msg.contains("HTTP 429")
        || msg.contains("status=429")
}

pub fn is_lighter_sendtx_quota_error(msg: &str) -> bool {
    (msg.contains("sendTxBatch") || msg.contains("sendtx"))
        && msg.contains("23000")
        && msg.contains("Not enough volume quota")
}

fn encode_form_field(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for b in value.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{:02X}", b)),
        }
    }
    out
}

pub(super) fn format_query_string(pairs: &[(impl AsRef<str>, String)]) -> String {
    let mut out = String::new();
    for (idx, (k, v)) in pairs.iter().enumerate() {
        if idx > 0 {
            out.push('&');
        }
        out.push_str(&encode_form_field(k.as_ref()));
        out.push('=');
        out.push_str(&encode_form_field(v));
    }
    out
}

pub(super) fn truncate_for_log(value: &str, limit: usize) -> String {
    if value.chars().count() <= limit {
        return value.to_string();
    }
    value.chars().take(limit).collect::<String>() + "..."
}

pub(super) fn is_relevant_lighter_ws_payload(raw: &str) -> bool {
    raw.contains("\"jsonapi/response\"")
        || raw.contains("\"jsonapi/sendtxbatch\"")
        || raw.contains("\"update/account_tx\"")
        || raw.contains("\"update/account_txs\"")
        || raw.contains("\"update/account_orders\"")
        || raw.contains("\"update/account_all_orders\"")
}

pub(super) fn log_full_lighter_ws_payload(direction: &str, raw: &str) {
    let _ = (direction, raw);
}

pub(super) fn parse_tx_lookup_response(
    body: &str,
    requested_hash: &str,
) -> Result<LighterTxStatusResponse> {
    let parsed: LighterTxStatusResponse = serde_json::from_str(body).with_context(|| {
        format!(
            "invalid tx lookup json for hash {}: {}",
            requested_hash,
            truncate_for_log(body, 512)
        )
    })?;
    if parsed.code != 200 {
        bail!(
            "tx lookup error {} hash={} message={}",
            parsed.code,
            requested_hash,
            parsed.message.clone().unwrap_or_default()
        );
    }
    if parsed.hash.trim().is_empty() {
        bail!(
            "tx lookup response missing hash for requested hash {}",
            requested_hash
        );
    }
    Ok(parsed)
}

pub(super) fn log_api_call(
    last_api_call: &Mutex<Option<Instant>>,
    method: &str,
    url: &str,
    body: Option<&str>,
) {
    let now = Instant::now();
    let mut guard = last_api_call.lock();
    *guard = Some(now);
    let _ = (method, url, body);
}

pub(super) fn ws_url_from_base(base_url: &str) -> Result<String> {
    let url = Url::parse(base_url)?;
    let host = url
        .host_str()
        .ok_or_else(|| anyhow!("lighter base_url missing host"))?;
    let scheme = match url.scheme() {
        "http" => "ws",
        "https" => "wss",
        "ws" | "wss" => url.scheme(),
        _ => "wss",
    };
    let mut out = format!("{scheme}://{host}");
    if let Some(port) = url.port() {
        out.push(':');
        out.push_str(&port.to_string());
    }
    out.push_str("/stream");
    Ok(out)
}

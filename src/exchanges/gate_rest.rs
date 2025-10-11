use crate::exchanges::endpoints::GateioGet;

#[derive(Debug, Clone, Default)]
pub struct GateContractMeta {
    pub quanto_multiplier: Option<f64>,
    pub min_order_size: Option<f64>,
    pub funding_interval: Option<u64>,
    pub rounding_precision: Option<f64>,
    pub order_price_round: Option<f64>,
    pub in_delisting: Option<bool>,
}

pub fn fetch_contract_meta(contract: &str) -> Option<GateContractMeta> {
    let url = format!(
        "{}{}",
        GateioGet::BASE,
        GateioGet::single_contract(contract)
    );

    let rt = tokio::runtime::Runtime::new().ok()?;
    rt.block_on(async move {
        let client = reqwest::Client::new();
        let resp = client.get(url).send().await.ok()?;
        if !resp.status().is_success() {
            return None;
        }
        let value: serde_json::Value = resp.json().await.ok()?;

        Some(GateContractMeta {
            quanto_multiplier: get_f64(&value, "quanto_multiplier"),
            min_order_size: get_f64(&value, "order_size_min"),
            funding_interval: get_u64(&value, "funding_interval"),
            rounding_precision: get_f64(&value, "order_price_round"),
            order_price_round: get_f64(&value, "order_price_round"),
            in_delisting: get_bool(&value, "in_delisting"),
        })
    })
}

#[cfg(feature = "gate_exec")]
pub async fn fetch_contract_meta_async(contract: &str) -> Option<GateContractMeta> {
    let url = format!(
        "{}{}",
        GateioGet::BASE,
        GateioGet::single_contract(contract)
    );

    let client = reqwest::Client::new();
    let resp = client.get(url).send().await.ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let value: serde_json::Value = resp.json().await.ok()?;

    Some(GateContractMeta {
        quanto_multiplier: get_f64(&value, "quanto_multiplier"),
        min_order_size: get_f64(&value, "order_size_min"),
        funding_interval: get_u64(&value, "funding_interval"),
        rounding_precision: get_f64(&value, "order_price_round"),
        order_price_round: get_f64(&value, "order_price_round"),
        in_delisting: get_bool(&value, "in_delisting"),
    })
}

fn get_f64(value: &serde_json::Value, key: &str) -> Option<f64> {
    match value.get(key)? {
        serde_json::Value::Number(n) => n.as_f64(),
        serde_json::Value::String(s) => s.parse::<f64>().ok(),
        _ => None,
    }
}

fn get_u64(value: &serde_json::Value, key: &str) -> Option<u64> {
    match value.get(key)? {
        serde_json::Value::Number(n) => n.as_u64(),
        serde_json::Value::String(s) => s.parse::<u64>().ok(),
        _ => None,
    }
}

fn get_bool(value: &serde_json::Value, key: &str) -> Option<bool> {
    match value.get(key)? {
        serde_json::Value::Bool(b) => Some(*b),
        serde_json::Value::Number(n) => Some(n.as_i64().unwrap_or(0) != 0),
        serde_json::Value::String(s) => {
            if s.eq_ignore_ascii_case("true") || s == "1" {
                Some(true)
            } else if s.eq_ignore_ascii_case("false") || s == "0" {
                Some(false)
            } else {
                None
            }
        }
        _ => None,
    }
}

use crate::exchanges::endpoints::GateioGet;
use crate::utils::parsing::log_parse_drop;
use std::time::Duration;

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

    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(err) => {
            eprintln!("ERROR: failed to create tokio runtime for Gate REST: {err}");
            return None;
        }
    };
    rt.block_on(async move {
        let url = match reqwest::Url::parse(&url) {
            Ok(url) => url,
            Err(err) => {
                eprintln!("ERROR: invalid Gate REST url {url}: {err}");
                return None;
            }
        };
        let client = match reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
        {
            Ok(client) => client,
            Err(err) => {
                eprintln!("ERROR: failed to build Gate REST client: {err}");
                return None;
            }
        };
        let resp = match client.get(url.clone()).send().await {
            Ok(resp) => resp,
            Err(err) => {
                eprintln!("ERROR: Gate REST GET {} failed: {err}", url);
                return None;
            }
        };
        let status = resp.status();
        let body = match resp.text().await {
            Ok(text) => text,
            Err(err) => {
                eprintln!("ERROR: Gate REST read body {} failed: {err}", url);
                return None;
            }
        };
        if !status.is_success() {
            eprintln!(
                "ERROR: Gate REST GET {} returned {} body=\"{}\"",
                url,
                status,
                body.chars().take(256).collect::<String>()
            );
            return None;
        }
        let value: serde_json::Value = match serde_json::from_str(&body) {
            Ok(value) => value,
            Err(err) => {
                log_parse_drop("gate_rest", "json", &err, &body);
                return None;
            }
        };

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

pub async fn fetch_contract_meta_async(contract: &str) -> Option<GateContractMeta> {
    let url = format!(
        "{}{}",
        GateioGet::BASE,
        GateioGet::single_contract(contract)
    );

    let url = match reqwest::Url::parse(&url) {
        Ok(url) => url,
        Err(err) => {
            eprintln!("ERROR: invalid Gate REST url {url}: {err}");
            return None;
        }
    };
    let client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
    {
        Ok(client) => client,
        Err(err) => {
            eprintln!("ERROR: failed to build Gate REST client: {err}");
            return None;
        }
    };
    let resp = match client.get(url.clone()).send().await {
        Ok(resp) => resp,
        Err(err) => {
            eprintln!("ERROR: Gate REST GET {} failed: {err}", url);
            return None;
        }
    };
    let status = resp.status();
    let body = match resp.text().await {
        Ok(text) => text,
        Err(err) => {
            eprintln!("ERROR: Gate REST read body {} failed: {err}", url);
            return None;
        }
    };
    if !status.is_success() {
        eprintln!(
            "ERROR: Gate REST GET {} returned {} body=\"{}\"",
            url,
            status,
            body.chars().take(256).collect::<String>()
        );
        return None;
    }
    let value: serde_json::Value = match serde_json::from_str(&body) {
        Ok(value) => value,
        Err(err) => {
            log_parse_drop("gate_rest", "json", &err, &body);
            return None;
        }
    };

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
        serde_json::Value::String(s) => match s.parse::<f64>() {
            Ok(v) if v.is_finite() => Some(v),
            Ok(_) => {
                log_parse_drop("gate_rest", "non_finite", &"non-finite number", s);
                None
            }
            Err(err) => {
                log_parse_drop("gate_rest", "f64", &err, s);
                None
            }
        },
        _ => None,
    }
}

fn get_u64(value: &serde_json::Value, key: &str) -> Option<u64> {
    match value.get(key)? {
        serde_json::Value::Number(n) => n.as_u64(),
        serde_json::Value::String(s) => match s.parse::<u64>() {
            Ok(v) => Some(v),
            Err(err) => {
                log_parse_drop("gate_rest", "u64", &err, s);
                None
            }
        },
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

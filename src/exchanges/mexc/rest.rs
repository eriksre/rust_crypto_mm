use serde_json::Value;
use std::time::Duration;

use crate::utils::parsing::log_parse_drop;

#[derive(Debug, Clone, Default)]
pub struct MexcContractMeta {
    pub contract_size: Option<f64>,
}

impl MexcContractMeta {
    #[inline(always)]
    pub fn qty_multiplier(&self) -> Option<f64> {
        self.contract_size
    }
}

fn parse_f64(value: Option<&Value>) -> Option<f64> {
    match value? {
        Value::Number(n) => {
            let v = n.as_f64()?;
            if v.is_finite() {
                Some(v)
            } else {
                log_parse_drop(
                    "mexc_rest",
                    "non_finite",
                    &"non-finite number",
                    &n.to_string(),
                );
                None
            }
        }
        Value::String(s) => match s.parse::<f64>() {
            Ok(v) if v.is_finite() => Some(v),
            Ok(_) => {
                log_parse_drop("mexc_rest", "non_finite", &"non-finite number", s);
                None
            }
            Err(err) => {
                log_parse_drop("mexc_rest", "f64", &err, s);
                None
            }
        },
        _ => None,
    }
}

/// Fetch contract metadata for MEXC futures (USDT-margined) to recover contract size.
pub async fn fetch_contract_meta(symbol: &str) -> Result<Option<MexcContractMeta>, reqwest::Error> {
    let url = format!("https://contract.mexc.com/api/v1/contract/detail?symbol={symbol}");
    let url = match reqwest::Url::parse(&url) {
        Ok(url) => url,
        Err(err) => {
            eprintln!("ERROR: invalid MEXC REST url {url}: {err}");
            return Ok(None);
        }
    };
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;
    let resp = client.get(url.clone()).send().await?;
    let status = resp.status();
    let body = resp.text().await?;
    if !status.is_success() {
        eprintln!(
            "ERROR: MEXC REST GET {} returned {} body=\"{}\"",
            url,
            status,
            body.chars().take(256).collect::<String>()
        );
        return Ok(None);
    }
    let value: Value = match serde_json::from_str(&body) {
        Ok(value) => value,
        Err(err) => {
            log_parse_drop("mexc_rest", "json", &err, &body);
            return Ok(None);
        }
    };
    if value
        .get("code")
        .and_then(|c| c.as_i64())
        .unwrap_or_default()
        != 0
    {
        eprintln!(
            "ERROR: MEXC REST GET {} returned code {:?}",
            url,
            value.get("code")
        );
        return Ok(None);
    }
    let data = match value.get("data") {
        Some(Value::Object(map)) => map,
        _ => return Ok(None),
    };

    let contract_size = parse_f64(data.get("contractSize"));

    Ok(Some(MexcContractMeta { contract_size }))
}

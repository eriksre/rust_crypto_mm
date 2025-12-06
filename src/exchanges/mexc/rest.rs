use serde_json::Value;

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
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.parse::<f64>().ok(),
        _ => None,
    }
}

/// Fetch contract metadata for MEXC futures (USDT-margined) to recover contract size.
pub async fn fetch_contract_meta(symbol: &str) -> Result<Option<MexcContractMeta>, reqwest::Error> {
    let url = format!("https://contract.mexc.com/api/v1/contract/detail?symbol={symbol}");
    let client = reqwest::Client::new();
    let resp = client.get(url).send().await?;
    if !resp.status().is_success() {
        return Ok(None);
    }
    let value: Value = resp.json().await?;
    if value
        .get("code")
        .and_then(|c| c.as_i64())
        .unwrap_or_default()
        != 0
    {
        return Ok(None);
    }
    let data = match value.get("data") {
        Some(Value::Object(map)) => map,
        _ => return Ok(None),
    };

    let contract_size = parse_f64(data.get("contractSize"));

    Ok(Some(MexcContractMeta { contract_size }))
}

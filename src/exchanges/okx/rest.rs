use serde_json::Value;

#[derive(Debug, Clone, Default)]
pub struct OkxInstrumentMeta {
    pub inst_id: String,
    pub ct_val: Option<f64>,
    pub ct_val_ccy: Option<String>,
    pub ct_type: Option<String>,
    pub lot_size: Option<f64>,
}

impl OkxInstrumentMeta {
    #[inline(always)]
    pub fn qty_multiplier(&self) -> Option<f64> {
        let base_ccy = self
            .inst_id
            .split('-')
            .next()
            .unwrap_or_default()
            .to_ascii_lowercase();
        let ct_val = self.ct_val?;
        let ct_val_ccy = self.ct_val_ccy.as_ref()?.to_ascii_lowercase();
        if ct_val_ccy == base_ccy {
            Some(ct_val)
        } else {
            None
        }
    }
}

fn parse_f64(value: Option<&Value>) -> Option<f64> {
    match value? {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.parse::<f64>().ok(),
        _ => None,
    }
}

fn parse_string(value: Option<&Value>) -> Option<String> {
    value.and_then(|v| v.as_str().map(|s| s.to_string()))
}

/// Fetch OKX instrument metadata (ctVal, ctValCcy, lotSz) for the given instId.
///
/// Returns `Ok(Some(meta))` when the instrument exists and the response could be parsed,
/// `Ok(None)` when the API returns a non-zero code or an empty data array, and
/// propagates transport/JSON errors to let the caller decide whether to keep the feed enabled.
pub async fn fetch_instrument_meta(
    inst_id: &str,
) -> Result<Option<OkxInstrumentMeta>, reqwest::Error> {
    let url =
        format!("https://www.okx.com/api/v5/public/instruments?instType=SWAP&instId={inst_id}");
    let client = reqwest::Client::new();
    let resp = client.get(url).send().await?;
    if !resp.status().is_success() {
        return Ok(None);
    }
    let value: Value = resp.json().await?;
    if value
        .get("code")
        .and_then(|code| code.as_str())
        .unwrap_or_default()
        != "0"
    {
        return Ok(None);
    }
    let entry = value
        .get("data")
        .and_then(|data| data.as_array())
        .and_then(|arr| arr.first());
    let Some(entry) = entry else {
        return Ok(None);
    };

    let meta = OkxInstrumentMeta {
        inst_id: inst_id.to_string(),
        ct_val: parse_f64(entry.get("ctVal")),
        ct_val_ccy: parse_string(entry.get("ctValCcy")),
        ct_type: parse_string(entry.get("ctType")),
        lot_size: parse_f64(entry.get("lotSz")),
    };

    Ok(Some(meta))
}

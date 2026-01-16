#[cfg(any(feature = "parsing", feature = "parse_binance", feature = "gate_exec"))]
use serde_json::Value;
#[cfg(any(feature = "parsing", feature = "parse_binance", feature = "gate_exec"))]
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(any(feature = "parsing", feature = "parse_binance", feature = "gate_exec"))]
const PARSE_LOG_EVERY: u64 = 100;

#[cfg(any(feature = "parsing", feature = "parse_binance", feature = "gate_exec"))]
static PARSE_FAIL_COUNT: AtomicU64 = AtomicU64::new(0);

#[cfg(any(feature = "parsing", feature = "parse_binance", feature = "gate_exec"))]
fn truncate_payload(payload: &str, max_chars: usize) -> String {
    if payload.len() <= max_chars {
        return payload.to_string();
    }
    payload.chars().take(max_chars).collect()
}

#[cfg(any(feature = "parsing", feature = "parse_binance", feature = "gate_exec"))]
pub fn log_parse_drop(source: &str, event: &str, err: &dyn std::fmt::Display, payload: &str) {
    let count = PARSE_FAIL_COUNT.fetch_add(1, Ordering::Relaxed) + 1;
    if count == 1 || count % PARSE_LOG_EVERY == 0 {
        let sample = truncate_payload(payload, 256);
        eprintln!(
            "WARN: parse drop source={} event={} count={} err={} sample=\"{}\"",
            source, event, count, err, sample
        );
    }
}

#[cfg(any(feature = "parsing", feature = "parse_binance", feature = "gate_exec"))]
pub fn log_parse_drop_bytes(
    source: &str,
    event: &str,
    err: &dyn std::fmt::Display,
    payload: &[u8],
) {
    let sample = String::from_utf8_lossy(payload);
    log_parse_drop(source, event, err, &sample);
}

/// Converts a JSON Value to f64, handling both Number and String types.
#[cfg(any(feature = "parsing", feature = "parse_binance", feature = "gate_exec"))]
pub fn value_to_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(n) => {
            let v = n.as_f64()?;
            if v.is_finite() {
                Some(v)
            } else {
                log_parse_drop("value_to_f64", "non_finite", &"non-finite number", &v.to_string());
                None
            }
        }
        Value::String(s) => match s.parse::<f64>() {
            Ok(v) if v.is_finite() => Some(v),
            Ok(_) => {
                log_parse_drop("value_to_f64", "non_finite", &"non-finite number", s);
                None
            }
            Err(err) => {
                log_parse_drop("value_to_f64", "parse", &err, s);
                None
            }
        },
        _ => None,
    }
}

/// Converts a JSON Value to u64, handling both Number and String types.
#[cfg(any(feature = "parsing", feature = "parse_binance", feature = "gate_exec"))]
pub fn value_to_u64(value: &Value) -> Option<u64> {
    match value {
        Value::Number(n) => {
            let v = n.as_u64();
            if v.is_none() {
                log_parse_drop("value_to_u64", "non_u64", &"non-u64 number", &n.to_string());
            }
            v
        }
        Value::String(s) => match s.parse::<u64>() {
            Ok(v) => Some(v),
            Err(err) => {
                log_parse_drop("value_to_u64", "parse", &err, s);
                None
            }
        },
        _ => None,
    }
}

/// Converts a JSON Value to i64, handling both Number and String types.
#[cfg(any(feature = "parsing", feature = "parse_binance", feature = "gate_exec"))]
pub fn value_to_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Number(n) => {
            let v = n.as_i64();
            if v.is_none() {
                log_parse_drop("value_to_i64", "non_i64", &"non-i64 number", &n.to_string());
            }
            v
        }
        Value::String(s) => match s.parse::<i64>() {
            Ok(v) => Some(v),
            Err(err) => {
                log_parse_drop("value_to_i64", "parse", &err, s);
                None
            }
        },
        _ => None,
    }
}

/// Converts a JSON Value to String, handling both Number and String types.
#[cfg(any(feature = "parsing", feature = "parse_binance", feature = "gate_exec"))]
pub fn value_to_string(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

/// Extracts a user_id from a potentially nested JSON structure.
/// Searches for fields: "user_id", "uid", or "userId".
#[cfg(any(feature = "parsing", feature = "parse_binance", feature = "gate_exec"))]
pub fn extract_user_id(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Object(map) => {
            if let Some(uid) = map
                .get("user_id")
                .or_else(|| map.get("uid"))
                .or_else(|| map.get("userId"))
            {
                extract_user_id(uid)
            } else {
                map.values().find_map(extract_user_id)
            }
        }
        Value::Array(items) => items.iter().find_map(extract_user_id),
        _ => None,
    }
}

#[cfg(test)]
#[cfg(any(feature = "parsing", feature = "parse_binance", feature = "gate_exec"))]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_value_to_f64() {
        assert_eq!(value_to_f64(&json!(42.5)), Some(42.5));
        assert_eq!(value_to_f64(&json!("123.456")), Some(123.456));
        assert_eq!(value_to_f64(&json!(null)), None);
    }

    #[test]
    fn test_value_to_u64() {
        assert_eq!(value_to_u64(&json!(123)), Some(123));
        assert_eq!(value_to_u64(&json!("456")), Some(456));
        assert_eq!(value_to_u64(&json!(-1)), None);
    }

    #[test]
    fn test_value_to_string() {
        assert_eq!(value_to_string(&json!("hello")), Some("hello".to_string()));
        assert_eq!(value_to_string(&json!(42)), Some("42".to_string()));
    }

    #[test]
    fn test_extract_user_id() {
        let nested = json!({
            "data": {
                "user_id": "12345"
            }
        });
        assert_eq!(extract_user_id(&nested), Some("12345".to_string()));

        let direct = json!({"uid": 67890});
        assert_eq!(extract_user_id(&direct), Some("67890".to_string()));
    }
}

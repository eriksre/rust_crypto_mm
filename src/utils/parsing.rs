#[cfg(any(feature = "parsing", feature = "parse_binance", feature = "gate_exec"))]
use serde_json::Value;
#[cfg(any(feature = "parsing", feature = "parse_binance", feature = "gate_exec"))]
use std::collections::HashMap;
#[cfg(any(feature = "parsing", feature = "parse_binance", feature = "gate_exec"))]
use std::sync::{Mutex, OnceLock};

#[cfg(any(feature = "parsing", feature = "parse_binance", feature = "gate_exec"))]
const PARSE_LOG_EVERY: u64 = 100;

#[cfg(any(feature = "parsing", feature = "parse_binance", feature = "gate_exec"))]
type ParseDropKey = (String, String);

#[cfg(any(feature = "parsing", feature = "parse_binance", feature = "gate_exec"))]
static PARSE_FAIL_COUNTS: OnceLock<Mutex<HashMap<ParseDropKey, u64>>> = OnceLock::new();

#[cfg(any(feature = "parsing", feature = "parse_binance", feature = "gate_exec"))]
fn parse_fail_counts() -> &'static Mutex<HashMap<ParseDropKey, u64>> {
    PARSE_FAIL_COUNTS.get_or_init(|| Mutex::new(HashMap::new()))
}

#[cfg(any(feature = "parsing", feature = "parse_binance", feature = "gate_exec"))]
fn next_parse_drop_count(source: &str, event: &str) -> u64 {
    let mut counts = parse_fail_counts().lock().unwrap_or_else(|poisoned| {
        panic!(
            "ERROR: parse drop counter lock poisoned while recording source={} event={}: {}",
            source, event, poisoned
        )
    });
    let key = (source.to_string(), event.to_string());
    let count = counts.entry(key).or_insert(0);
    *count += 1;
    *count
}

#[cfg(any(feature = "parsing", feature = "parse_binance", feature = "gate_exec"))]
fn truncate_payload(payload: &str, max_chars: usize) -> String {
    if payload.len() <= max_chars {
        return payload.to_string();
    }
    payload.chars().take(max_chars).collect()
}

#[cfg(any(feature = "parsing", feature = "parse_binance", feature = "gate_exec"))]
pub fn log_parse_drop(source: &str, event: &str, err: &dyn std::fmt::Display, payload: &str) {
    let count = next_parse_drop_count(source, event);
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
                log_parse_drop(
                    "value_to_f64",
                    "non_finite",
                    &"non-finite number",
                    &v.to_string(),
                );
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

    fn reset_parse_drop_counts_for_tests() {
        let mut counts = parse_fail_counts().lock().unwrap_or_else(|poisoned| {
            panic!(
                "ERROR: parse drop counter lock poisoned during test reset: {}",
                poisoned
            )
        });
        counts.clear();
    }

    fn parse_drop_count_for_tests(source: &str, event: &str) -> u64 {
        let counts = parse_fail_counts().lock().unwrap_or_else(|poisoned| {
            panic!(
                "ERROR: parse drop counter lock poisoned during test read: {}",
                poisoned
            )
        });
        counts
            .get(&(source.to_string(), event.to_string()))
            .copied()
            .unwrap_or(0)
    }

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

    #[test]
    fn parse_drop_counts_are_tracked_per_source_and_event() {
        reset_parse_drop_counts_for_tests();

        log_parse_drop("gate_collector", "missing_ts", &"missing ts", "{}");
        log_parse_drop("gate_collector", "missing_ts", &"missing ts", "{}");
        log_parse_drop("okx_collector", "missing_seq", &"missing seq", "{}");

        assert_eq!(
            parse_drop_count_for_tests("gate_collector", "missing_ts"),
            2
        );
        assert_eq!(
            parse_drop_count_for_tests("okx_collector", "missing_seq"),
            1
        );
    }
}

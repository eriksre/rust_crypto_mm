#[cfg(any(feature = "parsing", feature = "parse_binance", feature = "gate_exec"))]
use serde_json::Value;

/// Converts a JSON Value to f64, handling both Number and String types.
#[cfg(any(feature = "parsing", feature = "parse_binance", feature = "gate_exec"))]
pub fn value_to_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.parse::<f64>().ok(),
        _ => None,
    }
}

/// Converts a JSON Value to u64, handling both Number and String types.
#[cfg(any(feature = "parsing", feature = "parse_binance", feature = "gate_exec"))]
pub fn value_to_u64(value: &Value) -> Option<u64> {
    match value {
        Value::Number(n) => n.as_u64(),
        Value::String(s) => s.parse::<u64>().ok(),
        _ => None,
    }
}

/// Converts a JSON Value to i64, handling both Number and String types.
#[cfg(any(feature = "parsing", feature = "parse_binance", feature = "gate_exec"))]
pub fn value_to_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Number(n) => n.as_i64(),
        Value::String(s) => s.parse::<i64>().ok(),
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

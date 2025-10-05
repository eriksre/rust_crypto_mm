#![allow(dead_code)]

pub fn find_json_string<'a>(s: &'a str, key: &str) -> Option<&'a str> {
    let k = format!("\"{}\"", key);
    let pos = s.find(&k)?;
    let rest = &s[pos + k.len()..];
    let colon = rest.find(':')?;
    let rest = &rest[colon + 1..];
    let q = rest.find('"')?;
    let rest2 = &rest[q + 1..];
    let end = rest2.find('"')?;
    Some(&rest2[..end])
}

pub fn extract_first_price_in_array(s: &str, keys: &[&str]) -> Option<f64> {
    for key in keys {
        let k = format!("\"{}\":", key);
        if let Some(pos) = s.find(&k) {
            let rest = &s[pos + k.len()..];
            if let Some(lb) = rest.find('[') {
                let rest = &rest[lb + 1..];
                if let Some(q) = rest.find('\"') {
                    let rest2 = &rest[q + 1..];
                    if let Some(end) = rest2.find('\"') {
                        if let Ok(v) = rest2[..end].parse::<f64>() {
                            return Some(v);
                        }
                    }
                }
            }
        }
    }
    None
}

pub fn find_first_string_number(s: &str, keys: &[&str]) -> Option<f64> {
    for key in keys {
        let k = format!("\"{}\":\"", key);
        if let Some(pos) = s.find(&k) {
            let rest = &s[pos + k.len()..];
            let end = rest.find('"')?;
            if let Ok(v) = rest[..end].parse::<f64>() {
                return Some(v);
            }
        }
    }
    None
}

pub fn find_first_number(s: &str, keys: &[&str]) -> Option<f64> {
    for key in keys {
        let k = format!("\"{}\":", key);
        if let Some(pos) = s.find(&k) {
            let rest = &s[pos + k.len()..];
            let mut end = 0;
            for (i, ch) in rest.char_indices() {
                if !(ch.is_ascii_digit() || ch == '.' || ch == '-') {
                    break;
                }
                end = i + ch.len_utf8();
            }
            if end == 0 {
                continue;
            }
            if let Ok(v) = rest[..end].parse::<f64>() {
                return Some(v);
            }
        }
    }
    None
}

pub fn find_first_bool(s: &str, keys: &[&str]) -> Option<bool> {
    for key in keys {
        let k = format!("\"{}\":", key);
        if let Some(pos) = s.find(&k) {
            let rest = &s[pos + k.len()..];
            if rest.starts_with("true") {
                return Some(true);
            }
            if rest.starts_with("false") {
                return Some(false);
            }
        }
    }
    None
}

use std::fs::read_to_string;
use std::path::Path;

pub const DEFAULT_CONFIG_PATH: &str = "config/gate_mvp.yaml";

pub fn default_symbol() -> String {
    symbol_from_config(Path::new(DEFAULT_CONFIG_PATH)).unwrap_or_else(|| "BTCUSDT".to_string())
}

pub fn symbol_from_config(path: &Path) -> Option<String> {
    let contents = read_to_string(path).ok()?;
    let mut in_strategy = false;
    for line in contents.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        if !line.starts_with(' ') && trimmed.ends_with(':') {
            in_strategy = trimmed == "strategy:";
            continue;
        }
        if !in_strategy {
            continue;
        }
        if let Some(value) = trimmed.strip_prefix("symbol:") {
            let mut sym = value.trim();
            if let Some(idx) = sym.find('#') {
                sym = sym[..idx].trim();
            }
            sym = sym.trim_matches(|c| c == '"' || c == '\'');
            if !sym.is_empty() {
                return Some(sym.to_ascii_uppercase());
            }
        }
    }
    None
}

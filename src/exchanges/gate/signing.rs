use hmac::{Hmac, Mac};
use sha2::{Digest, Sha512};

/// Return lowercase hex digest for the provided bytes.
pub fn hex_bytes(bytes: impl AsRef<[u8]>) -> String {
    bytes
        .as_ref()
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect()
}

/// Compute SHA512 over the input string and return the hex digest.
pub fn sha512_hex(input: &str) -> String {
    let mut hasher = Sha512::new();
    hasher.update(input.as_bytes());
    hex_bytes(hasher.finalize())
}

/// Convenience helper to HMAC-SHA512 sign an arbitrary payload string with the given secret.
pub fn hmac_sha512_hex(secret: &str, payload: &str) -> String {
    let mut mac =
        Hmac::<Sha512>::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key size");
    mac.update(payload.as_bytes());
    hex_bytes(mac.finalize().into_bytes())
}

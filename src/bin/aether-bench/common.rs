use rand_core::{OsRng, RngCore};

/// Generate random bytes of the given size.
pub fn random_bytes(size: usize) -> Vec<u8> {
    let mut buf = vec![0u8; size];
    OsRng.fill_bytes(&mut buf);
    buf
}

/// Generate a key for the i-th operation.
/// If `sequential`, key is a fixed-width decimal string.
/// Otherwise, key is random bytes.
pub fn generate_key(i: u64, size: usize, sequential: bool) -> Vec<u8> {
    if sequential {
        let s = format!("{i:0width$}", width = size.max(1) - 1);
        let mut key = s.into_bytes();
        key.resize(size, b'0');
        key
    } else {
        random_bytes(size)
    }
}

//! Keyed 64-bit fingerprinting for routing keys.
//!
//! Same primitive Prisma's tiered-index design uses (SipHash-2-4), so an
//! index run built here is conceptually comparable to theirs even though the
//! on-disk run format (`run.rs`) is our own, not a byte-for-byte port.

use siphasher::sip::SipHasher24;
use std::hash::Hasher;

/// A 16-byte key, generated once per stream and stored in that stream's
/// manifest (mirrors "index_secret" in Prisma's design) so fingerprints are
/// stable across index rebuilds but not guessable/collidable cross-stream.
#[derive(Clone, Copy)]
pub struct FingerprintKey(pub [u8; 16]);

impl FingerprintKey {
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        FingerprintKey(bytes)
    }

    fn hasher(&self) -> SipHasher24 {
        let k0 = u64::from_le_bytes(self.0[0..8].try_into().unwrap());
        let k1 = u64::from_le_bytes(self.0[8..16].try_into().unwrap());
        SipHasher24::new_with_keys(k0, k1)
    }

    pub fn fingerprint(&self, routing_key: &[u8]) -> u64 {
        let mut h = self.hasher();
        h.write(routing_key);
        h.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_key_same_input_is_stable() {
        let k = FingerprintKey::from_bytes([7u8; 16]);
        assert_eq!(k.fingerprint(b"conv:abc"), k.fingerprint(b"conv:abc"));
    }

    #[test]
    fn different_keys_diverge() {
        let a = FingerprintKey::from_bytes([1u8; 16]);
        let b = FingerprintKey::from_bytes([2u8; 16]);
        assert_ne!(a.fingerprint(b"conv:abc"), b.fingerprint(b"conv:abc"));
    }
}

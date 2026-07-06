//! L0 index runs: an immutable, sorted (fingerprint -> segment-mask) table
//! covering a fixed span of consecutive sealed segments, plus a binary fuse
//! filter for a fast negative lookup before the (larger) sorted table is
//! touched. Structurally inspired by Prisma streams' documented tiered-index
//! design (`docs/tiered-index.md`: 64-bit SipHash fingerprints, a 16-bit
//! per-segment mask, immutable run objects) — this is an independent
//! implementation, not a byte-for-byte port of their on-disk format.
//!
//! A run is built once its covered segments are sealed (see
//! `../../../docs/integration-points.md` #3) and never mutated afterward;
//! rebuilding the index after a crash just means replaying manifests and
//! re-deriving runs, the same recovery story the rest of the crate already
//! has for segments.

use crate::fingerprint::FingerprintKey;
use xorf::{BinaryFuse8, Filter};

/// Default span, matching Prisma's L0 run (16 segments) — narrow enough that
/// a run rebuild after a crash is cheap, wide enough to amortize per-run
/// overhead (filter + sorted table) across enough segments to matter.
pub const DEFAULT_RUN_SPAN: u64 = 16;

#[derive(Clone)]
pub struct RunEntry {
    pub fingerprint: u64,
    /// Bit i set => routing key present in segment (start_segment + i).
    pub segment_mask: u16,
}

#[derive(Clone)]
pub struct IndexRun {
    pub start_segment: u64,
    pub end_segment: u64, // exclusive
    /// Sorted by fingerprint — enables binary search on lookup.
    entries: Vec<RunEntry>,
    /// Fast negative-reject filter over the same fingerprint set. A miss
    /// here means "definitely not in this run" without touching `entries`;
    /// a hit still requires the binary search (fuse filters have a small
    /// false-positive rate by design).
    filter: BinaryFuse8,
}

pub struct RunBuilder {
    key: FingerprintKey,
    start_segment: u64,
    // (fingerprint, mask) pairs accumulated while segments in this run's
    // span are being sealed; one entry per distinct routing key observed.
    pending: std::collections::HashMap<u64, u16>,
}

impl RunBuilder {
    pub fn new(key: FingerprintKey, start_segment: u64) -> Self {
        RunBuilder {
            key,
            start_segment,
            pending: std::collections::HashMap::new(),
        }
    }

    /// Record that `routing_key` appears in `segment_index`, which must fall
    /// within `[start_segment, start_segment + DEFAULT_RUN_SPAN)`.
    pub fn observe(&mut self, routing_key: &[u8], segment_index: u64) {
        let bit = segment_index - self.start_segment;
        debug_assert!(bit < 16, "segment out of this run's span");
        let fp = self.key.fingerprint(routing_key);
        *self.pending.entry(fp).or_insert(0) |= 1u16 << bit;
    }

    /// Seal the run. Fails only if there are zero distinct keys (an empty
    /// fuse filter is degenerate) — callers should skip publishing in that
    /// case rather than treat it as an error.
    pub fn build(self) -> Result<IndexRun, EmptyRunError> {
        if self.pending.is_empty() {
            return Err(EmptyRunError);
        }
        let mut entries: Vec<RunEntry> = self
            .pending
            .into_iter()
            .map(|(fingerprint, segment_mask)| RunEntry {
                fingerprint,
                segment_mask,
            })
            .collect();
        entries.sort_unstable_by_key(|e| e.fingerprint);

        let fps: Vec<u64> = entries.iter().map(|e| e.fingerprint).collect();
        let filter = BinaryFuse8::try_from(&fps).map_err(|_| EmptyRunError)?;

        Ok(IndexRun {
            start_segment: self.start_segment,
            end_segment: self.start_segment + DEFAULT_RUN_SPAN,
            entries,
            filter,
        })
    }
}

#[derive(Debug)]
pub struct EmptyRunError;

impl IndexRun {
    /// Candidate segment indices for `routing_key` within this run's span,
    /// or `None` if the filter rejects it outright (definitely absent).
    /// `Some(vec![])` should not happen in practice (a filter hit always
    /// corresponds to a real entry) but is handled rather than unwrapped.
    pub fn candidates(&self, key: &FingerprintKey, routing_key: &[u8]) -> Option<Vec<u64>> {
        let fp = key.fingerprint(routing_key);
        if !self.filter.contains(&fp) {
            return None;
        }
        let idx = self.entries.binary_search_by_key(&fp, |e| e.fingerprint).ok()?;
        let mask = self.entries[idx].segment_mask;
        Some(
            (0..16)
                .filter(|bit| mask & (1u16 << bit) != 0)
                .map(|bit| self.start_segment + bit as u64)
                .collect(),
        )
    }

    pub fn covers(&self, segment_index: u64) -> bool {
        segment_index >= self.start_segment && segment_index < self.end_segment
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_a_key_to_its_segments() {
        let key = FingerprintKey::from_bytes([9u8; 16]);
        let mut b = RunBuilder::new(key, 0);
        b.observe(b"conv:a", 0);
        b.observe(b"conv:a", 3);
        b.observe(b"conv:b", 1);
        let run = b.build().expect("non-empty run");

        let mut a_segs = run.candidates(&key, b"conv:a").expect("present");
        a_segs.sort_unstable();
        assert_eq!(a_segs, vec![0, 3]);

        let b_segs = run.candidates(&key, b"conv:b").expect("present");
        assert_eq!(b_segs, vec![1]);
    }

    #[test]
    fn absent_key_is_rejected_by_filter_most_of_the_time() {
        let key = FingerprintKey::from_bytes([3u8; 16]);
        let mut b = RunBuilder::new(key, 0);
        for i in 0..500 {
            b.observe(format!("conv:{i}").as_bytes(), i % 16);
        }
        let run = b.build().expect("non-empty run");
        // Binary fuse filters have a small false-positive rate; a single
        // absent key should almost always be rejected. Not asserting zero
        // FPs — that would be asserting an implementation detail of xorf.
        assert!(run.candidates(&key, b"definitely-not-present").is_none());
    }

    #[test]
    fn empty_run_is_rejected() {
        let key = FingerprintKey::from_bytes([1u8; 16]);
        let b = RunBuilder::new(key, 0);
        assert!(b.build().is_err());
    }
}

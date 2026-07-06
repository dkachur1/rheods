//! The bridge between the server's write/seal path and index runs.
//!
//! The vendored server has no routing-key concept (see
//! `../../../docs/write-path-design.md`): appends are opaque byte ranges that
//! land at increasing logical offsets, and `maybe_seal` (`tier.rs:349`) turns
//! `[sealed_offset, tail)` into an immutable `SegmentEntry` when the unsealed
//! tail grows past the segment size. To index by routing key we must (a)
//! capture a key per append (a `Stream-Key` header — patch 0002) and (b)
//! remember which byte offset it landed at, so that when a segment seals we
//! know which keys it contains.
//!
//! This accumulator owns step (b) and the run-building it feeds. It is
//! deliberately server-agnostic: the patched server calls `observe` from the
//! append path and `on_segment_sealed` from `maybe_seal`; everything about
//! runs, fingerprints, and filters stays in this crate and stays unit-testable
//! without standing up a server.

use crate::fingerprint::FingerprintKey;
use crate::run::{IndexRun, RunBuilder, DEFAULT_RUN_SPAN};

/// Per-stream accumulator. One of these exists per open stream in the patched
/// server (alongside its `StreamState`), created with the stream's stable
/// fingerprint key (persisted in the stream manifest — the `index_secret`
/// equivalent).
pub struct SegmentAccumulator {
    key: FingerprintKey,
    /// Observations not yet assigned to a sealed segment: (routing key bytes,
    /// logical byte offset the append landed at). Kept sorted-by-arrival,
    /// which is sorted-by-offset since appends are monotonic.
    pending: Vec<(Vec<u8>, u64)>,
    /// Ordinal index of the next segment to seal (0-based position in the
    /// stream's `segments` vec). Drives run-span bucketing.
    next_segment_index: u64,
    /// The run currently being filled, and the segment index its span starts
    /// at. `None` until the first segment seals.
    current_run: Option<(u64, RunBuilder)>,
}

impl SegmentAccumulator {
    pub fn new(key: FingerprintKey) -> Self {
        SegmentAccumulator {
            key,
            pending: Vec::new(),
            next_segment_index: 0,
            current_run: None,
        }
    }

    /// Record that an append carrying `routing_key` landed at `logical_offset`.
    /// Called from the patched append path once the write is durable.
    pub fn observe(&mut self, routing_key: &[u8], logical_offset: u64) {
        self.pending.push((routing_key.to_vec(), logical_offset));
    }

    /// A segment covering the logical byte range `[seg_start, seg_end)` just
    /// sealed. Assign every pending observation in that range to this
    /// segment's ordinal index, and — if this completes a run's span — return
    /// the finished run for the caller to publish. Observations at offsets
    /// beyond `seg_end` (a rare interleave where a later append raced ahead)
    /// stay pending for the next seal.
    #[must_use = "a returned run must be published or it is lost"]
    pub fn on_segment_sealed(&mut self, seg_start: u64, seg_end: u64) -> Option<IndexRun> {
        let seg_index = self.next_segment_index;
        self.next_segment_index += 1;

        let run_start = seg_index - (seg_index % DEFAULT_RUN_SPAN);
        if self.current_run.is_none() {
            self.current_run = Some((run_start, RunBuilder::new(self.key, run_start)));
        }

        // Assign in-range observations to this segment; retain the rest.
        let mut still_pending = Vec::with_capacity(self.pending.len());
        for (rk, off) in std::mem::take(&mut self.pending) {
            if off >= seg_start && off < seg_end {
                if let Some((_, builder)) = self.current_run.as_mut() {
                    builder.observe(&rk, seg_index);
                }
            } else {
                still_pending.push((rk, off));
            }
        }
        self.pending = still_pending;

        // A run closes after DEFAULT_RUN_SPAN segments have sealed into it.
        let run_complete = (seg_index + 1) % DEFAULT_RUN_SPAN == 0;
        if run_complete {
            if let Some((_, builder)) = self.current_run.take() {
                // An empty span (segments sealed but no keyed appends) yields
                // no run — nothing to publish, not an error.
                return builder.build().ok();
            }
        }
        None
    }

    /// Flush a partial run early — e.g. on stream close, when fewer than
    /// `DEFAULT_RUN_SPAN` segments have sealed but the sealed ones still need
    /// to be queryable. Returns `None` if the current run is empty.
    #[must_use = "a returned run must be published or it is lost"]
    pub fn flush(&mut self) -> Option<IndexRun> {
        self.current_run.take().and_then(|(_, b)| b.build().ok())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> FingerprintKey {
        FingerprintKey::from_bytes([11u8; 16])
    }

    #[test]
    fn observations_land_in_the_segment_that_covers_their_offset() {
        let mut acc = SegmentAccumulator::new(key());
        // Two keys in segment 0 ([0, 100)), one in segment 1 ([100, 200)).
        acc.observe(b"conv:a", 10);
        acc.observe(b"conv:b", 50);
        assert!(acc.on_segment_sealed(0, 100).is_none()); // run not full yet
        acc.observe(b"conv:a", 150);
        assert!(acc.on_segment_sealed(100, 200).is_none());

        // Force the partial run out and inspect it.
        let run = acc.flush().expect("keys were observed");
        let k = key();
        let mut a = run.candidates(&k, b"conv:a").expect("a present");
        a.sort_unstable();
        assert_eq!(a, vec![0, 1]); // conv:a in both segments
        assert_eq!(run.candidates(&k, b"conv:b"), Some(vec![0]));
    }

    #[test]
    fn a_full_span_auto_emits_a_run() {
        let mut acc = SegmentAccumulator::new(key());
        let mut emitted = None;
        for seg in 0..DEFAULT_RUN_SPAN {
            let start = seg * 100;
            acc.observe(b"conv:x", start + 1);
            let r = acc.on_segment_sealed(start, start + 100);
            if seg + 1 == DEFAULT_RUN_SPAN {
                emitted = r;
            } else {
                assert!(r.is_none(), "run should not close mid-span");
            }
        }
        let run = emitted.expect("full span emits a run");
        let mut segs = run.candidates(&key(), b"conv:x").expect("present");
        segs.sort_unstable();
        assert_eq!(segs, (0..DEFAULT_RUN_SPAN).collect::<Vec<_>>());
    }

    #[test]
    fn out_of_range_observation_stays_pending() {
        let mut acc = SegmentAccumulator::new(key());
        // An append that raced ahead into the next segment's range.
        acc.observe(b"conv:late", 250);
        acc.observe(b"conv:early", 10);
        let _ = acc.on_segment_sealed(0, 100);
        // conv:late wasn't in [0,100); it must survive for a later seal.
        let _ = acc.on_segment_sealed(100, 300);
        let run = acc.flush().expect("both keys eventually land");
        assert_eq!(run.candidates(&key(), b"conv:early"), Some(vec![0]));
        assert_eq!(run.candidates(&key(), b"conv:late"), Some(vec![1]));
    }
}

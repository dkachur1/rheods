# Plan

## Status right now

**Working, tested, real:**
- `crates/ds-index` — fingerprinting (`fingerprint.rs`), L0 run build/lookup
  (`run.rs`), and an in-memory `RunStore` (`store.rs`). `cargo test -p
  ds-index` passes (6 tests) as of this scaffold.

**Scaffolded, not wired:**
- `patches/0001-add-key-query-param.patch` — parses `?key=` on `handlers.rs`.
  Applies cleanly + patched crate compiles (verified). Doesn't yet consult the
  index with it.
- `patches/0002-capture-stream-key-on-append.patch` — adds the `stream-key`
  request header and captures it in the append handler. Applies cleanly +
  compiles (one expected unused-variable warning). Doesn't yet feed the
  accumulator.
- Object-store-backed `RunStore` (real S3-compatible backend, reusing the
  vendored server's `tier` config) — not started.
- The seal-time hook that calls `SegmentAccumulator::on_segment_sealed` and
  publishes runs (`tier.rs:587-593`, right after `m.segments.push`) — designed
  (`docs/write-path-design.md`), not wired.
- `handle_read` branching on `q.key` to consult the index before falling
  through to `tier::resolve_range` — not started.
- ds-bench deployment manifest for this patched server — not started (see
  `bench/README.md` for what's known and what to verify).

**Open questions from the original scaffold — now RESOLVED** (see
`docs/write-path-design.md`): the server has no routing-key concept on writes
at all → introduce it via a `Stream-Key` header (byte-mode keying, matches the
real ProDex deployment), not JSON-field extraction. Appends → segments is a
pure offset-range mapping, so the per-segment key set needs no new lock
coordination; `ds-index`'s `SegmentAccumulator` implements that bridge and is
tested. Fork turned out to be already fully implemented in the Rust server, so
it's orthogonal to this project, not a blocker.

## Phase 0 — this scaffold

Vendor the real source, confirm the exact integration points against actual
code (not docs), stand up `ds-index` as an independently-testable crate. Done.

## Phase 1 — `ds-index` hardening

- Property tests (proptest is already a dev-dependency, unused so far):
  random routing-key sets, random segment assignments, assert every observed
  (key, segment) pair round-trips through `build` -> `candidates`.
- Decide the real `RunStore` backend. The trait is deliberately generic;
  the natural choice is reusing whatever `object_store` client the patched
  server already constructs from its `TierConfig`; concretely, this is
  probably a thin adapter crate rather than a new dependency.
- Multi-run lookup: today a caller must already know which run(s) to check.
  Add a per-stream run catalog (list of `(start_segment, end_segment)`
  spans) so a lookup for an arbitrary segment range resolves to the right
  run(s) without scanning all of them — this is the "consult local SQLite"
  step in Prisma's design; doesn't need to be SQLite here, just *some* cheap
  catalog (even an in-memory sorted `Vec` keyed by stream is fine to start).

## Phase 2 — wire into the vendored server

- Land `0001-add-key-query-param.patch` for real (apply, adjust for any
  drift from 0.1.2, `cargo build` the patched vendor tree).
- Seal hook: after `maybe_seal` offloads segments, feed each sealed
  segment's routing keys (however the server currently reads/tracks a
  stream's routing key today — check `store.rs`/`api.rs` for how a request's
  key would even be captured on write, since right now there is no `key` on
  the write/PUT path either, only read is scoped here) into a `RunBuilder`,
  and `publish` once a run's span (16 segments) is fully sealed.
- `handle_read`: if `q.key` is set, look up covering runs, get candidate
  segments, intersect with the requested offset range, and pass that
  narrowed set to (a modified) `resolve_range` instead of the full range.
  Correctness fallback: no runs found (or index unavailable) => today's
  unmodified linear behavior. Never let an index miss return wrong data —
  only let it widen the scan, never narrow it incorrectly.

## Phase 3 — benchmark

Run `ds-bench` (Apache-2.0, https://github.com/electric-sql/ds-bench)
against both the unpatched vendored binary and the patched one, same suite,
same hardware shape as the published `results-2026-06-30/REPORT.md`
(currently: unindexed durable-streams leads write throughput ~928k append/s
and catch-up read ~2.0GB/s at 100k streams). Goal isn't necessarily to beat
that number — it's to show the patched server holds roughly the same write/
catch-up numbers *and* adds a `?key=` filtered-read benchmark none of the
four systems in that report currently have at all. That comparison (filtered
read latency/throughput, indexed vs. linear-scan) is the actual case for
this project; matching raw unindexed throughput is the bar for "didn't
regress," not the bar for "succeeded."

## Open questions

- ~~How does a routing key get associated with a stream/segment on write?~~
  **Resolved** — it doesn't today; `docs/write-path-design.md` Finding 1
  settles the approach (`Stream-Key` header).
- Whether indexing should be per-stream (many small conversation streams,
  matching this project's original motivating use case) or within-stream
  (one big stream, many routing keys inside it, matching Prisma's actual
  design target). Still open, but less blocking than thought: the
  `SegmentAccumulator` is per-stream and works either way — within-stream just
  means many distinct keys flow through one stream's accumulator. Confirm the
  deployment shape before building the multi-run *catalog* (Phase 1), since
  that's where the two differ (one big stream wants a denser run index).
- Fork + index interaction: should a fork inherit the parent's index runs for
  its shared byte prefix, or rebuild? Inherit = optimization; rebuild =
  correct-but-slower default. Only relevant once both fork and index are in
  use together.

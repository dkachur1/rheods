# Write-path design (Phase 2 investigation — resolved)

This resolves the two open questions `docs/plan.md` flagged. Everything here
was verified by reading the vendored 0.1.2 source and compiling patched
against it — not inferred.

## Finding 1 — the server has NO routing-key concept on the write path

The append handler (`handlers.rs:894 handle_append_inner`) reads exactly these
request headers: producer id/epoch/seq (`parse_producer_headers`),
`stream-closed`, `stream-seq`, `content-type`. The create handler
(`handlers.rs:367 handle_create`) additionally reads `stream-ttl` and the
three fork headers (`stream-forked-from`, `stream-fork-offset`,
`stream-fork-sub-offset`). The full request-header constant list is
`handlers.rs:15-30` — there is no `stream-key` or `routing-key`.

A write's body is opaque: `encode_wire` (byte or JSON framing) then an append
under the per-stream `appender` lock. In JSON mode the server knows enough to
find value *boundaries* (`tier.rs:118 last_json_value_boundary`, used to seal
on a clean boundary) but never extracts a *field* from a value. The `key`/
`Key` identifiers in `tier.rs`/`store.rs` are all S3 object keys
(`segment_key`, `remote_key`) — cold-storage paths, not routing keys.

**So a routing key has to be introduced. Two options:**

- **A — `Stream-Key` header on append (byte-mode keying).** Client stamps a
  key per append; server records (key, offset). Works for opaque byte streams,
  no schema, no JSON parsing. Matches how ProDex actually keys today
  (`Stream-Key` = conversation id per append) and pairs 1:1 with the `?key=`
  read filter (patch 0001).
- **B — schema-configured JSON field extraction (Prisma's model).** Server
  parses each JSON value and pulls a configured field as the key. More
  machinery (a schema/profile registry the Rust server doesn't have), JSON
  only.

**Decision: A first.** It's the smaller change, matches the real deployment,
and doesn't require inventing a schema-registry subsystem the Rust server
lacks entirely. Patch `0002-capture-stream-key-on-append.patch` adds the
`stream-key` header and captures it in the append handler (capture only —
compiles with an expected unused-variable warning until the accumulator is
wired). B remains possible later as an additive extraction mode.

## Finding 2 — how appends map to segments (grounds the seal hook)

A segment is a contiguous logical byte range. `maybe_seal` (`tier.rs:349`)
seals `[sealed_offset, tail)` into a new `SegmentEntry { logical_start, len }`
(`tier.rs:59`), pushes it onto `manifest.segments`, and advances
`sealed_offset` (`tier.rs:555-593`). Segments are contiguous and ordered:
`segments[i].end() == segments[i+1].logical_start`. Default segment size is
~8 MiB (tier config).

So the per-segment key set is derivable from offsets alone: an append carrying
key K lands at logical offset O; when the segment whose range covers O seals,
K is present in that segment. No new coordination with the append lock is
needed beyond recording (K, O) as the write commits.

**This is exactly what `ds-index`'s `SegmentAccumulator` implements** (tested,
no server needed): `observe(key, logical_offset)` from the append path;
`on_segment_sealed(seg_start, seg_end)` from `maybe_seal`, which buckets
pending observations into the sealing segment's ordinal index and emits a
finished `IndexRun` every `DEFAULT_RUN_SPAN` (16) segments; `flush()` on
stream close for a partial run. Note the segment *ordinal index* (position in
the `segments` vec) is what a run addresses — segment byte-size is irrelevant
to the run model, so the ~8 MiB variable segment size doesn't matter.

## Finding 3 — fork is already fully implemented

Not an open question anymore. `handle_create` handles all three fork headers
(`handlers.rs:405-428`); `StreamState`/`Meta` carry `forked_from`,
`fork_offset_raw`, `fork_sub_offset` (`store.rs:43-45, 900-902`); reads walk
the fork parent chain (`tier::resolve_range`, per `ARCHITECTURE.md`). So the
"prisma/streams has no fork" gap you found does NOT apply to the Rust server —
if fork/branch is the need, the Rust server already has it. The index and fork
are orthogonal: a fork is a new stream that inherits a byte prefix, so it would
get its own accumulator and its own runs (open detail: whether a fork should
inherit the parent's index runs for the shared prefix, or rebuild — inheriting
is the optimization, rebuilding is the correct-but-slower default to start).

## Remaining wiring (not done — Phase 2 continued)

1. Thread `stream_key` from the append handler into the store so it's recorded
   with the committed offset (needs a per-`StreamState` `SegmentAccumulator`;
   where exactly the offset is known post-commit is the next read — around the
   append's state update under the `appender` lock, `store.rs` append path).
2. Call `accumulator.on_segment_sealed(seg_start, seg_start + seg_len)` from
   `maybe_seal` right after `m.segments.push(...)` (`tier.rs:587-593`), and
   `publish` any returned run via a `RunStore`.
3. Persist the per-stream fingerprint key in the manifest (the `index_secret`
   equivalent) so runs survive restart and rebuild deterministically.
4. Read side: branch `handle_read` on `q.key` to consult runs → candidate
   segments → narrowed `resolve_range`. Miss/absent index ⇒ today's linear
   scan (correctness floor).

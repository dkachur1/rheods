# Integration points (verified against durable-streams 0.1.2 source)

Everything below was confirmed by downloading the real crate tarball
(`static.crates.io/crates/durable-streams/durable-streams-0.1.2.crate`,
sha256 `786189b7260fd00fa877c067f12b94fc7a02bd932d28fd8ec0f4a48087702ad1`) and
reading the actual source — not inferred from docs. Line numbers are against
that exact version; re-check them after `vendor-upstream.sh` if you target a
newer release.

## Why this is a source fork, not a library dependency

`src/main.rs` exists, `src/lib.rs` does not — this crate builds a single
binary (`durable-streams-server`), not a library other crates can depend on
and extend via traits. The `repository` field in its `Cargo.toml` points at
`github.com/durable-streams/durable-streams`, but that repo's tree only
contains `packages/client-rust` (a client) and a TypeScript reference server —
no Rust server source, no issue tracker for this specific crate. So there is
no upstream to `git clone` or send a PR to; vendoring the crates.io tarball
and patching in place is the only integration path.

## 1. No routing-key parameter exists on the read path today

`src/handlers.rs:160-196`:

```rust
struct Query {
    offset: Option<String>,
    live: Option<String>,
    cursor: Option<u64>,
}

fn parse_query(q: Option<&str>) -> Result<Query, &'static str> {
    // ...
    match k {
        "offset" => { /* ... */ }
        "live" => out.live = Some(v),
        "cursor" => out.cursor = v.parse().ok(),
        _ => {}
    }
    // ...
}
```

No `key` (or `stream-key`) arm — any such query param is silently dropped by
the `_ => {}` catch-all today. `patches/0001-add-key-query-param.patch` adds
the field and parse arm; it does not yet change read behavior.

## 2. The read dispatch entry point

`src/handlers.rs:1596`, `async fn handle_read(store: Arc<Store>, req: Req, path: String) -> Resp`.
Parses the query at `:1610` via `parse_query`. This is where a `q.key` value
would branch into an index-assisted path before falling through to the
existing offset/cursor resolution.

## 3. Segment sealing / cold-tier offload — the index's natural producer

`src/tier.rs`:
- `SegmentEntry` (`:59`), `Manifest` (`:78`) — the sealed-segment catalog a
  stream already maintains.
- `impl Store` (`:314-772`) owns `maybe_seal`, called from the append path
  (`src/store.rs`, e.g. `store.maybe_seal(&st).await` after buffered writes —
  see the seal/offload/compact tests around `store.rs:1704` and `:2037` for
  the exact call shape and its hard-delete/compaction interactions).
- `resolve_range(st, start, end, out)` (`tier.rs:867`) and `ResolvedSlice`
  (`tier.rs:837`) are what a catch-up read currently walks — linearly, by
  segment — to build the response body.

The index's job is narrowing the segment set `resolve_range` has to consider.
It should NOT replace `resolve_range` — it should run before it and hand it a
restricted candidate list, so a miss/absent-index degrades to exactly today's
linear behavior (see `docs/plan.md` Phase 2).

**Hook point for publishing an index run:** immediately after `maybe_seal`
offloads a batch of segments to the cold tier (same place `Manifest`/
`SegmentEntry` get updated), publish a run covering those segments — mirrors
Prisma's design (an index run is produced once its covered segments are
sealed and immutable, never before).

## 4. Cold-tier object store is already S3-compatible

`tier.rs` `TierConfig`/`TierKind` (`:172-232`) and the crate's `object_store`
dependency (gated behind the `tier` feature, per `Cargo.toml.orig`) mean the
cold tier already speaks a generic S3 API — same shape Prisma's server uses
for its `.idx` run objects. An index-run object store backend can reuse
whatever bucket/endpoint config the `tier` feature already accepts; no new
storage integration needed, just a new object key prefix (see
`crates/ds-index/src/store.rs`).

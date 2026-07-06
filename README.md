# ds-indexed-rust

A routing-key index for the [`durable-streams`](https://crates.io/crates/durable-streams)
Rust server — the fastest known open-source Durable Streams implementation
(~928k appends/s, ~2.0GB/s catch-up reads at 100k streams, per
[electric-sql/ds-bench](https://github.com/electric-sql/ds-bench)'s published
results), but with zero support for filtered/keyed reads: every catch-up read
scans a stream linearly, and the read-path query parser silently drops any
`key`/`stream-key` parameter today.

[Prisma's `streams`](https://github.com/prisma/streams) (Bun/TypeScript, same
protocol) solved the filtering problem — a tiered fingerprint index
(SipHash-2-4 + binary fuse filters over immutable per-segment runs) that
keeps filtered reads at `O(candidate segments)` instead of `O(stream)`, even
at 12x scale (verified independently: 14->173 segments, `?key=` reads flat at
1.6-5.1ms). It just isn't Rust, and doesn't have the kernel-speed I/O
(`sendfile`/`splice`, wire-format-on-disk) the Rust server has.

This project ports that *design* (not the code — Prisma's is TypeScript, this
project independently reimplements the documented format) onto the Rust
server, via source patches rather than a library dependency, because the
Rust crate is a binary (`src/main.rs`, no `lib.rs`) with no discoverable
upstream dev repository to depend on or contribute to (see
`docs/integration-points.md` for exactly how that was confirmed).

## Status

Working keyed-by-conversation reads on the Rust server. Delivered as patches
over pristine `durable-streams-0.1.2` (`patches/`, verified apply+compile+test
via `scripts/verify-patches.sh`):

- **`Stream-Key` on append + `?key=` filtered reads** — isolate one conversation
  out of a multiplexed byte-log stream; composes with `?offset=` and live modes.
- **Fast** — coalesced spans + resident-cache-first serving: ~16k rps vs ~24k
  for a full read, while returning 50× less data (`bench/bench-keyed.sh`).
- **Durable** — a per-stream `.keys` journal rebuilds the index on restart
  (`src/key_journal.rs`); survives a simulated crash (e2e test).
- **Live** — keyed long-poll + SSE, offset advances past other keys' data.
- **Real-client verified** — `@durable-streams/state` `createStreamDB` folds a
  `?key=` read into just that conversation's rows (`client-verify/`).
- **100 tests pass** (upstream 87 + keying/persistence/live/journal).

Honest caveats (crash-tail durability window, higher keyed-read CPU, the
Linux-cfg zero-copy guard being macOS-uncompiled) are documented in
`patches/README.md` and `bench/README.md`.

`crates/ds-index` (a Prisma-style fingerprint+fuse-filter segment index)
remains a tested standalone crate, deliberately NOT wired in — the exact
in-memory directory makes it unnecessary until a stream outgrows memory or
spans thousands of cold segments. See `docs/plan.md` and `bench/README.md`
for that reasoning.

## Layout

```
scripts/vendor-upstream.sh   fetch the real durable-streams 0.1.2 source from crates.io
vendor/                      (gitignored) upstream source lands here after vendoring
crates/ds-index/             the index itself — fingerprinting, run format, run storage trait
patches/                     hand-written diffs against the vendored source
docs/integration-points.md   exact file:line hooks, verified against real source
docs/plan.md                 phased roadmap + open questions
bench/                       ds-bench integration notes (unstarted)
```

## Getting started

```bash
cargo test -p ds-index          # the part that actually exists today
scripts/vendor-upstream.sh      # pulls real upstream source into vendor/
```

Then read `docs/integration-points.md` before touching anything in `vendor/`.

## License

Apache-2.0 (see `LICENSE`). `NOTICE` documents provenance: patches against
vendored `durable-streams` source (Apache-2.0), and `ds-index` as an
independent reimplementation of Prisma streams' publicly documented index
design (also Apache-2.0) — no code copied from either project.

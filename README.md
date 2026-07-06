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

## Benchmarks

> **Scope:** these are *same-machine, relative* numbers (a MacBook, `oha` and
> the server sharing cores, loopback, no cgroup pinning) — the vendored crate's
> own `.bench-local.sh` methodology. They measure baseline-vs-change on one box,
> **not** a head-to-head against Prisma's Bun server (that requires both on
> identical dedicated hardware via the ds-bench K8s harness, which has not been
> run). Reproduce with `bench/bench-keyed.sh` / the crate's `.bench-local.sh`.

**Keyed read isolates one conversation** (`bench/bench-keyed.sh`, one stream,
2000 appends round-robin across K=50 keys, ~200 B each; medians of 3×5 s):

| scenario | rps | p50 | p99 | data returned |
|---|---|---|---|---|
| `?key=conv-7` (one conversation) | 16,237 | 3.2 ms | 14.6 ms | **8 KB** |
| full stream (client-side filter) | 24,356 | 2.6 ms | 3.4 ms | 400 KB |

The keyed read returns **50× less data** (8 KB vs 400 KB — exactly 1/K, proving
correct server-side filtering) at ~⅔ the throughput of reading everything. It
costs more CPU (filtering a byte-log reads the coalesced superset and copies out
the wanted spans) — the win is wire-data reduction, decisive when the network,
not the CPU, is the bottleneck.

**Why it's fast** — the keyed read went 1,644 → 16,237 rps (~10×) once it (a)
read resident-cache-first instead of per-span file reads and (b) coalesced a
key's scattered spans into few contiguous reads. Porting Prisma's *serving
pattern* ("one contiguous read, filter in RAM"), not its probabilistic index.

**Base server (unpatched, hot stream, `.bench-local.sh`):** read1k 161,768 rps
(p50 0.39 ms), read1m 10,715 rps (p50 2.95 ms, `sendfile`), append 7,808 rps
(p50 8.1 ms, fsync-bound). The kernel-speed ceiling (~860k appends/s, ~2 GB/s
reads, ~515 MB @ 100k streams) is documented upstream on dedicated hardware.

**Not measured:** a true Bun-vs-this comparison on identical hardware. On
hot/small streams this is plausibly comparable-or-better; on a huge stream with
one key sparse across thousands of *cold* segments, Prisma's segment index
likely wins until `crates/ds-index` is wired in (see `bench/README.md`).

### Base path (no keying): native Rust vs interpreted JS

Stripped of keying — plain append + plain read on the same data — the case for
this server being faster than Prisma's Bun server is much stronger than for the
keyed path, for two reasons:

1. **The keying patches don't touch the unkeyed path.** An append with no
   `Stream-Key` does zero extra work; an unkeyed read is byte-for-byte the
   original code. So the base-path performance *is* the upstream "kernel-speed"
   Rust server's, unchanged — `sendfile`/`splice` zero-copy reads,
   wire-format-on-disk.
2. **Native Rust vs interpreted JS.** Prisma's server is TypeScript on the Bun
   runtime (JIT'd, GC'd) — a different tier for raw throughput and memory than
   a native, zero-copy server.

The closest apples-to-apples proxy is the [ds-bench](https://github.com/electric-sql/ds-bench)
run (same hardware), comparing the Rust server against the **Node.js** reference
(a JS server):

| same hardware (ds-bench) | Rust base server | Node.js reference (JS) |
|---|---|---|
| append throughput | ~928k/s @ 100k streams | ~101k/s @ 10k streams |
| memory @ 100k streams | ~515 MB | **OOM** |

~9× on appends, and the JS server ran out of memory where Rust held ~515 MB.

**The honest catch:** that reference is **Node, not Bun** — Bun's runtime is
faster than Node, and prisma/streams was never in the ds-bench run, so Bun would
land *above Node, below native Rust*. There is **no direct Bun-vs-this
measurement**. But on the raw unkeyed path there's no architectural reason an
interpreted-JS-with-GC server catches a native zero-copy one — so: almost
certainly faster on the base path, though the exact multiple vs Bun is
unmeasured until both are run through ds-bench on the same box.

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

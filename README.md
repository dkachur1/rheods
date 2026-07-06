# rheoDS

> **Keyed-by-conversation reads for the Rust Durable Streams server.**
> Read one conversation out of a multiplexed stream — fast, durable, live.

One stream per factory, `Stream-Key: <conversation>` on write, `?key=<conversation>`
on read. That's the [Prisma Bun server's](https://github.com/prisma/streams)
model — now on the [`durable-streams`](https://crates.io/crates/durable-streams)
Rust server, which had **no keyed reads at all** before this.

## Why

| Capability | This (Rust) | Prisma (Bun) |
|---|:---:|:---:|
| Native, zero-copy I/O | ✓ | ✗ |
| Fork / branching | ✓ | ✗ |
| Keyed-by-conversation reads | ✓ *(new)* | ✓ |
| Durable keyed index | ✓ *(new)* | ✓ |
| Live keyed reads | ✓ *(new)* | ✓ |

The Rust server was the fastest known Durable Streams implementation but couldn't
filter by key; Bun could filter but isn't native and can't fork. The bottom three
rows are what this project adds — so the Rust server can be the substrate and
*keep* its speed + fork advantage. Delivered as source patches over pristine
`durable-streams-0.1.2` (the crate is a binary with no public upstream repo to
depend on — see `docs/integration-points.md`).

## Benchmarks

Both servers in **one Linux container, same kernel**, same `oha` harness
(N=2000, K=50, ~200 B, 6 s × 3), Rust's zero-copy paths compiled in. Both keyed
reads are zero-config (default profile).

| scenario | This (Rust) | Bun | Rust |
|---|--:|--:|:--:|
| keyed read `?key=` (one conversation) | **41,113 rps** · 1.3 ms | 5,787 rps · 9.9 ms | **~7×** |
| full read | **18,000 rps** · 3.4 ms | 803 rps · 77 ms | **~22×** |
| keyed CPU / request | 0.007 | 0.017 | ~2.4× leaner |

The keyed read returns **50× less data** (8 KB vs 400 KB — exactly 1/K, correct
server-side filtering) and beats Bun ~7× while using less CPU per request; full
reads are ~22× (native zero-copy vs interpreted). Laptop-Docker relative, not
dedicated-hardware absolutes; reproduce with `bench/bun/linux/build-and-run.sh`,
details in `bench/bun/linux/RESULTS.md`.

## What works

- **`Stream-Key` on append + `?key=` filtered reads** — isolate one conversation; composes with `?offset=`.
- **Fast** — coalesced spans + resident-cache-first serving: **~41k rps on Linux (~7× Bun), 50× less data** than a full read.
- **Durable at ack** — a per-stream `.keys` journal fsyncs before the append is acked; rebuilt on restart. No crash-tail window.
- **Live** — keyed long-poll + SSE; a reader advances past other keys' data.
- **Real-client verified** — `@durable-streams/state` `createStreamDB` folds a `?key=` read into just that conversation's rows.
- **101 tests** (87 upstream + keying / persistence / live / journal), patch set verified to apply-clean + compile.

## How it works

**Writes are durable *before* the ack** — the routing key lives only in a request
header, so it's journaled and fsync'd alongside the data's own durability:

```mermaid
sequenceDiagram
    participant C as Client
    participant S as Server
    participant W as WAL (data)
    participant J as .keys journal

    C->>S: POST  (Stream-Key: conv-7)
    S->>W: append + group-commit fsync
    W-->>S: durable
    S->>J: record (offset, len) for conv-7 + fsync
    J-->>S: durable
    S-->>C: 2xx ack
    Note over S,J: key routing is durable BEFORE the ack → survives a hard crash
```

**Reads filter server-side, cheaply** — the exact per-append directory gives byte
ranges directly (no probabilistic index needed); scattered spans are coalesced
into few contiguous reads, served resident-cache-first, sliced in memory:

```mermaid
flowchart TD
    R["GET ?key=conv-7&offset=N"] --> K["keyed_spans()<br/>exact (offset,len) in window"]
    K --> C["coalesce nearby spans<br/>→ few contiguous ranges"]
    C --> H{"resident<br/>cache?"}
    H -->|hit| M["read from memory<br/>(hot tail)"]
    H -->|miss| F["read segment / file"]
    M --> P["slice out matching spans"]
    F --> P
    P --> B["response = only conv-7's bytes<br/><b>50× less data</b>"]
```

On restart, the `.keys` journal is replayed (torn-tail-safe, like the WAL) to
rebuild the in-memory directory — so keyed reads survive a crash unchanged.

## Honest caveats

- **Keyed-read CPU** — on Linux, keyed reads are CPU-competitive with a full read and *leaner per request than Bun* (the ~1,180% CPU seen on macOS was a zero-copy-disabled fallback artifact, not a real property).
- **Per-append fsync** on keyed writes (WAL + journal) buys durable-at-ack; batching into the WAL group commit would amortize it — a future perf optimization, not a correctness need.
- **Linux zero-copy guard** (`#[cfg(target_os = "linux")]`) is hand-reviewed but not compiled on macOS — needs a Linux/CI build.
- **`crates/ds-index`** (Prisma-style fuse-filter segment index) is tested but deliberately **not wired in** — the exact in-memory directory makes it unnecessary until a stream outgrows memory or spans thousands of cold segments. At that extreme, Bun's segment index likely beats the coalesce-and-filter approach until this is wired.

## Layout

```
patches/            generated diffs over pristine durable-streams-0.1.2 (the feature)
scripts/
  vendor-upstream.sh  fetch the real 0.1.2 source from crates.io
  verify-patches.sh   apply all patches to a fresh pristine tree + compile
crates/ds-index/    standalone Prisma-style segment index (tested, unwired)
bench/              bench-keyed.sh + results/ (keyed vs full-read)
client-verify/      real @durable-streams/state createStreamDB compat test
docs/               integration-points.md, write-path-design.md, plan.md
vendor/             (gitignored) upstream source lands here after vendoring
```

## Getting started

```bash
scripts/vendor-upstream.sh          # fetch pristine durable-streams 0.1.2
scripts/verify-patches.sh           # apply the feature patches + compile (verifies the set)
cargo test -p ds-index              # the standalone index crate

# run the patched server + benchmark
cd vendor/durable-streams-0.1.2 && CARGO_TARGET_DIR=/tmp/ds-bench-target cargo build --release
cd ../.. && bench/bench-keyed.sh demo
```

Read `docs/integration-points.md` before touching `vendor/`.

## License

Apache-2.0 — see `LICENSE` and `NOTICE` (provenance).

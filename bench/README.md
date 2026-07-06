# Benchmarking

## What actually runs today: `.bench-local.sh` (in the vendored crate)

The vendored crate ships a same-machine regression harness,
`vendor/durable-streams-0.1.2/.bench-local.sh` — much lighter than the
Kubernetes ds-bench harness below and explicitly designed for
"baseline-vs-change on the SAME machine." Needs `oha` (`brew install oha`) +
`jq`. Numbers are RELATIVE (client and server share cores on a laptop, no
cgroup pinning) — not comparable to the published GKE absolutes, only to each
other on the same box.

```bash
cd vendor/durable-streams-0.1.2
CARGO_TARGET_DIR=/tmp/ds-bench-target cargo build --release   # bench script expects the binary here
DUR=6s REPEATS=3 ./.bench-local.sh baseline
```

### Baseline captured 2026-07-06 (this laptop, pristine 0.1.2, DUR=6s x3)

Full JSON in `results/results-baseline.json`. Medians:

| scenario     | rps     | p50 (ms) | p99 (ms) | note                          |
|--------------|---------|----------|----------|-------------------------------|
| read1k       | 161,768 | 0.39     | 0.54     | cached catch-up GET, no tier  |
| read1m       | 10,715  | 2.95     | 4.13     | large resident read (sendfile)|
| append100    | 7,808   | 8.10     | 14.01    | group-commit append           |
| append_tier  | 8,064   | 7.10     | 17.86    | append with sealing active    |

Full vendored test suite alongside this: **87 passed, 0 failed**
(`cargo test` in the vendored crate).

### Keyed reads landed — result 2026-07-06 (`bench-keyed.sh`, N=2000 K=50 x3)

Keyed reads are now wired end-to-end (append directory + `?key=` filter). Full
JSON in `results/results-with-index-keyed.json`. The correctness signal is
unambiguous:

| measure | keyed (`?key=conv-7`) | full stream | ratio |
|---------|-----------------------|-------------|-------|
| body bytes | 8,000 | 400,000 | **50.0×** (== K) |

`?key=` returns *exactly* one conversation's 40 appends out of 2000 — the
data-transfer win is real and exact.

**But throughput is currently a REGRESSION, not a win:**

| scenario | rps | p50 | p99 | server CPU |
|----------|-----|-----|-----|------------|
| keyed_read | 1,644 | 37.8 ms | 75.7 ms | ~1206% |
| full_read_client_filter | 24,483 | 2.6 ms | 3.6 ms | ~376% |

The keyed read moves 50× *less* data but is ~15× *slower* and burns ~3× the
CPU. Why: the naive append-directory implementation gathers conv-7's 40 spans
as **40 separate `read_range_bytes` calls**, each going through
`resolve_range` + segment materialization (and NOT the resident tail-chunk
cache — a noted `read_range_bytes` limitation), then copies into a `Vec` and
serves `Body::Full`. The full read is a single contiguous zero-copy `sendfile`
of 400 KB straight from page cache. On loopback, the per-span gather cost
dominates and the transfer saving is invisible (no NIC bottleneck).

This was the correct-but-slow first cut. It is now FIXED (below).

### Keyed reads made fast — result 2026-07-06 (`bench-keyed.sh`, same N=2000 K=50)

Full JSON in `results/results-keyed-fast.json`. Same correctness (ratio 50.0,
89 tests pass), ~10× faster:

| measure | naive keyed | **coalesced + resident-first** | full read |
|---------|-------------|-------------------------------|-----------|
| rps | 1,644 | **16,629** | 23,728 |
| p50 | 37.8 ms | **3.09 ms** | 2.66 ms |
| p99 | 75.7 ms | **13.6 ms** | 4.87 ms |
| body | 8 KB | 8 KB | 400 KB |

Keyed reads are now within ~30% rps / ~16% p50 of reading the whole stream,
while returning **50× less data**.

**What did it — and what did NOT.** A research pass over Prisma's reader
(`docs/tiered-index.md`, `docs/routing-key-performance.md`, `src/reader.ts`,
`src/index/*`) found that Prisma's real perf lever is NOT its fuse-filter /
mask16 segment index — that index exists only to answer "does this key live in
this cold object-store segment?" *without reading it*, a problem created by
their object-storage architecture. We already hold the exact answer: an
in-memory `(offset,len,key)` directory with zero false positives. Porting the
probabilistic index would re-derive information we already have exactly. What
DID port is Prisma's serving pattern (*"one contiguous read, filter in RAM;
mmap the whole local segment, single forward pass"*):

1. **Resident-cache-first** — the naive version used `read_range_bytes`, which
   bypasses `tail_chunk_slice` (the shared in-memory hot-tail path the normal
   read uses). Reading recent data from memory instead of per-span file reads
   is most of the win.
2. **Span coalescing** — merge a key's sorted spans separated by
   `< KEYED_COALESCE_GAP` (1 MiB) into one contiguous read, slice the kept
   spans out in memory. Turns N scattered reads into a few contiguous ones;
   the gap cap bounds bytes read-and-discarded on a sparse large log.

**Remaining honest gap:** keyed still uses more CPU than the full read
(~1150% vs ~378%) because filtering a byte-log inherently means reading the
coalesced superset region and copying out the wanted spans — the full read
serves its resident bytes with zero per-span work. That's intrinsic to
filtering, not a defect; the payoff is 50× less data on the wire at
near-parity latency. Further tuning (per-cluster reads instead of one wide
coalesce; zero-copy `FileRange` for local coalesced ranges) is possible but
diminishing returns.

### On Prisma's segment index (`crates/ds-index`)

Kept as a tested standalone crate and NOT wired in — deliberately. Per the
research it only earns its keep at a scale we're not at (the in-memory
directory stops fitting in memory, or the log spans thousands of cold
segments). At that point the simpler first move is sharding the directory by
segment, not building probabilistic fuse filters. Documented as a future
scaling lever, not a current need.

Unkeyed traffic is unaffected: appends without a `Stream-Key` header take no
`key_dir` write, and unkeyed reads are byte-for-byte the old path — so the
append/read numbers in the baseline table above do not regress.

### THE ORIGINAL GAP (now partly closed): the base harness still can't show the index's value

None of the four scenarios do a `?key=` filtered read — they're unfiltered
catch-up reads and appends. The index only changes two things:

1. **Write path** — capturing a key + accumulating observations + building/
   publishing runs at seal adds overhead. On these scenarios that can only
   show as a *regression* in append100/append_tier, never a gain.
2. **Keyed reads** — where the index actually helps. There is NO keyed-read
   scenario here, and (bigger problem) the index is NOT wired into the server
   request path yet: patches 0001/0002 only *parse* `?key=` and *capture* the
   `stream-key` header; no seal hook, no `RunStore`, no read consult (see
   `../docs/write-path-design.md` "Remaining wiring"). Re-running this exact
   script against a "with-index" binary today would produce statistically
   identical numbers, because no index code executes in these paths.

Even more fundamental: the Rust server has **no keyed-read capability at all**
today — not even a slow full-scan filter. So "add the index and run again"
isn't a flip-the-switch step. To produce a meaningful before/after it needs,
in order:

1. End-to-end index wiring (Phase 2 in `../docs/plan.md`): thread `stream_key`
   into the store with its committed offset → `SegmentAccumulator` → `RunStore`
   → seal hook in `maybe_seal` → `handle_read` consult.
2. A NEW keyed-read benchmark scenario: seed a stream with many keys, then
   `GET ?key=X`, comparing indexed lookup vs. the status-quo workaround
   (read whole stream + filter client-side). That's the number that makes the
   case for this project — and it's a number none of the four existing
   scenarios, nor any of the four systems in the ds-bench report, measure.

Until step 1 exists, "run again with the index" would be measuring nothing —
so it hasn't been run. The baseline above is the real, reproducible
"everything works" checkpoint; the with-index comparison is gated on the
wiring, not on this harness.

## The heavyweight option: ds-bench (Kubernetes)

[electric-sql/ds-bench](https://github.com/electric-sql/ds-bench) (Apache-2.0)
already benchmarks the unpatched `durable-streams` Rust server, the Node.js
reference server, `ursula`, and `S2` — see `results-2026-06-30/REPORT.md` in
that repo for current numbers (unindexed durable-streams leads write
throughput and catch-up read on every metric it reports).

Confirmed about the harness (from its README, not yet independently verified
by running it): it deploys each target as a Docker image against a `kind`
cluster + MinIO, adding a new target means a deployment manifest plus
matching its wire-protocol expectations, and `scripts/run-matrix.sh` runs all
targets side by side. **Not yet verified**: the exact manifest schema —
`docs/adding-a-target.md` in that repo 404'd when this scaffold was put
together, so read it directly in the repo before assuming the shape below is
right.

## What to actually do here

1. Get `ds-bench` running locally against the *unpatched* vendored binary
   first (`kind` cluster + MinIO, per its own README) — reproduce the
   published baseline numbers before changing anything. If you can't
   reproduce them, don't trust comparisons against a patched build.
2. Build the patched server (Phase 2 in `docs/plan.md`) as its own Docker
   image, add it as a new ds-bench target.
3. Run the existing write-throughput / catch-up-read suites against the
   patched target — confirm no regression vs. step 1's baseline.
4. Write a **new** suite: a filtered/keyed read benchmark. None of
   write-throughput.json / catch-up-read.json (or whatever the actual suite
   files turn out to be named) test `?key=` reads today, because none of the
   four existing targets support them. This is the actual number this
   project exists to produce — indexed filtered-read throughput/latency at
   increasing stream cardinality, probably shaped like the routing-key-index
   benchmarks Prisma's own docs describe (segment-scan count staying flat as
   cardinality grows), not like anything in the current ds-bench report.

`Dockerfile` in this directory is an unfilled stub — fill it in once Phase 2
produces an actual patched binary to build.

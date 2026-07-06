# Bun (prisma/streams) same-machine benchmark

Counterpart to `../bench-keyed.sh` (our patched Rust `durable-streams` server),
run against Prisma's Bun + TypeScript implementation
([github.com/prisma/streams](https://github.com/prisma/streams), Apache-2.0)
in its **local mode** — single SQLite, loopback, no object store, no auth (per
`prisma-streams/docs/local-dev.md`). Same box, same `oha`, same harness
conventions as our own bench.

## Head-to-head (same machine, N=2000 K=50 appendsize=200B, c=64 dur=6s ×3 median)

Fair apples-to-apples: Rust (ours, `../results/results-final.json`) vs Bun in
its **uncapped** config (`../bench/bun/results-bun-keyed-uncapped.json`), where
both servers' `full_read` returns the true full 400 KB stream and both keyed
reads return the same 8 KB (ratio 50.0 = K on both). See the "bounded reads"
caveat below for why the uncapped Bun run is the fair one to compare.

| scenario | metric | **Rust (ours)** | **Bun (prisma/streams)** | winner |
|----------|--------|-----------------|--------------------------|--------|
| keyed_read (`?key=conv-7`) | rps | **16,237** | 6,485 | Rust ~2.5× |
| | p50 ms | **3.20** | 9.46 | Rust ~3× |
| | p99 ms | 14.56 | **15.08** | ~tie |
| | server cpu% | 1,183% | **102%** | Bun (far less CPU) |
| full_read (whole stream) | rps | **24,356** | 1,136 | Rust ~21× |
| | p50 ms | **2.62** | 54.54 | Rust |
| | p99 ms | **3.38** | 84.27 | Rust |
| | server cpu% | **351%** | 124% | Bun (less CPU) |
| body bytes | keyed / full | 8,000 / 400,000 | 8,000 / 400,000 | identical |
| keyed filtering | ratio (full/keyed) | 50.0 (=K) | 50.0 (=K) | identical, both zero-config |

Read: **Rust wins throughput and latency on both scenarios** (dramatically so
on `full_read`), while **Bun uses far less CPU** — it isn't burning cores to
filter, it's just slower per request. Keyed filtering itself is correct and
zero-config on both (exact 50× data reduction, no schema/profile setup on
either side). The one number where Bun is competitive is keyed p99 (~tie).

Caveats that bound this comparison's fairness: Bun's default read cap (below),
Bun version skew (below), and the fact that these are RELATIVE laptop numbers
(client + server share cores, no cgroup pinning) — same as our own bench's
standing caveat.

## Version under test

- Repo: `https://github.com/prisma/streams`, cloned into `prisma-streams/`
  (untracked checkout, not a submodule).
- Commit: `b8918773b1455e1a8197ba6b653965f257902929` (2026-06-12).
- `package.json` version: `0.1.11`.
- Bun runtime: `1.3.9` (repo's `packageManager` field pins `bun@1.3.6`; we used
  whatever `bun` was already on `PATH`, not a re-pinned version — a minor,
  disclosed version skew).

## Exact run commands

```bash
git clone https://github.com/prisma/streams prisma-streams
cd prisma-streams && bun install

# local-mode server, no auth, no object store, single SQLite:
DS_LOCAL_DATA_ROOT=<data-dir> bun run src/local/cli.ts start \
  --name <server-name> --hostname 127.0.0.1 --port 4900
```

There is no `--data-dir` CLI flag; storage root is controlled only by the
`DS_LOCAL_DATA_ROOT` env var (`src/local/paths.ts`). Stop with `SIGTERM` (the
process handles it gracefully) — matches how our own harness stops the Rust
binary.

Benchmarks:

```bash
./bench-bun-keyed.sh keyed              # stock config, default DS_READ_MAX_RECORDS=1000
DS_READ_MAX_RECORDS=4000 ./bench-bun-keyed.sh keyed-uncapped   # see caveat below
./bench-bun-base.sh base                # unkeyed append100 / read1k, nice-to-have
```

Both scripts mirror `../bench-keyed.sh` verbatim: same `run_oha`/`median`/
`scenario` helpers, same `cpu_secs` (`ps -p PID -o time=`), same defaults
(`CONN=64 DUR=6s REPEATS=3`, `N=2000 K=50 APPENDSIZE=200`), same two read
scenarios. Differences are limited to what the protocol actually requires:

- stream URL is `/v1/stream/<name>`, not `/<name>`.
- server start/stop command (above) instead of a compiled binary + `--data-dir`.

## Keyed reads: zero-config, same as ours

`Stream-Key: conv-N` on byte-mode appends + `GET /v1/stream/<name>?key=conv-N`
on read work out of the box on the default (`generic`) stream profile — **no
`/_profile` or `/_schema` setup required**, confirmed by curl before
benchmarking:

```
PUT  /v1/stream/convstream                       -> 201
POST /v1/stream/convstream  Stream-Key: conv-0   -> 204
GET  /v1/stream/convstream?key=conv-0            -> 200, body = only conv-0's bytes
```

This matches our own server's zero-config keyed-read story exactly — no
schema/profile divergence to report here.

## Fairness caveat that DOES matter: bounded reads by default

Prisma's server bounds every non-live read response to
`DS_READ_MAX_RECORDS=1000` records (and `DS_READ_MAX_BYTES=1MiB`) **by
default** (`src/config.ts`). Our Rust server has no such cap — a single `GET`
with no offset returns the entire stream in one response, unbounded.

At `N=2000`, a stock-config `full_read` therefore returns only **1000 of 2000
entries (200,000 of 400,000 bytes)** — half the stream — not the true full
stream. This is a real, out-of-the-box behavioral difference, not a tuning
choice we made:

- `results-bun-keyed.json` (label `keyed`) — **stock config**, no env
  overrides. `full_bytes=200000`, `full_over_keyed_ratio=25.0` (half of the
  expected 50.0=K, because `full_read` is truncated by the default cap).
- `results-bun-keyed-uncapped.json` (label `keyed-uncapped`) — same run with
  `DS_READ_MAX_RECORDS=4000` set, so `full_read` returns the true full stream
  (`full_bytes=400000`, ratio `50.0`, matching K exactly). This is the number
  that's genuinely comparable to our Rust server's unbounded full-stream read.

We did not raise `DS_READ_MAX_RECORDS` on the primary/stock run — that would
be tuning Bun differently than its shipped default, which the fairness rules
for this bench explicitly rule out. Both files are provided so the comparison
can be read either way, honestly labeled.

## Base path (unkeyed), nice-to-have

`results-bun-base.json`: `read1k` (1 KB cached GET) and `append100` (100 B
POST), unkeyed, no tiering (local mode has no tier/object-store concept to
compare against `append_tier`). `append100` measured ~203 rps / p50 332 ms at
`c=64` — much lower than the Rust server's ~7,800 rps. Latency (`p50≈332ms`)
is consistent with queueing under sustained concurrency=64 against a
low-throughput append path (Little's law: `64 conns / 203 rps ≈ 315 ms`,
close to the measured p50), not request failures (`success_min=1.0` on every
scenario in every file). We did not profile further to pin an exact cause;
plausible contributors are the local auto-tune preset's
`ingestConcurrency=2` at the 1024 MB tier (`src/auto_tune.ts`) combined with
SQLite WAL commit/fsync cost, possibly worse on macOS than on the Rust
server's storage engine — flagged as an open question, not a verified root
cause.

## Reproduce

```bash
cd bench/bun
./bench-bun-keyed.sh <label>
DS_READ_MAX_RECORDS=4000 ./bench-bun-keyed.sh <label>-uncapped
./bench-bun-base.sh <label>
```

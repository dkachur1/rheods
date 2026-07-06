# client-verify

Verifies the **real** Durable Streams JS client packages against
`ds-indexed-rust`'s patched keyed-read support (`?key=<key>`), which had only
been exercised via curl/oha (`../bench/bench-keyed.sh`) until now. No Rust
source is touched here — this only runs the prebuilt server binary and drives
it with the actual npm clients.

## Run it

```bash
cd client-verify
bun install        # already done if node_modules/ is present
bun run.mjs        # or: node run.mjs
```

Starts the copied server binary on `127.0.0.1:4799` with a fresh temp
data-dir, seeds it, runs both checks below, prints PASS/FAIL per assertion, and
tears the server + temp dir down on exit (exit code 0 = all passed, 1 = any
failure).

## Result: PASS (7/7)

```
--- Test 1: low-level keyed read (@durable-streams/client) ---
PASS: keyed read key=conv-a returns only its own messages, in order
PASS: keyed read key=conv-b returns only its own messages, in order
PASS: keyed read key=conv-c returns only its own messages, in order
PASS: keyed reads are mutually exclusive (no cross-key leakage)
PASS: unkeyed full read sanity check: all 15 messages present

--- Test 2: createStreamDB keyed fold (@durable-streams/state) ---
PASS: createStreamDB(key=conv-1) materializes ONLY that conversation's rows
PASS: createStreamDB(key=conv-2) materializes ONLY that conversation's rows

OVERALL: PASS (7/7)
```

## Packages under test

| package | version | subpath used |
|---|---|---|
| `@durable-streams/client` | 0.2.6 (npm `latest`) | root (`stream()`, `DurableStream`) |
| `@durable-streams/state` | 0.3.1 (npm `latest`) | `/db` (`createStreamDB`, `createStateSchema`) |
| `@tanstack/db` | 0.6.0 (peer dep, pinned to match what durable-streams' own test suite uses) | — |

Both durable-streams packages are Apache-2.0. Versions were cross-checked
against a local clone of the upstream source
(`/Users/danylokachur/durable-streams`, same versions in `package.json`) to
read the real implementation rather than guessing from `.d.ts` files alone.

## What was verified and how

### 1. Low-level keyed read — PASS

`@durable-streams/client`'s standalone `stream()` function takes a `params`
option (`ParamsRecord`) that gets set as query string params on **every**
request it makes (`streamInternal` in `packages/client/src/stream-api.ts`).
Passing `params: { key: 'conv-a' }` puts `?key=conv-a` on the wire — confirmed
directly by wrapping `fetch` and logging the actual request URL:

```
http://127.0.0.1:4798/wirecheck?offset=-1&key=conv-x
```

Appends were tagged with the `Stream-Key` header using the client's real
per-handle `headers` option (`StreamHandleOptions.headers`, not raw
fetch/curl) — one `DurableStream` handle per key, each constructed with
`headers: { 'Stream-Key': key }` and `batching: false`, so every `append()`
call is its own POST carrying that key's header with no batching-related
interleaving risk. `run.mjs` seeds 15 byte-mode messages round-robined across
3 keys, then reads each key back via `dsStream({ url, params: { key }, live:
false })` and asserts the folded body is *exactly* that key's messages, in
append order, with zero cross-key leakage, plus a sanity check that an
unkeyed read still returns all 15.

### 2. createStreamDB keyed fold — PASS (the ProDex-critical result)

`createStreamDB`'s `streamOptions` is a `DurableStreamOptions`, which extends
`StreamHandleOptions` and carries the same `params` field — it flows straight
through to the internal `DurableStream` instance and from there into every
`stream()` call the StateDB's consumer makes (`packages/state/src/stream-db.ts`,
`startConsumer` → `stream.stream({ live, json: true, signal })`, which merges
in the handle-level `params` on each request). So `streamOptions: { url,
contentType: 'application/json', params: { key: conv } }` is enough — no
special-casing needed.

`run.mjs` models this on how ProDex uses it: a JSON stream carrying
state-protocol upsert events (`streamState.messages.upsert({ key, value })`
from `createStateSchema`), keyed by conversation via the same per-handle
`Stream-Key` header technique as test 1. Two conversations (`conv-1`,
`conv-2`), two messages each, interleaved on the same stream. Two separate
`createStreamDB` instances are pointed at the same URL with `params: { key:
'conv-1' }` and `params: { key: 'conv-2' }` respectively; after `db.preload()`,
each one's `collections.messages` contains *only* its own conversation's rows
(`size` matches, `.get(id)` on the other conversation's ids is `undefined`).

This also exercises the server's JSON re-wrapping path correctly: the patched
`handle_keyed_catchup` (see `../patches/0001-keying-handlers.patch`) strips
each recorded span's trailing comma and wraps the concatenated spans in a
single `[...]` for JSON-content-type streams, so a keyed JSON read is valid
JSON `res.json()`/`subscribeJson()` can parse — confirmed working, not just
assumed.

## Client API gaps found (both auxiliary, neither blocking)

- **No per-call `append()` header override.** `AppendOptions` (the second arg
  to `DurableStream.append()`) only has `seq`, `contentType`, `signal`,
  `producerId/Epoch/Seq` — no `headers`. So there is no way to send a
  *different* `Stream-Key` on each `append()` call against one handle.
  Workaround used here (a legitimate, supported pattern, not a fallback to
  raw fetch): construct one `DurableStream` handle per key against the same
  URL, each with a static `headers: { 'Stream-Key': key }` at construction
  time (`batching: false` to keep each append a single POST). This is real
  client API usage — `StreamHandleOptions.headers` — just applied per-key
  instead of per-call.
- (Not exercised here, noted for completeness) `HeadersRecord` values *can*
  be functions re-evaluated per request, so a single handle with `headers: {
  'Stream-Key': () => currentKey }` mutated between sequentially-awaited
  `append()` calls would also work — but that relies on the caller never
  firing two appends concurrently (batching would coalesce them under
  whichever key was current when the batch's request headers were built).
  The one-handle-per-key approach above avoids that footgun entirely, so it's
  what `run.mjs` uses.

No gap was found in the **read** path — `params` passthrough exists on both
`stream()` and `DurableStream`/`createStreamDB`, is exactly what's needed for
`?key=`, and works end-to-end with the real client, unmodified.

## Files

- `run.mjs` — the whole test, self-contained (spawns server, seeds, asserts, tears down).
- `durable-streams-server` — a copy of the prebuilt binary (`/tmp/ds-bench-target/release/durable-streams-server`), copied here so a concurrent rebuild of the workspace target can't disturb this run.
- `package.json` / `bun.lock` — pinned to `@durable-streams/client@0.2.6`, `@durable-streams/state@0.3.1`, `@tanstack/db@0.6.0`.

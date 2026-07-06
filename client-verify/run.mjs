#!/usr/bin/env node
// Verifies the REAL @durable-streams/client + @durable-streams/state JS clients
// against the patched ds-indexed-rust server's keyed reads (?key=<key>).
//
// Two things under test, in priority order (see ../README.md at repo root and
// this dir's README.md for the full writeup):
//
//   1. Low-level keyed read: @durable-streams/client's stream() function,
//      pointed at GET /<stream>?key=<key> via its `params` option, returns
//      ONLY that key's appends, in order. Appends are tagged via the
//      Stream-Key header using the client's real (per-handle) headers API.
//
//   2. createStreamDB keyed fold: @durable-streams/state's createStreamDB
//      (the StateDB ProDex uses) pointed at the same keyed endpoint via
//      `streamOptions.params`, materializing ONLY that conversation's rows.
//
// Usage: node run.mjs   (or: bun run.mjs)
// Exit code 0 = all PASS, 1 = any FAIL.

import { spawn } from "node:child_process"
import { mkdtempSync, rmSync } from "node:fs"
import { tmpdir } from "node:os"
import path from "node:path"
import { setTimeout as sleep } from "node:timers/promises"
import { fileURLToPath } from "node:url"

import { DurableStream, stream as dsStream } from "@durable-streams/client"
import { createStateSchema, createStreamDB } from "@durable-streams/state/db"

const __dirname = path.dirname(fileURLToPath(import.meta.url))
const BIN = path.join(__dirname, "durable-streams-server")
const HOST = "127.0.0.1"
const PORT = process.env.PORT ? Number(process.env.PORT) : 4799
const BASE = `http://${HOST}:${PORT}`
const dataDir = mkdtempSync(path.join(tmpdir(), "ds-client-verify-"))

let serverProc = null
const results = []

function record(status, name, detail) {
  results.push({ status, name, detail })
  const line = detail ? `${status}: ${name}\n  ${detail}` : `${status}: ${name}`
  console.log(line)
}
const ok = (name, detail) => record("PASS", name, detail)
const fail = (name, detail) => record("FAIL", name, detail)

function startServer() {
  serverProc = spawn(
    BIN,
    ["--host", HOST, "--port", String(PORT), "--data-dir", dataDir],
    { stdio: ["ignore", "pipe", "pipe"] }
  )
  let stderrBuf = ""
  serverProc.stderr.on("data", (chunk) => {
    stderrBuf += chunk.toString()
  })
  serverProc.on("exit", (code, signal) => {
    if (code !== null && code !== 0) {
      console.error(`server exited unexpectedly: code=${code} signal=${signal}`)
      console.error(stderrBuf)
    }
  })
}

async function waitForServer() {
  for (let i = 0; i < 100; i++) {
    try {
      const res = await fetch(`${BASE}/__client_verify_readiness_probe__`, {
        method: "HEAD",
      })
      // Any HTTP response (404 for a nonexistent stream is expected) proves
      // the server is accepting connections.
      if (res.status) return
    } catch {
      // connection refused while still booting - keep polling
    }
    await sleep(100)
  }
  throw new Error("server did not become ready in time")
}

function stopServer() {
  if (serverProc && serverProc.exitCode === null && !serverProc.killed) {
    serverProc.kill("SIGTERM")
  }
}

// ============================================================================
// Test 1: low-level keyed read via @durable-streams/client's stream()
// ============================================================================
async function testLowLevelKeyedRead() {
  console.log("\n--- Test 1: low-level keyed read (@durable-streams/client) ---")

  const url = `${BASE}/convstream-${Date.now()}`
  const contentType = "application/octet-stream"

  // Create the stream via the real client (PUT create-only).
  await DurableStream.create({ url, contentType })

  // Appends are tagged with a Stream-Key header via the client's real
  // per-handle `headers` option (StreamHandleOptions.headers) - one handle
  // per key, batching disabled so each append() is its own POST with that
  // key's header (no interleaving risk).
  const keys = ["conv-a", "conv-b", "conv-c"]
  const handles = Object.fromEntries(
    keys.map((k) => [
      k,
      new DurableStream({
        url,
        contentType,
        headers: { "Stream-Key": k },
        batching: false,
      }),
    ])
  )

  const expected = Object.fromEntries(keys.map((k) => [k, []]))
  let total = 0
  for (let i = 0; i < 15; i++) {
    const key = keys[i % keys.length]
    const payload = `${key}#${i}`
    expected[key].push(payload)
    await handles[key].append(`${payload}\n`)
    total++
  }

  const readKey = async (key) => {
    // The keyed read: stream()'s `params` option is forwarded verbatim as
    // query params on every request - this is what puts `?key=<key>` on the
    // wire against our patched server's read-path filter.
    const res = await dsStream({ url, params: { key }, live: false })
    const bytes = await res.body()
    return new TextDecoder().decode(bytes).split("\n").filter(Boolean)
  }

  for (const key of keys) {
    const lines = await readKey(key)
    const want = expected[key]
    if (JSON.stringify(lines) === JSON.stringify(want)) {
      ok(
        `keyed read key=${key} returns only its own messages, in order`,
        `${lines.length} messages`
      )
    } else {
      fail(
        `keyed read key=${key} returns only its own messages, in order`,
        `expected ${JSON.stringify(want)}\n  got      ${JSON.stringify(lines)}`
      )
    }
  }

  // Cross-check: no key's read leaks another key's payloads.
  const allLeaked = []
  for (const key of keys) {
    const lines = await readKey(key)
    const otherKeys = keys.filter((k) => k !== key)
    for (const line of lines) {
      if (otherKeys.some((ok2) => line.startsWith(`${ok2}#`))) {
        allLeaked.push(line)
      }
    }
  }
  if (allLeaked.length === 0) {
    ok("keyed reads are mutually exclusive (no cross-key leakage)")
  } else {
    fail("keyed reads are mutually exclusive (no cross-key leakage)", JSON.stringify(allLeaked))
  }

  // Sanity: unkeyed full read still returns everything (byte-log superset).
  const fullRes = await dsStream({ url, live: false })
  const fullLines = new TextDecoder()
    .decode(await fullRes.body())
    .split("\n")
    .filter(Boolean)
  if (fullLines.length === total) {
    ok(`unkeyed full read sanity check: all ${total} messages present`)
  } else {
    fail(
      "unkeyed full read sanity check",
      `expected ${total} total messages, got ${fullLines.length}`
    )
  }
}

// ============================================================================
// Test 2: createStreamDB folds a keyed read into per-conversation state
// ============================================================================
async function testStreamDBKeyedFold() {
  console.log("\n--- Test 2: createStreamDB keyed fold (@durable-streams/state) ---")

  const url = `${BASE}/convdb-${Date.now()}`
  const contentType = "application/json"

  await DurableStream.create({ url, contentType })

  // Minimal StandardSchemaV1 (no zod dependency needed) - same pattern the
  // durable-streams repo's own tests use.
  const messageSchema = {
    "~standard": {
      version: 1,
      vendor: "client-verify",
      validate: (value) => {
        if (typeof value !== "object" || value === null) {
          return { issues: [{ message: "invalid message" }] }
        }
        return { value }
      },
    },
  }

  const streamState = createStateSchema({
    messages: { schema: messageSchema, type: "message", primaryKey: "id" },
  })

  // State-protocol upsert events, keyed by conversation - models how ProDex
  // uses createStreamDB (upsert-fold over a stream, keyed by conversation).
  const conversations = {
    "conv-1": [
      { id: "m1", text: "hello from conv-1" },
      { id: "m2", text: "second message in conv-1" },
    ],
    "conv-2": [
      { id: "m3", text: "hello from conv-2" },
      { id: "m4", text: "second message in conv-2" },
    ],
  }

  for (const [conv, msgs] of Object.entries(conversations)) {
    const handle = new DurableStream({
      url,
      contentType,
      headers: { "Stream-Key": conv },
      batching: false,
    })
    for (const m of msgs) {
      await handle.append(
        JSON.stringify(streamState.messages.upsert({ key: m.id, value: m }))
      )
    }
  }

  for (const [conv, msgs] of Object.entries(conversations)) {
    const otherConvs = Object.keys(conversations).filter((c) => c !== conv)

    // The ProDex-critical wiring: streamOptions.params passed straight
    // through to the underlying DurableStream -> stream() -> ?key=<conv> on
    // every catch-up request the StateDB issues.
    const db = createStreamDB({
      streamOptions: { url, contentType, params: { key: conv } },
      state: streamState,
      live: false,
    })

    try {
      await db.preload()

      const gotIds = msgs.every((m) => db.collections.messages.get(m.id)?.text === m.text)
      const sizeMatches = db.collections.messages.size === msgs.length
      const noLeakage = otherConvs.every((otherConv) =>
        conversations[otherConv].every(
          (m) => db.collections.messages.get(m.id) === undefined
        )
      )

      if (gotIds && sizeMatches && noLeakage) {
        ok(
          `createStreamDB(key=${conv}) materializes ONLY that conversation's rows`,
          `size=${db.collections.messages.size}`
        )
      } else {
        fail(
          `createStreamDB(key=${conv}) materializes ONLY that conversation's rows`,
          `gotIds=${gotIds} sizeMatches=${sizeMatches} (size=${db.collections.messages.size}, want=${msgs.length}) noLeakage=${noLeakage}`
        )
      }
    } finally {
      db.close()
    }
  }
}

// ============================================================================
// Main
// ============================================================================
async function main() {
  startServer()
  try {
    await waitForServer()
    await testLowLevelKeyedRead()
    await testStreamDBKeyedFold()
  } finally {
    stopServer()
    rmSync(dataDir, { recursive: true, force: true })
  }

  const failures = results.filter((r) => r.status === "FAIL")
  console.log("\n=== SUMMARY ===")
  for (const r of results) {
    console.log(`${r.status === "PASS" ? "  ✓" : "  ✗"} ${r.name}`)
  }
  console.log(
    failures.length === 0
      ? `\nOVERALL: PASS (${results.length}/${results.length})`
      : `\nOVERALL: FAIL (${results.length - failures.length}/${results.length} passed, ${failures.length} failed)`
  )
  process.exitCode = failures.length === 0 ? 0 : 1
}

main().catch((err) => {
  console.error("run.mjs crashed:", err)
  stopServer()
  try {
    rmSync(dataDir, { recursive: true, force: true })
  } catch {}
  process.exitCode = 1
})

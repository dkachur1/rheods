#!/usr/bin/env bash
# Same-machine, same-methodology counterpart to ../bench-keyed.sh, but against
# Prisma's Bun + TypeScript `durable-streams` server (github.com/prisma/streams)
# running in its LOCAL mode (single SQLite, loopback, no object store, no auth
# — see prisma-streams/docs/local-dev.md).
#
# This script is a byte-for-byte port of ../bench-keyed.sh's methodology:
# same run_oha/median/scenario helpers, same cpu_secs (macOS `ps -p PID -o
# time=`), same defaults (DUR=6s REPEATS=3 CONN=64 N=2000 K=50 APPENDSIZE=200),
# same two scenarios (keyed_read = GET ?key=conv-7, full_read = plain GET),
# same body-size measurement. Only the protocol surface differs where Prisma's
# spec requires it:
#   - stream URL is /v1/stream/<name> (not /<name>)
#   - server is started via `bun run src/local/cli.ts start --name ... --port
#     ... --hostname 127.0.0.1`, data root pinned via DS_LOCAL_DATA_ROOT (no
#     --data-dir flag exists; see docs/local-dev.md)
# Stream-Key on append + ?key= on read are zero-config on the `generic`
# profile in BOTH servers — no schema/profile setup needed for either side.
set -u

LABEL="${1:?usage: ./bench-bun-keyed.sh <label>}"
REPO="${REPO:-/Users/danylokachur/durable-streams-keyed/bench/bun/prisma-streams}"
PORT="${PORT:-4900}"
STREAM="convstream"
URL="http://127.0.0.1:${PORT}/v1/stream/${STREAM}"
CONN="${CONN:-64}"
DUR="${DUR:-6s}"
REPEATS="${REPEATS:-3}"
N="${N:-2000}"          # total appends seeded, round-robin across K keys
K="${K:-50}"             # distinct routing keys (conv-0 .. conv-(K-1))
APPENDSIZE="${APPENDSIZE:-200}"  # bytes per append
TESTKEY="${TESTKEY:-conv-$((7 % K))}"  # the single key measured by keyed_read
ROOT=/tmp/ds-bun-bench
OUT="$ROOT/results-${LABEL}.json"
mkdir -p "$ROOT"

for cmd in oha jq curl bun; do
  command -v "$cmd" >/dev/null 2>&1 || { echo "missing dependency: $cmd"; exit 1; }
done
[ -d "$REPO" ] || { echo "missing prisma-streams checkout: $REPO"; exit 1; }

SRV=""
stop_server() { [ -n "$SRV" ] && kill "$SRV" 2>/dev/null; wait "$SRV" 2>/dev/null; SRV=""; }
trap 'stop_server' EXIT

start_server() {  # $1 data root
  local data="$1"
  rm -rf "$data"; mkdir -p "$data"
  local extra_env=()
  [ -n "${DS_READ_MAX_RECORDS:-}" ] && extra_env+=("DS_READ_MAX_RECORDS=$DS_READ_MAX_RECORDS")
  ( cd "$REPO" && env DS_LOCAL_DATA_ROOT="$data" "${extra_env[@]}" \
      bun run src/local/cli.ts start \
      --name "bench-${LABEL}" --hostname 127.0.0.1 --port "$PORT" ) \
    >"$ROOT/server-${LABEL}.log" 2>&1 &
  SRV=$!
  local i
  for i in $(seq 1 100); do
    curl -fsS -X PUT "$URL" -H 'Content-Type: application/octet-stream' >/dev/null 2>&1 && return 0
    sleep 0.1
  done
  echo "server did not become ready"; cat "$ROOT/server-${LABEL}.log"; exit 1
}

# cumulative CPU seconds of the server process (macOS ps TIME = [hh:]mm:ss[.ss])
cpu_secs() {
  local t; t=$(ps -p "$SRV" -o time= 2>/dev/null | tr -d ' ')
  [ -z "$t" ] && { echo 0; return; }
  awk -F: '{ s=0; for(i=1;i<=NF;i++) s=s*60+$i; printf "%.2f", s }' <<<"$t"
}

# median of stdin numbers
median() { sort -n | awk '{a[NR]=$1} END{ if(NR%2){print a[(NR+1)/2]} else {print (a[NR/2]+a[NR/2+1])/2} }'; }

seed_stream() {  # seed N appends of APPENDSIZE bytes, round-robin across K keys
  head -c "$APPENDSIZE" /dev/zero | tr '\0' 'x' > "$ROOT/append.bin"
  echo "  seeding $N appends x ${APPENDSIZE}B across $K keys (conv-0..conv-$((K - 1)))..." >&2
  local progress_every=$((N / 10)); [ "$progress_every" -lt 1 ] && progress_every=1
  local i key
  for ((i = 0; i < N; i++)); do
    key="conv-$((i % K))"
    curl -fsS -X POST "$URL" -H 'Content-Type: application/octet-stream' \
      -H "Stream-Key: $key" --data-binary @"$ROOT/append.bin" >/dev/null
    if (( (i + 1) % progress_every == 0 )); then
      echo "    seeded $((i + 1))/$N..." >&2
    fi
  done
  echo "  seed done." >&2
}

# one-off measurement: status code + body size for a GET, without timing
measure_body() {  # $1 url, $2 outfile -> echoes "status bytes"
  local url="$1" out="$2" status
  status=$(curl -s -o "$out" -w '%{http_code}' "$url")
  echo "$status $(wc -c < "$out" | tr -d ' ')"
}

# run oha once, echo "rps p50ms p99ms success cpu%"
run_oha() {  # $1 method, $2 conn, $3 bodyfile-or-empty, $4 url
  local method="$1" conn="$2" body="$3" url="$4"
  local c0 c1 args=( -z "$DUR" -c "$conn" --no-tui --output-format json -m "$method" )
  [ -n "$body" ] && args+=( -D "$body" -T application/octet-stream )
  c0=$(cpu_secs)
  oha "${args[@]}" "$url" > "$ROOT/oha.json" 2>/dev/null
  c1=$(cpu_secs)
  local secs; secs=$(awk -v d="$DUR" 'BEGIN{gsub(/s/,"",d); print d}')
  jq -r --arg c0 "$c0" --arg c1 "$c1" --arg secs "$secs" '
    .summary.requestsPerSec as $r | .summary.successRate as $sr |
    (.latencyPercentiles.p50*1000) as $p50 | (.latencyPercentiles.p99*1000) as $p99 |
    (((($c1|tonumber)-($c0|tonumber))/($secs|tonumber))*100) as $cpu |
    "\($r) \($p50) \($p99) \($sr) \($cpu)"' "$ROOT/oha.json"
}

# scenario: collect REPEATS runs, emit a JSON object for the results file
scenario() {  # $1 name, $2 method, $3 conn, $4 body, $5 url
  local name="$1" method="$2" conn="$3" body="$4" url="$5"
  local rps=() p50=() p99=() cpu=() sr_min=100 i line
  echo "  -- $name (m=$method c=$conn x$REPEATS) url=$url --" >&2
  for i in $(seq 1 "$REPEATS"); do
    line=$(run_oha "$method" "$conn" "$body" "$url")
    read -r r p5 p9 sr cp <<<"$line"
    rps+=("$r"); p50+=("$p5"); p99+=("$p9"); cpu+=("$cp")
    awk -v a="$sr" -v b="$sr_min" 'BEGIN{exit !(a<b)}' && sr_min="$sr"
    printf "     run %d: %8.0f rps  p50=%.3fms p99=%.3fms  cpu=%.0f%%  ok=%s\n" \
      "$i" "$r" "$p5" "$p9" "$cp" "$sr" >&2
  done
  local mr mp5 mp9 mc
  mr=$(printf '%s\n' "${rps[@]}" | median)
  mp5=$(printf '%s\n' "${p50[@]}" | median)
  mp9=$(printf '%s\n' "${p99[@]}" | median)
  mc=$(printf '%s\n' "${cpu[@]}" | median)
  printf "     MEDIAN: %8.0f rps  p50=%.3fms p99=%.3fms cpu=%.0f%%\n" "$mr" "$mp5" "$mp9" "$mc" >&2
  jq -n --arg n "$name" --argjson rps "$mr" --argjson p50 "$mp5" \
        --argjson p99 "$mp9" --argjson cpu "$mc" --argjson srmin "$sr_min" \
    '{name:$n, rps_median:$rps, p50ms_median:$p50, p99ms_median:$p99, cpu_pct_median:$cpu, success_min:$srmin}'
}

echo "### bench label=$LABEL  repo=$REPO port=$PORT conn=$CONN dur=$DUR x$REPEATS  n=$N k=$K appendsize=${APPENDSIZE}B testkey=$TESTKEY" >&2
RESULTS=()

start_server "$ROOT/data-${LABEL}"
seed_stream

# one-off body-size measurements — NOT timed
read -r KEYED_STATUS KEYED_BYTES <<<"$(measure_body "${URL}?key=${TESTKEY}" "$ROOT/keyed.body")"
read -r FULL_STATUS FULL_BYTES <<<"$(measure_body "$URL" "$ROOT/full.body")"
echo "  body sizes: keyed(key=$TESTKEY) status=$KEYED_STATUS bytes=$KEYED_BYTES | full status=$FULL_STATUS bytes=$FULL_BYTES" >&2

RESULTS+=("$(scenario keyed_read GET "$CONN" "" "${URL}?key=${TESTKEY}")")
RESULTS+=("$(scenario full_read GET "$CONN" "" "$URL")")
stop_server

RATIO=$(awk -v f="$FULL_BYTES" -v k="$KEYED_BYTES" 'BEGIN{ if (k > 0) printf "%.1f", f / k; else print "inf" }')

BODYSIZES=$(jq -n --argjson keyed "$KEYED_BYTES" --argjson full "$FULL_BYTES" \
  --arg keyed_status "$KEYED_STATUS" --arg full_status "$FULL_STATUS" \
  --arg ratio "$RATIO" --arg testkey "$TESTKEY" \
  '{keyed_bytes:$keyed, full_bytes:$full, keyed_status:$keyed_status, full_status:$full_status, full_over_keyed_ratio:$ratio, test_key:$testkey}')

printf '%s\n' "${RESULTS[@]}" | jq -s --arg label "$LABEL" --argjson n "$N" --argjson k "$K" \
  --argjson appendsize "$APPENDSIZE" --argjson body "$BODYSIZES" \
  '{label:$label, n:$n, k:$k, appendsize:$appendsize, body_sizes:$body, scenarios:.}' > "$OUT"
echo "### wrote $OUT" >&2
jq . "$OUT"

echo "" >&2
echo "### SUMMARY: keyed vs full body size ###" >&2
printf "  keyed_read (key=%s):  %8d bytes\n" "$TESTKEY" "$KEYED_BYTES" >&2
printf "  full_read (all):      %8d bytes\n" "$FULL_BYTES" >&2
printf "  full / keyed ratio:   %8s   (expected ~%s if keying is fully effective, K=%s)\n" \
  "$RATIO" "$K" "$K" >&2

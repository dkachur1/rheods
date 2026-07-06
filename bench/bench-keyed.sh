#!/usr/bin/env bash
# Local regression harness (macOS dev box) for the KEYED-READ feature — the
# ?key=<key> filter that's supposed to let a client pull just ONE conversation
# out of a stream that interleaves MANY conversations, instead of reading the
# whole stream and filtering client-side.
#
# Sibling of .bench-local.sh (vendor/durable-streams-0.1.2/.bench-local.sh) —
# same conventions: same server start/stop, run_oha/median/scenario helpers,
# same RELATIVE-numbers caveat (client+server share cores, no cgroup pinning;
# baseline-vs-change on the SAME machine only).
#
# HONESTY NOTE — read this before trusting a result:
#   This script only produces a MEANINGFUL comparison if the server actually
#   implements `?key=` filtering server-side. Against an UNPATCHED server that
#   parses-and-ignores `?key=` (or doesn't recognize it at all), BOTH
#   scenarios below hit the exact same code path and return the FULL stream —
#   so `keyed_bytes` and `full_bytes` will come out EQUAL. That is not a
#   trick, it's the point: equal body sizes is itself the useful signal that
#   keyed filtering is NOT wired up. Only `keyed_bytes` << `full_bytes`
#   (roughly full/K) means the index is actually doing something. The script
#   prints both sizes and their ratio prominently in the summary so this is
#   never ambiguous.
#
# Usage:  ./bench-keyed.sh <label>        # e.g. baseline | with-index
# Output: /tmp/ds-bench/results-<label>.json  (+ a human summary on stdout)
#
# Scenarios:
#   keyed_read              GET /<stream>?key=<key>   — indexed/filtered read
#                            of ONE conversation's appends out of K interleaved
#   full_read_client_filter  GET /<stream>             — status-quo workaround:
#                            fetch the whole stream, client filters locally
set -u

LABEL="${1:?usage: ./bench-keyed.sh <label>}"
BIN="${BIN:-/tmp/ds-bench-target/release/durable-streams-server}"
PORT="${PORT:-4712}"
STREAM="convstream"
URL="http://127.0.0.1:${PORT}/${STREAM}"
CONN="${CONN:-64}"
DUR="${DUR:-6s}"
REPEATS="${REPEATS:-3}"
N="${N:-2000}"          # total appends seeded, round-robin across K keys
K="${K:-50}"             # distinct routing keys (conv-0 .. conv-(K-1))
APPENDSIZE="${APPENDSIZE:-200}"  # bytes per append
TESTKEY="${TESTKEY:-conv-$((7 % K))}"  # the single key measured by keyed_read
ROOT=/tmp/ds-bench
OUT="$ROOT/results-${LABEL}.json"
mkdir -p "$ROOT"

for cmd in oha jq curl; do
  command -v "$cmd" >/dev/null 2>&1 || { echo "missing dependency: $cmd"; exit 1; }
done
[ -x "$BIN" ] || { echo "missing binary: $BIN"; exit 1; }

SRV=""
stop_server() { [ -n "$SRV" ] && kill "$SRV" 2>/dev/null; wait "$SRV" 2>/dev/null; SRV=""; }
trap 'stop_server' EXIT

start_server() {  # extra args passed through
  local data="$1"; shift
  rm -rf "$data"; mkdir -p "$data"
  "$BIN" --host 127.0.0.1 --port "$PORT" --data-dir "$data" "$@" \
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

echo "### bench label=$LABEL  bin=$BIN  conn=$CONN dur=$DUR x$REPEATS  n=$N k=$K appendsize=${APPENDSIZE}B testkey=$TESTKEY" >&2
RESULTS=()

start_server "$ROOT/data-keyed"
seed_stream

# one-off body-size measurements — NOT timed, this is the "how much data comes
# back" comparison, independent of throughput
read -r KEYED_STATUS KEYED_BYTES <<<"$(measure_body "${URL}?key=${TESTKEY}" "$ROOT/keyed.body")"
read -r FULL_STATUS FULL_BYTES <<<"$(measure_body "$URL" "$ROOT/full.body")"
echo "  body sizes: keyed(key=$TESTKEY) status=$KEYED_STATUS bytes=$KEYED_BYTES | full status=$FULL_STATUS bytes=$FULL_BYTES" >&2

RESULTS+=("$(scenario keyed_read GET "$CONN" "" "${URL}?key=${TESTKEY}")")
RESULTS+=("$(scenario full_read_client_filter GET "$CONN" "" "$URL")")
stop_server

RATIO=$(awk -v f="$FULL_BYTES" -v k="$KEYED_BYTES" 'BEGIN{ if (k > 0) printf "%.1f", f / k; else print "inf" }')
EXPECTED_RATIO="$K"

BODYSIZES=$(jq -n --argjson keyed "$KEYED_BYTES" --argjson full "$FULL_BYTES" \
  --arg keyed_status "$KEYED_STATUS" --arg full_status "$FULL_STATUS" \
  --arg ratio "$RATIO" --arg testkey "$TESTKEY" \
  '{keyed_bytes:$keyed, full_bytes:$full, keyed_status:$keyed_status, full_status:$full_status, full_over_keyed_ratio:$ratio, test_key:$testkey}')

# assemble results file
printf '%s\n' "${RESULTS[@]}" | jq -s --arg label "$LABEL" --argjson n "$N" --argjson k "$K" \
  --argjson appendsize "$APPENDSIZE" --argjson body "$BODYSIZES" \
  '{label:$label, n:$n, k:$k, appendsize:$appendsize, body_sizes:$body, scenarios:.}' > "$OUT"
echo "### wrote $OUT" >&2
jq . "$OUT"

echo "" >&2
echo "### SUMMARY: keyed vs full body size (this is the win, or the lack of one) ###" >&2
printf "  keyed_read (key=%s):            %8d bytes\n" "$TESTKEY" "$KEYED_BYTES" >&2
printf "  full_read_client_filter (all):   %8d bytes\n" "$FULL_BYTES" >&2
printf "  full / keyed ratio:              %8s   (expected ~%s if keying is fully effective, K=%s)\n" \
  "$RATIO" "$EXPECTED_RATIO" "$K" >&2
if [ "$KEYED_BYTES" = "$FULL_BYTES" ]; then
  echo "  >>> EQUAL SIZES: keyed filtering does NOT appear to be active — the server" >&2
  echo "  >>> returned the full stream for both requests (?key= ignored or unsupported)." >&2
elif awk -v r="$RATIO" 'BEGIN{exit !(r >= 2)}' 2>/dev/null; then
  echo "  >>> keyed_read returns substantially less data than the full-stream read —" >&2
  echo "  >>> keying appears to be working." >&2
else
  echo "  >>> keyed_read returns less data, but not by much — check whether ?key=" >&2
  echo "  >>> filtering is fully wired or only partially effective." >&2
fi

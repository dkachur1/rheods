#!/usr/bin/env bash
# Base-path (unkeyed) counterpart to vendor/durable-streams-0.1.2/.bench-local.sh's
# read1k / append100 scenarios, run against Prisma's Bun local-mode server for a
# same-machine unkeyed number alongside the keyed comparison in bench-bun-keyed.sh.
# No Stream-Key on these appends, no tiering (Bun local mode has no tier concept).
set -u

LABEL="${1:?usage: ./bench-bun-base.sh <label>}"
REPO="${REPO:-/Users/danylokachur/durable-streams-keyed/bench/bun/prisma-streams}"
PORT="${PORT:-4900}"
STREAM="basestream"
URL="http://127.0.0.1:${PORT}/v1/stream/${STREAM}"
CONN="${CONN:-64}"
DUR="${DUR:-6s}"
REPEATS="${REPEATS:-3}"
ROOT=/tmp/ds-bun-bench
OUT="$ROOT/results-${LABEL}.json"
mkdir -p "$ROOT"

for cmd in oha jq curl bun; do
  command -v "$cmd" >/dev/null 2>&1 || { echo "missing dependency: $cmd"; exit 1; }
done

SRV=""
stop_server() { [ -n "$SRV" ] && kill "$SRV" 2>/dev/null; wait "$SRV" 2>/dev/null; SRV=""; }
trap 'stop_server' EXIT

start_server() {  # $1 data root
  local data="$1"
  rm -rf "$data"; mkdir -p "$data"
  ( cd "$REPO" && env DS_LOCAL_DATA_ROOT="$data" bun run src/local/cli.ts start \
      --name "base-${LABEL}" --hostname 127.0.0.1 --port "$PORT" ) \
    >"$ROOT/server-base-${LABEL}.log" 2>&1 &
  SRV=$!
  local i
  for i in $(seq 1 100); do
    curl -fsS -X PUT "$URL" -H 'Content-Type: application/octet-stream' >/dev/null 2>&1 && return 0
    sleep 0.1
  done
  echo "server did not become ready"; cat "$ROOT/server-base-${LABEL}.log"; exit 1
}

cpu_secs() {
  local t; t=$(ps -p "$SRV" -o time= 2>/dev/null | tr -d ' ')
  [ -z "$t" ] && { echo 0; return; }
  awk -F: '{ s=0; for(i=1;i<=NF;i++) s=s*60+$i; printf "%.2f", s }' <<<"$t"
}

median() { sort -n | awk '{a[NR]=$1} END{ if(NR%2){print a[(NR+1)/2]} else {print (a[NR/2]+a[NR/2+1])/2} }'; }

seed_bytes() {  # $1 byte count, written as one POST, verify via GET
  head -c "$1" /dev/zero | tr '\0' 'x' > "$ROOT/seed.bin"
  curl -fsS -X POST "$URL" -H 'Content-Type: application/octet-stream' --data-binary @"$ROOT/seed.bin" >/dev/null
  local got; got=$(curl -fsS "$URL" | wc -c | tr -d ' ')
  [ "$got" = "$1" ] || { echo "seed mismatch: wanted $1 got $got"; exit 1; }
}

run_oha() {
  local method="$1" conn="$2" body="$3"
  local c0 c1 args=( -z "$DUR" -c "$conn" --no-tui --output-format json -m "$method" )
  [ -n "$body" ] && args+=( -D "$body" -T application/octet-stream )
  c0=$(cpu_secs)
  oha "${args[@]}" "$URL" > "$ROOT/oha.json" 2>/dev/null
  c1=$(cpu_secs)
  local secs; secs=$(awk -v d="$DUR" 'BEGIN{gsub(/s/,"",d); print d}')
  jq -r --arg c0 "$c0" --arg c1 "$c1" --arg secs "$secs" '
    .summary.requestsPerSec as $r | .summary.successRate as $sr |
    (.latencyPercentiles.p50*1000) as $p50 | (.latencyPercentiles.p99*1000) as $p99 |
    (((($c1|tonumber)-($c0|tonumber))/($secs|tonumber))*100) as $cpu |
    "\($r) \($p50) \($p99) \($sr) \($cpu)"' "$ROOT/oha.json"
}

scenario() {
  local name="$1" method="$2" conn="$3" body="$4"
  local rps=() p50=() p99=() cpu=() sr_min=100 i line
  echo "  -- $name (m=$method c=$conn x$REPEATS) --" >&2
  for i in $(seq 1 "$REPEATS"); do
    line=$(run_oha "$method" "$conn" "$body")
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

echo "### bench label=$LABEL  repo=$REPO port=$PORT conn=$CONN dur=$DUR x$REPEATS (unkeyed base path)" >&2
RESULTS=()

# read1k — no tier, no keys
start_server "$ROOT/data-base-${LABEL}-r1k" ; seed_bytes 1024
RESULTS+=("$(scenario read1k GET "$CONN" "")")
stop_server

# append100 — no tier, no keys
head -c 100 /dev/zero | tr '\0' 'x' > "$ROOT/append100.bin"
start_server "$ROOT/data-base-${LABEL}-a100"
RESULTS+=("$(scenario append100 POST "$CONN" "$ROOT/append100.bin")")
stop_server

printf '%s\n' "${RESULTS[@]}" | jq -s --arg label "$LABEL" '{label:$label, scenarios:.}' > "$OUT"
echo "### wrote $OUT" >&2
jq . "$OUT"

#!/usr/bin/env bash
# M5 hot-path drills: latency under load, the read-after-write race, broker loss, and the
# ledger under crashes and a Postgres kill. Run from the repository root with the compose stack
# up and the venv active (pip install -e ".[dev,streaming,serving]"). Needs
# data/processed/events_sorted.parquet and models/2e632818a205. Takes about 40 minutes.
#
# Uses Redis database 10 (emptied), empties the Postgres ledger tables, and creates a per-run
# topic. Every load run uses its own 1/64 card slice, so runs never share a card.
#
# STEPS selects the steps to run (default: all). Step 2 (setup) is needed by steps 3-8, for
# example: STEPS="2 7" bash scripts/m5_drills.sh
set -euo pipefail

set -a
if [ -f .env ]; then . ./.env; fi
set +a
OUT=notes/m5
RUN=$(date +%s)
TOPIC="m5-$RUN"
REDIS=redis://127.0.0.1:6379/10
MODEL=${MODEL:-models/2e632818a205}
PG_USER=${POSTGRES_USER:-finspeq}
PG_DB=${POSTGRES_DB:-finspeq}
PG_PORT=${FINSPEQ_POSTGRES_PORT:-15432}
DSN="postgresql://$PG_USER:${POSTGRES_PASSWORD:-finspeq}@127.0.0.1:$PG_PORT/$PG_DB"
WARMUP=2026-03-09
FROM=2026-03-10
TO=2026-06-14
PORT=${FINSPEQ_API_PORT_BASE:-18000}  # API ports used: PORT .. PORT+31
mkdir -p "$OUT"
PYTHON=$(python -c 'import sys; print(sys.executable)')

STEPS=${STEPS:-1 2 3 4 5 6 7 8}
slice=0
step() { printf '\n=== %s\n' "$1"; }
want() { [[ " $STEPS " == *" $1 "* ]]; }
stop() {  # stop <pid>: end a background process and its children (Windows-aware)
  if [ -r "/proc/$1/winpid" ] && command -v taskkill >/dev/null; then
    taskkill //F //T //PID "$(cat "/proc/$1/winpid")" >/dev/null 2>&1 || true
  else
    kill "$1" 2>/dev/null || true
  fi
  wait "$1" 2>/dev/null || true
}
BACKGROUND=()
cleanup() {  # stop every background process this script started, even after a failure
  for pid in "${BACKGROUND[@]}"; do stop "$pid"; done
}
trap cleanup EXIT
serve() {  # serve <port> <log> [flags...]: start the API in the background, wait until ready
  local port=$1 log=$2
  shift 2
  if curl -s -o /dev/null "http://127.0.0.1:$port/"; then
    echo "port $port is already in use; set FINSPEQ_API_PORT_BASE to a free range" >&2
    return 1
  fi
  finspeq-serve --model-dir "$MODEL" --port "$port" --topic "$TOPIC" --redis-url "$REDIS" \
    "$@" >"$log" 2>&1 &
  API_PID=$!
  BACKGROUND+=("$API_PID")
  for _ in $(seq 1 120); do
    if curl -sf "http://127.0.0.1:$port/readyz" >/dev/null; then return 0; fi
    sleep 1
  done
  echo "API on port $port did not become ready; see $log" >&2
  return 1
}
load() {  # load <port> <output> [flags...]: one load-test run against the API on port
  local port=$1 output=$2
  shift 2
  "$PYTHON" scripts/load_test.py --url "http://127.0.0.1:$port" --from-date "$FROM" \
    --to-date "$TO" --warmup-from "$WARMUP" --output "$output" "$@"
}

if want 1; then
  step "1/8 how close together a card's transactions are in the data"
  "$PYTHON" scripts/card_gaps.py --output "$OUT/card_gaps.json"
fi

if want 2; then
  step "2/8 topic, empty stores, feature worker, one warm-up day"
  docker compose exec -T redpanda rpk topic create "$TOPIC" -p 4 -c write.caching=false
  docker compose exec -T redis redis-cli -n 10 FLUSHDB >/dev/null
  docker compose exec -T postgres psql -q -U "$PG_USER" -d "$PG_DB" \
    -c "TRUNCATE decisions, consumer_positions"
  finspeq-stream worker --topic "$TOPIC" --group "m5-features-$RUN" --redis-url "$REDIS" \
    --threads 4 --start-timeout-s 1200 --idle-exit-s 1200 >"$OUT/feature_worker.json" 2>&1 &
  WORKER_PID=$!
  BACKGROUND+=("$WORKER_PID")
  finspeq-stream simulate --from-date "$WARMUP" --to-date "$FROM" --topic "$TOPIC" \
    --chunk-size 2000 | tee "$OUT/warmup_simulate.json"
  "$PYTHON" scripts/lag_monitor.py --topic "$TOPIC" --partitions 4 --redis-url "$REDIS" \
    --output "$OUT/warmup_lag.json" --settle-s 3 --duration-s 300 >/dev/null
fi

if want 3; then
  step "3/8 latency at 100, 250, 500, 750 requests/s (1 API process, linger 1 ms)"
  serve "$PORT" "$OUT/api_rate.log" --linger-ms 1
  for rate in 100 250 500 750; do
    load "$PORT" "$OUT/rate_$rate.json" --plan rate --rate "$rate" --limit $((rate * 30)) \
      --slice "64:$slice"
    slice=$((slice + 1))
  done
  stop "$API_PID"
fi

if want 4; then
  step "4/8 uvicorn workers 2 and 4 at 500 requests/s"
  for workers in 2 4; do
    serve $((PORT + workers)) "$OUT/api_workers_$workers.log" --workers "$workers" \
    --linger-ms 1
    load $((PORT + workers)) "$OUT/workers_${workers}_500.json" --plan rate --rate 500 \
      --limit 15000 --slice "64:$slice"
    slice=$((slice + 1))
    stop "$API_PID"
  done
fi

if want 5; then
  step "5/8 producer linger 0 and 5 ms at 250 requests/s"
  for linger in 0 5; do
    serve $((PORT + 10 + linger)) "$OUT/api_linger_$linger.log" --linger-ms "$linger"
    load $((PORT + 10 + linger)) "$OUT/linger_${linger}_250.json" --plan rate --rate 250 \
      --limit 7500 --slice "64:$slice"
    slice=$((slice + 1))
    stop "$API_PID"
  done
fi

if want 6; then
  step "6/8 the race: same-card bursts, single writer vs read-your-writes"
  serve $((PORT + 20)) "$OUT/api_bursts_single.log" --linger-ms 1
  SINGLE_PID=$API_PID
  serve $((PORT + 21)) "$OUT/api_bursts_ryw.log" --read-your-writes --linger-ms 1
  RYW_PID=$API_PID
  for gap in 10 50 100 250 500; do
    for mode in single ryw; do
      port=$((PORT + 20))
      if [ "$mode" = ryw ]; then port=$((PORT + 21)); fi
      load "$port" "$OUT/bursts_${mode}_$gap.json" --plan bursts --gap-ms "$gap" \
        --burst-size 10 --limit 150 --slice "64:$slice"
      slice=$((slice + 1))
    done
  done
  stop "$SINGLE_PID"
  stop "$RYW_PID"
fi

if want 7; then
  step "7/8 broker down for 10 s during 200 requests/s"
  serve $((PORT + 30)) "$OUT/api_broker_down.log"
  rm -f "$OUT/broker_down.started"
  (while [ ! -f "$OUT/broker_down.started" ]; do sleep 0.2; done \
    && sleep 20 && docker compose stop redpanda >/dev/null && echo "redpanda stopped at +20 s" \
    && sleep 10 && docker compose start redpanda >/dev/null && echo "redpanda started at +30 s") &
  OUTAGE_PID=$!
  load $((PORT + 30)) "$OUT/broker_down.json" --plan rate --rate 200 --limit 18000 \
    --slice "64:$slice" --ready-file "$OUT/broker_down.started"
  wait "$OUTAGE_PID"
  stop "$API_PID"
fi

if want 8; then
  step "8/8 ledger: record every scored event with crashes and a Postgres SIGKILL"
  (sleep 8 && docker compose kill -s SIGKILL postgres >/dev/null && echo "postgres killed" \
    && docker compose start postgres >/dev/null && echo "postgres started") &
  KILL_PID=$!
  "$PYTHON" scripts/crash_drill.py --report "$OUT/ledger_crash_drill.json" --topic "$TOPIC" \
    --partitions 4 --group "m5-ledger-$RUN" --max-failures 10 -- \
    finspeq-stream ledger --topic "$TOPIC" --group "m5-ledger-$RUN" --dsn "$DSN" \
    --batch-size 200 --idle-exit-s 15 --crash-point after-apply --crash-probability 0.05
  wait "$KILL_PID"
  finspeq-stream verify-ledger --topic "$TOPIC" --partitions 4 --dsn "$DSN" \
    | tee "$OUT/ledger_verify.json"
  docker compose exec -T postgres psql -q -U "$PG_USER" -d "$PG_DB" -At \
    -c "SELECT count(*), count(DISTINCT tx_id) FROM decisions" | tee "$OUT/ledger_counts.txt"
fi
if [ -n "${WORKER_PID:-}" ]; then stop "$WORKER_PID"; fi

step "done: reports in $OUT/"

#!/usr/bin/env bash
# M4 scaling drills. Run from the repository root with the compose stack up and the venv
# active. Needs data/processed/events_sorted.parquet and models/2e632818a205.
#
# Uses Redis database 9 (emptied before every run) and per-run topics and groups, so the
# script can be rerun. Reports: notes/m4/. Takes about 45 minutes.
set -euo pipefail

OUT=notes/m4
RUN=$(date +%s)
DB=9
REDIS=redis://127.0.0.1:6379/$DB
MODEL=${MODEL:-models/2e632818a205}
SMALL=(--from-date 2026-03-01 --to-date 2026-06-15 --slice 20:0)
LARGE=(--from-date 2026-03-01 --to-date 2026-06-15 --slice 4:0)
mkdir -p "$OUT"
PYTHON=$(python -c 'import sys; print(sys.executable)')

flush() { docker compose exec -T redis redis-cli -n "$DB" FLUSHDB >/dev/null; }
step() { printf '\n=== %s\n' "$1"; }
topic() { docker compose exec -T redpanda rpk topic create "$1" -p "$2" -c write.caching=false; }
drop() { docker compose exec -T redpanda rpk topic delete "$1" >/dev/null; }
monitor() {  # monitor <topic> <partitions> <output> [settle s]: lag sampler in the background
  "$PYTHON" scripts/lag_monitor.py --topic "$1" --partitions "$2" --redis-url "$REDIS" \
    --output "$3" --duration-s 900 --settle-s "${4:-5}" >/dev/null &
}
worker() {  # worker <topic> <group> <output> [extra flags...]: one worker in the background
  local t=$1 g=$2 o=$3
  shift 3
  finspeq-stream worker --topic "$t" --group "$g" --redis-url "$REDIS" \
    --idle-exit-s 10 "$@" >"$o" &
}

step "1/8 partitioner: broker partitions equal Python murmur2"
FINSPEQ_KAFKA_BOOTSTRAP=127.0.0.1:19092 FINSPEQ_REDIS_URL=redis://127.0.0.1:6379/0 \
  "$PYTHON" -m pytest -q -m integration tests/integration -k murmur2 | tee "$OUT/partitioner.txt"

step "2/8 keyed vs round-robin publishing, 4 partitions, 4 workers"
for mode in keyed round-robin; do
  t="m4-$mode-$RUN"
  topic "$t" 4
  flags=()
  if [ "$mode" = round-robin ]; then flags=(--round-robin-partitions 4); fi
  finspeq-stream simulate "${SMALL[@]}" --topic "$t" --chunk-size 2000 "${flags[@]}" \
    >"$OUT/${mode}_simulate.json"
  flush
  for i in 0 1 2 3; do worker "$t" "m4-$mode-$RUN" "$OUT/${mode}_worker_$i.json"; done
  wait
  finspeq-stream verify-state "${SMALL[@]}" --topic "$t" --partitions 4 --redis-url "$REDIS" \
    | tee "$OUT/${mode}_verify.json"
  drop "$t"
done
finspeq-offline lateness-sweep "${SMALL[@]}" --lateness 3600 --round-robin-partitions 4 \
  --consumer-batch-size 500 --model-dir "$MODEL" --output "$OUT/round_robin_sweep.json" \
  >/dev/null

step "3/8 load the scaling topics (slice 4:0, 4 and 8 partitions)"
for p in 4 8; do
  topic "m4-p$p-$RUN" "$p"
  finspeq-stream simulate "${LARGE[@]}" --topic "m4-p$p-$RUN" --chunk-size 5000 \
    --linger-ms 50 | tee "$OUT/load_p${p}_simulate.json"
done

step "4/8 scaling: workers per partition count, measured by the lag monitor"
for run in "4 1" "4 2" "4 4" "8 1" "8 2" "8 4" "8 8" "4 threads"; do
  read -r p n <<<"$run"
  t="m4-p$p-$RUN"
  name="scale_p${p}_n$n"
  flush
  monitor "$t" "$p" "$OUT/${name}_lag.json"
  if [ "$n" = threads ]; then
    worker "$t" "$name-$RUN" "$OUT/${name}_worker_0.json" --threads 4
  else
    for ((i = 0; i < n; i++)); do worker "$t" "$name-$RUN" "$OUT/${name}_worker_$i.json"; done
  fi
  if [ "$name" = scale_p4_n4 ]; then
    docker compose exec -T redis timeout 15 redis-cli --latency-history -i 5 \
      >"$OUT/redis_latency_history.txt" 2>&1 || true
  fi
  wait
  if [ "$name" = scale_p4_n4 ]; then
    docker compose exec -T redis redis-cli INFO memory >"$OUT/redis_info_memory.txt"
    docker compose exec -T redis redis-cli INFO commandstats >"$OUT/redis_commandstats.txt"
  fi
  printf '%s: ' "$name"
  "$PYTHON" -c 'import json,sys; s=json.load(open(sys.argv[1]))["summary"]; print(s)' \
    "$OUT/${name}_lag.json"
  if [ "$n" = 4 ] || [ "$n" = 8 ]; then
    finspeq-stream verify-state "${LARGE[@]}" --topic "$t" --partitions "$p" \
      --redis-url "$REDIS" | tee "$OUT/${name}_verify.json"
  fi
done

step "5/8 batch-size sweep: one worker, 4 partitions"
for size in 50 200 500 2000; do
  flush
  finspeq-stream worker --topic "m4-p4-$RUN" --group "batch-$size-$RUN" --redis-url "$REDIS" \
    --idle-exit-s 10 --batch-size "$size" | tee "$OUT/batch_$size.json"
done
if command -v py-spy >/dev/null; then
  flush
  py-spy record --subprocesses -o "$OUT/profile_worker.svg" -- finspeq-stream worker \
    --topic "m4-p4-$RUN" --group "profile-$RUN" --redis-url "$REDIS" --idle-exit-s 10 \
    >/dev/null || true
fi

step "6/8 rebalances: join, join, leave; range vs cooperative-sticky"
for strategy in range cooperative-sticky; do
  name="rebalance_$strategy"
  flush
  monitor "m4-p4-$RUN" 4 "$OUT/${name}_lag.json"
  flags=(--assignment-strategy "$strategy" --batch-size 100)
  worker "m4-p4-$RUN" "$name-$RUN" "$OUT/${name}_a.json" "${flags[@]}"
  sleep 5
  worker "m4-p4-$RUN" "$name-$RUN" "$OUT/${name}_b.json" "${flags[@]}" --max-batches 300
  sleep 5
  worker "m4-p4-$RUN" "$name-$RUN" "$OUT/${name}_c.json" "${flags[@]}"
  wait
  "$PYTHON" scripts/rebalance_report.py --output "$OUT/${name}_pauses.json" \
    "$OUT/${name}_a.json" "$OUT/${name}_b.json" "$OUT/${name}_c.json"
  finspeq-stream verify-state "${LARGE[@]}" --topic "m4-p4-$RUN" --partitions 4 \
    --redis-url "$REDIS" | tee "$OUT/${name}_verify.json"
done

step "7/8 zombie: a worker stalls past max.poll.interval.ms, then applies its stale batch"
flush
zombie=(--max-poll-interval-ms 10000 --batch-size 200)
worker "m4-p4-$RUN" "zombie-$RUN" "$OUT/zombie_stalled.json" "${zombie[@]}" \
  --stall-s 20 --stall-at-batch 30 --idle-exit-s 30
worker "m4-p4-$RUN" "zombie-$RUN" "$OUT/zombie_peer.json" "${zombie[@]}" --idle-exit-s 30
wait
"$PYTHON" scripts/rebalance_report.py --output "$OUT/zombie_pauses.json" \
  "$OUT/zombie_stalled.json" "$OUT/zombie_peer.json"
finspeq-stream verify-state "${LARGE[@]}" --topic "m4-p4-$RUN" --partitions 4 \
  --redis-url "$REDIS" | tee "$OUT/zombie_verify.json"

step "8/8 live load: publish at a target rate while 4 workers consume"
for rate in 5000 10000; do
  t="m4-live-$rate-$RUN"
  topic "$t" 4
  flush
  monitor "$t" 4 "$OUT/live_${rate}_lag.json" 20
  for i in 0 1 2 3; do
    worker "$t" "live-$rate-$RUN" "$OUT/live_${rate}_worker_$i.json" --start-timeout-s 120 \
      --idle-exit-s 15
  done
  finspeq-stream simulate "${LARGE[@]}" --topic "$t" --limit 300000 --rate "$rate" \
    --chunk-size 1000 --linger-ms 20 | tee "$OUT/live_${rate}_simulate.json"
  wait
  "$PYTHON" -c 'import json,sys; print(json.load(open(sys.argv[1]))["summary"])' \
    "$OUT/live_${rate}_lag.json"
  drop "$t"
done
drop "m4-p4-$RUN"
drop "m4-p8-$RUN"

step "done: reports in $OUT/"

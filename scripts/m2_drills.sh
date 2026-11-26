#!/usr/bin/env bash
# M2 recovery drills. Run from the repository root with the compose stack up, the venv active,
# and the M1 slice already in the `transactions` topic (finspeq-stream simulate ... --slice 20:0).
#
# Uses Redis databases 2-6 (each emptied first) and never touches database 0. Consumer groups
# and temporary topics get a per-run suffix, so the script can be rerun. Reports: notes/m2/.
set -euo pipefail

OUT=notes/m2
RUN=$(date +%s)
REDIS=redis://127.0.0.1:6379
CRASH_P=${CRASH_P:-0.05}
RANGE=(--from-date 2026-03-01 --to-date 2026-06-15 --slice 20:0)
mkdir -p "$OUT"
# Absolute interpreter path: on Windows a bare "python" in a child process can resolve to the
# base interpreter instead of the venv.
PYTHON=$(python -c 'import sys; print(sys.executable)')

flush() { docker compose exec -T redis redis-cli -n "$1" FLUSHDB >/dev/null; }
step() { printf '\n=== %s\n' "$1"; }

step "1/6 naive counter, crash after apply (p=$CRASH_P per batch)"
flush 2
"$PYTHON" scripts/crash_drill.py --report "$OUT/naive_crash_drill.json" \
  --group "naive-$RUN" --redis-url "$REDIS/2" -- \
  "$PYTHON" experiments/naive_counter.py run --redis-url "$REDIS/2" --group "naive-$RUN" \
  --instance-id naive-0 --crash-point after-apply --crash-probability "$CRASH_P"
"$PYTHON" experiments/naive_counter.py compare "${RANGE[@]}" --redis-url "$REDIS/2" \
  | tee "$OUT/naive_compare.json"

step "2/6 feature worker, crash after apply (p=$CRASH_P per batch)"
flush 3
"$PYTHON" scripts/crash_drill.py --report "$OUT/worker_crash_drill.json" \
  --group "drill-$RUN" --redis-url "$REDIS/3" -- \
  finspeq-stream worker --group "drill-$RUN" --redis-url "$REDIS/3" --instance-id drill-0 \
  --batch-size 50 --idle-exit-s 10 --crash-point after-apply --crash-probability "$CRASH_P"
finspeq-stream verify-state "${RANGE[@]}" --redis-url "$REDIS/3" \
  | tee "$OUT/worker_crash_verify.json"

step "3/6 Redis SIGKILL mid-run (AOF everysec), worker restarted by the drill"
flush 4
"$PYTHON" scripts/crash_drill.py --report "$OUT/redis_kill_drill.json" \
  --group "kill-$RUN" --redis-url "$REDIS/4" --kill-redis-after-s 6 -- \
  finspeq-stream worker --group "kill-$RUN" --redis-url "$REDIS/4" --instance-id kill-0 \
  --batch-size 50 --idle-exit-s 10
finspeq-stream verify-state "${RANGE[@]}" --redis-url "$REDIS/4" \
  | tee "$OUT/redis_kill_verify.json"

step "4/6 total loss: rebuild an empty store from offset 0 (appendfsync everysec)"
flush 5
finspeq-stream worker --group "rebuild-$RUN" --redis-url "$REDIS/5" --idle-exit-s 10 \
  | tee "$OUT/rebuild_worker.json"
finspeq-stream verify-state "${RANGE[@]}" --redis-url "$REDIS/5" \
  | tee "$OUT/rebuild_verify.json"

step "5/6 the same rebuild with appendfsync always"
flush 6
docker compose exec -T redis redis-cli CONFIG SET appendfsync always
finspeq-stream worker --group "aof-always-$RUN" --redis-url "$REDIS/6" --idle-exit-s 10 \
  | tee "$OUT/aof_always_worker.json"
docker compose exec -T redis redis-cli CONFIG SET appendfsync everysec

step "6/6 publish rate with broker write caching off and on"
for caching in false true; do
  topic="wc-$caching-$RUN"
  docker compose exec -T redpanda rpk topic create "$topic" -p 1 -c "write.caching=$caching"
  finspeq-stream simulate "${RANGE[@]}" --topic "$topic" \
    | tee "$OUT/simulate_write_caching_$caching.json"
  docker compose exec -T redpanda rpk topic delete "$topic"
done

step "done: reports in $OUT/"

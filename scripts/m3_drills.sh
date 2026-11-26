#!/usr/bin/env bash
# M3 disorder and lateness drills. Run from the repository root with the compose stack up and
# the venv active. Needs data/processed/events_sorted.parquet and models/2e632818a205.
#
# Uses Redis databases 7-8 (each emptied first) and per-run topics and groups, so the script
# can be rerun. Reports: notes/m3/.
set -euo pipefail

OUT=notes/m3
RUN=$(date +%s)
REDIS=redis://127.0.0.1:6379
MODEL=${MODEL:-models/2e632818a205}
CRASH_P=${CRASH_P:-0.05}
RANGE=(--from-date 2026-03-01 --to-date 2026-06-15 --slice 20:0)
MILD=(--delayed-fraction 0.1 --mean-delay-s 300 --max-delay-s 7200
  --duplicate-fraction 0.01 --disorder-seed 1)
HEAVY=(--delayed-fraction 0.2 --mean-delay-s 3600 --max-delay-s 86400
  --duplicate-fraction 0.01 --disorder-seed 2)
mkdir -p "$OUT"
# Absolute interpreter path: on Windows a bare "python" in a child process can resolve to the
# base interpreter instead of the venv.
PYTHON=$(python -c 'import sys; print(sys.executable)')

flush() { docker compose exec -T redis redis-cli -n "$1" FLUSHDB >/dev/null; }
step() { printf '\n=== %s\n' "$1"; }

step "1/4 lateness sweep, mild disorder (10% delayed, mean 5 min, cap 2 h)"
finspeq-offline lateness-sweep "${RANGE[@]}" "${MILD[@]}" --model-dir "$MODEL" \
  --output "$OUT/lateness_sweep_mild.json" >/dev/null

step "2/4 lateness sweep, heavy disorder (20% delayed, mean 1 h, cap 24 h)"
finspeq-offline lateness-sweep "${RANGE[@]}" "${HEAVY[@]}" --model-dir "$MODEL" \
  --output "$OUT/lateness_sweep_heavy.json" >/dev/null

for lateness in 0 3600; do
  db=$((lateness == 0 ? 7 : 8))
  topic="m3-heavy-$lateness-$RUN"
  step "$((lateness == 0 ? 3 : 4))/4 heavy disorder via the broker, lateness ${lateness}s"
  flush "$db"
  docker compose exec -T redpanda rpk topic create "$topic" -p 1 -c write.caching=false
  finspeq-stream simulate "${RANGE[@]}" "${HEAVY[@]}" --topic "$topic" \
    | tee "$OUT/heavy_${lateness}_simulate.json"
  "$PYTHON" scripts/crash_drill.py --report "$OUT/heavy_${lateness}_crash_drill.json" \
    --topic "$topic" --group "m3-$lateness-$RUN" --redis-url "$REDIS/$db" -- \
    finspeq-stream worker --topic "$topic" --group "m3-$lateness-$RUN" \
    --redis-url "$REDIS/$db" --instance-id "m3-$lateness" --lateness-s "$lateness" \
    --batch-size 50 --idle-exit-s 10 --crash-point after-apply --crash-probability "$CRASH_P"
  finspeq-stream verify-state "${RANGE[@]}" "${HEAVY[@]}" --topic "$topic" \
    --redis-url "$REDIS/$db" --lateness-s "$lateness" \
    | tee "$OUT/heavy_${lateness}_verify.json"
  docker compose exec -T redpanda rpk topic delete "$topic"
done

step "done: reports in $OUT/"

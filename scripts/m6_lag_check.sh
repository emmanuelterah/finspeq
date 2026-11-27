#!/usr/bin/env bash
# Checks the fq_consumer_lag gauge against an independent lag measurement. Publishes two weeks
# of events to a new 4-partition topic faster than one feature worker applies them, while
# scripts/lag_monitor.py samples, twice a second, high watermark minus the Redis stored position
# and the gauge from the worker's metrics page. Afterwards reads sum(fq_consumer_lag) from
# Prometheus over the same span. Run from the
# repository root with the compose stack up and the venv active. Uses Redis database 12
# (emptied) and metrics port 9101. Takes about 5 minutes.
set -euo pipefail

OUT=notes/m6
RUN=$(date +%s)
TOPIC="m6-lag-$RUN"
REDIS=redis://127.0.0.1:6379/12
PYTHON=$(python -c 'import sys; print(sys.executable)')
mkdir -p "$OUT"
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

docker compose --profile observability up -d prometheus grafana
docker compose exec -T redpanda rpk topic create "$TOPIC" -p 4 -c write.caching=false
docker compose exec -T redis redis-cli -n 12 FLUSHDB >/dev/null
finspeq-stream worker --topic "$TOPIC" --group "m6-lag-$RUN" --redis-url "$REDIS" --threads 1 \
  --metrics-port 9101 --start-timeout-s 300 --idle-exit-s 30 >"$OUT/lag_worker.log" 2>&1 &
WORKER_PID=$!
BACKGROUND+=("$WORKER_PID")
sleep 5
START=$(date +%s)
"$PYTHON" scripts/lag_monitor.py --topic "$TOPIC" --partitions 4 --redis-url "$REDIS" \
  --output "$OUT/lag_monitor.json" --settle-s 10 --duration-s 400 --interval-s 0.5 \
  --metrics-url http://127.0.0.1:9101/ >/dev/null &
MONITOR_PID=$!
BACKGROUND+=("$MONITOR_PID")
finspeq-stream simulate --from-date 2026-03-10 --to-date 2026-03-24 --topic "$TOPIC" \
  --chunk-size 2000 | tee "$OUT/lag_simulate.json"
wait "$MONITOR_PID"
sleep 10
END=$(date +%s)
curl -s "http://127.0.0.1:9090/api/v1/query_range" \
  --data-urlencode 'query=sum(fq_consumer_lag)' --data-urlencode "start=$START" \
  --data-urlencode "end=$END" --data-urlencode 'step=5' >"$OUT/lag_prometheus.json"
"$PYTHON" -c 'import json, sys
series = json.load(open(sys.argv[1]))["data"]["result"]
points = [(float(t), float(v)) for t, v in series[0]["values"]] if series else []
monitor = json.load(open(sys.argv[2]))
print("prometheus samples:", len(points))
if points:
    peak = max(points, key=lambda p: p[1])
    print("gauge peak:", int(peak[1]), "at +%.0f s" % (peak[0] - points[0][0]))
    print("gauge last:", int(points[-1][1]))
print("monitor summary:", json.dumps(monitor["summary"]))
rows = [s for s in monitor["samples"] if s["gauge"] is not None]
for s in rows[:: max(1, len(rows) // 15)]:
    lag = sum(s["end_offsets"]) - sum(s["applied"])
    print("t=%6.1f s  stored lag %7d  gauge %7d" % (s["t"], lag, s["gauge"]))' \
  "$OUT/lag_prometheus.json" \
  "$OUT/lag_monitor.json"
docker compose --profile observability stop prometheus grafana

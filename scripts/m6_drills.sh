#!/usr/bin/env bash
# M6 drills: drift, delayed labels, a shadow model under load, Kafka transactions vs plain commits
# under crashes, the observability stack, and the envelope benchmark. Run from the repository
# root with the compose stack up and the venv active
# (pip install -e ".[dev,data,training,streaming,serving,bench]"). Needs
# data/processed/events_sorted.parquet, data/processed/training_v2, models/2e632818a205 and
# models/candidates/v2-sqrt. Takes about 40 minutes.
#
# Uses Redis database 11 (emptied), empties the Postgres ledger tables, and creates per-run
# topics. The API listens on FINSPEQ_API_PORT_BASE (default 18000) and the feature worker serves
# metrics on 9101, the two ports infra/prometheus/prometheus.yml scrapes.
#
# STEPS selects the steps to run (default: all). Step 3 (setup) is needed by steps 4-6, for
# example: STEPS="3 4" bash scripts/m6_drills.sh
set -euo pipefail

set -a
if [ -f .env ]; then . ./.env; fi
set +a
OUT=notes/m6
RUN=$(date +%s)
TOPIC="m6-$RUN"
REDIS=redis://127.0.0.1:6379/11
DATASET=${DATASET:-data/processed/training_v2}
MODEL=${MODEL:-models/2e632818a205}
CANDIDATE=${CANDIDATE:-models/candidates/v2-sqrt}
PG_USER=${POSTGRES_USER:-finspeq}
PG_DB=${POSTGRES_DB:-finspeq}
PG_PORT=${FINSPEQ_POSTGRES_PORT:-15432}
DSN="postgresql://$PG_USER:${POSTGRES_PASSWORD:-finspeq}@127.0.0.1:$PG_PORT/$PG_DB"
WARMUP=2026-03-09
FROM=2026-03-10
TO=2026-06-14
PORT=${FINSPEQ_API_PORT_BASE:-18000}
mkdir -p "$OUT"
PYTHON=$(python -c 'import sys; print(sys.executable)')

STEPS=${STEPS:-1 2 3 4 5 6 7 8}
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
json_field() {  # json_field <file> <key>...: print one nested field of a JSON file
  "$PYTHON" -c 'import json, sys
value = json.load(open(sys.argv[1]))
for key in sys.argv[2:]:
    value = value[key]
print(value)' "$@"
}

if want 1; then
  step "1/8 drift: PSI per feature and TEST month against TRAIN"
  finspeq-offline drift --dataset-dir "$DATASET" --reference-output "$OUT/drift_reference.json" \
    --output "$OUT/drift_monthly.json" >/dev/null
  "$PYTHON" -c 'import json, sys
for m in json.load(open(sys.argv[1]))["months"]:
    print(m["period"], m["rows"], "alerts:", m["alerts"])' "$OUT/drift_monthly.json"
fi

if want 2; then
  step "2/8 delayed labels: observed vs true recall as chargebacks arrive"
  finspeq-offline delayed-labels --dataset-dir "$DATASET" --model-dir "$MODEL" \
    --output "$OUT/delayed_labels.json" >/dev/null
  echo "wrote $OUT/delayed_labels.json"
fi

if want 3; then
  step "3/8 setup: shadow model, schema, topic, stores, observability, feature worker"
  finspeq-offline export --candidate-dir "$CANDIDATE" --dataset-dir "$DATASET" >/dev/null
  json_field "$CANDIDATE/export_report.json" model directory | tr '\\' '/' >"$OUT/shadow_model.txt"
  echo "shadow model: $(cat "$OUT/shadow_model.txt")"
  docker compose exec -T postgres psql -q -U "$PG_USER" -d "$PG_DB" \
    <infra/postgres/init/002_shadow.sql
  docker compose exec -T postgres psql -q -U "$PG_USER" -d "$PG_DB" \
    -c "TRUNCATE decisions, consumer_positions"
  docker compose exec -T redis redis-cli -n 11 FLUSHDB >/dev/null
  docker compose exec -T redpanda rpk topic create "$TOPIC" -p 4 -c write.caching=false
  docker compose --profile observability up -d prometheus grafana
  finspeq-stream worker --topic "$TOPIC" --group "m6-features-$RUN" --redis-url "$REDIS" \
    --threads 4 --metrics-port 9101 --start-timeout-s 1200 --idle-exit-s 1200 \
    >"$OUT/feature_worker.log" 2>&1 &
  WORKER_PID=$!
  BACKGROUND+=("$WORKER_PID")
  finspeq-stream simulate --from-date "$WARMUP" --to-date "$FROM" --topic "$TOPIC" \
    --chunk-size 2000 | tee "$OUT/warmup_simulate.json"
  "$PYTHON" scripts/lag_monitor.py --topic "$TOPIC" --partitions 4 --redis-url "$REDIS" \
    --output "$OUT/warmup_lag.json" --settle-s 3 --duration-s 300 >/dev/null
fi

if want 4; then
  step "4/8 200 requests/s for 7.5 minutes: live model alone, then live + shadow (Prometheus)"
  serve() {  # serve <log> [flags...]: start the API on PORT in the background, wait until ready
    local log=$1
    shift
    if curl -s -o /dev/null "http://127.0.0.1:$PORT/"; then
      echo "port $PORT is already in use; set FINSPEQ_API_PORT_BASE to a free port" >&2
      return 1
    fi
    finspeq-serve --model-dir "$MODEL" --port "$PORT" --topic "$TOPIC" --redis-url "$REDIS" \
      --linger-ms 1 "$@" >"$log" 2>&1 &
    API_PID=$!
    BACKGROUND+=("$API_PID")
    for _ in $(seq 1 120); do
      if curl -sf "http://127.0.0.1:$PORT/readyz" >/dev/null; then return 0; fi
      sleep 1
    done
    echo "API did not become ready; see $log" >&2
    return 1
  }
  load() {  # load <output> <slice>: 90000 requests at 200/s from one 1/16 card slice
    "$PYTHON" scripts/load_test.py --url "http://127.0.0.1:$PORT" --from-date "$FROM" \
      --to-date "$TO" --warmup-from "$WARMUP" --output "$1" --plan rate --rate 200 \
      --limit 90000 --slice "16:$2"
  }
  serve "$OUT/api_live_only.log"
  load "$OUT/live_only_load.json" 1
  stop "$API_PID"
  serve "$OUT/api_shadow.log" --shadow-model-dir "$(cat "$OUT/shadow_model.txt")"
  load "$OUT/shadow_load.json" 0
  sleep 20
  curl -s "http://127.0.0.1:9090/api/v1/targets?state=active" >"$OUT/prometheus_targets.json"
  curl -s "http://127.0.0.1:9090/api/v1/query" \
    --data-urlencode 'query=sum by (decision) (increase(fq_decisions_total[7m]))' \
    >"$OUT/prometheus_decisions.json"
  curl -s "http://127.0.0.1:9090/api/v1/query" \
    --data-urlencode 'query=sum by (decision) (increase(fq_shadow_decisions_total[7m]))' \
    >"$OUT/prometheus_shadow.json"
  curl -s "http://127.0.0.1:3000/api/dashboards/uid/finspeq" >"$OUT/grafana_dashboard.json"
  "$PYTHON" -c 'import json, sys
targets = json.load(open(sys.argv[1]))["data"]["activeTargets"]
for t in targets: print("target", t["labels"]["job"], t["health"], t["lastError"] or "")
for name in sys.argv[2:4]:
    for r in json.load(open(name))["data"]["result"]:
        print(name.rsplit("/", 1)[-1], r["metric"].get("decision"), round(float(r["value"][1])))
panels = json.load(open(sys.argv[4]))["dashboard"]["panels"]
print("grafana panels:", len(panels))' "$OUT/prometheus_targets.json" \
    "$OUT/prometheus_decisions.json" "$OUT/prometheus_shadow.json" "$OUT/grafana_dashboard.json"
  stop "$API_PID"
fi

if want 5; then
  step "5/8 ledger, shadow report, and drift of the recorded feature vectors"
  finspeq-stream ledger --topic "$TOPIC" --group "m6-ledger-$RUN" --dsn "$DSN" \
    --batch-size 500 --idle-exit-s 15 >"$OUT/ledger.log" 2>&1
  finspeq-stream verify-ledger --topic "$TOPIC" --partitions 4 --dsn "$DSN" \
    | tee "$OUT/ledger_verify.json"
  finspeq-stream shadow-report --dsn "$DSN" --output "$OUT/shadow_report.json"
  finspeq-stream drift-ledger --dsn "$DSN" --reference "$OUT/drift_reference.json" \
    --min-rows 200 --output "$OUT/drift_ledger.json" >/dev/null
  "$PYTHON" scripts/ledger_skew.py --dsn "$DSN" --dataset-dir "$DATASET" \
    --reference "$OUT/drift_reference.json" --output "$OUT/ledger_skew.json"
fi
if [ -n "${WORKER_PID:-}" ]; then stop "$WORKER_PID"; fi

if want 6; then
  step "6/8 relay: plain commits vs Kafka transactions, with crashes"
  INPUT="m6-relay-in-$RUN"
  docker compose exec -T redpanda rpk topic create "$INPUT" -p 4 -c write.caching=false
  finspeq-stream simulate --from-date "$FROM" --to-date 2026-03-12 --topic "$INPUT" \
    --chunk-size 2000 | tee "$OUT/relay_input.json"
  for mode in plain transactional; do
    output="m6-relay-$mode-$RUN"
    docker compose exec -T redpanda rpk topic create "$output" -p 4 -c write.caching=false
    "$PYTHON" scripts/crash_drill.py --report "$OUT/relay_${mode}_crash_drill.json" \
      --topic "$INPUT" --partitions 4 --group "m6-relay-$mode-$RUN" --max-failures 10 -- \
      "$PYTHON" experiments/transactional_relay.py run --mode "$mode" --input-topic "$INPUT" \
      --output-topic "$output" --group "m6-relay-$mode-$RUN" --idle-exit-s 10 \
      --crash-point after-apply --crash-probability 0.05
    "$PYTHON" experiments/transactional_relay.py count --input-topic "$INPUT" \
      --output-topic "$output" --partitions 4 | tee "$OUT/relay_${mode}_count.json"
  done
  for mode in plain transactional; do
    output="m6-relay-$mode-clean-$RUN"
    docker compose exec -T redpanda rpk topic create "$output" -p 4 -c write.caching=false
    "$PYTHON" experiments/transactional_relay.py run --mode "$mode" --input-topic "$INPUT" \
      --output-topic "$output" --group "m6-relay-$mode-clean-$RUN" --idle-exit-s 10 \
      | tee "$OUT/relay_${mode}_throughput.json"
  done
fi

if want 7; then
  step "7/8 envelope encode/decode: stdlib JSON codec vs msgspec JSON and MessagePack"
  "$PYTHON" scripts/bench_envelope.py --messages 20000 --repeats 5 --output "$OUT/envelope.json"
fi

if want 8; then
  step "8/8 stop the observability profile"
  docker compose --profile observability stop prometheus grafana
fi

step "done: reports in $OUT/"

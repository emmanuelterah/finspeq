# Finspeq

Real-time card fraud scoring built to learn streaming internals: Redpanda, Redis, Postgres,
LightGBM/ONNX, and FastAPI on one laptop. Organized as a hexagonal architecture with deterministic
replay as the core invariant.

- [DESIGN.md](DESIGN.md): goals, data profile, milestones
- [ARCHITECTURE.md](ARCHITECTURE.md): hexagonal design, SOLID, ACID, failure modes, ADRs
- [CODING_STANDARDS.md](CODING_STANDARDS.md): 100-character lines, docstring rules, enforcement

## Setup (Windows PowerShell)

```powershell
py -3.11 -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install -e ".[dev,data,training]"
python scripts/ci.py
```

## Setup (macOS / Linux / WSL)

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev,data,training]"
python scripts/ci.py
```

Extra dependency groups: `training` (LightGBM, ONNX), `serving` (FastAPI, ONNX Runtime),
`streaming` (Kafka client, Redis, Postgres).

## Data

1. Put the raw CSV at `data/raw/combined_transactions.csv`.
2. Run `python scripts/convert_csv_to_parquet.py`. This writes
   `data/processed/transactions.parquet`.

`data/` is git-ignored.

## M0 pipeline (offline model)

```bash
python scripts/sort_events.py          # canonical event file + events_sorted.manifest.json
python scripts/data_quality.py         # notes/data_quality.md
python -m finspeq build-dataset --feature-set v2 --jobs 8
python -m finspeq baselines --dataset-dir data/processed/training_v2
python -m finspeq train --dataset-dir data/processed/training_v2 --name v2-none --threads 8
python -m finspeq export --candidate-dir models/candidates/v2-none \
    --dataset-dir data/processed/training_v2 --threads 8
```

Install the extra groups first: `python -m pip install -e ".[dev,data,training]"`.

## M1 pipeline (streaming)

Install the streaming extra: `python -m pip install -e ".[dev,data,training,streaming]"`.

```bash
docker compose up -d
python -m finspeq simulate --from-date 2026-03-01 --to-date 2026-06-15 --slice 20:0
python -m finspeq worker --idle-exit-s 10 --metrics-port 9101
python -m finspeq verify-state --from-date 2026-03-01 --to-date 2026-06-15 --slice 20:0
python -m finspeq verify-replay --from-date 2026-03-01 --to-date 2026-06-15 --slice 20:0 \
    --partitions 4 --redeliver-every 7 --redis-url redis://localhost:6379/1 --flush
```

Integration tests (compose stack running; they use Redis database 15 and temporary topics):

```bash
export FINSPEQ_KAFKA_BOOTSTRAP=127.0.0.1:19092 FINSPEQ_REDIS_URL=redis://127.0.0.1:6379/0
python -m pytest -m integration tests/contract tests/integration
```

## Infrastructure

```bash
docker compose up -d                       # Redpanda, Redis, Postgres; creates the topic
FINSPEQ_PARTITIONS=4 docker compose up -d  # topic with 4 partitions (first creation only)
docker compose down                        # stop; add -v to delete the volumes
```

| Service | Address |
|---|---|
| Redpanda (Kafka API) | `127.0.0.1:19092` |
| Redis | `localhost:6379` |
| Postgres | `postgresql://finspeq:finspeq@localhost:15432/finspeq` (host port: `FINSPEQ_POSTGRES_PORT`) |

Copy `.env.example` to `.env` to override the settings.

## Layout

```
finspeq/
  domain/        value objects, window, features, policy (standard library only)
  application/   ports (Protocols), messages, use cases
  adapters/      inbound (HTTP, workers, CLI) and outbound (Redis, Kafka, ONNX, ...)
  bootstrap.py   composition roots
tests/
  unit/          domain and application, no containers
  contract/      one suite per port, parametrized over adapters
  integration/   docker compose stack
  replay/        replay-equality scenarios
scripts/         ci.py, check_line_length.py, convert_csv_to_parquet.py
infra/           Postgres schema
```

## Checks

`python scripts/ci.py` runs, in order:

1. `ruff format --check`
2. `ruff check`
3. `mypy` (strict)
4. `lint-imports` (hexagonal contracts)
5. `check_line_length.py`
6. `pytest tests/unit tests/contract`

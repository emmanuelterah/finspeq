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
python -m pip install -e ".[dev,data]"
python scripts/ci.py
```

## Setup (macOS / Linux / WSL)

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev,data]"
python scripts/ci.py
```

Extra dependency groups: `training` (LightGBM, ONNX), `serving` (FastAPI, ONNX Runtime),
`streaming` (Kafka client, Redis, Postgres).

## Data

1. Put the raw CSV at `data/raw/combined_transactions.csv`.
2. Run `python scripts/convert_csv_to_parquet.py`. This writes
   `data/processed/transactions.parquet`.

`data/` is git-ignored.

## Infrastructure

```bash
docker compose up -d                       # Redpanda, Redis, Postgres; creates the topic
FINSPEQ_PARTITIONS=4 docker compose up -d  # topic with 4 partitions (first creation only)
docker compose down                        # stop; add -v to delete the volumes
```

| Service | Address |
|---|---|
| Redpanda (Kafka API) | `localhost:19092` |
| Redis | `localhost:6379` |
| Postgres | `postgresql://finspeq:finspeq@localhost:5432/finspeq` |

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

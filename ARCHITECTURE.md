# Finspeq — Architecture Analysis

- **Status:** v1.0 (2026-11-26)
- **Companion docs:** [DESIGN.md](DESIGN.md) (goals, data, milestones) and
  [CODING_STANDARDS.md](CODING_STANDARDS.md) (rules every line of code follows).
- **Scope of this doc:** structure, boundaries, transactional guarantees, and the reasons behind them.
  The reasons live here and in the ADRs (§9), never in code docstrings.

---

## 1. Architectural drivers

These are the quality attributes, ranked. When two of them conflict, the higher-ranked one wins.

| Rank | Attribute | Concrete target |
|---|---|---|
| 1 | Determinism | The same ordered event log gives identical feature state, vectors, and decisions |
| 2 | Correctness under failure | A crash, duplicate, redelivery, or rebalance never corrupts state |
| 3 | Testability | Every port has a contract test suite; the core is testable without containers |
| 4 | Latency | Scoring p99 under 50 ms at 500 req/s on one machine (Intel Core i7, 32 GB RAM) |
| 5 | Changeability | A new feature, store, or model is a new class plus wiring; the core is untouched |

**Constraints:** Python 3.11+, one machine (Intel Core i7, 32 GB RAM), Docker Compose, $0.

---

## 2. Hexagonal architecture (ports and adapters)

### 2.1 Rings and the dependency rule

The code is organized in rings. Source-code imports point **inward only**.

| Ring | Contains | Allowed imports |
|---|---|---|
| Domain | Value objects, entities, feature math, decision policy | Python standard library only |
| Application | Use cases; the port interfaces they need | Domain and the standard library |
| Adapters | All I/O: HTTP, Kafka, Redis, Postgres, ONNX, Parquet, metrics | Application, domain, third-party |
| Bootstrap | Composition roots: build the adapters and inject them into use cases | Everything |

The domain and application rings import no third-party package. That means no `numpy`, `pydantic`,
`redis`, and so on. Import-linter enforces this in CI (CODING_STANDARDS §8).

### 2.2 Component view

```
     INBOUND ADAPTERS                    CORE                        OUTBOUND ADAPTERS
                          ┌───────────────────────────────────┐
 HTTP /score (FastAPI) ─▶ │ APPLICATION (use cases + ports)   │ ─▶ WindowReader   ─▶ Redis
 Feature worker ────────▶ │   ScoreTransaction                │ ─▶ WindowWriter   ─▶ Redis/Memory
 Ledger worker ─────────▶ │   ApplyEvents                     │ ─▶ FraudModel     ─▶ ONNX Runtime
 CLI (offline jobs) ────▶ │   RecordDecisions                 │ ─▶ EventPublisher ─▶ Redpanda
                          │   BuildTrainingSet                │ ─▶ EventSource    ─▶ Kafka/Parquet
                          │   TrainModel, VerifyReplay        │ ─▶ DecisionLedger ─▶ Postgres
                          │  ┌─────────────────────────────┐  │ ─▶ ModelTrainer   ─▶ LightGBM
                          │  │ DOMAIN                      │  │ ─▶ TrainingSink   ─▶ Parquet
                          │  │ Transaction, Money, CardId  │  │ ─▶ Metrics        ─▶ Prometheus
                          │  │ CardWindow, FeatureSet      │  │
                          │  │ DecisionPolicy, Thresholds  │  │
                          │  └─────────────────────────────┘  │
                          └───────────────────────────────────┘
```

### 2.3 Domain model

| Type | Kind | Fields | Invariants checked at construction |
|---|---|---|---|
| `CardId` | value | `value: str` | 8–19 ASCII decimal digits (ISO/IEC 7812) |
| `TxId` | value | `value: str` | 32 lowercase hex characters |
| `Money` | value | `cents: int` | `cents > 0` |
| `EventTime` | value | `epoch_s: int` | `epoch_s >= 0` |
| `GeoPoint` | value | `lat: float`, `lon: float` | lat in [-90, 90], lon in [-180, 180] |
| `Transaction` | entity | tx_id, card, time, amount, merchant, category, merchant point, home point | category in the known set |
| `EventPosition` | value | `partition: int`, `offset: int` | both >= 0; ordered by offset |
| `WindowEntry` | value | tx_id, time, amount, merchant | — |
| `OrderKey` | value | `time: EventTime`, `tx_id: TxId` | ordered by time, then tx_id (canonical order) |
| `CardWindow` | value | `card`, `until: OrderKey`, `entries: tuple[WindowEntry, ...]` | entries strictly ordered by `OrderKey`; each key `< until` and time `>= until.time - 86400` |
| `FeatureVector` | value | `schema: str`, `values: tuple[float, ...]` | length matches the schema |
| `Score` | value | `fraud_probability: float` | in [0, 1] |
| `Thresholds` | value | `review: float`, `block: float` | `0 <= review < block <= 1` |
| `Decision` | enum | `APPROVE`, `REVIEW`, `BLOCK` | — |
| `DecisionRecord` | value | vector, score, decision, model version | — |
| `ScoredTransaction` | value | transaction, record: `DecisionRecord` | — |

**Domain services** (pure functions or stateless objects):

- `FeatureContext` holds a transaction and its card's window, and rejects a window that belongs
  to another card or does not end at the transaction.
- `Feature` is a protocol: a `name`, plus `compute(context: FeatureContext) -> float`.
- `FeatureSet` is an ordered, versioned list of `Feature`s. `compute(tx, window) -> FeatureVector`.
- `DecisionPolicy` turns a `Score` into a `Decision` using `Thresholds`.

Example of the domain style:

```python
@dataclass(frozen=True, slots=True)
class Money:
    """Positive amount of US currency stored as integer cents."""

    cents: int

    def __post_init__(self) -> None:
        """Raise InvalidMoneyError unless cents is greater than zero."""
        if self.cents <= 0:
            raise InvalidMoneyError(f"cents must be > 0, got {self.cents}")


class Feature(Protocol):
    """One named numeric input to the fraud model."""

    name: str

    def compute(self, context: FeatureContext) -> float:
        """Return the feature value for context.tx given context.window."""
        ...
```

### 2.4 Ports

**Inbound ports** are the use cases themselves. Each one exposes a single `execute` method.

| Use case | Signature | Caller |
|---|---|---|
| `ScoreTransaction` | `execute(tx: Transaction) -> ScoredTransaction` | HTTP adapter |
| `ApplyEvents` | `execute(batch: EventBatch) -> ApplyResult` | Feature worker |
| `RecordDecisions` | `execute(batch: EventBatch) -> int` | Ledger worker |
| `BuildTrainingSet` | `execute(time_range: TimeRange) -> DatasetSummary` | CLI |
| `TrainModel` | `execute(dataset: DatasetRef) -> ModelRef` | CLI |
| `VerifyReplay` | `execute(scenario: ReplayScenario) -> ReplayReport` | CLI / tests |

**Outbound ports** are defined in `application/ports.py` as `typing.Protocol` classes.

| Port | Methods | Contract highlights | Adapters |
|---|---|---|---|
| `WindowReader` | `read(card, until: OrderKey) -> CardWindow` | Returns entries before `until` in canonical order | Redis, Memory |
| `WindowWriter` | `apply(batch) -> ApplyResult`; `position(partition)` | All-or-nothing; skips offsets at or below the stored position | Redis, Memory |
| `FraudModel` | `predict(vector) -> Score`; `version`; `schema` | Rejects a vector whose schema differs from its own | ONNX |
| `ModelTrainer` | `train(dataset, params) -> ModelArtifact` | Same data and params give the same artifact | LightGBM |
| `ModelExporter` | `export(artifact, path) -> ModelRef` | Writes ONNX plus metadata atomically | onnxmltools |
| `EventPublisher` | `publish_raw(tx)`, `publish_scored(scored)` -> `EventPosition` | Returns only after a durable broker ack | Redpanda, Memory |
| `BulkPublisher` | `publish_raw_many(txs) -> tuple[EventPosition, ...]` | Returns after every ack, positions in input order | Redpanda |
| `EventSource` | `poll(max_events) -> EventBatch`; `seek(position)`; `acknowledge(batch)` | One partition per batch, order preserved; acknowledge is for lag only | Redpanda |
| `HistoricalSource` | `iter_labeled(time_range) -> Iterator[LabeledTransaction]` | Canonical order enforced | Parquet, Memory |
| `DecisionLedger` | `record(batch) -> int`; `position(partition)` | One row per tx_id; rows and position in one transaction | Postgres, Memory |
| `TrainingSink` | `write(rows)`; `close() -> DatasetRef` | File becomes visible only when complete | Parquet |
| `Metrics` | `observe(name, value, labels)`; `increment(...)` | Never raises | Prometheus, Null |
| `Clock` | `now() -> float` | Used for latency metrics only, never for features | System, Fixed |

The key interface decision is that atomicity belongs to the `WindowWriter` and `DecisionLedger`
**contracts**; it is not left to whichever adapter happens to implement them. That makes the ACID
guarantees testable with the same contract suite against every adapter (§3, Liskov).

### 2.5 Use-case flows

**ScoreTransaction** (hot path, synchronous):

1. The HTTP adapter validates the JSON body with Pydantic and maps it to a domain `Transaction`.
2. `WindowReader.read(tx.card, until=tx.order_key)` returns the card's entries before tx in
   canonical order and no older than 24 h. Same-second entries with a smaller tx_id are included.
3. `FeatureSet.compute(tx, window)` returns a `FeatureVector`.
4. `FraudModel.predict(vector)` returns a `Score`.
5. `DecisionPolicy.decide(score)` returns a `Decision`.
6. `EventPublisher.publish_scored(scored)` blocks until the broker acknowledges.
7. The adapter returns the decision. If step 6 fails, the API returns 503 and no decision leaves the
   system without a log entry.

**ApplyEvents** (feature worker):

1. On partition assignment: `p = WindowWriter.position(partition)`, then `EventSource.seek(p + 1)`.
2. `EventSource.poll(500)` returns a batch.
3. `WindowWriter.apply(batch)` atomically inserts the events, prunes by event time, and stores the
   new position.
4. Commit the Kafka group offset. It is advisory and only feeds the consumer-lag metric (§4.5).

**RecordDecisions** (ledger worker) uses the same loop with `DecisionLedger` in place of
`WindowWriter`.

**BuildTrainingSet** (offline): `ParquetSource` yields events in canonical order. For each event it
calls `read`, then `compute`, then writes the row (vector plus label), then calls `apply` with
`MemoryWindowStore`. The feature math runs through the **same `FeatureSet` object** as the hot path,
so feature-logic skew is impossible by construction. Any remaining skew can only come from store
semantics, which the contract tests and `VerifyReplay` cover.

**VerifyReplay**: runs one event slice through the memory reference and through Redpanda → worker →
Redis with faults injected, then compares per-event vectors, final state, and decisions.

### 2.6 Composition roots

`finspeq/bootstrap/` has one module per process. Each module imports, at the top, only the
adapters its process wires, so a process never loads another process's libraries. There is no DI
framework.

| Module | Console command | Wires |
|---|---|---|
| `offline.py` | `finspeq-offline` | Parquet source and sink, MemoryWindowStore, LightGBM, ONNX exporter, baselines |
| `streaming.py` | `finspeq-stream` | RedpandaPublisher, RedpandaSource, RedisWindowStore, Prometheus metrics |
| `serving.py` (M5) | `finspeq-serve` | RedisWindowStore (as a reader), OnnxFraudModel, RedpandaPublisher |

At startup each root checks that `FraudModel.schema == FeatureSet.version`, and refuses to start
on a mismatch.

### 2.7 Package layout

```
finspeq/
├── domain/
│   ├── model.py            # CardId, TxId, Money, EventTime, GeoPoint, Transaction
│   ├── window.py           # WindowEntry, CardWindow, canonical ordering key
│   ├── features.py         # Feature protocol, FeatureSet, concrete features
│   ├── policy.py           # Thresholds, Decision, DecisionPolicy
│   └── errors.py           # FinspeqError hierarchy for domain rule violations
├── application/
│   ├── ports.py            # outbound Protocols
│   ├── messages.py         # EventBatch, EventPosition, ApplyResult, ScoredTransaction
│   ├── score.py            # ScoreTransaction
│   ├── apply_events.py     # ApplyEvents
│   ├── record.py           # RecordDecisions
│   ├── build_dataset.py    # BuildTrainingSet
│   ├── train.py            # TrainModel
│   └── verify.py           # VerifyReplay
├── adapters/
│   ├── codec.py            # event envelope <-> bytes (JSON, schema_version)
│   ├── inbound/
│   │   ├── http_api.py     # FastAPI routes and Pydantic request/response models
│   │   ├── feature_worker.py
│   │   ├── ledger_worker.py
│   │   └── cli.py
│   └── outbound/
│       ├── redis_window.py
│       ├── lua/apply_batch.lua
│       ├── memory_window.py
│       ├── onnx_model.py
│       ├── lightgbm_trainer.py
│       ├── onnx_exporter.py
│       ├── redpanda.py     # publisher and source
│       ├── parquet.py      # source and training sink
│       ├── postgres_ledger.py
│       ├── prometheus.py
│       └── clock.py
├── settings.py             # pydantic-settings; imported by bootstrap only
└── bootstrap/            # offline.py, streaming.py: one composition root per process
tests/
├── unit/                   # domain + application with memory adapters, no containers
├── contract/               # one suite per port, parametrized over every adapter
├── integration/            # real containers via docker compose
└── replay/                 # VerifyReplay scenarios with fault injection
```

---

## 3. SOLID applied

### S — Single Responsibility: one reason to change per class

| Class | Changes only when |
|---|---|
| Each `Feature` | that feature's formula changes |
| `FeatureSet` | the selection or order of features changes |
| `DecisionPolicy` | business threshold rules change |
| `ScoreTransaction` | the order of hot-path steps changes |
| `RedisWindowStore` | the Redis key layout or script changes |
| `EnvelopeCodec` | the wire format changes |
| `http_api` | the HTTP contract changes |

Rejected shape: a `FraudService` that parses JSON, queries Redis, runs ONNX, and writes logs. It has
four reasons to change, and it can't be tested without containers.

### O — Open/Closed: extend by adding, not by editing

- **New feature:** add a `Feature` class, append it to the `FeatureSet` definition, bump
  `FeatureSet.version`, and retrain. The stores, use cases, and API stay unchanged.
- **New store or broker:** add an adapter that passes the port's contract suite, and wire it in
  `bootstrap`.
- **New model type:** add a `FraudModel` adapter. `ScoreTransaction` stays unchanged.
- The `FeatureSet` definition list is the one deliberate edit point. The version check at startup
  (§2.6) prevents a feature/model mismatch.

### L — Liskov Substitution: contract tests per port

Every adapter of a port passes the **same** test suite. For example, the `WindowWriter` contract:

1. `apply` is all-or-nothing. An injected failure leaves the state and position unchanged.
2. Re-applying a batch whose offsets are at or below the stored position changes nothing.
3. Applying the same tx_id twice in different batches stores it once.
4. Entries are pruned by **event time** relative to the newest event for the card.
5. `read` returns entries in `(time, tx_id)` order.
6. `position` never decreases.

The memory adapter used offline must not be "nicer" than Redis. If it deduplicated by tx_id and
Redis didn't, offline features would differ from online ones. That is exactly the skew this project
exists to prevent. `VerifyReplay` is the end-to-end Liskov test.

### I — Interface Segregation: small ports, no unused capabilities

- `WindowReader` and `WindowWriter` are separate. `ScoreTransaction` receives only a
  `WindowReader`, so the hot path is type-checked to be read-only.
- `EventSource` and `EventPublisher` are separate.
- `FraudModel` (predict only) and `ModelTrainer` are separate. The serving image never installs
  LightGBM.

### D — Dependency Inversion: the core owns the interfaces

- The ports live in `application/ports.py`, owned by the code that consumes them. Adapters import
  the ports and implement them.
- Use cases receive their ports through `__init__`. Nothing in the core constructs an adapter.
- Adapters subclass their Protocol explicitly (for example,
  `class RedisWindowStore(WindowReader, WindowWriter)`), so mypy checks conformance where the class
  is defined.

---

## 4. ACID analysis

ACID describes transactions inside one data store. Finspeq has several stores, so the analysis below
has two parts: where each guarantee holds, and what replaces it across store boundaries (a single
log write, positions stored together with state, and idempotency).

### 4.1 Inventory of state

| Store | Holds | Source of truth? | Transaction mechanism | Durability setting |
|---|---|---|---|---|
| Redpanda `transactions` | Immutable event log | **Yes** | Idempotent producer, `acks=all` | 1 broker, RF=1, unlimited retention |
| Redis | Card windows, partition positions | No (derived) | One Lua script per batch | AOF, `appendfsync everysec` |
| Postgres | Decision ledger, partition positions | No (derived; used for audit queries) | SQL transaction | `fsync=on`, `synchronous_commit=on` |
| Parquet files | Training datasets | No (derived) | Write a temp file, then `os.replace` | n/a |
| Model files | `.onnx` + `metadata.json` | No (derived) | Write a temp file, then `os.replace`; content hash | n/a |

### 4.2 Atomicity

- **Redis key layout** (standalone Redis; one batch touches keys of many cards):

  | Key | Type | Content |
  |---|---|---|
  | `fq:card:{<card>}:ev` | sorted set | member tx_id, score event time; equal scores sort by tx_id |
  | `fq:card:{<card>}:tx` | hash | field tx_id, value `time|cents|merchant` |
  | `fq:card:{<card>}:newest` | string | newest event time seen for the card |
  | `fq:pos:<topic>:<partition>` | string | last applied offset |

  `read_window.lua` reads the sorted set and the hash in one call, so a read is one round trip
  and one consistent snapshot.
- **Redis, `apply_batch.lua`:** one script call per batch. Inside the script:
  1. Validate every argument. If anything is invalid, return an error before the first write.
  2. Read the stored position. Drop events at or below it.
  3. For each event: `ZADD card:{id}:ev <time> <tx_id>`, then `HSET card:{id}:tx <tx_id> <payload>`.
  4. Prune by event time: find the ids older than `newest - 86400 - lateness`, `HDEL` them, then
     `ZREMRANGEBYSCORE`.
  5. `SET pos:{topic}:{partition} <last_offset>`.

  Redis runs a script without interleaving other commands. It does **not** roll back writes made
  before a runtime error, which is why validation comes before any write (step 1).
- **Postgres:** `BEGIN; INSERT ... ON CONFLICT (tx_id) DO NOTHING; UPDATE positions SET offset = $n
  WHERE partition = $p AND offset < $n; COMMIT;`
- **Files:** write to a `*.tmp` file in the same directory, `fsync`, then `os.replace` it into place.
- **Hot path:** no decision is returned unless its event is in the log (§4.6).

### 4.3 Consistency (invariants and where they are enforced)

| Invariant | Enforced by |
|---|---|
| Money is positive; CardId and TxId formats are valid | Domain value-object constructors |
| `review < block` | `Thresholds` constructor |
| Window entries are before `until`, no older than 24 h, and strictly ordered | `CardWindow` constructor |
| Position is monotonic per partition | Lua script check; SQL `WHERE offset < $n` |
| No offset gaps inside a partition | Lua script: `first_offset == stored + 1`, otherwise reject |
| One ledger row per tx_id | Postgres `UNIQUE (tx_id)` |
| Model schema equals `FeatureSet.version` | Startup check in bootstrap |
| Vector length equals the schema length | `FeatureVector` constructor |

**About the gap check:** with a non-transactional idempotent producer and no compaction, Redpanda
offsets in a partition are contiguous. A gap therefore means lost data or a bug, so the batch is
rejected and an alarm is raised.

### 4.4 Isolation

- **Writers are isolated by ownership.** Events are keyed by card, so a card lives in one partition.
  Each partition is owned by one consumer in the group, so each card has one writer. Redis also runs
  scripts serially, so writes are serializable.
- **Zombie writers:** after a rebalance, the old owner may still finish a batch. The events are
  immutable and the position check is in the script, so a late duplicate apply is a no-op. A stale
  owner can only rewrite identical data.
- **The hot-path read is read-committed, not serializable.** The API sees the last applied batch. A
  transaction from the same card that is still in flight is invisible to it. This is the one place
  isolation is deliberately relaxed for latency. It is measured as `feature_staleness_ms`, studied in
  M5, and modeled in training if the measured-staleness option is chosen.

### 4.5 Durability

- The **log is the source of truth.** Redis durability only speeds up recovery; correctness doesn't
  depend on it.
- **Positions are stored with the state, in the same atomic unit.** If Redis loses its last second
  of writes, it loses that state *and* the matching position together. The worker then resumes from
  the older position and re-applies. The two can never disagree.
- It follows that workers seek to the **store's** position, never to the Kafka group offset. Group
  offsets exist only so that lag can be measured.
- Full rebuild: `FLUSHALL`, then replay from offset 0. This requires unlimited topic retention
  (a few GB on disk).
- **Single-machine limitation (accepted):** one broker with RF=1 means losing the disk loses the
  log.
- **Broker acknowledgement means fsync:** the topic is created with `write.caching=false`.
  Redpanda's default (`true`) acknowledges `acks=all` writes before they reach disk, which would
  let a broker crash lose acknowledged events. M2 measures the latency cost of both settings.

### 4.6 The dual-write problem and the chosen solution

The hot path has to produce four results: a response to the caller, an event in the log, updated
features, and a ledger row. Writing to several systems without a shared transaction can leave them
disagreeing.

| Option | Description | Verdict |
|---|---|---|
| A. API writes Redis, Postgres, and Redpanda | Three independent writes | Rejected: a partial failure leaves them inconsistent |
| B. Transactional outbox | API writes decision + outbox row in one Postgres transaction; a relay publishes | Correct, but puts Postgres and a relay process on the critical path |
| **C. Log as the only write** | API publishes one event and waits for the ack; consumers derive Redis and Postgres | **Chosen.** One write is atomic by construction |

The cost of option C is twofold. Hot-path latency includes the broker ack (an estimated 1–5 ms
locally). And feature freshness lags by however far behind the consumer is, which is the M5 race.
This settles open question 4 in DESIGN.md: **publish before responding.**

---

## 5. Determinism rules

1. **Money is integer cents.** Sums are exact integer arithmetic. Ratios are computed once in float64
   from those integers.
2. **Canonical order is `(unix_time, tx_id)`** everywhere: Parquet replay, the simulator, windows,
   and tests.
3. **Event time only.** The `Clock` port never feeds a feature. Redis keys have **no** wall-clock
   TTLs; memory is bounded by event-time pruning.
4. **No ordered output from hash-ordered collections.** Never iterate a `set` to produce an order,
   because `PYTHONHASHSEED` varies. Counting a set's elements is fine.
5. **Float boundary:** features are float64 in the domain. The ONNX adapter is the only place that
   casts to float32. The harness compares integer features exactly and float32 model inputs exactly.
6. **ONNX Runtime:** `intra_op_num_threads=1` and `inter_op_num_threads=1`.
7. **LightGBM:** `deterministic=True`, fixed `num_threads`, fixed `seed`, the training set built in
   canonical order.
8. **Versioning:** `FeatureSet.version` is stored in the model metadata. The event envelope carries
   `schema_version`.
9. **Randomness** (simulator fault injection only) comes from an injected `random.Random(seed)`.

---

## 6. Failure-mode analysis

| Failure | Effect without countermeasure | Countermeasure | Verified by |
|---|---|---|---|
| Worker crashes after the Redis apply, before the group commit | Batch redelivered and double-counted | Position check in the script skips it | Replay: `FAULTS=crash_after_apply` |
| Worker crashes mid-batch | Half a batch applied | Script is atomic; validate before write | Contract test: injected failure |
| Redis loses its last second (AOF everysec) | State behind the log | State and position roll back together; worker re-applies | Integration: kill -9 Redis |
| Redis loses all data | No features | Replay from offset 0 | Drill: `FLUSHALL` + rebuild, compare |
| Duplicate HTTP request (client retry) | Two events with the same tx_id | ZADD dedupes by tx_id; ledger UNIQUE keeps the first | Contract tests |
| Rebalance mid-batch | Two writers for a partition | Position check makes the zombie's writes no-ops | Replay: `FAULTS=rebalance` |
| Producer sends without a key | Card split across partitions; order lost | Codec rejects envelopes without a key | Unit test plus an M4 demonstration |
| Late or out-of-order event | Wrong window | 1 h allowed lateness; late events applied and counted (ADR-011) | `lateness-sweep`; `verify-state` with disorder flags |
| Redpanda unavailable on the hot path | Decision without a log entry | API returns 503 | Integration: stop the broker |
| Model / FeatureSet version mismatch | Garbage scores | Startup check refuses to run | Unit test of bootstrap |
| Offset gap in a partition | Silent data loss | Script rejects the batch and raises an alarm | Contract test |

---

## 7. Hot-path latency budget (p99 target 50 ms)

Budgets were planned before M5; the measured column is M5 at 100 requests/s on one machine
(`notes/m5.md`), p50 / p99.

| Step | Budget | Measured |
|---|---|---|
| HTTP parse and Pydantic validation | 2 ms | not split out |
| Redis window read (one round trip, about 6 entries/day per card) | 2 ms | 1.5 / 4.9 ms |
| Feature computation (pure Python) | 1 ms | 0.06 / 0.14 ms |
| ONNX predict (single thread) | 3 ms | 0.44 / 1.2 ms |
| Publish and broker ack | 5 ms | 11.3 / 19.1 ms at linger 1 ms; 4.5 ms p50 at linger 0 |
| Headroom for queueing, GC, scheduling | 37 ms | end to end 7.4 / 39 ms |

The sync route in the thread pool was kept; one process saturates at about 310–370 requests/s
on the machine.

---

## 8. Event envelope (wire format)

The envelope is JSON with an explicit version. Pydantic exists at the HTTP edge only; the codec maps
JSON to and from domain types.

```json
{
  "schema_version": 1,
  "key": "4903494466574378",
  "transaction": {
    "tx_id": "46511f9f55abe5e215041022ea7bd0e0",
    "card": "4903494466574378",
    "time": 1764799793,
    "amount_cents": 100140,
    "merchant": "fraud_Pacocha-Bauch",
    "category": "shopping_pos",
    "merchant_point": [45.020525, -90.81791],
    "home_point": [45.1891, -90.8257]
  },
  "scored": null
}
```

`scored` is `null` for events the simulator produces directly (M1–M4). In M5, events published by
the API fill it with the vector, score, decision, and model version.

---

## 9. Architecture Decision Records (summary)

| ADR | Decision | Status |
|---|---|---|
| 001 | Hexagonal architecture; ports are `typing.Protocol` in the application ring | Accepted |
| 002 | Synchronous scoring; the log is the only write on the hot path | Accepted |
| 003 | Consumer positions are stored with the derived state, not in Kafka group offsets | Accepted |
| 004 | Redis stores raw windowed events; feature math lives only in the Python domain, never in Lua | Accepted |
| 005 | Money as integer cents | Accepted |
| 006 | Postgres holds the decision ledger (full ACID, UNIQUE tx_id) | Accepted |
| 007 | No stream-processing framework; hand-written consumer loops | Accepted |
| 008 | No DI framework; manual composition roots | Accepted |
| 009 | Drop the category feature (FEATURE_SET_V2) because of the category–time confound | Accepted |
| 010 | Crash testing: seeded fault injection, supervisor drills, naive baseline | Accepted |
| 011 | Allowed lateness 1 h; late events are applied, never dropped | Accepted |
| 012 | Keyed partitioning required; recovery never depends on partition ownership | Accepted |
| 013 | One writer per card; hot-path staleness is measured, not removed | Accepted |
| 014 | When the broker is down, the API answers 503 | Accepted |
| 015 | Positions stay with the state; Kafka transactions only for Kafka-to-Kafka steps | Accepted |
| 016 | The event envelope stays JSON | Accepted |
| 017 | Drift is measured by PSI, and live drift is checked against offline vectors | Accepted |

Full ADRs go in `docs/adr/NNN-title.md`, one page each: context, decision, consequences.

---

## 10. Open questions

1. ~~Allowed-lateness value and late-event policy.~~ Decided in M3: 1 h, apply (ADR-011).
2. ~~The hot path when the broker is down.~~ Decided in M5: 503 (ADR-014).
3. ~~Async versus sync FastAPI route.~~ Sync route kept in M5; async not benchmarked.
4. ~~JSON versus a binary envelope (`msgspec`) if serialization shows up in profiles.~~ Decided
   in M6: JSON (ADR-016).

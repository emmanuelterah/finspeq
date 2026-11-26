# Finspeq — Design Doc

Real-time fraud scoring pipeline, built as a deep learning exercise in streaming internals.

- **Status:** Draft v0.3 (2026-11-26)
- **Companion docs:** [ARCHITECTURE.md](ARCHITECTURE.md) (hexagonal design, SOLID, ACID) and
  [CODING_STANDARDS.md](CODING_STANDARDS.md) (100-char lines, docstring rules, enforcement)
- **Stack:** Python 3.11+, Redpanda, Redis, Postgres, LightGBM → ONNX Runtime, FastAPI, Docker Compose
- **Hardware budget:** one laptop (i7, 32 GB RAM), $0

---

## 1. Goal

Learn streaming systems from first principles by building a fraud-scoring pipeline. Wherever practical, write the primitives yourself instead of hiding them behind frameworks.

The ML model is a fixed component, not the point of the project. Spend days on it, not weeks.

### Non-goals (for now)

- State-of-the-art fraud model accuracy
- Production polish: Grafana dashboards, Evidently drift reports, auth, Kubernetes
- Stream-processing frameworks (Faust, Bytewax, Flink). We write the consumer loop ourselves.

---

## 2. Core invariant

> **Feeding the same event log through the system gives exactly the same feature state and decisions every time. That holds even if a worker crashes, restarts, falls behind, or gets split across more consumers.**

Every milestone works the same way:

1. Learn the concept.
2. Break the invariant on purpose.
3. Fix it.
4. Prove the fix with the replay-equality harness (§6).

---

## 3. Architecture

```
                 ┌──────── HOT PATH (synchronous, p99 < 50 ms) ────────┐
 payment ──────▶ │ Scoring API (FastAPI)                               │ ──▶ APPROVE|REVIEW|BLOCK
 request         │  1. read card window from Redis (one round trip)    │
                 │  2. compute features (shared domain FeatureSet)     │
                 │  3. ONNX Runtime predict                            │
                 │  4. apply thresholds                                │
                 │  5. publish event + decision; wait for broker ack   │
                 └─────────────────────────┬───────────────────────────┘
                                           ▼
                 ┌──────── COLD PATH (asynchronous) ───────────────────┐
                 │ Redpanda topic `transactions` (key = card)          │
                 │   ├─▶ Feature worker ─▶ Redis (windows + position)  │
                 │   └─▶ Ledger worker  ─▶ Postgres (decisions + pos.) │
                 └─────────────────────────────────────────────────────┘

 Simulator / replayer ──▶ replays history in (unix_time, trans_num) order at N events/s,
                          with optional shuffle / delay / duplicate injection
```

**Key decision:** scoring is **synchronous**, and the event log is the hot path's only write. Redis
and Postgres are derived from the log by consumers (ARCHITECTURE.md §4.6).

A ledger worker also consumes the topic and writes decisions to Postgres (ARCHITECTURE.md §4.6).

**Key rule:** the API accepts only the **raw transaction** (card, amount, merchant, category, timestamp, location). It never accepts precomputed features from the caller.

---

## 4. Data

**Dataset:** `combined_transactions.csv`, a Sparkov-generated credit card set (4 GB). It has been converted once to `data/processed/transactions.parquet` (485 MB, zstd). The conversion keeps only modeling columns and drops PII: SSN, names, and street address. All downstream code reads the Parquet file.

### Profile (measured 2026-11-26)

| | |
|---|---|
| Transactions | 12,553,594 (every `trans_num` is unique) |
| Cards | 9,970 (one account per card), 12 behavior profiles |
| Merchants / categories | 693 / 14 |
| Time range | 2025-01-01 → 2026-06-14 (UTC) |
| Fraud | 94,295 (0.75%) |
| Activity | median card about 2 tx/day; p99 about 6 tx/day |

### What the fraud looks like

- **Bursts:** 9,798 of the 9,970 cards (98%) contain exactly one fraud episode. Each episode is about 10 transactions (p10–p90: 7–12) packed into about 46 hours. Velocity features should be strong.
- **Amount:** median fraud amount is $464, against $44 for legitimate transactions. Fraud p25 is $259, against a legitimate p75 of $81.
- **Time of day:** 19:00–01:00 UTC carries a 2–3.5% fraud rate; every other hour is about 0.15%.
- **Category:** `shopping_net` (2.4%), `misc_net` (2.1%), and `grocery_pos` (1.9%) are the riskiest. `home` and `health_fitness` are about 0.2%.
- The `fraud_` prefix appears on **every** merchant name. It is a Sparkov naming artifact, not label leakage.

**Takeaway:** the model will score very well. That suits this project, because the ML is meant to be easy. Don't read high PR-AUC as an achievement; the effort goes into the streaming system.

### Gotchas that matter for the streaming system

- **The file is not in time order.** It is grouped by card, with 6.27M timestamp inversions. The simulator must replay in event-time order.
- **Timestamps tie:** 583 card/second pairs hold more than one transaction. The canonical order is therefore **`(unix_time, trans_num)`**, used everywhere: offline replay, the simulator, and tests. Without that tiebreak the core invariant does not hold.
- **The volume is seasonal:** Dec 2025 has 1.34M rows at a 0.39% fraud rate, and Jun 2026 is only partial.
- `cc_num` has up to 19 digits, so it is stored as a string. Some exceed the range of a signed 64-bit integer.

### Split (by time, never random)

| Split | Range | Purpose |
|---|---|---|
| Train | 2025-01 → 2025-12 | fit |
| Validation | 2026-01 → 2026-02 | early stopping, thresholds |
| Test | 2026-03 → 2026-06 | final numbers, plus the replay source for the streaming milestones |

The windows look back at most 24 h, so offline features are computed over the **full** sorted history before splitting. That way the first day of each split is not starved of history.

---

## 5. Feature definition: the single source of truth

The feature math exists once, in `finspeq/domain/features.py`, as a versioned `FeatureSet`. State
access goes through two ports (ARCHITECTURE.md §2.4):

```python
class WindowReader(Protocol):
    """Read access to per-card event windows."""

    def read(self, card: CardId, until: OrderKey) -> CardWindow:
        """Return the card's entries before until and within WINDOW_SECONDS of it."""
        ...


class WindowWriter(Protocol):
    """Atomic write access to per-card event windows."""

    def apply(self, batch: EventBatch) -> ApplyResult:
        """Insert the batch's events, prune by event time, and store the new position."""
        ...
```

Each port has two adapters. Both must pass the same contract test suite:

- **`MemoryWindowStore`** is used offline. It replays the sorted history to build the training set.
- **`RedisWindowStore`** is used online. It is backed by ZSETs, a hash, and a Lua script.

For each event, both paths run `read → FeatureSet.compute → (score or write row) → apply`.

### Initial features

| Feature | Window | Notes |
|---|---|---|
| `amount` | none | raw |
| `tx_count_1h`, `tx_count_24h` | event-time | ZCOUNT |
| `spend_24h` | event-time | sum of amounts in the window |
| `amount_ratio_to_avg_24h` | event-time | amount / mean of the window |
| `secs_since_last_tx` | none | |
| `distinct_merchants_24h` | event-time | |
| `category`, `hour_of_day` | none | encoded |

### Rules that prevent training-serving skew

- **Point-in-time:** decide once whether a feature includes the current transaction (default: it **excludes** it, which is "state before this event"). Both backends follow that decision.
- **Event time only:** windows use the transaction's own timestamp, never the clock time when it arrives.
- **Idempotent writes:** the ZSET member is `tx_id`; amount (integer cents) and merchant live in a
  companion hash keyed by `tx_id`.
- **Feature math lives only in the Python domain.** Redis stores raw windowed events, not features
  (ADR-004).
- **Staleness is modeled:** if the hot path can read stale state (§7, M5), training must include the same measured staleness.

---

## 6. Replay-equality harness

This is the most important piece of test infrastructure in the project.

1. Take a fixed slice of events (for example, 50k events from 500 cards), ordered by `(unix_time, trans_num)`.
2. **Reference:** compute features with `MemoryWindowStore`.
3. **System under test:** run the slice through Redpanda → worker → Redis, optionally with faults injected (crashes, duplicates, shuffling, rebalances).
4. **Compare:** check each event's feature vector against the reference, plus the final Redis state. Integer features must match exactly; float features must match within `1e-9`.
5. Score both vectors with ONNX and assert the decisions are identical.

The harness runs in CI-style scripts (`make replay-test FAULTS=crash,dup`).

---

## 7. Milestones

Each milestone ends with a replay test that passes and a short write-up in `notes/`.

### M0: Offline baseline (days, not weeks)

- ~~Download dataset; convert to Parquet~~ (done). ~~Write the domain model, `FeatureSet`, and `MemoryWindowStore`~~ (done). Build the training set.
- Train LightGBM, evaluating with PR-AUC and recall at 0.5% FPR.
- Choose thresholds from the false-positive budget on the validation set.
- Export to ONNX with `zipmap=False`. Assert LightGBM and ONNX outputs agree within 1e-5.

### M1: Single partition, single worker

- Write `docker-compose.yml` for Redpanda (`redpandadata/redpanda`) and Redis.
- Build a simulator that replays events at N per second.
- Write the feature worker: poll loop, `RedisWindowStore` with `apply_batch.lua`, manual commit.
- Build the replay-equality harness (§6). It passes with no faults.

### M2: Crashes and delivery guarantees

- Kill the worker at random points (after the Redis write, before the commit).
- Show that a naive `INCR` counter double-counts, while an idempotent `ZADD` on `tx_id` does not.
- Store the committed offset in Redis **in the same MULTI/EXEC transaction** as the state update. Restart from the stored offset ("effectively once").
- Recovery drill: run `FLUSHALL` on Redis, rebuild from offset 0, and check that the state is identical.

### M3: Disorder and lateness

- The simulator injects bounded shuffling and long-tail delays.
- Implement a watermark: `max_event_time_seen - allowed_lateness`.
- Choose a policy for events that arrive after the watermark (drop, side-output, or correct the state), and measure how it affects features.
- Metrics: late-event count and watermark lag.

### M4: Scaling out

- Use 4–8 partitions keyed by `card_id`; run several workers in one consumer group.
- Experiment: produce with no key and watch per-card ordering, and therefore the features, break.
- Push the producer to more than 5k events per second; watch consumer lag; observe a rebalance mid-stream.
- Handle rebalances correctly: commit on partition revoke, and avoid processing during the handoff.

### M5: Hot path and the race

- Build the FastAPI scoring endpoint: pipelined Redis read → ONNX → decision → publish.
- **The race:** two transactions from the same card arrive milliseconds apart, so the second read can miss the first write.
  - Option A: the API writes state synchronously (read-your-writes), at the cost of latency.
  - Option B: accept the staleness, measure it, and model it in training.
  - Implement both and measure each one's effect on latency and recall.
- Load test with Locust or k6; report p50, p95, and p99.

### Optional M6

Evidently drift reports, Grafana, a second model in shadow mode, delayed-label simulation.

---

## 8. Observability (added at each milestone, not at the end)

Prometheus metrics:

- `inference_latency_ms` (histogram)
- `redis_lookup_ms`
- `consumer_lag` per partition
- `events_processed_total`, `duplicates_skipped_total`, `late_events_total`
- `feature_staleness_ms`
- `decisions_total{decision=...}`

---

## 9. Repository layout

See ARCHITECTURE.md §2.7 for the hexagonal package layout (domain / application / adapters /
bootstrap) and the test layout.

---

## 10. Open questions

1. How long should late events be allowed to arrive (allowed lateness)? Decide in M3 from the data.
2. ~~Should `amount` live in a companion hash or in the ZSET member?~~ Decided: companion hash.
3. Which side of the race to choose (M5): read-your-writes or measured staleness?
4. ~~Publish before or after returning the decision?~~ Decided: before (ARCHITECTURE.md §4.6).

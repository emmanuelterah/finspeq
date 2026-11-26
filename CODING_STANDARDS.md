# Finspeq — Coding Standards

These rules apply to every file in the repository. CI enforces the ones that tooling can check (§8).
Reviews enforce the rest.

---

## 1. Line length

- **Hard limit: 100 characters per line.** This covers code, comments, docstrings, and strings in
  `.py`, `.lua`, `.sql`, `.toml`, `.yml`, and `.md` code blocks.
- Wrap long expressions inside parentheses. Do not use backslash continuation.
- Split long strings with implicit concatenation inside parentheses.

## 2. Docstrings

### 2.1 Coverage

Every module, class, function, and method has a docstring. That includes private ones (`_name`),
`__init__`, and dunder methods. Test functions also have one.

### 2.2 Content: what it does, not why it exists

A docstring states facts a caller needs:

- What the code does, as one imperative summary line ("Return …", "Apply …", "Raise …").
- Inputs, outputs, and exceptions (`Args:`, `Returns:`, `Yields:`, `Raises:`). Include only the
  sections that apply.
- Side effects (I/O, mutation, network calls), preconditions, ordering, and **units**
  (cents, epoch seconds, milliseconds).

A docstring **never** contains:

- Rationale or justification ("we chose this because …", "this is better than …").
- History ("previously …", "after the bug …", "now uses …").
- Promotional or vague words: *robust, seamless, efficient, powerful, elegant, leverage, utilize,
  ensure* (as a claim), *simply, just, various, appropriate, handle* (unless it says how).
- First-person pronouns: *we, our, I*.
- A restatement of the type signature without new information.

Rationale belongs in ARCHITECTURE.md or an ADR (`docs/adr/`). If a reader needs the reason, the
docstring can point there in one short line: `See ADR-003.`

### 2.3 Form

- Google style. The summary line fits on one line, ends with a period, and is in the imperative
  mood.
- Leave one blank line between the summary and the details.
- Keep it as short as the facts allow. A function with an obvious contract gets a one-line docstring.
- Module docstring: one line saying what the module contains.
- Class docstring: what one instance represents or does. Constructor arguments go in the `__init__`
  docstring.

### 2.4 Examples

Rejected:

```python
def read(self, card: CardId, until: OrderKey) -> CardWindow:
    """Robustly and efficiently fetch the window for the card.

    We use a pipeline here because it is faster than separate calls, which ensures
    scalability and avoids the latency issues we saw earlier.
    """
```

Accepted:

```python
def read(self, card: CardId, until: OrderKey) -> CardWindow:
    """Return the card's entries before until and within WINDOW_SECONDS of it.

    Sends one Redis round trip. Entries are ordered by (time, tx_id).

    Args:
        card: Card whose window is read.
        until: Exclusive upper bound of the window (the scored transaction's key).

    Returns:
        The window. Empty if the card has no stored entries.

    Raises:
        StoreUnavailableError: Redis did not reply within the configured timeout.
    """
```

Accepted, short form:

```python
def decide(self, score: Score) -> Decision:
    """Return BLOCK at or above block, REVIEW at or above review, otherwise APPROVE."""
```

Accepted, test:

```python
def test_reapplying_stored_batch_changes_nothing(store: WindowWriter) -> None:
    """Apply a batch twice and assert the state and position match a single apply."""
```

### 2.5 Comments

Comments follow the same rules as docstrings. Use inline comments only for a fact the code cannot
show, such as `# Redpanda offsets are contiguous without transactions.` Never commit commented-out
code.

## 3. Architecture rules (see ARCHITECTURE.md §2)

- `domain` and `application` import only the standard library and inner rings.
- Adapters do not import other adapters, except the shared `adapters/codec.py`.
- Only the composition roots in `finspeq/bootstrap/` construct adapters.
- Every import is at the top of its module (ruff `PLC0415`): no imports inside functions and no
  `TYPE_CHECKING` blocks. A process that must not load a library gets its own composition root
  (`bootstrap/offline.py`, `bootstrap/streaming.py`) instead of a deferred import.
- No module-level side effects: no connections, file reads, or model loads at import time.
- No global mutable state. Pass dependencies through `__init__`.
- Read time through the `Clock` port. Never call `time.time()` in the core.

## 4. Types and data

- Every function signature is fully annotated. mypy runs in `--strict` mode.
- No `Any` in `domain` or `application`.
- Value objects are `@dataclass(frozen=True, slots=True)` and validate in `__post_init__`.
- Use `enum.Enum` for closed sets (`Decision`, `Category`).
- Money is `int` cents. Time is `int` epoch seconds in the domain.
- Collections in value objects are tuples, never lists.

## 5. Functions and classes

- A function does one thing (SOLID-S). Soft limit: 30 lines of body.
- At most 5 parameters. Group related ones into a value object.
- No mutable default arguments.
- No unused parameters (ruff `ARG`). When an interface passes data that some implementations
  do not need, pass one parameter object instead (for example `FeatureContext`). The one
  exception is a callback whose signature a third-party library fixes; name its unused parameters
  with a leading underscore (for example `_consumer`).
- No boolean flag parameters that switch behavior. Write two functions instead.
- Ports are `typing.Protocol` classes in `application/ports.py`. Adapters subclass them explicitly.

## 6. Errors

- All project exceptions derive from `FinspeqError`. Domain rule violations derive from
  `DomainError`; port failures derive from `PortError` (for example `StoreUnavailableError` or
  `PublishFailedError`).
- Exception names end in `Error` (ruff N818).
- Adapters catch library exceptions and raise the matching port error, chained with `from exc`.
- No bare `except:`. No `except Exception` outside the inbound adapters' top-level handlers.
- Never swallow an error silently. Either raise, or count it in a metric and log it.

## 7. Tests

| Layer | Location | Needs containers | Rule |
|---|---|---|---|
| Unit | `tests/unit/` | No | Domain and use cases with memory adapters |
| Contract | `tests/contract/` | Redis/Postgres adapters only | One suite per port, parametrized over every adapter |
| Integration | `tests/integration/` | Yes | Real broker, Redis, and Postgres via compose |
| Replay | `tests/replay/` | Yes | `VerifyReplay` scenarios with fault injection |

- Test names state the behavior: `test_<condition>_<expected_result>`.
- Tests are deterministic: fixed seeds, `FixedClock`, no sleeps for synchronization.

## 8. Enforcement

`pyproject.toml`:

```toml
[tool.ruff]
line-length = 100
target-version = "py311"

[tool.ruff.lint]
select = ["E", "W", "F", "I", "N", "D", "UP", "B", "SIM", "ANN", "ARG", "PLR", "RUF"]
extend-select = ["D401", "W505"]

[tool.ruff.lint.pydocstyle]
convention = "google"

[tool.ruff.lint.pycodestyle]
max-line-length = 100
max-doc-length = 100

[tool.ruff.lint.pylint]
max-args = 5

[tool.mypy]
strict = true
python_version = "3.11"

[tool.importlinter]
root_package = "finspeq"
include_external_packages = true

[[tool.importlinter.contracts]]
name = "Hexagonal rings"
type = "layers"
layers = [
    "finspeq.bootstrap",
    "finspeq.adapters",
    "finspeq.application",
    "finspeq.domain",
]

[[tool.importlinter.contracts]]
name = "Core imports no third-party packages"
type = "forbidden"
source_modules = ["finspeq.domain", "finspeq.application"]
forbidden_modules = [
    "redis", "confluent_kafka", "onnxruntime", "lightgbm", "numpy", "pandas",
    "duckdb", "pyarrow", "fastapi", "pydantic", "psycopg", "prometheus_client",
]

[[tool.importlinter.contracts]]
name = "Inbound and outbound adapters are independent"
type = "independence"
modules = ["finspeq.adapters.inbound", "finspeq.adapters.outbound"]
```

The `pydocstyle` Google convention turns off D401 (imperative mood), so `extend-select` turns it
back on.

`W505` applies the 100-character limit to docstrings and comments.

Files that are not Python (`.lua`, `.sql`, `.yml`, `.toml`) are checked by
`scripts/check_line_length.py`, run from pre-commit.

The CI order is: `ruff format --check`, `ruff check`, `mypy`, `lint-imports`,
`check_line_length`, `pytest tests/unit tests/contract`.

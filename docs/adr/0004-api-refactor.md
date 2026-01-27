# 4. trie-matching

## Status

Proposed

## Context

For alpha, we've kept the API simple and functional to allow for rapid iteration. However, as we
approach a stable 1.0 release, it's important to consider usability and maintainability for a broader
audience.

Whilst the functional API has been good for iteration and prototyping, it has several limitations for a beta build:

- Users must understand the internal data flow between cleaning, exact matching, and
  probabilistic linkage stages.
- DuckDB connection management is manual and requires constant injections.
- Checkpointing and reuse of cleaned canonical datasets is not as straightforward as we would like it to be.
- The API surface is broad, making it difficult to maintain backwards compatibility. Ideally we'd have a more abstract interface that shields users from internal changes.
- It's very difficult to ensure the API remains stable as we add new features or change internal implementations. With a class-based approach, we can keep a stable interface whilst evolving the internals.

This ADR proposes a class-based API using the **builder design pattern**, inspired by
[Splink](https://github.com/moj-analytical-services/splink). This will provide a more
intuitive, discoverable interface whilst preserving flexibility for advanced users.

## Decision

We will introduce a central `AddressMatcher` (final name tbc) class that encapsulates the matching
workflow. Users will interact with the library through method chaining and clearly
defined configuration objects.

This is a standard pattern for data processing and machine learning libraries, and will make it easier
for new users to get started quickly.

---

## Pipeline stage notes

Our pipelines currently consist of three main stages:
1. Data cleaning and preparation, broken down into standard cleaning and tokenisation 
2. Deterministic exact matching
3. Probabilistic matching via Splink

These stages are interwoven amongst various utility functions. In the new design, these will be
encapsulated within methods of the `AddressMatcher` class. The internal implementation can evolve
without affecting the public API.

---

## API Design Overview

### Core Classes

| Class | Responsibility |
|-------|----------------|
| `AddressMatcher` | Primary entry point; orchestrates the full matching pipeline |
| `Addresses` | Wrapper for address data (canonical or messy) with persistence |
| `MatchResult` | Container for match outputs with convenience methods |
| `MatcherSettings` | Configuration for matching behaviour and thresholds |

> [!NOTE]
> **Design note:** We use a single `Addresses` class rather than separate
> `CanonicalAddresses` and `AddressesToMatch` classes. The underlying schema is
> identical, and the role (canonical vs. messy) is already clear from the argument
> names in the API. This keeps the interface simple whilst allowing flexibility.

### High-Level Workflow

<details>
<summary>Example: Basic matching workflow</summary>

```python
from uk_address_matcher import AddressMatcher, Addresses

# Load address datasets
canonical = Addresses.from_file("./canonical_addresses.parquet")
messy = Addresses.from_file("./messy_addresses.parquet")

# Create the matcher
matcher = AddressMatcher(canonical_addresses=canonical, addresses_to_match=messy)

# Run matching stages
exact = matcher.match_deterministic()
prob = matcher.match_probabilistic()
final = matcher.combine_results()

# Inspect results
final.summary().show()
final.to_parquet("./matched_addresses.parquet")
```

</details>

---

## Detailed API Specification

### 1. `Addresses`

Represents a cleaned, indexed address dataset. Used for both canonical/gazetteer data
and messy input addresses. Supports loading from various sources and persisting to disk
for reuse.

#### Construction

```python
from uk_address_matcher import Addresses
import duckdb

# From a file path (parquet, CSV, or other DuckDB-supported formats)
addresses = Addresses.from_file(
    path: str | Path,
    *,
    cleaned: bool | None = None,  # None = auto-detect
    con: duckdb.DuckDBPyConnection | str | None = None,
)

# From a DuckDB relation
addresses = Addresses.from_relation(
    relation: duckdb.DuckDBPyRelation,
    *,
    cleaned: bool | None = None,
    con: duckdb.DuckDBPyConnection | str | None = None,
)

# From a pandas DataFrame
addresses = Addresses.from_dataframe(
    df: pd.DataFrame,
    *,
    cleaned: bool | None = None,
    con: duckdb.DuckDBPyConnection | str | None = None,
)
```

An alternative to this could be to simply have a single `Addresses()` constructor that
accepts multiple input types. However, I feel that using class methods improves discoverability and
clarity in code completion.

#### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `path` | `str \| Path` | — | Path to input file |
| `relation` | `duckdb.DuckDBPyRelation` | — | DuckDB relation containing address data |
| `df` | `pd.DataFrame` | — | Pandas DataFrame with address data |
| `cleaned` | `bool \| None` | `None` | Whether data is pre-cleaned (`None` = auto-detect) |
| `con` | `duckdb.DuckDBPyConnection \| str \| None` | `None` | DuckDB connection, path to database file, or `None` for in-memory |

#### Required Input Schema

We previously required that input data must contain the following columns:

| Column | DuckDB Type | Description |
|--------|-------------|-------------|
| `unique_id` | `BIGINT` or `VARCHAR` | Unique identifier for each record |
| `source_dataset` | `VARCHAR` | Source dataset label (e.g., `'os_addressbase'`) |
| `address_concat` | `VARCHAR` | Full address excluding postcode |
| `postcode` | `VARCHAR` | UK postcode |

But this requires:
1. Users to manually create a `unique_id` column if not present, despite the fact that we have moved to our own internal unique ID (`ukam_address_id`) for consistency.
2. The `source_dataset` column is dropped during cleaning and not used in matching, so it's unnecessary overhead.
3. We require users to split `address_concat` and `postcode`, which is inconvenient. Most users seem to have full addresses including postcodes in a single column.

In the new design, we will relax these requirements. The only mandatory column will be
`address_concat` (which may include postcode). If users have a `unique_id` column (which they will likely want to preserve), we can create a basic lookup view to map to our internal IDs.

#### Methods

```python
# Save cleaned data for later reuse (saves as parquet)
addresses.save(path: str | Path, overwrite: bool = False) -> None
# Alternatively, we could expose this as plain parquet saving - with optional partitioning?
addresses.to_parquet(path: str | Path, overwrite: bool = False) -> None

# Access the underlying DuckDB relation
addresses.relation -> duckdb.DuckDBPyRelation

# Access the DuckDB connection
addresses.con -> duckdb.DuckDBPyConnection

# Check if data has been cleaned
addresses.is_cleaned -> bool

# Preview the data
addresses.head(n: int = 10) -> duckdb.DuckDBPyRelation

# Get record count
len(addresses) -> int
```

<details>
<summary>Example: Building and saving a canonical dataset</summary>

```python
from uk_address_matcher import Addresses

# Clean and prepare the canonical dataset (expensive operation)
canonical = Addresses.from_file(
    "./raw_os_addressbase.parquet",
    con="./matcher.duckdb",  # Persist to disk
)

# Save cleaned version for future reuse
canonical.save("./cleaned_canonical.parquet")
# or
canonical.to_parquet("./cleaned_canonical.parquet")

# In future sessions, load the pre-cleaned file:
canonical = Addresses.from_file("./cleaned_canonical.parquet")  # Auto-detects cleaned
```

</details>

---

### 2. `MatcherSettings`

Configuration object for controlling matching behaviour. We keep a single settings object so users
only have one place to look for defaults, we avoid duplicated options across stages, and we can
add new settings without breaking the public API. Per-call overrides remain possible for the odd
run that needs different thresholds.

> [!NOTE]
> `MatcherSettings` is not imperative for 1.0. We can defer it if users have sufficient control
> via `AddressMatcher` constructor and method parameters. However, it does improve discoverability.

#### Construction

```python
from uk_address_matcher import MatcherSettings

settings = MatcherSettings(
    # Thresholds
    match_weight_threshold: float = 15.0,
    distinguishability_threshold: float | None = None,
    splink_prediction_threshold: float = -50.0,

    # Deterministic matching options
    enable_trie_matching: bool = True,

    # Output options
    include_unmatched: bool = True,
    retain_intermediate_columns: bool = False,
)
```

#### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `match_weight_threshold` | `float` | `15.0` | Minimum match weight for a confident match |
| `distinguishability_threshold` | `float \| None` | `None` | Minimum gap to second-best match (optional) |
| `splink_prediction_threshold` | `float` | `-50.0` | Threshold for Splink candidate generation |
| `enable_trie_matching` | `bool` | `True` | Use trie-based suffix matching in deterministic pass |
| `enable_numeric_token_matching` | `bool` | `True` | Use numeric token matching in deterministic pass |
| `include_unmatched` | `bool` | `True` | Include records with no confident match in output |
| `retain_intermediate_columns` | `bool` | `False` | Keep Splink comparison columns in output |

---

### 3. `AddressMatcher`

The primary entry point for address matching operations.

#### Construction

```python
from uk_address_matcher import AddressMatcher, Addresses, MatcherSettings

# Basic construction
matcher = AddressMatcher(
    canonical_addresses: Addresses,
    addresses_to_match: Addresses,
    *,
    settings: MatcherSettings | None = None,
    con: duckdb.DuckDBPyConnection | str | None = None,
)

# With custom settings
settings = MatcherSettings(match_weight_threshold=20.0)
matcher = AddressMatcher(canonical_addresses=canonical, addresses_to_match=messy, settings=settings)
```

#### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `canonical_addresses` | `Addresses` | The canonical address dataset to match against |
| `addresses_to_match` | `Addresses` | The address dataset to match |
| `settings` | `MatcherSettings \| None` | Matching configuration (uses defaults if `None`) |
| `con` | `duckdb.DuckDBPyConnection \| str \| None` | DuckDB connection override |

> [!NOTE]
> **Connection resolution order:** When `con` is not supplied, the matcher will:
> 1. Use the connection from `canonical_addresses` if available.
> 2. Fall back to the connection from `addresses_to_match`.
> 3. Create a new in-memory DuckDB connection if neither has one.
>
> This allows you to share a connection across datasets or let the library manage it for you.

#### Primary Methods

We expose the matching pipeline as separate methods so users can inspect intermediate results,
tune thresholds, and compose stages as needed. This mirrors the real workflow where you typically
want to check exact-match rates before deciding whether to run the more expensive probabilistic
pass.

> [!WARNING]
> **Stage ordering:** Calling `match_probabilistic()` before `match_deterministic()` raises
> `MatcherStateError`. Similarly, `combine_results()` requires both prior stages to have run.
> This ensures the internal state is consistent and prevents silent errors.

##### `match_deterministic()`

Run the deterministic matching stages (exact postcode + address, trie-based suffix matching,
numeric token alignment). Returns a `MatchResult` containing exact matches and unmatched records.

```python
def match_deterministic(
    self,
    *,
    settings: MatcherSettings | None = None,
) -> MatchResult:
    ...
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `settings` | `MatcherSettings \| None` | Override settings for this run only |

**Returns:** `MatchResult` with exact matches and unmatched records.

<details>
<summary>Example</summary>

```python
exact_results = matcher.match_deterministic()

# Inspect before deciding to run probabilistic
print(f"Exact match rate: {exact_results.match_rate:.1%}")
exact_results.by_match_reason().show()
```

</details>

##### `match_probabilistic()`

Run the probabilistic (Splink) stage on records that were not matched deterministically. Must be
called after `match_deterministic()`. The matcher tracks state internally, so you don't need to
pass the deterministic result.

```python
def match_probabilistic(
    self,
    *,
    settings: MatcherSettings | None = None,
) -> MatchResult:
    ...
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `settings` | `MatcherSettings \| None` | Override settings for this run only |

**Returns:** `MatchResult` with probabilistic matches for previously unmatched records.

<details>
<summary>Example</summary>

```python
exact_results = matcher.match_deterministic()
prob_results = matcher.match_probabilistic()

# Combine or inspect separately
prob_results.above_threshold(match_weight=15).show()
```

</details>

##### `combine_results()`

Merge deterministic and probabilistic results into a single `MatchResult`, applying final
thresholds and producing the unified output schema.

The matcher tracks which stages have been run internally, so you don't need to pass the
intermediate results—just call `combine_results()` and it pulls together whatever has been
computed.

```python
def combine_results(
    self,
    *,
    match_weight_threshold: float = 15.0,
    distinguishability_threshold: float | None = None,
    include_unmatched: bool = True,
) -> MatchResult:
    ...
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `match_weight_threshold` | `float` | Minimum weight for confident probabilistic match |
| `distinguishability_threshold` | `float \| None` | Minimum gap to second-best candidate |
| `include_unmatched` | `bool` | Whether to include unmatched records in output |

**Returns:** `MatchResult` with all matches combined.

<details>
<summary>Example</summary>

```python
matcher.match_deterministic()
matcher.match_probabilistic()
final = matcher.combine_results(match_weight_threshold=20)

final.to_parquet("./matched_addresses.parquet")
```

</details>

<details>
<summary>Example: Typical workflow</summary>

```python
from uk_address_matcher import AddressMatcher, Addresses

canonical = Addresses.from_file("./os_addressbase.parquet")
messy = Addresses.from_file("./epc_addresses.parquet")

matcher = AddressMatcher(canonical_addresses=canonical, addresses_to_match=messy)

# Stage 1: Deterministic
exact = matcher.match_deterministic()
print(f"Exact matches: {exact.matched_count} ({exact.match_rate:.1%})")

# Stage 2: Probabilistic (only if needed)
if exact.unmatched_count > 0:
    prob = matcher.match_probabilistic()
    prob.above_threshold(match_weight=10).sample(20).show()

# Stage 3: Combine (pulls from internal state)
final = matcher.combine_results(match_weight_threshold=15)

final.to_parquet("./matched_epc.parquet")
```

</details>

#### Helper Methods and Utilities

##### `available_deterministic_stages()`

List the deterministic matching stages that can be enabled. `EXACT_MATCHES` is always on and not
listed here.

```python
from uk_address_matcher import available_deterministic_stages

for stage in available_deterministic_stages():
    print(f"  - {stage.value} (StageName.{stage.name})")
```

##### Enabling Additional Stages

Pass `enabled_stages` to `match_deterministic()` to opt-in to extra matching strategies:

```python
from uk_address_matcher import StageName

# Enable trigram matching (only if canonical is complete)
exact = matcher.match_deterministic(
    enabled_stages=[StageName.UNIQUE_TRIGRAM],
)
```

##### `match_one()`

Match a single address string. Useful for testing, debugging, or interactive exploration. The
address should include the postcode - the library will parse it automatically using regex.

> [!NOTE]
> `match_one()` is a standalone convenience method. It does **not** require prior calls to
> `match_deterministic()` or `match_probabilistic()` - it runs a self-contained matching
> pipeline against the canonical addresses.

```python
def match_one(
    self,
    address: str,
    *,
    top_n: int = 5,
) -> MatchResult:
    ...
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `address` | `str` | The full address string (including postcode) |
| `top_n` | `int` | Number of top candidates to return |

**Returns:** `MatchResult`

<details>
<summary>Example</summary>

```python
# Quick single-address lookup - cleaned on the fly
result = matcher.match_one("10 Downing Street, Westminster, SW1A 2AA")

# Show top candidates
result.head(5)
```

</details>

---

### 4. `MatchResult`

Container for match outputs with convenience methods for inspection and export.

Match results are also stored internally within the `AddressMatcher` instance, so users can
inspect intermediate outputs without needing to pass around `MatchResult` objects.

This allows us to wrap convenience methods around the result objects and consolidate our separate matching outputs.

#### Properties

```python
# Access the underlying DuckDB relation
result.relation -> duckdb.DuckDBPyRelation

# Get the number of input records
result.input_count -> int

# Get the number of matched records
result.matched_count -> int

# Get the number of unmatched records
result.unmatched_count -> int

# Get match rate as a proportion
result.match_rate -> float
```

#### Misc Methods

##### Summary Statistics

```python
# Get match metrics breakdown
result.summary() -> duckdb.DuckDBPyRelation

# Get breakdown by match reason
result.by_match_reason() -> duckdb.DuckDBPyRelation
```

##### Inspection and Debugging

```python
# Filter to specific match reasons
result.filter_by_reason(
    reason: str | list[str],
) -> MatchResult

# Get records above/below thresholds
result.above_threshold(
    match_weight: float | None = None,
    distinguishability: float | None = None,
) -> MatchResult

result.below_threshold(
    match_weight: float | None = None,
    distinguishability: float | None = None,
) -> MatchResult

# Sample random records for inspection
result.sample(n: int = 10) -> MatchResult
```

---

## Consequences

### Positive

- **Simpler onboarding**: New users can match addresses in three lines of code.
- **Checkpointing**: Expensive canonical cleaning happens once and is reusable.
- **Discoverable API**: Method chaining and IDE autocompletion guide users.
- **Stable interface**: Internal changes will not break user code.

### Negative

- **Migration effort**: Existing users must update their code.
- **Flexibility trade-off**: Advanced users may need to access internal APIs.

### Mitigation

- Expose `matcher.linker` and `matcher.con` for advanced use cases.
- Maintain functional utilities in a `uk_address_matcher.legacy` namespace for one
  major version cycle.

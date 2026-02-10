# 4. API Refactor

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

We will introduce a central `AddressMatcher` class that encapsulates the matching workflow. Users
will interact with the library through clearly defined configuration objects and a simple `match()`
method that encapsulates all matching stages.

This is a standard pattern for data processing and machine learning libraries, and will make it easier
for new users to get started quickly.

---

## Pipeline Stage Notes

Our pipelines currently consist of three main stages:
1. Data cleaning and preparation, broken down into standard cleaning and tokenisation
2. Deterministic exact matching
3. Probabilistic matching via Splink

These stages are interwoven amongst various utility functions. In the new design, these will be
encapsulated within the `AddressMatcher` class. The internal implementation can evolve without
affecting the public API.

---

## API Design Overview

### Core Components

| Component | Responsibility |
|-----------|----------------|
| `AddressMatcher` | Primary entry point; orchestrates the full matching pipeline |
| `MatchResult` | Container for match outputs with convenience methods - this is be handled in another PR w/ some notes available in https://github.com/moj-analytical-services/uk_address_matcher/issues/181 |
| `MatcherSettings` | Configuration for matching behaviour and thresholds |
| `prepare_and_persist_canonical_data()` | Function to pre-clean canonical data and save to folder |

### Canonical Data Modes

The matcher supports two modes for canonical data:

1. **On-the-fly cleaning**: Pass a raw `DuckDBPyRelation`. The matcher cleans the data and
   derives term frequencies internally. Simple but slower for repeated matching against a large canonical dataset.

2. **Pre-prepared folder**: Pass a path to a folder containing pre-cleaned artefacts. The matcher
   loads the files automatically. Faster for repeated matching against large canonical datasets. This is not intended for use initially by the messy addresses, but we could support this in future if there is demand.

#### Prepared Folder Structure

When using `prepare_and_persist_canonical_data()`, the function writes three files to a folder:

```
./prepared_canonical/
├── addresses.parquet       # Cleaned and tokenised addresses
├── term_frequencies.parquet # Term frequency lookup table
└── inverted_index.parquet   # Inverted index for candidate retrieval
```

The matcher recognises this structure and loads all three files when given the folder path.

Users should be able to overwrite this folder with new prepared data if needed, and the matcher should validate the presence of required files when loading.

---

## User Paths

### Path 1: Simple Matching (Most Common)

This is the typical non-power-user path. It should be as easy as possible with minimal understanding
of `uk_address_matcher` internals.

```python
import duckdb
from uk_address_matcher import AddressMatcher

con = duckdb.connect()

canonical = con.read_parquet("./canonical_addresses.parquet")
messy = con.read_parquet("./messy_addresses.parquet")

matcher = AddressMatcher(
    canonical_addresses=canonical,
    addresses_to_match=messy,
    con=con,
)

result = matcher.match()

result.to_parquet("./matched_addresses.parquet")
```

In this mode, the matcher:
- Cleans the canonical addresses on the fly
- Derives term frequencies on the fly
- Builds any required indices internally

This is the easiest path but involves repeated work if you match against the same canonical
dataset multiple times.

### Path 2: Pre-Prepared Canonical Data (Power Users)

For users matching against large canonical datasets (e.g. full AddressBase), pre-preparing the
canonical data avoids repeated expensive cleaning operations.

#### Step 1: Prepare and persist canonical data (one-time)

```python
import duckdb
from uk_address_matcher import prepare_and_persist_canonical_data

con = duckdb.connect()

df_os_raw = con.read_parquet("./raw_addressbase.parquet")

# One call prepares everything and writes to folder:
# - cleaned/tokenised addresses
# - term frequency table
# - inverted index
prepare_and_persist_canonical_data(
    df_os_raw,
    output_folder="./prepared_addressbase",
    con=con,
    overwrite=True,
    # optional:
    # address_column="full_address",
    # postcode_column=None,  # parse from address if not provided
)
```

This creates:
```
./prepared_addressbase/
├── addresses.parquet
├── term_frequencies.parquet
└── inverted_index.parquet
```

#### Step 2: Use prepared folder in matcher

```python
import duckdb
from uk_address_matcher import AddressMatcher, MatcherSettings

con = duckdb.connect()

df_messy = con.read_parquet("./messy_addresses.parquet")

# Point to prepared folder instead of raw relation
matcher = AddressMatcher(
    canonical_addresses="./prepared_addressbase",  # folder path
    addresses_to_match=df_messy,
    con=con,
    settings=MatcherSettings(
        match_weight_threshold=15,
        splink_prediction_threshold=-50,
        include_unmatched=True,
    ),
)

result = matcher.match()
```

When `canonical_addresses` is a string path to a folder, the matcher automatically loads:
- `addresses.parquet` as the cleaned canonical addresses
- `term_frequencies.parquet` as the TF lookup
- `inverted_index.parquet` as the inverted index

---

## Detailed API Specification

### 1. `MatcherSettings`

Configuration object for controlling matching behaviour. Implemented as a `dataclass` for:
- IDE autocomplete and type hints
- Clear documentation of defaults
- Easy introspection of available options

We keep a single settings object so users only have one place to look for defaults, we avoid
duplicated options across stages, and we can add new settings without breaking the public API.

#### Implementation

```python
from dataclasses import dataclass, field
from enum import Enum

class MatchingStage(Enum):
    """Available matching stages beyond exact matching."""
    EXACT_MATCH = "exact_match"           # Always runs, cannot be disabled
    TRIGRAM = "trigram"                   # Trigram-based fuzzy matching
    PROBABILISTIC = "probabilistic"       # Splink probabilistic matching

@dataclass
class MatcherSettings:
    """Configuration for address matching behaviour."""

    # === Thresholds ===
    match_weight_threshold: float = 15.0
    """Minimum match weight for a confident match."""

    distinguishability_threshold: float | None = None
    """Minimum gap between best and second-best match (optional)."""

    splink_prediction_threshold: float = -50.0
    """Threshold for Splink candidate generation."""

    # === Matching Stages ===
    # Exact matching always runs first. Additional stages are optional.
    additional_stages: list[MatchingStage] = field(
        default_factory=lambda: [MatchingStage.PROBABILISTIC]
    )
    """Additional matching stages to run after exact matching."""

    # === Output Options ===
    include_unmatched: bool = True
    """Include records with no confident match in output."""

    retain_intermediate_columns: bool = False
    """Keep Splink comparison columns in output for debugging."""

    @staticmethod
    def available_stages() -> list[MatchingStage]:
        """Return all available matching stages (for discoverability)."""
        return list(MatchingStage)

    @staticmethod
    def describe_stages() -> None:
        """Print descriptions of all available matching stages."""
        descriptions = {
            MatchingStage.EXACT_MATCH: "Exact postcode + address concat matching (always runs)",
            MatchingStage.TRIGRAM: "Trigram-based fuzzy matching (useful when canonical is complete)",
            MatchingStage.PROBABILISTIC: "Splink probabilistic matching for remaining unmatched",
        }
        print("Available matching stages:")
        for stage in MatchingStage:
            print(f"  - {stage.name}: {descriptions[stage]}")
```

#### Parameters

##### Thresholds

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `match_weight_threshold` | `float` | `15.0` | Minimum match weight for a confident match |
| `distinguishability_threshold` | `float \| None` | `None` | Minimum gap to second-best match (optional) |
| `splink_prediction_threshold` | `float` | `-50.0` | Threshold for Splink candidate generation |

##### Matching Stages

Exact matching (postcode + address concat) always runs first and cannot be disabled.
Additional stages are specified as a list. I think we probably also want probabilitsic matching by default, but listing it below so it is noted in our documentation and discoverable via `MatcherSettings.describe_stages()`.

| Stage | Description |
|-------|-------------|
| `MatchingStage.EXACT_MATCH` | Exact postcode + address concat matching (always runs) |
| `MatchingStage.TRIGRAM` | Trigram-based fuzzy matching |
| `MatchingStage.PROBABILISTIC` | Splink probabilistic matching |

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `additional_stages` | `list[MatchingStage]` | `[PROBABILISTIC]` | Stages to run after exact matching |

We can then log the stages being run at the start of `match()` for transparency.

##### Output Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `include_unmatched` | `bool` | `True` | Include unmatched records in output |
| `retain_intermediate_columns` | `bool` | `False` | Keep Splink comparison columns |

<details>
<summary>Example: Discovering available stages</summary>

```python
from uk_address_matcher import MatcherSettings, MatchingStage

# See all available stages
MatcherSettings.describe_stages()
# Available matching stages:
#   - EXACT_MATCH: Exact postcode + address concat matching (always runs)
#   - TRIGRAM: Trigram-based fuzzy matching (useful when canonical is complete)
#   - PROBABILISTIC: Splink probabilistic matching for remaining unmatched

# Or get the list programmatically
stages = MatcherSettings.available_stages()
```

</details>

<details>
<summary>Example: Customising settings</summary>

For this, we may want to add some additional functionality to our [match_reasons ENUM](../../uk_address_matcher/sql_pipeline/match_reasons.py) to ensure all info relating to match stages is grouped and we can more easily compile this info for users.

```python
from uk_address_matcher import MatcherSettings, MatchingStage

# Default settings (exact + probabilistic)
settings = MatcherSettings()

# Include trigram matching (when canonical data is complete)
settings = MatcherSettings(
    additional_stages=[MatchingStage.TRIGRAM, MatchingStage.PROBABILISTIC]
)

# Stricter thresholds
settings = MatcherSettings(
    match_weight_threshold=20.0,
    distinguishability_threshold=5.0,
)
```

</details>

---

### 2. `prepare_and_persist_canonical_data()`

A function to prepare canonical data and persist it to a folder for later use. This performs:
- Address cleaning and tokenisation
- Term frequency computation
- Inverted index generation

```python
from uk_address_matcher import prepare_and_persist_canonical_data

prepare_and_persist_canonical_data(
    data: duckdb.DuckDBPyRelation,
    output_folder: str | Path,
    *,
    con: duckdb.DuckDBPyConnection | None = None,
    overwrite: bool = False,
    address_column: str = "address_concat",
    postcode_column: str | None = "postcode",  # None = parse from address
) -> None
```

#### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `data` | `DuckDBPyRelation` | — | Raw canonical address data |
| `output_folder` | `str \| Path` | — | Folder to write prepared artefacts |
| `con` | `DuckDBPyConnection \| None` | `None` | DuckDB connection (uses data's connection if `None`) |
| `overwrite` | `bool` | `False` | Whether to overwrite existing files |
| `address_column` | `str` | `"address_concat"` | Column containing address text |
| `postcode_column` | `str \| None` | `"postcode"` | Column containing postcode (`None` to parse from address) |

For alpha, we may want to make this more prescriptive and omit `address_column` and `postcode_column` to reduce complexity. We can add these options in a future release if there is demand for more flexibility.

#### Output

Creates the following files in `output_folder`:

| File | Description |
|------|-------------|
| `addresses.parquet` | Cleaned and tokenised canonical addresses |
| `term_frequencies.parquet` | Term frequency lookup table |
| `inverted_index.parquet` | Inverted index for candidate retrieval |

<details>
<summary>Example</summary>

```python
import duckdb
from uk_address_matcher import prepare_and_persist_canonical_data

con = duckdb.connect()
df_os = con.read_parquet("./raw_addressbase.parquet")

prepare_and_persist_canonical_data(
    df_os,
    output_folder="./prepared_addressbase",
    con=con,
    overwrite=True,
)

# Creates:
# ./prepared_addressbase/addresses.parquet
# ./prepared_addressbase/term_frequencies.parquet
# ./prepared_addressbase/inverted_index.parquet
```

</details>

---

### 3. `AddressMatcher`

The primary entry point for address matching operations. Accepts either:
- A raw `DuckDBPyRelation` (cleaned on the fly)
- A path to a prepared folder (loads artefacts automatically)

#### Construction

```python
import duckdb
from uk_address_matcher import AddressMatcher, MatcherSettings

# Mode 1: Raw relation (cleaned on the fly)
matcher = AddressMatcher(
    canonical_addresses: duckdb.DuckDBPyRelation,
    addresses_to_match: duckdb.DuckDBPyRelation,
    *,
    con: duckdb.DuckDBPyConnection | None = None,
    settings: MatcherSettings | None = None,
)

# Mode 2: Pre-prepared folder
matcher = AddressMatcher(
    canonical_addresses: str | Path,  # folder path
    addresses_to_match: duckdb.DuckDBPyRelation,
    *,
    con: duckdb.DuckDBPyConnection | None = None,
    settings: MatcherSettings | None = None,
)
```

> [!NOTE]
> Our other option here would be to have two separate APIs for users and use overloading to route to the correct one based on input type. However, I think this would be less intuitive for users and more complex to maintain. By using a single class that can handle both modes, we provide a simpler interface and can manage the different data loading internally.

Where the system detects that the input addresses contain our cleaned canonical format (e.g. presence of `ukam_address_id`), assume cleaning has already been done. This allows users to clean the messy addresses themselves if they want to, and pass them in ready for matching.

#### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `canonical_addresses` | `DuckDBPyRelation \| str \| Path` | Raw relation OR path to prepared folder |
| `addresses_to_match` | `DuckDBPyRelation` | Addresses to match (always raw, cleaned internally) |
| `con` | `DuckDBPyConnection \| None` | DuckDB connection |
| `settings` | `MatcherSettings \| None` | Matching configuration (uses defaults if `None`) |

#### Canonical Data Resolution

When `canonical_addresses` is a **DuckDBPyRelation**:
- Data is cleaned on the fly
- Term frequencies are derived on the fly
- Inverted index is built internally

When `canonical_addresses` is a **string/Path** pointing to a folder:
- Loads `addresses.parquet` as cleaned canonical addresses
- Loads `term_frequencies.parquet` as TF lookup
- Loads `inverted_index.parquet` as inverted index

#### Primary Method

##### `match()`

Run the full matching pipeline. All enabled stages (deterministic and probabilistic) are executed
in sequence, with results combined automatically.

```python
def match(self) -> MatchResult:
    ...
```

**Returns:** `MatchResult` containing all matches with confidence scores and match reasons.

<details>
<summary>Example: Simple matching</summary>

```python
import duckdb
from uk_address_matcher import AddressMatcher

con = duckdb.connect()

canonical = con.read_parquet("./canonical.parquet")
messy = con.read_parquet("./messy.parquet")

matcher = AddressMatcher(
    canonical_addresses=canonical,
    addresses_to_match=messy,
    con=con,
)

result = matcher.match()

# Print the match reason breakdown to the console for quick inspection
result.match_reasons().show()
result.to_parquet("./matched.parquet")
```

</details>

<details>
<summary>Example: With pre-prepared canonical folder</summary>

```python
import duckdb
from uk_address_matcher import AddressMatcher, MatcherSettings

con = duckdb.connect()

messy = con.read_parquet("./messy.parquet")

matcher = AddressMatcher(
    canonical_addresses="./prepared_addressbase",  # folder path
    addresses_to_match=messy,
    con=con,
    settings=MatcherSettings(
        match_weight_threshold=20,
        include_unmatched=False,
    ),
)

result = matcher.match()
```

</details>

#### Helper Methods

##### `match_one()`

Match a single address string. Useful for testing, debugging, or interactive exploration. The
address should include the postcode - the library will parse it automatically using regex.

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

**Returns:** `MatchResult` with top candidates.

<details>
<summary>Example</summary>

```python
result = matcher.match_one("10 Downing Street, Westminster, SW1A 2AA")
result.head(5)
```

</details>

---

##### Summary Statistics

```python
# Get match metrics breakdown
result.summary() -> duckdb.DuckDBPyRelation

# Get breakdown by match reason
result.by_match_reason() -> duckdb.DuckDBPyRelation
```

We can gradually flesh these out as we go. There are also lots of Splink charts that we'd ideally give users access to at some point, but we can start with just a few key metrics and add more over time.

---

### 5. Low-Level Helper Functions (Advanced)

For users who want granular control over individual preparation steps (beyond what
`prepare_and_persist_canonical_data()` provides), we expose the underlying functions.
These return DuckDB relations that can be saved and loaded independently.

> [!NOTE]
> Most users should use `prepare_and_persist_canonical_data()` instead. These functions
> are for advanced use cases requiring custom pipelines or debugging.

#### `derive_term_frequencies_table()`

Compute term frequency statistics from canonical addresses.

```python
from uk_address_matcher import derive_term_frequencies_table

tf_table = derive_term_frequencies_table(
    data: duckdb.DuckDBPyRelation,
    *,
    con: duckdb.DuckDBPyConnection | None = None,
) -> duckdb.DuckDBPyRelation
```

#### `prepare_canonical_data_for_matching()`

Clean and tokenise canonical addresses, generating all required features.

```python
from uk_address_matcher import prepare_canonical_data_for_matching

df_clean = prepare_canonical_data_for_matching(
    data: duckdb.DuckDBPyRelation,
    *,
    con: duckdb.DuckDBPyConnection | None = None,
    term_frequency_lookup: duckdb.DuckDBPyRelation | None = None,
) -> duckdb.DuckDBPyRelation
```

#### `derive_inverted_index()`

Build an inverted index for efficient candidate retrieval during matching.

```python
from uk_address_matcher import derive_inverted_index

inverted_index = derive_inverted_index(
    cleaned_data: duckdb.DuckDBPyRelation,
    *,
    con: duckdb.DuckDBPyConnection | None = None,
) -> duckdb.DuckDBPyRelation
```

<details>
<summary>Example: Manual preparation workflow</summary>

```python
import duckdb
from uk_address_matcher import (
    derive_term_frequencies_table,
    prepare_canonical_data_for_matching,
    derive_inverted_index,
)

con = duckdb.connect()
df_os = con.read_parquet("./raw_addressbase.parquet")

# Step 1: Term frequencies
tf_table = derive_term_frequencies_table(df_os, con=con)

# Step 2: Clean addresses
df_os_clean = prepare_canonical_data_for_matching(
    df_os,
    con=con,
    term_frequency_lookup=tf_table,
)

# Step 3: Inverted index
inverted_index = derive_inverted_index(df_os_clean, con=con)

# Save artefacts
df_os_clean.write_parquet("./canonical_clean.parquet")
tf_table.write_parquet("./term_frequencies.parquet")
inverted_index.write_parquet("./inverted_index.parquet")
```

</details>

---

## Consequences

### Positive

- **Simpler onboarding**: Non-power users can match addresses with minimal library knowledge -
  just pass DuckDB relations and call `match()`.
- **Familiar interface**: Users work directly with `DuckDBPyRelation`, avoiding the need to learn
  wrapper classes or re-implemented DataFrame methods.
- **Checkpointing**: `prepare_and_persist_canonical_data()` writes artefacts to a folder that
  can be reloaded by simply passing the path to `AddressMatcher`.
- **Discoverable settings**: `MatcherSettings` provides IDE autocomplete for all configuration
  options.
- **Flexible power-user path**: Low-level helper functions give full control over each artefact.
- **Stable interface**: Internal implementation can evolve without breaking user code.
- **No custom abstractions**: Users don't need to learn new classes like `Addresses` or
  `CanonicalIndex` - just standard DuckDB relations and filesystem paths.

### Negative

- **Migration effort**: Existing users must update their code.
- **Folder convention**: Users must use the expected folder structure when loading prepared data.
- **To make bespoke changes to workflows requires more effort**: Power users who want to customise individual stages must use low-level functions and manage artefacts manually. This is balanced by the fact that the common use cases are now much simpler and should get people the majority of the way without needing to dive into the internals.

### Mitigation

- Provide clear migration guide with before/after examples.
- Expose `matcher.con` for advanced use cases needing raw DuckDB access.
- Validate folder contents on load and provide helpful error messages if files are missing.
- Maintain functional utilities in `uk_address_matcher.legacy` namespace for one major version
  cycle.

---

## Summary

| Use Case | Recommended Approach |
|----------|---------------------|
| Quick matching (small canonical) | Pass raw `DuckDBPyRelation` to `AddressMatcher`, call `match()` |
| Repeated matching (large canonical) | Use `prepare_and_persist_canonical_data()`, then pass folder path |
| Full control over artefacts | Use low-level helper functions, manage files manually |

The key principle is: **make the common case trivial, and the advanced case possible**.

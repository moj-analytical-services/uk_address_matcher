# API reference

## AddressMatcher

::: uk_address_matcher.AddressMatcher
    options:
      members:
        - __init__
        - match
        - available_stages

## Results

### MatchResult

::: uk_address_matcher.MatchResult
    options:
      members:
        - matches
        - join_input_columns
        - close
        - match_metrics
        - accuracy_analysis
        - accuracy_data

Consume lazy result relations before closing the result or its connection.
Prefer `with matcher.match() as result:`; materialised Python values remain
usable after the context exits. The caller retains ownership of the connection.
See [Reading results](get_started.md#reading-results) and the
[2.0 migration guide](get_started.md#migrating-to-20).

The [labelling workflow](labelling.md) is beta and has separate ID restrictions;
there is no stable `export_labelling_bundle` method.

## AddressRecord

::: uk_address_matcher.AddressRecord
    options:
      members:
        - __init__
        - from_dict
        - as_duckdb_relation

## Canonical preparation

::: uk_address_matcher.prepare_canonical_folder

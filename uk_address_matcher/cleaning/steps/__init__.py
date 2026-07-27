from __future__ import annotations

from uk_address_matcher.cleaning.steps.inverted_index import (
    BIGRAM_STRATEGY,
    DEFAULT_INDEXING_STRATEGIES,
    TRIGRAM_STRATEGY,
    InvertedIndexLookupStrategy,
    PhysicalIndexStrategy,
    _build_inverted_index_from_keys,
    _derive_keys_for_strategy,
    _lookup_keys_in_inverted_index,
    _set_exploding_unique_ids_to_self,
)
from uk_address_matcher.cleaning.steps.normalisation import (
    _canonicalise_postcode,
    _clean_address_string_first_pass,
    _extract_postcode_from_address,
    _join_excluding_with_next_token,
    _normalise_abbreviations_and_units,
    _preserve_original_address_concat,
    _remove_duplicate_end_tokens,
    _rename_and_select_columns,
    _strip_country_suffix,
    _trim_whitespace_address_and_postcode,
    _upper_case_address_and_postcode,
)
from uk_address_matcher.cleaning.steps.term_frequencies import (
    _add_numeric_term_frequencies_using_registered_df,
    _add_term_frequencies_to_address_tokens,
    _add_term_frequencies_to_address_tokens_using_registered_df,
    _create_histograms_from_token_frequencies,
    _first_unusual_token,
    _get_token_frequeny_table,
    _move_common_end_tokens_to_field,
    _separate_unusual_tokens,
    _use_first_unusual_token_if_no_numeric_token,
)
from uk_address_matcher.cleaning.steps.token_parsing import (
    _clean_address_string_second_pass,
    _generalised_token_aliases,
    _parse_out_business_unit,
    _parse_out_flat_position_and_letter,
    _parse_out_numbers,
    _parse_out_sub_premise_location,
    _separate_distinguishing_start_tokens_from_with_respect_to_adjacent_records,
)
from uk_address_matcher.cleaning.steps.tokenisation import (
    _split_numeric_tokens_to_cols,
    _tokenise_address_without_numbers,
)

__all__ = [
    # token_parsing
    "_parse_out_flat_position_and_letter",
    "_parse_out_sub_premise_location",
    "_parse_out_business_unit",
    "_parse_out_numbers",
    "_clean_address_string_second_pass",
    "_generalised_token_aliases",
    "_get_token_frequeny_table",
    "_separate_distinguishing_start_tokens_from_with_respect_to_adjacent_records",
    # normalisation
    "_extract_postcode_from_address",
    "_trim_whitespace_address_and_postcode",
    "_canonicalise_postcode",
    "_upper_case_address_and_postcode",
    "_clean_address_string_first_pass",
    "_strip_country_suffix",
    "_remove_duplicate_end_tokens",
    "_rename_and_select_columns",
    "_normalise_abbreviations_and_units",
    "_join_excluding_with_next_token",
    "_preserve_original_address_concat",
    # tokenisation
    "_split_numeric_tokens_to_cols",
    "_tokenise_address_without_numbers",
    # term_frequencies
    "_add_numeric_term_frequencies_using_registered_df",
    "_add_term_frequencies_to_address_tokens",
    "_add_term_frequencies_to_address_tokens_using_registered_df",
    "_move_common_end_tokens_to_field",
    "_first_unusual_token",
    "_use_first_unusual_token_if_no_numeric_token",
    "_separate_unusual_tokens",
    "_create_histograms_from_token_frequencies",
    # inverted_index
    "PhysicalIndexStrategy",
    "InvertedIndexLookupStrategy",
    "TRIGRAM_STRATEGY",
    "BIGRAM_STRATEGY",
    "DEFAULT_INDEXING_STRATEGIES",
    "_build_inverted_index_from_keys",
    "_derive_keys_for_strategy",
    "_lookup_keys_in_inverted_index",
    "_set_exploding_unique_ids_to_self",
]

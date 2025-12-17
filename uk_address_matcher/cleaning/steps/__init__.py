from __future__ import annotations

from uk_address_matcher.cleaning.steps.normalisation import (
    _add_ukam_address_id,
    _canonicalise_postcode,
    _classify_non_traditional_address,
    _clean_address_string_first_pass,
    _normalise_abbreviations_and_units,
    _remove_duplicate_end_tokens,
    _rename_and_select_columns,
    _trim_whitespace_address_and_postcode,
    _upper_case_address_and_postcode,
)
from uk_address_matcher.cleaning.steps.term_frequencies import (
    _add_term_frequencies_to_address_tokens,
    _add_term_frequencies_to_address_tokens_using_registered_df,
    _attach_numeric_term_frequencies,
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
    _separate_distinguishing_start_tokens_from_with_respect_to_adjacent_records,
)
from uk_address_matcher.cleaning.steps.tokenisation import (
    _split_numeric_tokens_to_cols,
    _tokenise_address_without_numbers,
)

__all__ = [
    # token_parsing
    "_parse_out_flat_position_and_letter",
    "_parse_out_business_unit",
    "_parse_out_numbers",
    "_clean_address_string_second_pass",
    "_generalised_token_aliases",
    "_get_token_frequeny_table",
    "_separate_distinguishing_start_tokens_from_with_respect_to_adjacent_records",
    # normalisation
    "_trim_whitespace_address_and_postcode",
    "_canonicalise_postcode",
    "_upper_case_address_and_postcode",
    "_clean_address_string_first_pass",
    "_remove_duplicate_end_tokens",
    "_rename_and_select_columns",
    "_normalise_abbreviations_and_units",
    "_add_ukam_address_id",
    # TODO(ThomasHepworth): this may be better extracted directly from the OS data?
    "_classify_non_traditional_address",
    # tokenisation
    "_split_numeric_tokens_to_cols",
    "_tokenise_address_without_numbers",
    # term_frequencies
    "_add_term_frequencies_to_address_tokens",
    "_add_term_frequencies_to_address_tokens_using_registered_df",
    "_attach_numeric_term_frequencies",
    "_move_common_end_tokens_to_field",
    "_first_unusual_token",
    "_use_first_unusual_token_if_no_numeric_token",
    "_separate_unusual_tokens",
    "_create_histograms_from_token_frequencies",
]

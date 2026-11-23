from __future__ import annotations

import duckdb
import pytest

from uk_address_matcher import SplinkStage
from uk_address_matcher.labelling.manifest import _splink_configuration


def _accepted_rows(
    con: duckdb.DuckDBPyConnection,
    stage: SplinkStage,
) -> list[tuple[float, float | None]]:
    return con.sql(
        f"""
        SELECT match_weight, distinguishability
        FROM (
            VALUES
                (9.0, 20.0),
                (10.0, 0.0),
                (10.0, 1.0),
                (10.0, 2.0),
                (13.0, 1.0),
                (15.0, 2.0),
                (20.0, 4.0),
                (20.0, NULL)
        ) AS candidates(match_weight, distinguishability)
        WHERE {stage._final_acceptance_filter_sql()}
        ORDER BY match_weight, distinguishability NULLS LAST
        """
    ).fetchall()


def test_default_acceptance_policy_rejects_zero_distinguishability() -> None:
    with duckdb.connect() as con:
        stage = SplinkStage(minimum_match_weight=10)
        rows = _accepted_rows(con, stage)

    assert stage.acceptance_profile == "balanced"
    assert stage.minimum_distinguishability == 0.1
    assert rows == [
        (10.0, 2.0),
        (13.0, 1.0),
        (15.0, 2.0),
        (20.0, 4.0),
        (20.0, None),
    ]


def test_balanced_profile_applies_linear_mw_distinguishability_boundary() -> None:
    with duckdb.connect() as con:
        rows = _accepted_rows(
            con,
            SplinkStage(
                acceptance_profile="balanced",
                minimum_match_weight=10,
                minimum_distinguishability=0,
            ),
        )

    assert rows == [
        (10.0, 2.0),
        (13.0, 1.0),
        (15.0, 2.0),
        (20.0, 4.0),
        (20.0, None),
    ]


def test_precision_profile_applies_decaying_distinguishability_requirement() -> None:
    with duckdb.connect() as con:
        rows = _accepted_rows(
            con,
            SplinkStage(
                acceptance_profile="precision",
                minimum_match_weight=10,
                minimum_distinguishability=0,
            ),
        )

    assert rows == [(20.0, 4.0), (20.0, None)]


def test_single_candidate_distinguishability_passes_profile_formula() -> None:
    with duckdb.connect() as con:
        rows = _accepted_rows(
            con,
            SplinkStage(
                acceptance_profile="precision",
                minimum_match_weight=20,
                minimum_distinguishability=10,
            ),
        )

    assert rows == [(20.0, None)]


def test_minimum_distinguishability_none_disables_the_minimum() -> None:
    stage = SplinkStage(
        minimum_match_weight=10,
        minimum_distinguishability=None,
    )

    assert (
        "(distinguishability IS NULL OR distinguishability >="
        not in stage._final_acceptance_filter_sql()
    )


def test_deprecated_threshold_names_remain_compatible() -> None:
    stage = SplinkStage(
        final_match_weight_threshold=12,
        final_distinguishability_threshold=2,
    )

    assert stage.minimum_match_weight == 12
    assert stage.minimum_distinguishability == 2
    assert stage.final_match_weight_threshold == 12
    assert stage.final_distinguishability_threshold == 2


def test_labelling_manifest_uses_new_acceptance_names() -> None:
    stage = SplinkStage(
        acceptance_profile="balanced",
        minimum_match_weight=10,
        minimum_distinguishability=1,
    )
    configuration = _splink_configuration(type("Result", (), {"_splink_stage": stage})())

    assert configuration["acceptance_profile"] == "balanced"
    assert configuration["minimum_match_weight"] == 10
    assert configuration["minimum_distinguishability"] == 1
    assert "final_match_weight_threshold" not in configuration
    assert "final_distinguishability_threshold" not in configuration


def test_new_and_deprecated_thresholds_cannot_both_be_set() -> None:
    with pytest.raises(ValueError, match="not both"):
        SplinkStage(
            minimum_match_weight=10,
            final_match_weight_threshold=12,
        )


@pytest.mark.parametrize("profile", ["coverage", "strict"])
def test_acceptance_profile_is_validated(profile: str) -> None:
    with pytest.raises(ValueError, match="acceptance_profile"):
        SplinkStage(acceptance_profile=profile)  # type: ignore[arg-type]

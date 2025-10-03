import pytest

from uk_address_matcher.sql_pipeline.validation import (
    ColumnSpec,
    validate_table,
    validate_tables,
)


@pytest.fixture
def required_schema() -> list[ColumnSpec]:
    return [
        ColumnSpec("unique_id", "BIGINT"),
        ColumnSpec("source_dataset", "VARCHAR"),
        ColumnSpec("address_concat", "VARCHAR"),
        ColumnSpec("postcode", "VARCHAR"),
    ]


@pytest.fixture
def valid_relation(duck_con):
    return duck_con.sql(
        """
        SELECT
            1::BIGINT AS unique_id,
            'companies_house'::VARCHAR AS source_dataset,
            '10 DOWNING STREET'::VARCHAR AS address_concat,
            'SW1A 2AA'::VARCHAR AS postcode
        """
    )


def test_validate_table_accepts_valid_relation(valid_relation, required_schema):
    errors = validate_table(valid_relation, required_schema)

    assert errors == []


def test_validate_table_reports_missing_and_type_mismatch(duck_con, required_schema):
    relation = duck_con.sql(
        """
        SELECT
            '1'::VARCHAR AS unique_id,
            'companies_house'::VARCHAR AS source_dataset,
            '10 DOWNING STREET'::VARCHAR AS address_concat
        """
    )

    errors = validate_table(
        relation,
        required_schema,
        raise_on_error=False,
    )

    assert "[input_table] missing column 'postcode'" in errors
    assert "[input_table] column 'unique_id': expected BIGINT, found VARCHAR" in errors


def test_validate_table_accepts_length_qualified_varchar(duck_con, required_schema):
    relation = duck_con.sql(
        """
        SELECT
            1::BIGINT AS unique_id,
            'companies_house'::VARCHAR(50) AS source_dataset,
            '10 DOWNING STREET'::VARCHAR AS address_concat,
            'SW1A 2AA'::VARCHAR AS postcode
        """
    )

    errors = validate_table(
        relation,
        required_schema,
        raise_on_error=False,
    )
    assert errors == []


def test_validate_tables_returns_errors_per_relation(
    valid_relation, duck_con, required_schema
):
    failing_relation = duck_con.sql(
        """
        SELECT
            1::BIGINT AS unique_id,
            'companies_house'::VARCHAR AS source_dataset,
            'SW1A 2AA'::VARCHAR AS postcode
        """
    )

    results = validate_tables(
        {
            "companies_house": valid_relation,
            "fhrs": failing_relation,
        },
        required_schema,
        raise_on_error=False,
    )

    assert "companies_house" not in results
    assert "fhrs" in results
    assert results["fhrs"] == ["[fhrs] missing column 'address_concat'"]

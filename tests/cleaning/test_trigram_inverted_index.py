class TestTrigramGeneration:
    """Tests for trigram generation from token arrays."""

    def test_trigram_generation_basic(self, duck_con):
        """Test basic trigram generation from tokens."""
        # Input: ['9', 'LOVE', 'LANE', 'LONDON']
        # Expected: ['9 LOVE LANE', 'LOVE LANE LONDON']

        result = duck_con.sql("""
            WITH input AS (
                SELECT '9 LOVE LANE LONDON' AS clean_full_address
            )
            SELECT
                list_transform(
                    generate_series(1, len(tokenised) - 2),
                    i ->
                        tokenised[i] || ' ' || tokenised[i + 1] || ' ' || tokenised[i + 2]
                ) AS trigrams
            FROM (
                SELECT string_split(clean_full_address, ' ') AS tokenised
                FROM input
            )
        """).fetchone()[0]

        assert result == ["9 LOVE LANE", "LOVE LANE LONDON"]

    def test_trigram_generation_short_address(self, duck_con):
        """Test trigram generation with fewer than 3 tokens."""
        # Input: ['HIGH', 'STREET'] - only 2 tokens
        # Expected: [] (empty list)

        result = duck_con.sql("""
            WITH input AS (
                SELECT 'HIGH STREET' AS clean_full_address
            )
            SELECT
                CASE
                    WHEN len(tokenised) >= 3 THEN
                        list_transform(
                            generate_series(1, len(tokenised) - 2),
                            i ->
                                tokenised[i]
                                || ' '
                                || tokenised[i + 1]
                                || ' '
                                || tokenised[i + 2]
                        )
                    ELSE []
                END AS trigrams
            FROM (
                SELECT string_split(clean_full_address, ' ') AS tokenised
                FROM input
            )
        """).fetchone()[0]

        assert result == []

    def test_trigram_generation_exact_three_tokens(self, duck_con):
        """Test trigram generation with exactly 3 tokens."""
        # Input: ['1', 'HIGH', 'STREET']
        # Expected: ['1 HIGH STREET']

        result = duck_con.sql("""
            WITH input AS (
                SELECT '1 HIGH STREET' AS clean_full_address
            )
            SELECT
                list_transform(
                    generate_series(1, len(tokenised) - 2),
                    i ->
                        tokenised[i] || ' ' || tokenised[i + 1] || ' ' || tokenised[i + 2]
                ) AS trigrams
            FROM (
                SELECT string_split(clean_full_address, ' ') AS tokenised
                FROM input
            )
        """).fetchone()[0]

        assert result == ["1 HIGH STREET"]


class TestInvertedIndexBuilding:
    """Tests for inverted index construction."""

    def test_inverted_index_basic(self, duck_con):
        """Test basic inverted index building."""
        # Create test data with overlapping trigrams
        result = duck_con.sql("""
            WITH addresses AS (
                SELECT * FROM (VALUES
                    (1, '9 LOVE LANE LONDON'),
                    (2, '9 LOVE LANE BRIGHTON'),
                    (3, '8 LOVE LANE LONDON')
                ) AS t(unique_id, clean_full_address)
            ),
            trigrams_per_record AS (
                SELECT
                    unique_id,
                    list_transform(
                        generate_series(1, len(tokenised) - 2),
                        i ->
                            tokenised[i]
                            || ' '
                            || tokenised[i + 1]
                            || ' '
                            || tokenised[i + 2]
                    ) AS trigrams
                FROM (
                    SELECT
                        *,
                        string_split(clean_full_address, ' ') AS tokenised
                    FROM addresses
                )
            ),
            unnested AS (
                SELECT unique_id, unnest(trigrams) AS trigram
                FROM trigrams_per_record
            ),
            grouped AS (
                SELECT
                    trigram,
                    list(DISTINCT unique_id ORDER BY unique_id) AS unique_ids,
                    COUNT(DISTINCT unique_id) AS cnt
                FROM unnested
                GROUP BY trigram
            )
            SELECT trigram, unique_ids
            FROM grouped
            ORDER BY trigram
        """).fetchall()

        result_dict = {row[0]: row[1] for row in result}

        # '9 LOVE LANE' appears in records 1 and 2
        assert result_dict["9 LOVE LANE"] == [1, 2]

        # 'LOVE LANE LONDON' appears in records 1 and 3
        assert result_dict["LOVE LANE LONDON"] == [1, 3]

        # 'LOVE LANE BRIGHTON' appears only in record 2
        assert result_dict["LOVE LANE BRIGHTON"] == [2]

        # '8 LOVE LANE' appears only in record 3
        assert result_dict["8 LOVE LANE"] == [3]

    def test_inverted_index_filtering_by_frequency(self, duck_con):
        """Test that high-frequency trigrams are filtered out."""
        # Create data where one trigram appears in many records
        result = duck_con.sql("""
            WITH addresses AS (
                SELECT * FROM (VALUES
                    (1, 'COMMON TRIGRAM HERE A'),
                    (2, 'COMMON TRIGRAM HERE B'),
                    (3, 'COMMON TRIGRAM HERE C'),
                    (4, 'UNIQUE OTHER TOKENS D')
                ) AS t(unique_id, clean_full_address)
            ),
            trigrams_per_record AS (
                SELECT
                    unique_id,
                    list_transform(
                        generate_series(1, len(tokenised) - 2),
                        i ->
                            tokenised[i]
                            || ' '
                            || tokenised[i + 1]
                            || ' '
                            || tokenised[i + 2]
                    ) AS trigrams
                FROM (
                    SELECT
                        *,
                        string_split(clean_full_address, ' ') AS tokenised
                    FROM addresses
                )
            ),
            unnested AS (
                SELECT unique_id, unnest(trigrams) AS trigram
                FROM trigrams_per_record
            ),
            grouped AS (
                SELECT
                    trigram,
                    list(DISTINCT unique_id ORDER BY unique_id) AS unique_ids,
                    COUNT(DISTINCT unique_id) AS cnt
                FROM unnested
                GROUP BY trigram
            )
            SELECT trigram, unique_ids
            FROM grouped
            WHERE cnt <= 2  -- Filter trigrams appearing in more than 2 records
            ORDER BY trigram
        """).fetchall()

        result_dict = {row[0]: row[1] for row in result}

        # 'COMMON TRIGRAM HERE' should be filtered out (appears in 3 records)
        assert "COMMON TRIGRAM HERE" not in result_dict

        # 'UNIQUE OTHER TOKENS' should remain (appears in 1 record)
        assert result_dict["UNIQUE OTHER TOKENS"] == [4]


class TestDeriveInvertedIndexFunction:
    """Integration tests for derive_inverted_index function."""

    def test_derive_inverted_index_basic(self, duck_con):
        """Test derive_inverted_index with pre-cleaned address data."""
        from uk_address_matcher.cleaning.chunking_strategies import (
            derive_inverted_index,
            prepare_data_for_matching,
        )

        # Create test canonical addresses
        canonical_raw = duck_con.sql("""
            SELECT * FROM (VALUES
                ('1', '9 LOVE LANE LONDON', 'SW1A 1AA'),
                ('2', '9 LOVE LANE BRIGHTON', 'BN1 1AA'),
                ('3', '8 LOVE LANE LONDON', 'SW1A 1AB'),
                ('4', '10 HIGH STREET OXFORD', 'OX1 1AA')
            ) AS t(unique_id, address_concat, postcode)
        """)

        # First clean the canonical data
        canonical_clean = prepare_data_for_matching(
            canonical_raw, duck_con, num_of_chunks=1
        )

        # Now derive inverted index from cleaned data
        inverted_idx = derive_inverted_index(
            canonical_clean, duck_con, max_unique_ids_per_trigram=20
        )

        # Check structure
        assert "trigram" in inverted_idx.columns
        assert "unique_ids" in inverted_idx.columns

        # Check that we have some results
        count = inverted_idx.count("*").fetchone()[0]
        assert count > 0

        # Check specific trigram mappings
        result = inverted_idx.fetchall()
        result_dict = {row[0]: row[1] for row in result}

        # '9 LOVE LANE' should map to records 1 and 2
        assert "9 LOVE LANE" in result_dict
        assert sorted(result_dict["9 LOVE LANE"]) == ["1", "2"]

    def test_derive_inverted_index_filters_common_trigrams(self, duck_con):
        """Test that trigrams appearing in too many records are filtered out."""
        from uk_address_matcher.cleaning.chunking_strategies import (
            derive_inverted_index,
            prepare_data_for_matching,
        )

        # Create data where one trigram is very common
        canonical_raw = duck_con.sql("""
            SELECT * FROM (VALUES
                ('1', 'COMMON STREET NAME LONDON', 'SW1A 1AA'),
                ('2', 'COMMON STREET NAME BRIGHTON', 'BN1 1AA'),
                ('3', 'COMMON STREET NAME OXFORD', 'OX1 1AA'),
                ('4', 'UNIQUE ROAD HERE MANCHESTER', 'M1 1AA')
            ) AS t(unique_id, address_concat, postcode)
        """)

        # First clean the canonical data
        canonical_clean = prepare_data_for_matching(
            canonical_raw, duck_con, num_of_chunks=1
        )

        # Set max_unique_ids_per_trigram to 2 to filter out common trigrams
        inverted_idx = derive_inverted_index(
            canonical_clean, duck_con, max_unique_ids_per_trigram=2
        )

        result = inverted_idx.fetchall()
        result_dict = {row[0]: row[1] for row in result}

        # 'COMMON STREET NAME' appears in 3 records, should be filtered out
        assert "COMMON STREET NAME" not in result_dict


class TestPrepareDataForMatchingWithInvertedIndex:
    """Integration tests for prepare_data_for_matching with inverted_index parameter."""

    def test_prepare_data_with_inverted_index(self, duck_con):
        """Test full pipeline with inverted index."""
        from uk_address_matcher.cleaning.chunking_strategies import (
            derive_inverted_index,
            prepare_data_for_matching,
        )

        # Create canonical addresses
        canonical_raw = duck_con.sql("""
            SELECT * FROM (VALUES
                ('1', '9 LOVE LANE LONDON', 'SW1A 1AA'),
                ('2', '10 HIGH STREET OXFORD', 'OX1 1AA')
            ) AS t(unique_id, address_concat, postcode)
        """)

        # Create messy addresses
        messy = duck_con.sql("""
            SELECT * FROM (VALUES
                ('100', '9 LOVE LANE LONDON SW1A 1AA', 'SW1A 1AA')
            ) AS t(unique_id, address_concat, postcode)
        """)

        # First clean canonical data (no inverted index)
        canonical_clean = prepare_data_for_matching(
            canonical_raw, duck_con, num_of_chunks=1
        )

        # Derive inverted index from cleaned canonical
        inverted_idx = derive_inverted_index(canonical_clean, duck_con)

        # Prepare messy data with inverted index
        prepared_messy = prepare_data_for_matching(
            messy, duck_con, num_of_chunks=1, inverted_index=inverted_idx
        )

        # Check that exploding_unique_ids column exists
        assert "exploding_unique_ids" in prepared_messy.columns

        # Get the exploding_unique_ids for the messy record
        result = prepared_messy.select("exploding_unique_ids").fetchone()[0]

        # Should contain canonical unique_id '1' (matching trigrams)
        assert "1" in result

    def test_prepare_data_without_inverted_index(self, duck_con):
        """Test data without inverted index gets [unique_id] as exploding_unique_ids."""
        from uk_address_matcher.cleaning.chunking_strategies import (
            prepare_data_for_matching,
        )

        # Create canonical addresses
        canonical = duck_con.sql("""
            SELECT * FROM (VALUES
                ('1', '9 LOVE LANE LONDON', 'SW1A 1AA'),
                ('2', '10 HIGH STREET OXFORD', 'OX1 1AA')
            ) AS t(unique_id, address_concat, postcode)
        """)

        # Prepare without inverted index
        prepared = prepare_data_for_matching(canonical, duck_con, num_of_chunks=1)

        # Check that exploding_unique_ids column exists
        assert "exploding_unique_ids" in prepared.columns

        # Each record should have [unique_id] as exploding_unique_ids
        results = prepared.select("unique_id, exploding_unique_ids").fetchall()
        for unique_id, exploding_ids in results:
            assert exploding_ids == [unique_id]

    def test_exploding_unique_ids_empty_when_no_matches(self, duck_con):
        """Test that exploding_unique_ids is empty when no trigrams match."""
        from uk_address_matcher.cleaning.chunking_strategies import (
            derive_inverted_index,
            prepare_data_for_matching,
        )

        # Create canonical addresses
        canonical_raw = duck_con.sql("""
            SELECT * FROM (VALUES
                ('1', '9 LOVE LANE LONDON', 'SW1A 1AA')
            ) AS t(unique_id, address_concat, postcode)
        """)

        # Create messy address with completely different content
        messy = duck_con.sql("""
            SELECT * FROM (VALUES
                ('100', 'COMPLETELY DIFFERENT ADDRESS HERE', 'M1 1AA')
            ) AS t(unique_id, address_concat, postcode)
        """)

        # First clean canonical data
        canonical_clean = prepare_data_for_matching(
            canonical_raw, duck_con, num_of_chunks=1
        )

        # Derive inverted index from cleaned canonical
        inverted_idx = derive_inverted_index(canonical_clean, duck_con)

        # Prepare messy data with inverted index
        prepared_messy = prepare_data_for_matching(
            messy, duck_con, num_of_chunks=1, inverted_index=inverted_idx
        )

        # Get the exploding_unique_ids for the messy record
        result = prepared_messy.select("exploding_unique_ids").fetchone()[0]

        # Should be empty as no trigrams match
        assert result == []

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


class TestBigramGeneration:
    """Tests for bigram generation from token arrays."""

    def test_bigram_generation_basic(self, duck_con):
        """Test basic bigram generation from tokens."""
        result = duck_con.sql("""
            WITH input AS (
                SELECT '9 LOVE LANE LONDON' AS clean_full_address
            )
            SELECT
                list_transform(
                    generate_series(1, len(tokenised) - 1),
                    i ->
                        tokenised[i] || ' ' || tokenised[i + 1]
                ) AS bigrams
            FROM (
                SELECT string_split(clean_full_address, ' ') AS tokenised
                FROM input
            )
        """).fetchone()[0]

        assert result == ["9 LOVE", "LOVE LANE", "LANE LONDON"]

    def test_bigram_generation_single_token(self, duck_con):
        """Test bigram generation with a single token gives empty list."""
        result = duck_con.sql("""
            WITH input AS (
                SELECT 'LONDON' AS clean_full_address
            )
            SELECT
                CASE
                    WHEN len(tokenised) >= 2 THEN
                        list_transform(
                            generate_series(1, len(tokenised) - 1),
                            i -> tokenised[i] || ' ' || tokenised[i + 1]
                        )
                    ELSE []
                END AS bigrams
            FROM (
                SELECT string_split(clean_full_address, ' ') AS tokenised
                FROM input
            )
        """).fetchone()[0]

        assert result == []

    def test_bigram_generation_two_tokens(self, duck_con):
        """Test bigram generation with exactly 2 tokens."""
        result = duck_con.sql("""
            WITH input AS (
                SELECT 'HIGH STREET' AS clean_full_address
            )
            SELECT
                list_transform(
                    generate_series(1, len(tokenised) - 1),
                    i -> tokenised[i] || ' ' || tokenised[i + 1]
                ) AS bigrams
            FROM (
                SELECT string_split(clean_full_address, ' ') AS tokenised
                FROM input
            )
        """).fetchone()[0]

        assert result == ["HIGH STREET"]


class TestIndexingStrategySqlExpressions:
    """Tests that the SQL expressions from IndexingStrategy produce correct results."""

    def test_trigram_strategy_sql(self, duck_con):
        """Test that the TRIGRAM_INDEX keys_sql_expr works correctly."""
        from uk_address_matcher.cleaning.steps.inverted_index import TRIGRAM_INDEX

        result = duck_con.sql(f"""
            WITH input AS (
                SELECT
                    '9 LOVE LANE LONDON' AS clean_full_address,
                    string_split('9 LOVE LANE LONDON', ' ') AS __tokens
            )
            SELECT {TRIGRAM_INDEX.keys_sql_expr} AS keys
            FROM input
        """).fetchone()[0]

        assert result == ["9 LOVE LANE", "LOVE LANE LONDON"]

    def test_trigram_strategy_sql_short_address(self, duck_con):
        """Test TRIGRAM_INDEX with fewer than 3 tokens returns empty."""
        from uk_address_matcher.cleaning.steps.inverted_index import TRIGRAM_INDEX

        result = duck_con.sql(f"""
            WITH input AS (
                SELECT
                    'HIGH STREET' AS clean_full_address,
                    string_split('HIGH STREET', ' ') AS __tokens
            )
            SELECT {TRIGRAM_INDEX.keys_sql_expr} AS keys
            FROM input
        """).fetchone()[0]

        assert result == []

    def test_bigram_strategy_sql(self, duck_con):
        """Test that the BIGRAM_INDEX keys_sql_expr works correctly."""
        from uk_address_matcher.cleaning.steps.inverted_index import BIGRAM_INDEX

        result = duck_con.sql(f"""
            WITH input AS (
                SELECT
                    '9 LOVE LANE LONDON' AS clean_full_address,
                    string_split('9 LOVE LANE LONDON', ' ') AS __tokens
            )
            SELECT {BIGRAM_INDEX.keys_sql_expr} AS keys
            FROM input
        """).fetchone()[0]

        assert result == ["9 LOVE", "LOVE LANE", "LANE LONDON"]

    def test_bigram_strategy_sql_single_token(self, duck_con):
        """Test BIGRAM_INDEX with a single token returns empty."""
        from uk_address_matcher.cleaning.steps.inverted_index import BIGRAM_INDEX

        result = duck_con.sql(f"""
            WITH input AS (
                SELECT
                    'LONDON' AS clean_full_address,
                    string_split('LONDON', ' ') AS __tokens
            )
            SELECT {BIGRAM_INDEX.keys_sql_expr} AS keys
            FROM input
        """).fetchone()[0]

        assert result == []


class TestInvertedIndexBuilding:
    """Tests for inverted index construction."""

    def test_inverted_index_basic(self, duck_con):
        """Test basic inverted index building with trigrams."""
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

        canonical_raw = duck_con.sql("""
            SELECT * FROM (VALUES
                ('1', '9 LOVE LANE LONDON', 'SW1A 1AA'),
                ('2', '9 LOVE LANE BRIGHTON', 'BN1 1AA'),
                ('3', '8 LOVE LANE LONDON', 'SW1A 1AB'),
                ('4', '10 HIGH STREET OXFORD', 'OX1 1AA')
            ) AS t(unique_id, address_concat, postcode)
        """)

        canonical_clean = prepare_data_for_matching(
            canonical_raw, duck_con, num_of_chunks=1
        )

        inverted_idx = derive_inverted_index(canonical_clean, duck_con)

        # Check structure
        assert "key" in inverted_idx.columns
        assert "unique_ids" in inverted_idx.columns
        assert "index_strategy" in inverted_idx.columns

        # Check that we have results from both strategies
        count = inverted_idx.count("*").fetchone()[0]
        assert count > 0

        strategies = duck_con.sql("""
            SELECT DISTINCT index_strategy
            FROM inverted_idx
            ORDER BY index_strategy
        """).fetchall()
        strategy_names = [row[0] for row in strategies]
        assert "trigram" in strategy_names
        assert "bigram" in strategy_names

        # Check specific trigram mappings
        trigram_rows = duck_con.sql("""
            SELECT key, unique_ids
            FROM inverted_idx
            WHERE index_strategy = 'trigram'
        """).fetchall()
        trigram_dict = {row[0]: row[1] for row in trigram_rows}

        # '9 LOVE LANE' should map to records 1 and 2
        assert "9 LOVE LANE" in trigram_dict
        assert sorted(trigram_dict["9 LOVE LANE"]) == ["1", "2"]

    def test_derive_inverted_index_filters_common_keys(self, duck_con):
        """Test that keys appearing in too many records are filtered out."""
        from uk_address_matcher.cleaning.chunking_strategies import (
            derive_inverted_index,
            prepare_data_for_matching,
        )

        canonical_raw = duck_con.sql("""
            SELECT * FROM (VALUES
                ('1', 'COMMON STREET NAME LONDON', 'SW1A 1AA'),
                ('2', 'COMMON STREET NAME BRIGHTON', 'BN1 1AA'),
                ('3', 'COMMON STREET NAME OXFORD', 'OX1 1AA'),
                ('4', 'UNIQUE ROAD HERE MANCHESTER', 'M1 1AA')
            ) AS t(unique_id, address_concat, postcode)
        """)

        canonical_clean = prepare_data_for_matching(
            canonical_raw, duck_con, num_of_chunks=1
        )

        from dataclasses import replace

        from uk_address_matcher.cleaning.steps.inverted_index import (
            BIGRAM_INDEX,
            TRIGRAM_INDEX,
        )

        # Set the physical index posting caps to 2 to filter out common keys.
        inverted_idx = derive_inverted_index(
            canonical_clean,
            duck_con,
            strategies=[
                replace(BIGRAM_INDEX, maximum_posting_size=2),
                replace(TRIGRAM_INDEX, maximum_posting_size=2),
            ],
        )

        result = inverted_idx.fetchall()
        result_dict = {row[0]: row[1] for row in result}

        # 'COMMON STREET NAME' appears in 3 records as trigram, should be filtered
        assert "COMMON STREET NAME" not in result_dict

    def test_chunked_inverted_index_matches_non_chunked(self, duck_con):
        """Test that chunked inverted index produces the same results as single-pass."""
        from uk_address_matcher.cleaning.chunking_strategies import (
            derive_inverted_index,
            prepare_data_for_matching,
        )

        canonical_raw = duck_con.sql("""
            SELECT * FROM (VALUES
                ('1', '9 LOVE LANE LONDON', 'SW1A 1AA'),
                ('2', '9 LOVE LANE BRIGHTON', 'BN1 1AA'),
                ('3', '8 LOVE LANE LONDON', 'SW1A 1AB'),
                ('4', '10 HIGH STREET OXFORD', 'OX1 1AA'),
                ('5', '5 PARK AVENUE MANCHESTER', 'M1 2AB'),
                ('6', '12 CHURCH ROAD OXFORD', 'OX2 1BB'),
                ('7', '3 KINGS ROAD BRIGHTON', 'BN1 2CC'),
                ('8', '7 QUEENS DRIVE LONDON', 'SW2 3DD')
            ) AS t(unique_id, address_concat, postcode)
        """)

        canonical_clean = prepare_data_for_matching(
            canonical_raw, duck_con, num_of_chunks=1
        )

        # Build inverted index without chunking
        idx_no_chunk = derive_inverted_index(canonical_clean, duck_con, num_of_chunks=1)
        no_chunk_rows = idx_no_chunk.fetchall()
        no_chunk_dict = {(row[0], row[2]): sorted(row[1]) for row in no_chunk_rows}

        # Build inverted index with chunking (use an odd-ish number)
        idx_chunked = derive_inverted_index(canonical_clean, duck_con, num_of_chunks=5)
        chunked_rows = idx_chunked.fetchall()
        chunked_dict = {(row[0], row[2]): sorted(row[1]) for row in chunked_rows}

        assert no_chunk_dict == chunked_dict

    def test_derive_inverted_index_trigram_only(self, duck_con):
        """Test derive_inverted_index with only the trigram strategy."""
        from uk_address_matcher.cleaning.chunking_strategies import (
            derive_inverted_index,
            prepare_data_for_matching,
        )
        from uk_address_matcher.cleaning.steps.inverted_index import TRIGRAM_INDEX

        canonical_raw = duck_con.sql("""
            SELECT * FROM (VALUES
                ('1', '9 LOVE LANE LONDON', 'SW1A 1AA'),
                ('2', '10 HIGH STREET OXFORD', 'OX1 1AA')
            ) AS t(unique_id, address_concat, postcode)
        """)

        canonical_clean = prepare_data_for_matching(
            canonical_raw, duck_con, num_of_chunks=1
        )

        inverted_idx = derive_inverted_index(  # noqa: F841
            canonical_clean, duck_con, strategies=[TRIGRAM_INDEX]
        )

        strategies = duck_con.sql("""
            SELECT DISTINCT index_strategy FROM inverted_idx
        """).fetchall()
        strategy_names = [row[0] for row in strategies]
        assert strategy_names == ["trigram"]


class TestPrepareDataForMatchingWithInvertedIndex:
    """Integration tests for prepare_data_for_matching with inverted_index parameter."""

    def test_canonical_index_uses_rare_contiguous_keys_and_messy_uses_skips(
        self, duck_con
    ):
        """Keep skip keys transient while using them to find canonical candidates."""
        from uk_address_matcher.cleaning.chunking_strategies import (
            derive_inverted_index,
            prepare_data_for_matching,
        )
        from uk_address_matcher.cleaning.steps.inverted_index import (
            SKIP1_TRIGRAM_LOOKUP,
            SKIP2_TRIGRAM_LOOKUP,
        )

        canonical = duck_con.sql("""
            SELECT
                CAST(row_number AS VARCHAR) AS unique_id,
                'COMMON ROAD LONDON' AS address_concat,
                'SW1A 1AA' AS postcode
            FROM generate_series(1, 21) AS rows(row_number)
            UNION ALL
            SELECT 'skip1', 'ALPHA BRAVO CHARLIE DELTA', 'SW1A 1AB'
            UNION ALL
            SELECT 'skip2', 'LIME MANGO OLIVE PEAR', 'SW1A 1AC'
        """)
        messy = duck_con.sql("""
            SELECT * FROM (VALUES
                ('messy_skip1', 'ALPHA BRAVO XENON CHARLIE DELTA', 'SW1A 1AB'),
                ('messy_skip2', 'LIME MANGO XENON YTTRIUM OLIVE PEAR', 'SW1A 1AC')
            ) AS rows(unique_id, address_concat, postcode)
        """)

        canonical_clean = prepare_data_for_matching(
            canonical,
            duck_con,
            num_of_chunks=1,
            dataset_role="canonical",
        )
        inverted_index = derive_inverted_index(canonical_clean, duck_con)

        index_rows = set(inverted_index.select("key, index_strategy").fetchall())
        assert {index_strategy for _, index_strategy in index_rows} == {
            "bigram",
            "trigram",
        }
        assert ("COMMON ROAD", "bigram") not in index_rows
        assert ("COMMON ROAD LONDON", "trigram") not in index_rows
        assert ("ALPHA BRAVO", "bigram") in index_rows
        assert ("ALPHA BRAVO CHARLIE", "trigram") in index_rows
        assert ("ALPHA CHARLIE DELTA", "trigram") not in index_rows

        trigram_index = inverted_index.filter("index_strategy = 'trigram'")
        prepared_messy = prepare_data_for_matching(
            messy,
            duck_con,
            num_of_chunks=1,
            inverted_index=trigram_index,
            _inverted_index_strategies=[
                SKIP1_TRIGRAM_LOOKUP,
                SKIP2_TRIGRAM_LOOKUP,
            ],
            dataset_role="messy",
        )
        candidates = dict(
            prepared_messy.select("unique_id, exploding_unique_ids").fetchall()
        )
        signature_scores = dict(
            prepared_messy.select("unique_id, signature_score_map").fetchall()
        )
        assert candidates["messy_skip1"] == ["skip1"]
        assert candidates["messy_skip2"] == ["skip2"]
        assert signature_scores["messy_skip1"] == {}
        assert signature_scores["messy_skip2"] == {}

    def test_skip_bigrams_are_messy_only_and_deduplicate_candidates(self, duck_con):
        """Use ordered skip-bigrams for short messy addresses without duplicates."""
        from uk_address_matcher.cleaning.chunking_strategies import (
            derive_inverted_index,
            prepare_data_for_matching,
        )
        from uk_address_matcher.cleaning.steps.inverted_index import (
            MESSY_INVERTED_INDEX_LOOKUP_STRATEGIES,
        )

        canonical = duck_con.sql("""
            SELECT * FROM (VALUES
                ('skip1', 'ALPHA BRAVO', 'SW1A 1AA'),
                ('skip2', 'LIME PEAR', 'SW1A 1AB')
            ) AS rows(unique_id, address_concat, postcode)
        """)
        messy = duck_con.sql("""
            SELECT * FROM (VALUES
                ('skip1', 'ALPHA XENON BRAVO', 'SW1A 1AA'),
                ('skip2', 'LIME XENON YTTRIUM PEAR', 'SW1A 1AB'),
                ('reordered', 'BRAVO XENON ALPHA', 'SW1A 1AC')
            ) AS rows(unique_id, address_concat, postcode)
        """)

        canonical_clean = prepare_data_for_matching(
            canonical,
            duck_con,
            num_of_chunks=1,
            dataset_role="canonical",
        )
        inverted_index = derive_inverted_index(canonical_clean, duck_con)
        index_rows = set(inverted_index.select("key, index_strategy").fetchall())
        assert ("ALPHA BRAVO", "bigram") in index_rows
        assert ("ALPHA XENON BRAVO", "bigram") not in index_rows

        prepared_messy = prepare_data_for_matching(
            messy,
            duck_con,
            num_of_chunks=1,
            inverted_index=inverted_index,
            _inverted_index_strategies=MESSY_INVERTED_INDEX_LOOKUP_STRATEGIES,
            dataset_role="messy",
        )
        candidates = dict(
            prepared_messy.select("unique_id, exploding_unique_ids").fetchall()
        )
        assert candidates["skip1"] == ["skip1"]
        assert candidates["skip2"] == ["skip2"]
        assert candidates["reordered"] == []
        assert len(candidates["skip1"]) == len(set(candidates["skip1"]))

    def test_prepare_data_with_inverted_index(self, duck_con):
        """Test full pipeline with inverted index."""
        from uk_address_matcher.cleaning.chunking_strategies import (
            derive_inverted_index,
            prepare_data_for_matching,
        )

        canonical_raw = duck_con.sql("""
            SELECT * FROM (VALUES
                ('1', '9 LOVE LANE LONDON', 'SW1A 1AA'),
                ('2', '10 HIGH STREET OXFORD', 'OX1 1AA')
            ) AS t(unique_id, address_concat, postcode)
        """)

        messy = duck_con.sql("""
            SELECT * FROM (VALUES
                ('100', '9 LOVE LANE LONDON SW1A 1AA', 'SW1A 1AA')
            ) AS t(unique_id, address_concat, postcode)
        """)

        canonical_clean = prepare_data_for_matching(
            canonical_raw, duck_con, num_of_chunks=1
        )

        inverted_idx = derive_inverted_index(canonical_clean, duck_con)

        prepared_messy = prepare_data_for_matching(
            messy, duck_con, num_of_chunks=1, inverted_index=inverted_idx
        )

        assert "exploding_unique_ids" in prepared_messy.columns

        result = prepared_messy.select("exploding_unique_ids").fetchone()[0]

        # Should contain canonical unique_id '1' (matching keys)
        assert "1" in result

    def test_prepare_data_without_inverted_index(self, duck_con):
        """Test data without inverted index gets [unique_id] as exploding_unique_ids."""
        from uk_address_matcher.cleaning.chunking_strategies import (
            prepare_data_for_matching,
        )

        canonical = duck_con.sql("""
            SELECT * FROM (VALUES
                ('1', '9 LOVE LANE LONDON', 'SW1A 1AA'),
                ('2', '10 HIGH STREET OXFORD', 'OX1 1AA')
            ) AS t(unique_id, address_concat, postcode)
        """)

        prepared = prepare_data_for_matching(canonical, duck_con, num_of_chunks=1)

        assert "exploding_unique_ids" in prepared.columns

        results = prepared.select("unique_id, exploding_unique_ids").fetchall()
        for unique_id, exploding_ids in results:
            assert exploding_ids == [unique_id]

    def test_exploding_unique_ids_empty_when_no_matches(self, duck_con):
        """Test that exploding_unique_ids is empty when no keys match."""
        from uk_address_matcher.cleaning.chunking_strategies import (
            derive_inverted_index,
            prepare_data_for_matching,
        )

        canonical_raw = duck_con.sql("""
            SELECT * FROM (VALUES
                ('1', '9 LOVE LANE LONDON', 'SW1A 1AA')
            ) AS t(unique_id, address_concat, postcode)
        """)

        messy = duck_con.sql("""
            SELECT * FROM (VALUES
                ('100', 'COMPLETELY DIFFERENT ADDRESS HERE', 'M1 1AA')
            ) AS t(unique_id, address_concat, postcode)
        """)

        canonical_clean = prepare_data_for_matching(
            canonical_raw, duck_con, num_of_chunks=1
        )

        inverted_idx = derive_inverted_index(canonical_clean, duck_con)

        prepared_messy = prepare_data_for_matching(
            messy, duck_con, num_of_chunks=1, inverted_index=inverted_idx
        )

        result = prepared_messy.select("exploding_unique_ids").fetchone()[0]

        assert result == []

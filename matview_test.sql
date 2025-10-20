-- Performance and correctness test for refresh_by_match_merge function
-- This test creates large datasets and various edge cases to test REFRESH MATERIALIZED VIEW CONCURRENTLY
-- Run this test before and after modifications to compare performance and verify correctness

-- Clean up any existing test objects
DROP MATERIALIZED VIEW IF EXISTS test_matview_large CASCADE;
DROP MATERIALIZED VIEW IF EXISTS test_matview_duplicates CASCADE;
DROP MATERIALIZED VIEW IF EXISTS test_matview_nulls CASCADE;
DROP MATERIALIZED VIEW IF EXISTS test_matview_mixed CASCADE;
DROP TABLE IF EXISTS test_source_large CASCADE;
DROP TABLE IF EXISTS test_source_small CASCADE;
DROP FUNCTION IF EXISTS generate_test_data CASCADE;

-- Create base tables for testing
CREATE TABLE test_source_large (
    id SERIAL PRIMARY KEY,
    category VARCHAR(50),
    value INTEGER,
    data TEXT,
    created_at TIMESTAMP DEFAULT now()
);

CREATE TABLE test_source_small (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    score DECIMAL(10,2),
    active BOOLEAN DEFAULT true
);

-- Function to generate test data
CREATE OR REPLACE FUNCTION generate_test_data(num_rows INTEGER)
RETURNS VOID AS $$
BEGIN
    -- Insert large dataset with various patterns
    INSERT INTO test_source_large (category, value, data, created_at)
    SELECT
        'category_' || (i % 100)::TEXT,
        (random() * 10000)::INTEGER,
        'data_' || i::TEXT || '_' || repeat('x', (i % 50) + 10),
        now() - (random() * interval '365 days')
    FROM generate_series(1, num_rows) i;

    -- Insert smaller dataset for mixed operations
    INSERT INTO test_source_small (name, score, active)
    SELECT
        'name_' || i::TEXT,
        (random() * 100)::DECIMAL(10,2),
        (i % 3) != 0
    FROM generate_series(1, num_rows / 10) i;
END;
$$ LANGUAGE plpgsql;

-- Generate initial test data
\echo 'Generating test data...'
SELECT generate_test_data(100000);

-- Create indexes for performance
CREATE INDEX idx_test_source_large_category ON test_source_large(category);
CREATE INDEX idx_test_source_large_value ON test_source_large(value);
CREATE INDEX idx_test_source_large_created ON test_source_large(created_at);
CREATE INDEX idx_test_source_small_score ON test_source_small(score);

ANALYZE test_source_large;
ANALYZE test_source_small;

-- Test Case 1: Large materialized view with unique index
\echo 'Creating large materialized view...'
CREATE MATERIALIZED VIEW test_matview_large AS
SELECT
    category,
    COUNT(*) as count_records,
    AVG(value) as avg_value,
    MIN(value) as min_value,
    MAX(value) as max_value,
    SUM(value) as sum_value
FROM test_source_large
GROUP BY category;

CREATE UNIQUE INDEX idx_test_matview_large_category ON test_matview_large(category);

-- Test Case 2: Materialized view with potential duplicates (before unique index)
\echo 'Creating materialized view with duplicates...'
CREATE MATERIALIZED VIEW test_matview_duplicates AS
SELECT
    value % 1000 as mod_value,
    COUNT(*) as frequency
FROM test_source_large
GROUP BY value % 1000;

CREATE UNIQUE INDEX idx_test_matview_duplicates ON test_matview_duplicates(mod_value);

-- Test Case 3: Materialized view with NULLs
\echo 'Creating materialized view with NULLs...'
CREATE MATERIALIZED VIEW test_matview_nulls AS
SELECT
    category,
    CASE
        WHEN value % 10 = 0 THEN NULL
        ELSE value
    END as nullable_value,
    CASE
        WHEN value % 5 = 0 THEN NULL
        ELSE data
    END as nullable_data
FROM test_source_large
WHERE id % 3 = 0;  -- Subset to make it manageable

CREATE UNIQUE INDEX idx_test_matview_nulls ON test_matview_nulls(category, COALESCE(nullable_value, -1));

-- Test Case 4: Complex join materialized view
\echo 'Creating complex materialized view...'
CREATE MATERIALIZED VIEW test_matview_mixed AS
SELECT
    l.category,
    s.name,
    l.value * s.score as computed_value,
    l.created_at,
    s.active
FROM test_source_large l
JOIN test_source_small s ON (l.id % 10000) = s.id
WHERE l.value > 1000;

CREATE UNIQUE INDEX idx_test_matview_mixed ON test_matview_mixed(category, name);

-- Store initial row counts for comparison
CREATE TEMP TABLE initial_counts AS
SELECT
    'test_matview_large' as table_name,
    COUNT(*) as row_count,
    SUM(count_records) as sum_count,
    AVG(avg_value) as avg_avg
FROM test_matview_large
UNION ALL
SELECT
    'test_matview_duplicates',
    COUNT(*),
    SUM(frequency),
    AVG(mod_value)
FROM test_matview_duplicates
UNION ALL
SELECT
    'test_matview_nulls',
    COUNT(*),
    COUNT(nullable_value),
    AVG(COALESCE(nullable_value, 0))
FROM test_matview_nulls
UNION ALL
SELECT
    'test_matview_mixed',
    COUNT(*),
    SUM(computed_value::bigint),
    COUNT(CASE WHEN active THEN 1 END)
FROM test_matview_mixed;

\echo 'Initial materialized view contents:'
SELECT * FROM initial_counts ORDER BY table_name;

-- Performance Test 1: Modify source data significantly
\echo 'Modifying source data (25% updates, 25% deletes, 50% inserts)...'

-- Delete 25% of data
DELETE FROM test_source_large WHERE id % 4 = 0;

-- Update 25% of remaining data
UPDATE test_source_large
SET value = value + 1000,
    data = 'updated_' || data
WHERE id % 4 = 1;

-- Insert 50% new data
INSERT INTO test_source_large (category, value, data, created_at)
SELECT
    'new_category_' || (i % 50)::TEXT,
    (random() * 5000 + 5000)::INTEGER,
    'new_data_' || i::TEXT,
    now() + (random() * interval '30 days')
FROM generate_series(1, 50000) i;

-- Modify small table too
DELETE FROM test_source_small WHERE id % 3 = 0;
INSERT INTO test_source_small (name, score, active)
SELECT
    'new_name_' || i::TEXT,
    (random() * 200)::DECIMAL(10,2),
    true
FROM generate_series(1, 5000) i;

ANALYZE test_source_large;
ANALYZE test_source_small;

-- Performance Test: Time the concurrent refreshes
\timing on

\echo 'Starting REFRESH MATERIALIZED VIEW CONCURRENTLY tests...'
\echo 'Test 1: Large aggregated view'
REFRESH MATERIALIZED VIEW CONCURRENTLY test_matview_large;

\echo 'Test 2: View with duplicates'
REFRESH MATERIALIZED VIEW CONCURRENTLY test_matview_duplicates;

\echo 'Test 3: View with NULLs'
REFRESH MATERIALIZED VIEW CONCURRENTLY test_matview_nulls;

\echo 'Test 4: Complex join view'
REFRESH MATERIALIZED VIEW CONCURRENTLY test_matview_mixed;

\timing off

-- Verify correctness: Compare with fresh materialized views
\echo 'Verifying correctness by comparing with fresh views...'

-- Test 1: Large view
CREATE MATERIALIZED VIEW test_matview_large_fresh AS
SELECT
    category,
    COUNT(*) as count_records,
    AVG(value) as avg_value,
    MIN(value) as min_value,
    MAX(value) as max_value,
    SUM(value) as sum_value
FROM test_source_large
GROUP BY category;

SELECT
    COUNT(*) as refreshed_count,
    (SELECT COUNT(*) FROM test_matview_large_fresh) as fresh_count,
    COUNT(*) = (SELECT COUNT(*) FROM test_matview_large_fresh) as counts_match
FROM test_matview_large;

-- Detailed comparison
WITH comparison AS (
    SELECT category, count_records, avg_value FROM test_matview_large
    EXCEPT
    SELECT category, count_records, avg_value FROM test_matview_large_fresh
)
SELECT COUNT(*) as differences_count FROM comparison;

-- Test 2: Duplicates view
CREATE MATERIALIZED VIEW test_matview_duplicates_fresh AS
SELECT
    value % 1000 as mod_value,
    COUNT(*) as frequency
FROM test_source_large
GROUP BY value % 1000;

SELECT
    COUNT(*) as refreshed_count,
    (SELECT COUNT(*) FROM test_matview_duplicates_fresh) as fresh_count,
    COUNT(*) = (SELECT COUNT(*) FROM test_matview_duplicates_fresh) as counts_match
FROM test_matview_duplicates;

-- Test 3: NULLs view
CREATE MATERIALIZED VIEW test_matview_nulls_fresh AS
SELECT
    category,
    CASE
        WHEN value % 10 = 0 THEN NULL
        ELSE value
    END as nullable_value,
    CASE
        WHEN value % 5 = 0 THEN NULL
        ELSE data
    END as nullable_data
FROM test_source_large
WHERE id % 3 = 0;

SELECT
    COUNT(*) as refreshed_count,
    (SELECT COUNT(*) FROM test_matview_nulls_fresh) as fresh_count,
    COUNT(*) = (SELECT COUNT(*) FROM test_matview_nulls_fresh) as counts_match
FROM test_matview_nulls;

-- Test 4: Mixed view
CREATE MATERIALIZED VIEW test_matview_mixed_fresh AS
SELECT
    l.category,
    s.name,
    l.value * s.score as computed_value,
    l.created_at,
    s.active
FROM test_source_large l
JOIN test_source_small s ON (l.id % 10000) = s.id
WHERE l.value > 1000;

SELECT
    COUNT(*) as refreshed_count,
    (SELECT COUNT(*) FROM test_matview_mixed_fresh) as fresh_count,
    COUNT(*) = (SELECT COUNT(*) FROM test_matview_mixed_fresh) as counts_match
FROM test_matview_mixed;

-- Edge Case Tests

\echo 'Testing edge cases...'

-- Edge Case 1: Empty result set
CREATE MATERIALIZED VIEW test_matview_empty AS
SELECT category, COUNT(*) as count_val
FROM test_source_large
WHERE value < 0  -- This should return no rows
GROUP BY category;

CREATE UNIQUE INDEX idx_test_matview_empty ON test_matview_empty(category);

REFRESH MATERIALIZED VIEW CONCURRENTLY test_matview_empty;
SELECT COUNT(*) as empty_view_count FROM test_matview_empty;

-- Edge Case 2: Single row result
CREATE MATERIALIZED VIEW test_matview_single AS
SELECT 'single_category' as category, COUNT(*) as total_count
FROM test_source_large;

CREATE UNIQUE INDEX idx_test_matview_single ON test_matview_single(category);

REFRESH MATERIALIZED VIEW CONCURRENTLY test_matview_single;
SELECT * FROM test_matview_single;

-- Edge Case 3: All NULLs in key columns
INSERT INTO test_source_large (category, value, data) VALUES (NULL, NULL, 'null_test');

CREATE MATERIALIZED VIEW test_matview_all_nulls AS
SELECT
    category,
    COALESCE(value, -999) as safe_value
FROM test_source_large
WHERE category IS NULL;

CREATE UNIQUE INDEX idx_test_matview_all_nulls ON test_matview_all_nulls(COALESCE(category, 'NULL_CATEGORY'));

REFRESH MATERIALIZED VIEW CONCURRENTLY test_matview_all_nulls;
SELECT * FROM test_matview_all_nulls;

-- Stress Test: Multiple rapid refreshes
\echo 'Stress testing: Multiple rapid refreshes...'

\timing on
DO $$
BEGIN
    FOR i IN 1..5 LOOP
        -- Small modification
        INSERT INTO test_source_large (category, value, data)
        VALUES ('stress_test_' || i, i * 100, 'stress_data_' || i);

        -- Refresh
        REFRESH MATERIALIZED VIEW CONCURRENTLY test_matview_large;

        RAISE NOTICE 'Completed stress refresh %', i;
    END LOOP;
END $$;
\timing off

-- Final verification and statistics
\echo 'Final verification and statistics...'

SELECT
    schemaname,
    matviewname,
    hasindexes,
    ispopulated
FROM pg_matviews
WHERE matviewname LIKE 'test_matview%'
ORDER BY matviewname;

-- Check for any remaining temp tables or files
SELECT COUNT(*) as temp_relations_count
FROM pg_class
WHERE relname LIKE 'pg_temp%' OR relname LIKE '%temp%';

-- Performance summary
\echo 'Test completed. Compare timing results before and after modifications.';
\echo 'Verify that all correctness checks show counts_match = true.';
\echo 'Check that no temporary relations remain after refresh operations.';

-- Cleanup instructions (commented out - run manually if needed)
/*
DROP MATERIALIZED VIEW test_matview_large CASCADE;
DROP MATERIALIZED VIEW test_matview_duplicates CASCADE;
DROP MATERIALIZED VIEW test_matview_nulls CASCADE;
DROP MATERIALIZED VIEW test_matview_mixed CASCADE;
DROP MATERIALIZED VIEW test_matview_empty CASCADE;
DROP MATERIALIZED VIEW test_matview_single CASCADE;
DROP MATERIALIZED VIEW test_matview_all_nulls CASCADE;
DROP MATERIALIZED VIEW test_matview_large_fresh CASCADE;
DROP MATERIALIZED VIEW test_matview_duplicates_fresh CASCADE;
DROP MATERIALIZED VIEW test_matview_nulls_fresh CASCADE;
DROP MATERIALIZED VIEW test_matview_mixed_fresh CASCADE;
DROP TABLE test_source_large CASCADE;
DROP TABLE test_source_small CASCADE;
DROP FUNCTION generate_test_data CASCADE;
*/

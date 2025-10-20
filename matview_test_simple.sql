-- Comprehensive performance test for refresh_by_match_merge function
-- This test focuses specifically on REFRESH MATERIALIZED VIEW CONCURRENTLY
-- which calls refresh_by_match_merge (vs regular REFRESH which calls refresh_by_heap_swap)

-- Clean up any existing test objects
DROP MATERIALIZED VIEW IF EXISTS perf_test_simple CASCADE;
DROP MATERIALIZED VIEW IF EXISTS perf_test_large CASCADE;
DROP MATERIALIZED VIEW IF EXISTS perf_test_nulls CASCADE;
DROP MATERIALIZED VIEW IF EXISTS perf_test_duplicates CASCADE;
DROP MATERIALIZED VIEW IF EXISTS perf_test_mixed CASCADE;
DROP MATERIALIZED VIEW IF EXISTS perf_test_empty CASCADE;
DROP MATERIALIZED VIEW IF EXISTS perf_test_single CASCADE;
DROP MATERIALIZED VIEW IF EXISTS perf_test_wide CASCADE;
DROP TABLE IF EXISTS source_data CASCADE;
DROP TABLE IF EXISTS source_lookup CASCADE;

-- Create source tables
CREATE TABLE source_data (
    id SERIAL PRIMARY KEY,
    category VARCHAR(50),
    subcategory VARCHAR(50),
    value INTEGER,
    amount DECIMAL(15,2),
    data TEXT,
    active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP DEFAULT now()
);

CREATE TABLE source_lookup (
    lookup_id SERIAL PRIMARY KEY,
    lookup_code VARCHAR(20),
    lookup_name VARCHAR(100),
    multiplier DECIMAL(5,2) DEFAULT 1.0
);

-- Generate substantial test data (500K rows + lookup table)
\echo 'Generating large test dataset (this may take a minute)...'

-- Insert lookup data
INSERT INTO source_lookup (lookup_code, lookup_name, multiplier)
SELECT
    'code_' || i::TEXT,
    'name_' || i::TEXT,
    (random() * 5 + 0.5)::DECIMAL(5,2)
FROM generate_series(1, 1000) i;

-- Insert main dataset - 500K rows
INSERT INTO source_data (category, subcategory, value, amount, data, active, created_at)
SELECT
    'category_' || (i % 100)::TEXT,
    'sub_' || (i % 20)::TEXT,
    (random() * 100000)::INTEGER,
    (random() * 999999.99)::DECIMAL(15,2),
    'data_' || i::TEXT || '_' || repeat('x', (i % 100) + 1),
    (i % 4) != 0,
    now() - (random() * interval '2 years')
FROM generate_series(1, 500000) i;

CREATE INDEX idx_source_category ON source_data(category);
CREATE INDEX idx_source_subcategory ON source_data(subcategory);
CREATE INDEX idx_source_value ON source_data(value);
CREATE INDEX idx_source_active ON source_data(active);
CREATE INDEX idx_source_created ON source_data(created_at);
CREATE INDEX idx_lookup_code ON source_lookup(lookup_code);

ANALYZE source_data;
ANALYZE source_lookup;

\echo 'Test data generated: 500K rows in source_data, 1K rows in source_lookup';

-- Test Case 1: Simple aggregated view (main performance test)
\echo 'Creating aggregated materialized view...'
CREATE MATERIALIZED VIEW perf_test_simple AS
SELECT
    category,
    COUNT(*) as row_count,
    AVG(value) as avg_value,
    SUM(amount) as sum_amount,
    MAX(value) as max_value,
    MIN(created_at) as earliest_date,
    COUNT(CASE WHEN active THEN 1 END) as active_count
FROM source_data
GROUP BY category;

CREATE UNIQUE INDEX idx_perf_test_simple ON perf_test_simple(category);

-- Test Case 2: Large view with individual rows (tests tuple-by-tuple processing)
\echo 'Creating large individual rows view...'
CREATE MATERIALIZED VIEW perf_test_large AS
SELECT
    id,
    category,
    subcategory,
    value,
    amount,
    CASE
        WHEN value > 80000 THEN 'VERY_HIGH'
        WHEN value > 50000 THEN 'HIGH'
        WHEN value > 20000 THEN 'MEDIUM'
        ELSE 'LOW'
    END as value_class,
    CASE
        WHEN amount > 500000 THEN 'PREMIUM'
        ELSE 'STANDARD'
    END as amount_tier,
    active,
    extract(year from created_at) as created_year
FROM source_data
WHERE value > 5000;  -- About 400K rows

CREATE UNIQUE INDEX idx_perf_test_large ON perf_test_large(id);

-- Test Case 3: View with NULLs and complex conditions
\echo 'Creating view with NULLs and edge cases...'
CREATE MATERIALIZED VIEW perf_test_nulls AS
SELECT
    id,
    category,
    CASE
        WHEN value % 13 = 0 THEN NULL
        ELSE value
    END as nullable_value,
    CASE
        WHEN amount::INTEGER % 17 = 0 THEN NULL
        ELSE amount
    END as nullable_amount,
    CASE
        WHEN length(data) % 7 = 0 THEN NULL
        ELSE subcategory
    END as nullable_subcategory,
    active
FROM source_data
WHERE id % 3 = 0;  -- About 167K rows

CREATE UNIQUE INDEX idx_perf_test_nulls ON perf_test_nulls(id);

-- Test Case 4: View that can have duplicates before unique constraint
\echo 'Creating view with potential duplicates...'
CREATE MATERIALIZED VIEW perf_test_duplicates AS
SELECT
    category,
    subcategory,
    value % 1000 as mod_value,
    COUNT(*) as frequency,
    AVG(amount) as avg_amount
FROM source_data
WHERE active = true
GROUP BY category, subcategory, value % 1000;

CREATE UNIQUE INDEX idx_perf_test_duplicates ON perf_test_duplicates(category, subcategory, mod_value);

-- Test Case 5: Complex join view (tests join performance in refresh)
\echo 'Creating complex join view...'
CREATE MATERIALIZED VIEW perf_test_mixed AS
SELECT
    s.id,
    s.category,
    l.lookup_name,
    s.value * l.multiplier as computed_value,
    s.amount / l.multiplier as adjusted_amount,
    s.active,
    l.lookup_code
FROM source_data s
JOIN source_lookup l ON l.lookup_id = ((s.id % 1000) + 1)
WHERE s.value > 10000 AND s.active = true;

CREATE UNIQUE INDEX idx_perf_test_mixed ON perf_test_mixed(id);

-- Test Case 6: Wide view with many columns (tests memory usage)
\echo 'Creating wide materialized view...'
CREATE MATERIALIZED VIEW perf_test_wide AS
SELECT
    id,
    category,
    subcategory,
    value,
    amount,
    data,
    active,
    created_at,
    value * 2 as double_value,
    amount * 1.5 as inflated_amount,
    length(data) as data_length,
    upper(category) as upper_category,
    lower(subcategory) as lower_subcategory,
    CASE WHEN value > 50000 THEN 'high' ELSE 'normal' END as tier,
    extract(month from created_at) as created_month,
    extract(day from created_at) as created_day,
    value + id as combined_id,
    amount - value as difference,
    CASE WHEN active THEN 'Y' ELSE 'N' END as active_flag,
    substring(data, 1, 10) as data_prefix
FROM source_data
WHERE id % 5 = 0;  -- About 100K rows

CREATE UNIQUE INDEX idx_perf_test_wide ON perf_test_wide(id);

-- Edge Case 1: Empty result view (will become empty after data changes)
CREATE MATERIALIZED VIEW perf_test_empty AS
SELECT
    category,
    COUNT(*) as count_val
FROM source_data
WHERE category = 'nonexistent_category'
GROUP BY category;

CREATE UNIQUE INDEX idx_perf_test_empty ON perf_test_empty(category);

-- Edge Case 2: Single row view
CREATE MATERIALIZED VIEW perf_test_single AS
SELECT
    'global_stats' as stat_type,
    COUNT(*) as total_records,
    AVG(value) as global_avg_value,
    SUM(amount) as total_amount
FROM source_data;

CREATE UNIQUE INDEX idx_perf_test_single ON perf_test_single(stat_type);

-- Store initial counts for comparison
\echo 'Initial materialized view row counts:'
SELECT 'perf_test_simple' as view_name, COUNT(*) as rows FROM perf_test_simple
UNION ALL
SELECT 'perf_test_large', COUNT(*) FROM perf_test_large
UNION ALL
SELECT 'perf_test_nulls', COUNT(*) FROM perf_test_nulls
UNION ALL
SELECT 'perf_test_duplicates', COUNT(*) FROM perf_test_duplicates
UNION ALL
SELECT 'perf_test_mixed', COUNT(*) FROM perf_test_mixed
UNION ALL
SELECT 'perf_test_wide', COUNT(*) FROM perf_test_wide
UNION ALL
SELECT 'perf_test_empty', COUNT(*) FROM perf_test_empty
UNION ALL
SELECT 'perf_test_single', COUNT(*) FROM perf_test_single
ORDER BY view_name;

-- Make significant changes to source data (this is the heavy part)
\echo 'Making substantial changes to source data...';
\echo 'This simulates a real-world scenario with major data changes.';

-- Phase 1: Delete 25% of data (125K rows)
\echo 'Phase 1: Deleting 25% of source data...';
DELETE FROM source_data WHERE id % 4 = 0;

-- Phase 2: Update 30% of remaining data (about 112K rows)
\echo 'Phase 2: Updating 30% of remaining data...';
UPDATE source_data
SET value = value + (random() * 10000)::INTEGER,
    amount = amount * (1.0 + random() * 0.5),
    data = 'updated_' || data,
    updated_at = now(),
    subcategory = 'updated_' || subcategory
WHERE id % 3 = 1;

-- Phase 3: Insert substantial new data (250K new rows)
\echo 'Phase 3: Inserting 250K new rows...';
INSERT INTO source_data (category, subcategory, value, amount, data, active, created_at)
SELECT
    'new_cat_' || ((i % 75) + 100)::TEXT,
    'new_sub_' || (i % 25)::TEXT,
    (random() * 150000 + 10000)::INTEGER,
    (random() * 1500000.00)::DECIMAL(15,2),
    'new_data_' || i::TEXT || '_' || repeat('y', (i % 80) + 5),
    (i % 5) != 0,
    now() + (random() * interval '30 days')
FROM generate_series(1, 250000) i;

-- Phase 4: Modify lookup table
\echo 'Phase 4: Modifying lookup table...';
UPDATE source_lookup SET multiplier = multiplier * 1.1 WHERE lookup_id % 2 = 0;
INSERT INTO source_lookup (lookup_code, lookup_name, multiplier)
SELECT
    'new_code_' || i::TEXT,
    'new_name_' || i::TEXT,
    (random() * 3 + 1.0)::DECIMAL(5,2)
FROM generate_series(1001, 1200) i;

ANALYZE source_data;
ANALYZE source_lookup;

\echo 'Data modifications complete. Source now has:';
SELECT COUNT(*) as total_source_rows FROM source_data;

-- Performance test: Time the CONCURRENTLY refreshes
\echo '';
\echo '=== STARTING PERFORMANCE TESTS ===';
\echo 'Timing REFRESH MATERIALIZED VIEW CONCURRENTLY operations...';
\timing on

\echo 'Test 1: Refreshing aggregated view (100 categories)...';
REFRESH MATERIALIZED VIEW CONCURRENTLY perf_test_simple;

\echo 'Test 2: Refreshing large individual rows view (~300K rows)...';
REFRESH MATERIALIZED VIEW CONCURRENTLY perf_test_large;

\echo 'Test 3: Refreshing view with NULLs (~125K rows)...';
REFRESH MATERIALIZED VIEW CONCURRENTLY perf_test_nulls;

\echo 'Test 4: Refreshing duplicates/aggregation view...';
REFRESH MATERIALIZED VIEW CONCURRENTLY perf_test_duplicates;

\echo 'Test 5: Refreshing complex join view...';
REFRESH MATERIALIZED VIEW CONCURRENTLY perf_test_mixed;

\echo 'Test 6: Refreshing wide view with many columns...';
REFRESH MATERIALIZED VIEW CONCURRENTLY perf_test_wide;

\echo 'Test 7: Refreshing empty result view...';
REFRESH MATERIALIZED VIEW CONCURRENTLY perf_test_empty;

\echo 'Test 8: Refreshing single row view...';
REFRESH MATERIALIZED VIEW CONCURRENTLY perf_test_single;

\timing off
\echo '=== PERFORMANCE TESTS COMPLETED ===';
\echo '';

-- Verify correctness by creating fresh views and comparing
\echo 'Verifying correctness by comparing with fresh materialized views...';

CREATE MATERIALIZED VIEW perf_test_simple_fresh AS
SELECT
    category,
    COUNT(*) as row_count,
    AVG(value) as avg_value,
    SUM(amount) as sum_amount,
    MAX(value) as max_value,
    MIN(created_at) as earliest_date,
    COUNT(CASE WHEN active THEN 1 END) as active_count
FROM source_data
GROUP BY category;

CREATE MATERIALIZED VIEW perf_test_large_fresh AS
SELECT
    id,
    category,
    subcategory,
    value,
    amount,
    CASE
        WHEN value > 80000 THEN 'VERY_HIGH'
        WHEN value > 50000 THEN 'HIGH'
        WHEN value > 20000 THEN 'MEDIUM'
        ELSE 'LOW'
    END as value_class,
    CASE
        WHEN amount > 500000 THEN 'PREMIUM'
        ELSE 'STANDARD'
    END as amount_tier,
    active,
    extract(year from created_at) as created_year
FROM source_data
WHERE value > 5000;

CREATE MATERIALIZED VIEW perf_test_nulls_fresh AS
SELECT
    id,
    category,
    CASE
        WHEN value % 13 = 0 THEN NULL
        ELSE value
    END as nullable_value,
    CASE
        WHEN amount::INTEGER % 17 = 0 THEN NULL
        ELSE amount
    END as nullable_amount,
    CASE
        WHEN length(data) % 7 = 0 THEN NULL
        ELSE subcategory
    END as nullable_subcategory,
    active
FROM source_data
WHERE id % 3 = 0;

-- Compare results for correctness
\echo '';
\echo '=== CORRECTNESS VERIFICATION ===';

SELECT
    'perf_test_simple' as view_name,
    (SELECT COUNT(*) FROM perf_test_simple) as refreshed_count,
    (SELECT COUNT(*) FROM perf_test_simple_fresh) as fresh_count,
    (SELECT COUNT(*) FROM perf_test_simple) = (SELECT COUNT(*) FROM perf_test_simple_fresh) as counts_match,
    (SELECT SUM(row_count) FROM perf_test_simple) = (SELECT SUM(row_count) FROM perf_test_simple_fresh) as sums_match;

SELECT
    'perf_test_large' as view_name,
    (SELECT COUNT(*) FROM perf_test_large) as refreshed_count,
    (SELECT COUNT(*) FROM perf_test_large_fresh) as fresh_count,
    (SELECT COUNT(*) FROM perf_test_large) = (SELECT COUNT(*) FROM perf_test_large_fresh) as counts_match;

SELECT
    'perf_test_nulls' as view_name,
    (SELECT COUNT(*) FROM perf_test_nulls) as refreshed_count,
    (SELECT COUNT(*) FROM perf_test_nulls_fresh) as fresh_count,
    (SELECT COUNT(*) FROM perf_test_nulls) = (SELECT COUNT(*) FROM perf_test_nulls_fresh) as counts_match;

-- Test edge cases with more data changes
\echo '';
\echo '=== EDGE CASE TESTING ===';

-- Edge case: Make a view actually empty
DELETE FROM source_data WHERE category LIKE 'category_9%';
REFRESH MATERIALIZED VIEW CONCURRENTLY perf_test_simple;
SELECT
    COUNT(*) as categories_with_9,
    (SELECT COUNT(*) FROM perf_test_simple WHERE category LIKE 'category_9%') as refreshed_9_count
FROM source_data WHERE category LIKE 'category_9%';

-- Edge case: Create scenario with many duplicates
INSERT INTO source_data (category, subcategory, value, amount, data, active)
SELECT
    'duplicate_test',
    'dup_sub',
    12345,
    678.90,
    'duplicate_data_' || i::TEXT,
    true
FROM generate_series(1, 1000) i;

REFRESH MATERIALIZED VIEW CONCURRENTLY perf_test_duplicates;

-- Edge case: Very large single category
INSERT INTO source_data (category, subcategory, value, amount, data, active)
SELECT
    'massive_category',
    'sub_' || (i % 5)::TEXT,
    i,
    i * 1.5,
    'mass_data_' || i::TEXT,
    true
FROM generate_series(1, 50000) i;

REFRESH MATERIALIZED VIEW CONCURRENTLY perf_test_simple;
SELECT row_count FROM perf_test_simple WHERE category = 'massive_category';

-- Stress test: Multiple rapid refreshes
\echo '';
\echo '=== STRESS TEST: Multiple rapid refreshes ===';
\timing on

DO $$
DECLARE
    i INTEGER;
BEGIN
    FOR i IN 1..5 LOOP
        -- Small modifications
        INSERT INTO source_data (category, value, amount, data)
        VALUES ('stress_' || i, i * 1000, i * 100.0, 'stress_data_' || i);

        -- Refresh multiple views
        REFRESH MATERIALIZED VIEW CONCURRENTLY perf_test_simple;
        REFRESH MATERIALIZED VIEW CONCURRENTLY perf_test_single;

        RAISE NOTICE 'Completed stress iteration %', i;
    END LOOP;
END $$;

\timing off

-- Memory and resource usage test
\echo '';
\echo '=== RESOURCE USAGE CHECK ===';

-- Check for temporary objects
SELECT
    COUNT(*) as temp_objects_count,
    COALESCE(array_agg(relname), ARRAY[]::name[]) as temp_object_names
FROM pg_class
WHERE relname LIKE '%pg_temp%'
   OR relname LIKE '%matview%temp%'
   OR (relkind = 'r' AND relpersistence = 't');

-- Check current connections and locks
SELECT COUNT(*) as active_connections FROM pg_stat_activity WHERE state = 'active';

-- Final statistics
\echo '';
\echo '=== FINAL STATISTICS ===';

SELECT 'Final row counts:' as summary;
SELECT 'perf_test_simple' as view_name, COUNT(*) as rows FROM perf_test_simple
UNION ALL
SELECT 'perf_test_large', COUNT(*) FROM perf_test_large
UNION ALL
SELECT 'perf_test_nulls', COUNT(*) FROM perf_test_nulls
UNION ALL
SELECT 'perf_test_duplicates', COUNT(*) FROM perf_test_duplicates
UNION ALL
SELECT 'perf_test_mixed', COUNT(*) FROM perf_test_mixed
UNION ALL
SELECT 'perf_test_wide', COUNT(*) FROM perf_test_wide
UNION ALL
SELECT 'perf_test_empty', COUNT(*) FROM perf_test_empty
UNION ALL
SELECT 'perf_test_single', COUNT(*) FROM perf_test_single
UNION ALL
SELECT 'source_data', COUNT(*) FROM source_data
ORDER BY view_name;

\echo '';
\echo '=== TEST SUMMARY ===';
\echo 'Performance test completed successfully!';
\echo 'Key metrics to compare:';
\echo '  1. Timing results for each REFRESH MATERIALIZED VIEW CONCURRENTLY';
\echo '  2. All counts_match and sums_match should be TRUE';
\echo '  3. temp_objects_count should be 0 or very low';
\echo '  4. Memory usage during large refreshes';
\echo '';
\echo 'This test exercises refresh_by_match_merge with:';
\echo '  - 500K+ source rows across multiple scenarios';
\echo '  - Complex aggregations, joins, and computations';
\echo '  - NULL handling and edge cases';
\echo '  - Large data modifications (deletes, updates, inserts)';
\echo '  - Multiple concurrent refreshes';
\echo '';

-- Cleanup instructions (commented out - run manually if needed)
/*
DROP MATERIALIZED VIEW perf_test_simple CASCADE;
DROP MATERIALIZED VIEW perf_test_large CASCADE;
DROP MATERIALIZED VIEW perf_test_nulls CASCADE;
DROP MATERIALIZED VIEW perf_test_duplicates CASCADE;
DROP MATERIALIZED VIEW perf_test_mixed CASCADE;
DROP MATERIALIZED VIEW perf_test_wide CASCADE;
DROP MATERIALIZED VIEW perf_test_empty CASCADE;
DROP MATERIALIZED VIEW perf_test_single CASCADE;
DROP MATERIALIZED VIEW perf_test_simple_fresh CASCADE;
DROP MATERIALIZED VIEW perf_test_large_fresh CASCADE;
DROP MATERIALIZED VIEW perf_test_nulls_fresh CASCADE;
DROP TABLE source_data CASCADE;
DROP TABLE source_lookup CASCADE;
*/

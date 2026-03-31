-- Tests for extensible node and custom scan registration
-- (src/backend/nodes/extensible.c)

CREATE EXTENSION test_extensible;

-- ----------------------------------------------------------------
-- GetExtensibleNodeMethods() and GetCustomScanMethods() lookup tests
-- ----------------------------------------------------------------

-- GetExtensibleNodeMethods: known name returns the registered extnodename.
SELECT test_get_extensible_node_methods('TestExtNode', false);
-- GetExtensibleNodeMethods: unknown name with missing_ok=true returns NULL.
SELECT test_get_extensible_node_methods('NoSuchExtNode', true);
-- GetExtensibleNodeMethods: unknown name with missing_ok=false raises ERROR.
SELECT test_get_extensible_node_methods('NoSuchExtNode', false);

-- GetCustomScanMethods: known name returns the registered CustomName.
SELECT test_get_custom_scan_methods('TestCustomScan', false);
-- GetCustomScanMethods: unknown name with missing_ok=true returns NULL.
SELECT test_get_custom_scan_methods('NoSuchCustomScan', true);
-- GetCustomScanMethods: unknown name with missing_ok=false raises ERROR.
SELECT test_get_custom_scan_methods('NoSuchCustomScan', false);

-- Both lookup functions are STRICT, so a NULL argument yields NULL rather than
-- reaching the C code, which would dereference the argument unconditionally.
SELECT test_get_extensible_node_methods(NULL, false) IS NULL AS null_name;
SELECT test_get_extensible_node_methods('TestExtNode', NULL) IS NULL AS null_missing_ok;
SELECT test_get_custom_scan_methods(NULL, false) IS NULL AS null_name;
SELECT test_get_custom_scan_methods('TestCustomScan', NULL) IS NULL AS null_missing_ok;

-- ----------------------------------------------------------------
-- ExtensibleNodeMethods callbacks: nodeCopy, nodeEqual, nodeOut, nodeRead
-- ----------------------------------------------------------------

-- A TestExtNode travels between these functions in its serialized form, so
-- every one of them runs nodeOut, nodeRead or both. The text below is what the
-- nodeOut callback produced.
SELECT test_ext_node_make('1234'::oid, 2);

-- nodeRead has to restore both fields exactly as nodeOut wrote them.
SELECT test_ext_node_get_relid(n) AS relid,
       test_ext_node_get_repeat_count(n) AS repeat_count
  FROM (SELECT test_ext_node_make('1234'::oid, 2)) AS s(n);

-- nodeCopy: the copy compares equal to the original and, since it has to copy
-- every field, serializes to the very same string.
SELECT test_ext_node_equal(n, test_ext_node_copy(n)) AS copy_is_equal,
       test_ext_node_copy(n) = n AS copy_is_identical
  FROM (SELECT test_ext_node_make('1234'::oid, 2)) AS s(n);

-- nodeEqual: nodes differing in either field must not compare equal.
SELECT test_ext_node_equal(test_ext_node_make('1234'::oid, 2),
                           test_ext_node_make('1234'::oid, 3)) AS other_repeat_count,
       test_ext_node_equal(test_ext_node_make('1234'::oid, 2),
                           test_ext_node_make('5678'::oid, 2)) AS other_relid;

-- A string describing some other kind of node is rejected rather than being
-- misinterpreted as a TestExtNode. '(b 1 2)' is a serialized Bitmapset.
SELECT test_ext_node_get_repeat_count('(b 1 2)');

-- ----------------------------------------------------------------
-- End-to-end CustomScan test
-- ----------------------------------------------------------------

CREATE TABLE test_extensible_tbl (id integer, val text);
INSERT INTO test_extensible_tbl VALUES (1, 'one'), (2, 'two'), (3, 'three');

-- Verify the planner chose our CustomScan (not a SeqScan).
EXPLAIN (COSTS OFF) SELECT id, val FROM test_extensible_tbl ORDER BY id;

-- Execute through the CustomScan; each of the 3 inserted rows appears
-- twice (6 rows total), proving the custom scan logic is in effect.
SELECT id, val FROM test_extensible_tbl ORDER BY id;

-- The restriction clauses are passed to the CustomScan as its qual.
EXPLAIN (COSTS OFF) SELECT val FROM test_extensible_tbl WHERE id > 1;
SELECT val FROM test_extensible_tbl WHERE id > 1 ORDER BY id;

-- How many times a row is repeated is controlled by a GUC. It is read while
-- planning and stored in the ExtensibleNode, so it is the value in effect at
-- plan time that counts.
SET test_extensible.repeat_count = 3;
SELECT id, val FROM test_extensible_tbl ORDER BY id;
RESET test_extensible.repeat_count;

-- Out-of-range values are rejected by the GUC machinery.
SET test_extensible.repeat_count = 0;

-- ----------------------------------------------------------------
-- The CustomScan below a Gather
-- ----------------------------------------------------------------

-- Our CustomPath is parallel safe, so the plan can be pushed below a Gather.
-- This is the only case where the core calls the nodeOut and nodeRead
-- callbacks of our ExtensibleNode on its own: the plan tree is serialized into
-- dynamic shared memory and read back by the parallel worker, which finds our
-- callbacks through the names registered by _PG_init(). That is also why the
-- module has to be listed in shared_preload_libraries.
--
-- debug_parallel_query = regress forces the Gather and keeps it invisible to
-- EXPLAIN, so the plan below is expected to look exactly like the serial one.
SET debug_parallel_query = regress;
EXPLAIN (COSTS OFF) SELECT id, val FROM test_extensible_tbl ORDER BY id;
SELECT id, val FROM test_extensible_tbl ORDER BY id;
RESET debug_parallel_query;

DROP TABLE test_extensible_tbl;

DROP EXTENSION test_extensible;

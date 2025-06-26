CREATE EXTENSION test_binaryheap;

-- Test edge cases
SELECT test_binary_heap(1);
SELECT test_binary_heap(2);
SELECT test_binary_heap(3);

-- Test with small heap
SELECT test_binary_heap(10);

-- Test with larger heap
SELECT test_binary_heap(100);

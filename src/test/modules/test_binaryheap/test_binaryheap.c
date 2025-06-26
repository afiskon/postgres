/*--------------------------------------------------------------------------
 *
 * test_binaryheap.c
 *		Test correctness of binary heap implementation.
 *
 * Copyright (c) 2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_binaryheap/test_binaryheap.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "common/pg_prng.h"
#include "fmgr.h"
#include "lib/binaryheap.h"
#include "utils/memutils.h"

PG_MODULE_MAGIC;

/* Callback function types */
typedef binaryheap *(*heap_create_func) (int capacity);
typedef void (*heap_verify_func) (binaryheap *heap);
typedef int (*heap_expected_first_func) (binaryheap *heap);

/* Comparator for max-heap */
static int
max_heap_cmp_int(Datum a, Datum b, void *arg)
{
	return DatumGetInt32(a) - DatumGetInt32(b);
}

/* Comparator for min-heap */
static int
min_heap_cmp_int(Datum a, Datum b, void *arg)
{
	return DatumGetInt32(b) - DatumGetInt32(a);
}

/* Create a max-heap of integers */
static binaryheap *
create_max_heap(int capacity)
{
	return binaryheap_allocate(capacity, max_heap_cmp_int, NULL);
}

/* Create a min-heap of integers */
static binaryheap *
create_min_heap(int capacity)
{
	return binaryheap_allocate(capacity, min_heap_cmp_int, NULL);
}

/* Find maximum value in heap (for max-heap expected first) */
static int
get_max_value_from_heap(binaryheap *heap)
{
	int			i;
	int			size = binaryheap_size(heap);
	int			max_val = DatumGetInt32(binaryheap_get_node(heap, 0));

	for (i = 1; i < size; i++)
	{
		int			val = DatumGetInt32(binaryheap_get_node(heap, i));

		if (val > max_val)
			max_val = val;
	}
	return max_val;
}

/* Find minimum value in heap (for min-heap expected first) */
static int
get_min_value_from_heap(binaryheap *heap)
{
	int			i;
	int			size = binaryheap_size(heap);
	int			min_val = DatumGetInt32(binaryheap_get_node(heap, 0));

	for (i = 1; i < size; i++)
	{
		int			val = DatumGetInt32(binaryheap_get_node(heap, i));

		if (val < min_val)
			min_val = val;
	}
	return min_val;
}

/* Generate a random permutation of the integers 0..size-1 */
static int *
gen_permutation(int size)
{
	int		   *arr;
	int			i,
				j,
				temp;

	arr = (int *) palloc(size * sizeof(int));

	for (i = 0; i < size; i++)
		arr[i] = i;

	for (i = 0; i < size; i++)
	{
		j = pg_prng_uint64_range(&pg_global_prng_state, 0, size - 1);
		temp = arr[i];
		arr[i] = arr[j];
		arr[j] = temp;
	}

	return arr;
}

/* Verify that a max-heap satisfies the heap property. */
static void
verify_max_heap_property(binaryheap *heap)
{
	int			i;
	int			size = binaryheap_size(heap);

	for (i = 0; i < size; i++)
	{
		int			left_child = 2 * i + 1;
		int			right_child = 2 * i + 2;
		int			parent_val = DatumGetInt32(binaryheap_get_node(heap, i));

		if (left_child < size)
		{
			int			left_val = DatumGetInt32(binaryheap_get_node(heap, left_child));

			if (parent_val < left_val)
				elog(ERROR, "max-heap property violated: parent %d < left child %d",
					 parent_val, left_val);
		}

		if (right_child < size)
		{
			int			right_val = DatumGetInt32(binaryheap_get_node(heap, right_child));

			if (parent_val < right_val)
				elog(ERROR, "max-heap property violated: parent %d < right child %d",
					 parent_val, right_val);
		}
	}
}

/* Verify that a min-heap satisfies the heap property. */
static void
verify_min_heap_property(binaryheap *heap)
{
	int			i;
	int			size = binaryheap_size(heap);

	for (i = 0; i < size; i++)
	{
		int			left_child = 2 * i + 1;
		int			right_child = 2 * i + 2;
		int			parent_val = DatumGetInt32(binaryheap_get_node(heap, i));

		if (left_child < size)
		{
			int			left_val = DatumGetInt32(binaryheap_get_node(heap, left_child));

			if (parent_val > left_val)
				elog(ERROR, "min-heap property violated: parent %d > left child %d",
					 parent_val, left_val);
		}

		if (right_child < size)
		{
			int			right_val = DatumGetInt32(binaryheap_get_node(heap, right_child));

			if (parent_val > right_val)
				elog(ERROR, "min-heap property violated: parent %d > right child %d",
					 parent_val, right_val);
		}
	}
}

/* Test basic heap operations */
static void
test_heap_basic(int size, heap_create_func create_heap, heap_verify_func verify_heap, heap_expected_first_func expected_first)
{
	binaryheap *heap;
	int		   *permutation;
	int			i;

	heap = create_heap(size);

	/* Test empty heap */
	if (!binaryheap_empty(heap))
		elog(ERROR, "new heap should be empty");
	if (binaryheap_size(heap) != 0)
		elog(ERROR, "new heap should have size 0");

	permutation = gen_permutation(size);
	for (i = 0; i < size; i++)
	{
		binaryheap_add(heap, Int32GetDatum(permutation[i]));
		verify_heap(heap);
	}
	pfree(permutation);

	if (binaryheap_empty(heap))
		elog(ERROR, "heap should not be empty after adding elements");
	if (binaryheap_size(heap) != size)
		elog(ERROR, "heap size should be %d, got %d", size, binaryheap_size(heap));

	/* Test that binaryheap_first() returns expected value */
	if (DatumGetInt32(binaryheap_first(heap)) != expected_first(heap))
		elog(ERROR, "heap first element should be %d, got %d",
			 expected_first(heap), DatumGetInt32(binaryheap_first(heap)));

	/* Remove all elements and verify each one */
	for (i = 0; i < size; i++)
	{
		int			expected = expected_first(heap);
		int			actual = DatumGetInt32(binaryheap_remove_first(heap));

		if (actual != expected)
			elog(ERROR, "remove_first should return %d, got %d", expected, actual);
		verify_heap(heap);
	}

	if (!binaryheap_empty(heap))
		elog(ERROR, "heap should be empty after removing all elements");

	binaryheap_free(heap);
}

/* Test unordered add and build operations */
static void
test_heap_build(int size, heap_create_func create_heap, heap_verify_func verify_heap, heap_expected_first_func expected_first)
{
	binaryheap *heap;
	int		   *permutation;
	int			i;

	heap = create_heap(size);

	/* Add elements in unordered fashion */
	permutation = gen_permutation(size);
	for (i = 0; i < size; i++)
		binaryheap_add_unordered(heap, Int32GetDatum(permutation[i]));
	pfree(permutation);

	/* At this point, heap property should not hold */
	if (binaryheap_size(heap) != size)
		elog(ERROR, "heap size should be %d after unordered adds", size);

	/* Build the heap */
	binaryheap_build(heap);

	/* Now heap property should hold */
	verify_heap(heap);

	/* Test that binaryheap_first() returns expected value after build */
	if (DatumGetInt32(binaryheap_first(heap)) != expected_first(heap))
		elog(ERROR, "heap first element should be %d after build, got %d",
			 expected_first(heap), DatumGetInt32(binaryheap_first(heap)));

	/* Remove all elements and verify each one */
	while (!binaryheap_empty(heap))
	{
		int			expected = expected_first(heap);
		int			actual = DatumGetInt32(binaryheap_remove_first(heap));

		if (actual != expected)
			elog(ERROR, "remove_first should return %d, got %d", expected, actual);
		verify_heap(heap);
	}

	binaryheap_free(heap);
}

/* Test remove_node functionality */
static void
test_heap_remove_node(int size, heap_create_func create_heap, heap_verify_func verify_heap, heap_expected_first_func expected_first)
{
	binaryheap *heap;
	int		   *permutation;
	int			i;
	int			idx,
				remove_count;

	heap = create_heap(size);

	/* Add elements */
	permutation = gen_permutation(size);
	for (i = 0; i < size; i++)
		binaryheap_add(heap, Int32GetDatum(permutation[i]));
	pfree(permutation);

	/* Remove some random nodes */
	remove_count = size / 3;
	for (i = 0; i < remove_count; i++)
	{
		idx = pg_prng_uint64_range(&pg_global_prng_state, 0, binaryheap_size(heap) - 1);
		binaryheap_remove_node(heap, idx);
		verify_heap(heap);
	}

	if (binaryheap_size(heap) != size - remove_count)
		elog(ERROR, "heap size should be %d after removing %d nodes, got %d",
			 size - remove_count, remove_count, binaryheap_size(heap));

	/* Remove all remaining elements and verify each one */
	while (!binaryheap_empty(heap))
	{
		int			expected = expected_first(heap);
		int			actual = DatumGetInt32(binaryheap_remove_first(heap));

		if (actual != expected)
			elog(ERROR, "remove_first should return %d, got %d", expected, actual);
		verify_heap(heap);
	}

	binaryheap_free(heap);
}

/* Test replace_first functionality */
static void
test_heap_replace_first(int size, heap_create_func create_heap, heap_verify_func verify_heap, heap_expected_first_func expected_first)
{
	binaryheap *heap;
	int			i;

	heap = create_heap(size);

	/* Add elements 0 to size-1 */
	for (i = 0; i < size; i++)
		binaryheap_add(heap, Int32GetDatum(i));

	/* Replace first with a middle value */
	binaryheap_replace_first(heap, Int32GetDatum(size / 2));
	verify_heap(heap);

	/* Test that binaryheap_first() returns expected value after replacement */
	if (DatumGetInt32(binaryheap_first(heap)) != expected_first(heap))
		elog(ERROR, "first element should be %d after replacement, got %d",
			 expected_first(heap), DatumGetInt32(binaryheap_first(heap)));

	/* Replace first with an extreme value */
	binaryheap_replace_first(heap, Int32GetDatum(size + 10));
	verify_heap(heap);

	/*
	 * Test that binaryheap_first() returns expected value after second
	 * replacement
	 */
	if (DatumGetInt32(binaryheap_first(heap)) != expected_first(heap))
		elog(ERROR, "first element should be %d after replacement, got %d",
			 expected_first(heap), DatumGetInt32(binaryheap_first(heap)));

	binaryheap_free(heap);
}

/* Test heap with duplicate values */
static void
test_heap_duplicates(int size, heap_create_func create_heap, heap_verify_func verify_heap)
{
	binaryheap *heap;
	int			i;
	int			duplicate_value = 42;

	heap = create_heap(size);

	/* Add the same value multiple times */
	for (i = 0; i < size; i++)
	{
		binaryheap_add(heap, Int32GetDatum(duplicate_value));
		verify_heap(heap);
	}

	/* All elements should be the same */
	if (DatumGetInt32(binaryheap_first(heap)) != duplicate_value)
		elog(ERROR, "first element should be %d", duplicate_value);

	/* Remove all elements */
	for (i = 0; i < size; i++)
	{
		int			val = DatumGetInt32(binaryheap_remove_first(heap));

		if (val != duplicate_value)
			elog(ERROR, "all elements should be %d", duplicate_value);
	}

	binaryheap_free(heap);
}

/* Test heap reset functionality */
static void
test_heap_reset(int size, heap_create_func create_heap)
{
	binaryheap *heap;
	int			i;

	heap = create_heap(size);

	/* Add some elements */
	for (i = 0; i < size; i++)
		binaryheap_add(heap, Int32GetDatum(i));

	if (binaryheap_empty(heap))
		elog(ERROR, "heap should not be empty after adding elements");

	/* Reset the heap */
	binaryheap_reset(heap);

	if (!binaryheap_empty(heap))
		elog(ERROR, "heap should be empty after reset");
	if (binaryheap_size(heap) != 0)
		elog(ERROR, "heap size should be 0 after reset");

	/* Should be able to add elements again */
	binaryheap_add(heap, Int32GetDatum(42));
	if (DatumGetInt32(binaryheap_first(heap)) != 42)
		elog(ERROR, "heap should contain 42 after reset and add");

	binaryheap_free(heap);
}

/*
 * SQL-callable entry point to perform all tests
 *
 * Argument is the number of entries to put in the heaps
 */
PG_FUNCTION_INFO_V1(test_binary_heap);

Datum
test_binary_heap(PG_FUNCTION_ARGS)
{
	int			size = PG_GETARG_INT32(0);

	if (size <= 0 || size > MaxAllocSize / sizeof(Datum))
		elog(ERROR, "invalid size for test_binary_heap: %d", size);

	/* Test max-heap */
	test_heap_basic(size, create_max_heap, verify_max_heap_property, get_max_value_from_heap);
	test_heap_build(size, create_max_heap, verify_max_heap_property, get_max_value_from_heap);
	test_heap_remove_node(size, create_max_heap, verify_max_heap_property, get_max_value_from_heap);
	test_heap_replace_first(size, create_max_heap, verify_max_heap_property, get_max_value_from_heap);
	test_heap_duplicates(size, create_max_heap, verify_max_heap_property);
	test_heap_reset(size, create_max_heap);

	/* Test min-heap */
	test_heap_basic(size, create_min_heap, verify_min_heap_property, get_min_value_from_heap);
	test_heap_build(size, create_min_heap, verify_min_heap_property, get_min_value_from_heap);
	test_heap_remove_node(size, create_min_heap, verify_min_heap_property, get_min_value_from_heap);
	test_heap_replace_first(size, create_min_heap, verify_min_heap_property, get_min_value_from_heap);
	test_heap_duplicates(size, create_min_heap, verify_min_heap_property);
	test_heap_reset(size, create_min_heap);

	PG_RETURN_VOID();
}

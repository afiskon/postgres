/*--------------------------------------------------------------------------
 *
 * test_slru.c
 *		Test correctness of SLRU functions.
 *
 * Copyright (c) 2022, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_slru/test_slru.c
 *
 * -------------------------------------------------------------------------
 */

#include <postgres.h>
#include <miscadmin.h>
#include <storage/ipc.h>
#include <storage/shmem.h>
#include <storage/lwlock.h>
#include <utils/builtins.h>
#include "access/slru.h"
#include "access/transam.h"
#include "storage/fd.h"

PG_MODULE_MAGIC;

/*
 * SQL-callable entry point to perform all the tests
 */
PG_FUNCTION_INFO_V1(test_slru);


#define NUM_TEST_BUFFERS		16

/* SLRU control lock */
LWLock		TestSLRULock;
#define TestSLRULock (&TestSLRULock)

static SlruCtlData TestSlruCtlData;
#define TestSlruCtl			(&TestSlruCtlData)

static shmem_request_hook_type prev_shmem_request_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

const char	test_tranche_name[] = "test_slru_tranche";
const char	test_data[] = "Test data";

static int	CallbackCounter;

static void
EnsureStringEquals(const char *value, const char *expected)
{
	if (strcmp(value, expected) != 0)
		elog(ERROR, "Got '%s', expected '%s'", value, expected);
}

static void
EnsureIntEquals(int value, int expected)
{
	if (value != expected)
		elog(ERROR, "Got %d, expected %d", value, expected);
}

static bool
test_slru_page_precedes_logically(int page1, int page2)
{
	return page1 < page2;
}

static bool
test_slru_scan_cb(SlruCtl ctl, char *filename, int segpage, void *data)
{
	/*
	 * Since the scan order is not guaranteed, we don't display these values.
	 * What actually interests us is the number of times the callback will be
	 * executed.
	 */
	(void) filename;
	(void) segpage;

	elog(NOTICE, "test_slru_scan_cb() called, (char*))data = '%s'", (char *) data);
	EnsureStringEquals(data, test_data);

	CallbackCounter++;

	return false;				/* keep going */
}

static void
test_slru_shmem_request(void)
{
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();

	/* Reserve shared memory for the test SLRU */
	RequestAddinShmemSpace(SimpleLruShmemSize(NUM_TEST_BUFFERS, 0));
}

static void
test_slru_shmem_startup(void)
{
	const char	slru_dir_name[] = "pg_test_slru";
	int			test_tranche_id;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	elog(NOTICE, "Creating directory '%s', if not exists",
		 slru_dir_name);
	(void) MakePGDirectory(slru_dir_name);

	elog(NOTICE, "Initializing SLRU");

	test_tranche_id = LWLockNewTrancheId();
	LWLockRegisterTranche(test_tranche_id, "test_slru_tranche");
	LWLockInitialize(TestSLRULock, test_tranche_id);

	TestSlruCtl->PagePrecedes = test_slru_page_precedes_logically;
	SimpleLruInit(TestSlruCtl, "Test",
				  NUM_TEST_BUFFERS, 0, TestSLRULock, slru_dir_name,
				  test_tranche_id, SYNC_HANDLER_NONE);

	elog(NOTICE, "SLRU initialized");
}

void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		elog(FATAL, "Please use shared_preload_libraries");

	elog(LOG, "extension loaded");

	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = test_slru_shmem_request;

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = test_slru_shmem_startup;
}

Datum
test_slru(PG_FUNCTION_ARGS)
{
	bool		found;
	int			slotno,
				i;
	FileTag		ftag;
	char		path[MAXPGPATH];
	int			test_pageno = 12345;

	elog(NOTICE, "Calling SimpleLruZeroPage()");

	LWLockAcquire(TestSLRULock, LW_EXCLUSIVE);
	slotno = SimpleLruZeroPage(TestSlruCtl, test_pageno);

	TestSlruCtl->shared->page_dirty[slotno] = true;
	TestSlruCtl->shared->page_status[slotno] = SLRU_PAGE_VALID;
	strcpy(TestSlruCtl->shared->page_buffer[slotno], test_data);

	elog(NOTICE, "Success, shared->page_number[slotno] = %d",
		 TestSlruCtl->shared->page_number[slotno]);
	EnsureIntEquals(TestSlruCtl->shared->page_number[slotno], test_pageno);

	found = SimpleLruDoesPhysicalPageExist(TestSlruCtl, test_pageno);
	elog(NOTICE, "SimpleLruDoesPhysicalPageExist() returned %d. "
		 "Now writing the page.", found);
	EnsureIntEquals(found, false);

	SimpleLruWritePage(TestSlruCtl, slotno);
	LWLockRelease(TestSLRULock);

	found = SimpleLruDoesPhysicalPageExist(TestSlruCtl, test_pageno);
	elog(NOTICE, "SimpleLruDoesPhysicalPageExist() returned %d", found);
	EnsureIntEquals(found, true);

	elog(NOTICE, "Writing %d more pages...", NUM_TEST_BUFFERS * 3);
	for (i = 1; i <= NUM_TEST_BUFFERS * 3; i++)
	{
		LWLockAcquire(TestSLRULock, LW_EXCLUSIVE);
		slotno = SimpleLruZeroPage(TestSlruCtl, test_pageno + i);

		TestSlruCtl->shared->page_dirty[slotno] = true;
		TestSlruCtl->shared->page_status[slotno] = SLRU_PAGE_VALID;
		strcpy(TestSlruCtl->shared->page_buffer[slotno], test_data);
		LWLockRelease(TestSLRULock);
	}

	elog(NOTICE, "Reading page %d (in buffer) for read and write",
		 test_pageno + NUM_TEST_BUFFERS * 2);
	LWLockAcquire(TestSLRULock, LW_EXCLUSIVE);
	slotno = SimpleLruReadPage(TestSlruCtl, test_pageno + NUM_TEST_BUFFERS * 2,
							   true /* write_ok */ , InvalidTransactionId);
	LWLockRelease(TestSLRULock);

	/*
	 * Same for SimpleLruReadPage_ReadOnly. Note that the control lock should
	 * NOT be held at entry, but will be held at exit.
	 */
	elog(NOTICE, "Reading page %d (in buffer) for read-only",
		 test_pageno + NUM_TEST_BUFFERS * 2);
	slotno = SimpleLruReadPage_ReadOnly(TestSlruCtl,
										test_pageno + NUM_TEST_BUFFERS * 2, InvalidTransactionId);
	LWLockRelease(TestSLRULock);

	elog(NOTICE, "Reading page %d (not in buffer) for read and write",
		 test_pageno);
	LWLockAcquire(TestSLRULock, LW_EXCLUSIVE);
	slotno = SimpleLruReadPage(TestSlruCtl, test_pageno,
							   true /* write_ok */ , InvalidTransactionId);
	LWLockRelease(TestSLRULock);

	/*
	 * Same for SimpleLruReadPage_ReadOnly. Note that the control lock should
	 * NOT be held at entry, but will be held at exit.
	 */
	elog(NOTICE, "Reading page %d (not in buffer) for read-only",
		 test_pageno + 1);
	slotno = SimpleLruReadPage_ReadOnly(TestSlruCtl,
										test_pageno + 1, InvalidTransactionId);
	LWLockRelease(TestSLRULock);

	elog(NOTICE, "Calling SimpleLruWriteAll()");
	SimpleLruWriteAll(TestSlruCtl, true);

	ftag.segno = (test_pageno + NUM_TEST_BUFFERS * 3) / SLRU_PAGES_PER_SEGMENT;
	elog(NOTICE, "Calling SlruSyncFileTag() for segment %d", ftag.segno);
	SlruSyncFileTag(TestSlruCtl, &ftag, path);
	elog(NOTICE, "Done, path = %s", path);
	EnsureStringEquals(path, "pg_test_slru/0183");

	/* Test SlruDeleteSegment() and SlruScanDirectory() */

	/* check precondition */
	CallbackCounter = 0;
	elog(NOTICE, "Calling SlruScanDirectory()");
	SlruScanDirectory(TestSlruCtl, test_slru_scan_cb, (void *) test_data);
	EnsureIntEquals(CallbackCounter, 3);

	/* delete the segment */
	elog(NOTICE, "Deleting segment %d", ftag.segno);
	SlruDeleteSegment(TestSlruCtl, ftag.segno);

	/* check postcondition */
	CallbackCounter = 0;
	elog(NOTICE, "Calling SlruScanDirectory()");
	SlruScanDirectory(TestSlruCtl, test_slru_scan_cb, (void *) test_data);
	found = SimpleLruDoesPhysicalPageExist(TestSlruCtl,
										   (test_pageno + NUM_TEST_BUFFERS * 3));
	elog(NOTICE, "SimpleLruDoesPhysicalPageExist() returned %d", found);
	EnsureIntEquals(CallbackCounter, 2);
	EnsureIntEquals(found, false);

	/* Test SimpleLruTruncate() */

	/* check precondition */
	found = SimpleLruDoesPhysicalPageExist(TestSlruCtl,
										   test_pageno + NUM_TEST_BUFFERS * 2);
	elog(NOTICE, "SimpleLruDoesPhysicalPageExist() returned %d for page %d",
		 found, test_pageno + NUM_TEST_BUFFERS * 2);
	EnsureIntEquals(found, true);

	found = SimpleLruDoesPhysicalPageExist(TestSlruCtl, test_pageno);
	elog(NOTICE, "SimpleLruDoesPhysicalPageExist() returned %d for page %d",
		 found, test_pageno);
	EnsureIntEquals(found, true);

	/* do the call */
	elog(NOTICE, "Truncating pages prior to %d",
		 test_pageno + NUM_TEST_BUFFERS * 2);
	SimpleLruTruncate(TestSlruCtl, test_pageno + NUM_TEST_BUFFERS * 2);

	/* check postcondition */
	found = SimpleLruDoesPhysicalPageExist(TestSlruCtl,
										   test_pageno + NUM_TEST_BUFFERS * 2);
	elog(NOTICE, "SimpleLruDoesPhysicalPageExist() returned %d for page %d",
		 found, test_pageno + NUM_TEST_BUFFERS * 2);
	EnsureIntEquals(found, true);

	found = SimpleLruDoesPhysicalPageExist(TestSlruCtl, test_pageno);
	elog(NOTICE, "SimpleLruDoesPhysicalPageExist() returned %d for page %d",
		 found, test_pageno);
	EnsureIntEquals(found, false);

	/* this is to cover SlruScanDirCbDeleteAll() with tests as well */
	CallbackCounter = 0;
	elog(NOTICE, "Cleaning up.");
	SlruScanDirectory(TestSlruCtl, SlruScanDirCbDeleteAll, NULL);
	EnsureIntEquals(CallbackCounter, 0);

	found = SimpleLruDoesPhysicalPageExist(TestSlruCtl,
										   test_pageno + NUM_TEST_BUFFERS * 2);
	elog(NOTICE, "SimpleLruDoesPhysicalPageExist() returned %d for page %d",
		 found, test_pageno + NUM_TEST_BUFFERS * 2);
	EnsureIntEquals(found, false);

	found = SimpleLruDoesPhysicalPageExist(TestSlruCtl, test_pageno);
	elog(NOTICE, "SimpleLruDoesPhysicalPageExist() returned %d for page %d",
		 found, test_pageno);
	EnsureIntEquals(found, false);

	PG_RETURN_VOID();
}

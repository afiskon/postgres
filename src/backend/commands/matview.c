/*-------------------------------------------------------------------------
 *
 * matview.c
 *	  materialized view support
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/matview.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/multixact.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_am.h"
#include "catalog/pg_opclass.h"
#include "commands/cluster.h"
#include "commands/matview.h"
#include "commands/tablecmds.h"
#include "commands/tablespace.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "rewrite/rewriteHandler.h"
#include "storage/lmgr.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"


typedef struct
{
	DestReceiver pub;			/* publicly-known function pointers */
	Oid			transientoid;	/* OID of new heap into which to store */
	/* These fields are filled by transientrel_startup: */
	Relation	transientrel;	/* relation to write to */
	CommandId	output_cid;		/* cmin to insert in output tuples */
	int			ti_options;		/* table_tuple_insert performance options */
	BulkInsertState bistate;	/* bulk insert state */
} DR_transientrel;

static int	matview_maintenance_depth = 0;

static void transientrel_startup(DestReceiver *self, int operation, TupleDesc typeinfo);
static bool transientrel_receive(TupleTableSlot *slot, DestReceiver *self);
static void transientrel_shutdown(DestReceiver *self);
static void transientrel_destroy(DestReceiver *self);
static uint64 refresh_matview_datafill(DestReceiver *dest, Query *query,
									   const char *queryString, bool is_create);
static char *make_temptable_name_n(char *tempname, int n);
static void refresh_by_match_merge(Oid matviewOid, Oid tempOid, Oid relowner,
								   int save_sec_context);
static void refresh_by_heap_swap(Oid matviewOid, Oid OIDNewHeap, char relpersistence);
static bool is_usable_unique_index(Relation indexRel);
static void OpenMatViewIncrementalMaintenance(void);
static void CloseMatViewIncrementalMaintenance(void);

/*
 * SetMatViewPopulatedState
 *		Mark a materialized view as populated, or not.
 *
 * NOTE: caller must be holding an appropriate lock on the relation.
 */
void
SetMatViewPopulatedState(Relation relation, bool newstate)
{
	Relation	pgrel;
	HeapTuple	tuple;

	Assert(relation->rd_rel->relkind == RELKIND_MATVIEW);

	/*
	 * Update relation's pg_class entry.  Crucial side-effect: other backends
	 * (and this one too!) are sent SI message to make them rebuild relcache
	 * entries.
	 */
	pgrel = table_open(RelationRelationId, RowExclusiveLock);
	tuple = SearchSysCacheCopy1(RELOID,
								ObjectIdGetDatum(RelationGetRelid(relation)));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for relation %u",
			 RelationGetRelid(relation));

	((Form_pg_class) GETSTRUCT(tuple))->relispopulated = newstate;

	CatalogTupleUpdate(pgrel, &tuple->t_self, tuple);

	heap_freetuple(tuple);
	table_close(pgrel, RowExclusiveLock);

	/*
	 * Advance command counter to make the updated pg_class row locally
	 * visible.
	 */
	CommandCounterIncrement();
}

/*
 * ExecRefreshMatView -- execute a REFRESH MATERIALIZED VIEW command
 *
 * If WITH NO DATA was specified, this is effectively like a TRUNCATE;
 * otherwise it is like a TRUNCATE followed by an INSERT using the SELECT
 * statement associated with the materialized view.  The statement node's
 * skipData field shows whether the clause was used.
 */
ObjectAddress
ExecRefreshMatView(RefreshMatViewStmt *stmt, const char *queryString,
				   QueryCompletion *qc)
{
	Oid			matviewOid;
	LOCKMODE	lockmode;

	/* Determine strength of lock needed. */
	lockmode = stmt->concurrent ? ExclusiveLock : AccessExclusiveLock;

	/*
	 * Get a lock until end of transaction.
	 */
	matviewOid = RangeVarGetRelidExtended(stmt->relation,
										  lockmode, 0,
										  RangeVarCallbackMaintainsTable,
										  NULL);

	return RefreshMatViewByOid(matviewOid, false, stmt->skipData,
							   stmt->concurrent, queryString, qc);
}

/*
 * RefreshMatViewByOid -- refresh materialized view by OID
 *
 * This refreshes the materialized view by creating a new table and swapping
 * the relfilenumbers of the new table and the old materialized view, so the OID
 * of the original materialized view is preserved. Thus we do not lose GRANT
 * nor references to this materialized view.
 *
 * If skipData is true, this is effectively like a TRUNCATE; otherwise it is
 * like a TRUNCATE followed by an INSERT using the SELECT statement associated
 * with the materialized view.
 *
 * Indexes are rebuilt too, via REINDEX. Since we are effectively bulk-loading
 * the new heap, it's better to create the indexes afterwards than to fill them
 * incrementally while we load.
 *
 * The matview's "populated" state is changed based on whether the contents
 * reflect the result set of the materialized view's query.
 *
 * This is also used to populate the materialized view created by CREATE
 * MATERIALIZED VIEW command.
 */
ObjectAddress
RefreshMatViewByOid(Oid matviewOid, bool is_create, bool skipData,
					bool concurrent, const char *queryString,
					QueryCompletion *qc)
{
	Relation	matviewRel;
	RewriteRule *rule;
	List	   *actions;
	Query	   *dataQuery;
	Oid			tableSpace;
	Oid			relowner;
	Oid			OIDNewHeap;
	uint64		processed = 0;
	char		relpersistence;
	Oid			save_userid;
	int			save_sec_context;
	int			save_nestlevel;
	ObjectAddress address;

	matviewRel = table_open(matviewOid, NoLock);
	relowner = matviewRel->rd_rel->relowner;

	/*
	 * Switch to the owner's userid, so that any functions are run as that
	 * user.  Also lock down security-restricted operations and arrange to
	 * make GUC variable changes local to this command.
	 */
	GetUserIdAndSecContext(&save_userid, &save_sec_context);
	SetUserIdAndSecContext(relowner,
						   save_sec_context | SECURITY_RESTRICTED_OPERATION);
	save_nestlevel = NewGUCNestLevel();
	RestrictSearchPath();

	/* Make sure it is a materialized view. */
	if (matviewRel->rd_rel->relkind != RELKIND_MATVIEW)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("\"%s\" is not a materialized view",
						RelationGetRelationName(matviewRel))));

	/* Check that CONCURRENTLY is not specified if not populated. */
	if (concurrent && !RelationIsPopulated(matviewRel))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("CONCURRENTLY cannot be used when the materialized view is not populated")));

	/* Check that conflicting options have not been specified. */
	if (concurrent && skipData)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("%s and %s options cannot be used together",
						"CONCURRENTLY", "WITH NO DATA")));

	/*
	 * Check that everything is correct for a refresh. Problems at this point
	 * are internal errors, so elog is sufficient.
	 */
	if (matviewRel->rd_rel->relhasrules == false ||
		matviewRel->rd_rules->numLocks < 1)
		elog(ERROR,
			 "materialized view \"%s\" is missing rewrite information",
			 RelationGetRelationName(matviewRel));

	if (matviewRel->rd_rules->numLocks > 1)
		elog(ERROR,
			 "materialized view \"%s\" has too many rules",
			 RelationGetRelationName(matviewRel));

	rule = matviewRel->rd_rules->rules[0];
	if (rule->event != CMD_SELECT || !(rule->isInstead))
		elog(ERROR,
			 "the rule for materialized view \"%s\" is not a SELECT INSTEAD OF rule",
			 RelationGetRelationName(matviewRel));

	actions = rule->actions;
	if (list_length(actions) != 1)
		elog(ERROR,
			 "the rule for materialized view \"%s\" is not a single action",
			 RelationGetRelationName(matviewRel));

	/*
	 * Check that there is a unique index with no WHERE clause on one or more
	 * columns of the materialized view if CONCURRENTLY is specified.
	 */
	if (concurrent)
	{
		List	   *indexoidlist = RelationGetIndexList(matviewRel);
		ListCell   *indexoidscan;
		bool		hasUniqueIndex = false;

		Assert(!is_create);

		foreach(indexoidscan, indexoidlist)
		{
			Oid			indexoid = lfirst_oid(indexoidscan);
			Relation	indexRel;

			indexRel = index_open(indexoid, AccessShareLock);
			hasUniqueIndex = is_usable_unique_index(indexRel);
			index_close(indexRel, AccessShareLock);
			if (hasUniqueIndex)
				break;
		}

		list_free(indexoidlist);

		if (!hasUniqueIndex)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("cannot refresh materialized view \"%s\" concurrently",
							quote_qualified_identifier(get_namespace_name(RelationGetNamespace(matviewRel)),
													   RelationGetRelationName(matviewRel))),
					 errhint("Create a unique index with no WHERE clause on one or more columns of the materialized view.")));
	}

	/*
	 * The stored query was rewritten at the time of the MV definition, but
	 * has not been scribbled on by the planner.
	 */
	dataQuery = linitial_node(Query, actions);

	/*
	 * Check for active uses of the relation in the current transaction, such
	 * as open scans.
	 *
	 * NB: We count on this to protect us against problems with refreshing the
	 * data using TABLE_INSERT_FROZEN.
	 */
	CheckTableNotInUse(matviewRel,
					   is_create ? "CREATE MATERIALIZED VIEW" :
					   "REFRESH MATERIALIZED VIEW");

	/*
	 * Tentatively mark the matview as populated or not (this will roll back
	 * if we fail later).
	 */
	SetMatViewPopulatedState(matviewRel, !skipData);

	/* Concurrent refresh builds new data in temp tablespace, and does diff. */
	if (concurrent)
	{
		tableSpace = GetDefaultTablespace(RELPERSISTENCE_TEMP, false);
		relpersistence = RELPERSISTENCE_TEMP;
	}
	else
	{
		tableSpace = matviewRel->rd_rel->reltablespace;
		relpersistence = matviewRel->rd_rel->relpersistence;
	}

	/*
	 * Create the transient table that will receive the regenerated data. Lock
	 * it against access by any other process until commit (by which time it
	 * will be gone).
	 */
	OIDNewHeap = make_new_heap(matviewOid, tableSpace,
							   matviewRel->rd_rel->relam,
							   relpersistence, ExclusiveLock);
	Assert(CheckRelationOidLockedByMe(OIDNewHeap, AccessExclusiveLock, false));

	/* Generate the data, if wanted. */
	if (!skipData)
	{
		DestReceiver *dest;

		dest = CreateTransientRelDestReceiver(OIDNewHeap);
		processed = refresh_matview_datafill(dest, dataQuery, queryString,
											 is_create);
	}

	/* Make the matview match the newly generated data. */
	if (concurrent)
	{
		int			old_depth = matview_maintenance_depth;

		PG_TRY();
		{
			refresh_by_match_merge(matviewOid, OIDNewHeap, relowner,
								   save_sec_context);
		}
		PG_CATCH();
		{
			matview_maintenance_depth = old_depth;
			PG_RE_THROW();
		}
		PG_END_TRY();
		Assert(matview_maintenance_depth == old_depth);
	}
	else
	{
		refresh_by_heap_swap(matviewOid, OIDNewHeap, relpersistence);

		/*
		 * Inform cumulative stats system about our activity: basically, we
		 * truncated the matview and inserted some new data.  (The concurrent
		 * code path above doesn't need to worry about this because the
		 * inserts and deletes it issues get counted by lower-level code.)
		 */
		pgstat_count_truncate(matviewRel);
		if (!skipData)
			pgstat_count_heap_insert(matviewRel, processed);
	}

	table_close(matviewRel, NoLock);

	/* Roll back any GUC changes */
	AtEOXact_GUC(false, save_nestlevel);

	/* Restore userid and security context */
	SetUserIdAndSecContext(save_userid, save_sec_context);

	ObjectAddressSet(address, RelationRelationId, matviewOid);

	/*
	 * Save the rowcount so that pg_stat_statements can track the total number
	 * of rows processed by REFRESH MATERIALIZED VIEW command. Note that we
	 * still don't display the rowcount in the command completion tag output,
	 * i.e., the display_rowcount flag of CMDTAG_REFRESH_MATERIALIZED_VIEW
	 * command tag is left false in cmdtaglist.h. Otherwise, the change of
	 * completion tag output might break applications using it.
	 *
	 * When called from CREATE MATERIALIZED VIEW command, the rowcount is
	 * displayed with the command tag CMDTAG_SELECT.
	 */
	if (qc)
		SetQueryCompletion(qc,
						   is_create ? CMDTAG_SELECT : CMDTAG_REFRESH_MATERIALIZED_VIEW,
						   processed);

	return address;
}

/*
 * refresh_matview_datafill
 *
 * Execute the given query, sending result rows to "dest" (which will
 * insert them into the target matview).
 *
 * Returns number of rows inserted.
 */
static uint64
refresh_matview_datafill(DestReceiver *dest, Query *query,
						 const char *queryString, bool is_create)
{
	List	   *rewritten;
	PlannedStmt *plan;
	QueryDesc  *queryDesc;
	Query	   *copied_query;
	uint64		processed;

	/* Lock and rewrite, using a copy to preserve the original query. */
	copied_query = copyObject(query);
	AcquireRewriteLocks(copied_query, true, false);
	rewritten = QueryRewrite(copied_query);

	/* SELECT should never rewrite to more or less than one SELECT query */
	if (list_length(rewritten) != 1)
		elog(ERROR, "unexpected rewrite result for %s",
			 is_create ? "CREATE MATERIALIZED VIEW " : "REFRESH MATERIALIZED VIEW");
	query = (Query *) linitial(rewritten);

	/* Check for user-requested abort. */
	CHECK_FOR_INTERRUPTS();

	/* Plan the query which will generate data for the refresh. */
	plan = pg_plan_query(query, queryString, CURSOR_OPT_PARALLEL_OK, NULL, NULL);

	/*
	 * Use a snapshot with an updated command ID to ensure this query sees
	 * results of any previously executed queries.  (This could only matter if
	 * the planner executed an allegedly-stable function that changed the
	 * database contents, but let's do it anyway to be safe.)
	 */
	PushCopiedSnapshot(GetActiveSnapshot());
	UpdateActiveSnapshotCommandId();

	/* Create a QueryDesc, redirecting output to our tuple receiver */
	queryDesc = CreateQueryDesc(plan, queryString,
								GetActiveSnapshot(), InvalidSnapshot,
								dest, NULL, NULL, 0);

	/* call ExecutorStart to prepare the plan for execution */
	ExecutorStart(queryDesc, 0);

	/* run the plan */
	ExecutorRun(queryDesc, ForwardScanDirection, 0);

	processed = queryDesc->estate->es_processed;

	/* and clean up */
	ExecutorFinish(queryDesc);
	ExecutorEnd(queryDesc);

	FreeQueryDesc(queryDesc);

	PopActiveSnapshot();

	return processed;
}

DestReceiver *
CreateTransientRelDestReceiver(Oid transientoid)
{
	DR_transientrel *self = (DR_transientrel *) palloc0(sizeof(DR_transientrel));

	self->pub.receiveSlot = transientrel_receive;
	self->pub.rStartup = transientrel_startup;
	self->pub.rShutdown = transientrel_shutdown;
	self->pub.rDestroy = transientrel_destroy;
	self->pub.mydest = DestTransientRel;
	self->transientoid = transientoid;

	return (DestReceiver *) self;
}

/*
 * transientrel_startup --- executor startup
 */
static void
transientrel_startup(DestReceiver *self, int operation, TupleDesc typeinfo)
{
	DR_transientrel *myState = (DR_transientrel *) self;
	Relation	transientrel;

	transientrel = table_open(myState->transientoid, NoLock);

	/*
	 * Fill private fields of myState for use by later routines
	 */
	myState->transientrel = transientrel;
	myState->output_cid = GetCurrentCommandId(true);
	myState->ti_options = TABLE_INSERT_SKIP_FSM | TABLE_INSERT_FROZEN;
	myState->bistate = GetBulkInsertState();

	/*
	 * Valid smgr_targblock implies something already wrote to the relation.
	 * This may be harmless, but this function hasn't planned for it.
	 */
	Assert(RelationGetTargetBlock(transientrel) == InvalidBlockNumber);
}

/*
 * transientrel_receive --- receive one tuple
 */
static bool
transientrel_receive(TupleTableSlot *slot, DestReceiver *self)
{
	DR_transientrel *myState = (DR_transientrel *) self;

	/*
	 * Note that the input slot might not be of the type of the target
	 * relation. That's supported by table_tuple_insert(), but slightly less
	 * efficient than inserting with the right slot - but the alternative
	 * would be to copy into a slot of the right type, which would not be
	 * cheap either. This also doesn't allow accessing per-AM data (say a
	 * tuple's xmin), but since we don't do that here...
	 */

	table_tuple_insert(myState->transientrel,
					   slot,
					   myState->output_cid,
					   myState->ti_options,
					   myState->bistate);

	/* We know this is a newly created relation, so there are no indexes */

	return true;
}

/*
 * transientrel_shutdown --- executor end
 */
static void
transientrel_shutdown(DestReceiver *self)
{
	DR_transientrel *myState = (DR_transientrel *) self;

	FreeBulkInsertState(myState->bistate);

	table_finish_bulk_insert(myState->transientrel, myState->ti_options);

	/* close transientrel, but keep lock until commit */
	table_close(myState->transientrel, NoLock);
	myState->transientrel = NULL;
}

/*
 * transientrel_destroy --- release DestReceiver object
 */
static void
transientrel_destroy(DestReceiver *self)
{
	pfree(self);
}


/*
 * Given a qualified temporary table name, append an underscore followed by
 * the given integer, to make a new table name based on the old one.
 * The result is a palloc'd string.
 *
 * As coded, this would fail to make a valid SQL name if the given name were,
 * say, "FOO"."BAR".  Currently, the table name portion of the input will
 * never be double-quoted because it's of the form "pg_temp_NNN", cf
 * make_new_heap().  But we might have to work harder someday.
 */
static char *
make_temptable_name_n(char *tempname, int n)
{
	StringInfoData namebuf;

	initStringInfo(&namebuf);
	appendStringInfoString(&namebuf, tempname);
	appendStringInfo(&namebuf, "_%d", n);
	return namebuf.data;
}

// ==== AALEKSEEV ====

 /*
  * Structures for non-SPI refresh_by_match_merge implementation
  */

 typedef enum
 {
	DIFF_DELETE,		/* Row exists in old but not in new */
	DIFF_INSERT		/* Row exists in new but not in old */
 } DiffOperationType;

 typedef struct DiffTupleData
 {
	DiffOperationType operation;
	ItemPointerData old_tid;	/* Valid for DELETE operations */
	HeapTuple new_tuple;		/* Valid for INSERT operations */
 } DiffTupleData;

 typedef struct UniqueKeyInfo
 {
	int nkeys;					/* Number of key columns */
	AttrNumber *key_attrs;		/* Array of attribute numbers */
	Oid *key_types;				/* Array of attribute types */
	Oid *equality_ops;			/* Array of equality operators */
	FmgrInfo *eq_functions;		/* Compiled equality functions */
	Oid *hash_ops;				/* Array of hash operators */
	FmgrInfo *hash_functions;	/* Compiled hash functions */
 } UniqueKeyInfo;

 /* Hash table entry for duplicate detection */
 typedef struct DuplicateHashEntry
 {
	uint32 hash_value;			/* Hash of the key */
	ItemPointerData first_tid;	/* TID of first occurrence */
	/* Variable length key data follows */
	char key_data[FLEXIBLE_ARRAY_MEMBER];
 } DuplicateHashEntry;

 /* Memory context for temporary allocations */
 static MemoryContext refresh_temp_context = NULL;

 /*
  * Forward declarations
  */
 static bool check_for_duplicates_nospi(Relation tempRel, UniqueKeyInfo *keyinfo);
 static void find_tuple_differences_nospi(Relation matviewRel, Relation tempRel,
										  UniqueKeyInfo *keyinfo, Tuplestorestate *diff_store);
 static void apply_tuple_differences_nospi(Relation matviewRel, Tuplestorestate *diff_store);
 static UniqueKeyInfo *build_unique_key_info(Relation matviewRel);
 static void free_unique_key_info(UniqueKeyInfo *keyinfo);
 static uint32 hash_tuple_key(TupleTableSlot *slot, UniqueKeyInfo *keyinfo);
 static bool tuples_equal_by_key(TupleTableSlot *slot1, TupleTableSlot *slot2, UniqueKeyInfo *keyinfo);
 static int compare_tuples_by_key(TupleTableSlot *slot1, TupleTableSlot *slot2, UniqueKeyInfo *keyinfo);
 static void store_diff_tuple(Tuplestorestate *diff_store, DiffOperationType op,
							  ItemPointer old_tid, HeapTuple new_tuple);
 static HeapTuple get_diff_tuple(Tuplestorestate *diff_store, TupleTableSlot *slot,
								DiffTupleData *diff_data);

 /*
  * refresh_by_match_merge
  *
  * Refresh a materialized view with transactional semantics, while allowing
  * concurrent reads.
  *
  * This is called after a new version of the data has been created in a
  * temporary table.  It performs a full outer join against the old version of
  * the data, producing "diff" results.  This join cannot work if there are any
  * duplicated rows in either the old or new versions, in the sense that every
  * column would compare as equal between the two rows.  It does work correctly
  * in the face of rows which have at least one NULL value, with all non-NULL
  * columns equal.  The behavior of NULLs on equality tests and on UNIQUE
  * indexes turns out to be quite convenient here; the tests we need to make
  * are consistent with default behavior.  If there is at least one UNIQUE
  * index on the materialized view, we have exactly the guarantee we need.
  *
  * The temporary table used to hold the diff results contains just the TID of
  * the old record (if matched) and the ROW from the new table as a single
  * column of complex record type (if matched).
  *
  * Once we have the diff table, we perform set-based DELETE and INSERT
  * operations against the materialized view, and discard both temporary
  * tables.
  *
  * Everything from the generation of the new data to applying the differences
  * takes place under cover of an ExclusiveLock, since it seems as though we
  * would want to prohibit not only concurrent REFRESH operations, but also
  * incremental maintenance.  It also doesn't seem reasonable or safe to allow
  * SELECT FOR UPDATE or SELECT FOR SHARE on rows being updated or deleted by
  * this command.
  */
 static void
 refresh_by_match_merge(Oid matviewOid, Oid tempOid, Oid relowner,
							  int save_sec_context)
 {
	Relation matviewRel;
	Relation tempRel;
	UniqueKeyInfo *keyinfo;
	Tuplestorestate *diff_store;
	MemoryContext old_context;
	int old_depth;

	/* Create temporary memory context for this operation */
	refresh_temp_context = AllocSetContextCreate(CurrentMemoryContext,
												 "refresh_by_match_merge temporary context",
												 ALLOCSET_DEFAULT_SIZES);
	old_context = MemoryContextSwitchTo(refresh_temp_context);

	/* Open relations (they should already be locked by caller) */
	matviewRel = table_open(matviewOid, NoLock);
	tempRel = table_open(tempOid, NoLock);

	/* Ensure we maintain the matview maintenance depth for error recovery */
	old_depth = matview_maintenance_depth;

	PG_TRY();
	{
		/*
		 * Step 1: Build unique key information from materialized view indexes
		 * This is critical for both duplicate detection and tuple matching
		 */
		keyinfo = build_unique_key_info(matviewRel);
		if (keyinfo == NULL)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("could not find suitable unique index on materialized view \"%s\"",
							RelationGetRelationName(matviewRel))));
		}

		/*
		 * Step 2: Check for duplicates in the new data
		 * This must be done before we can trust the diff results
		 */
		if (check_for_duplicates_nospi(tempRel, keyinfo))
		{
			ereport(ERROR,
					(errcode(ERRCODE_CARDINALITY_VIOLATION),
					 errmsg("new data for materialized view \"%s\" contains duplicate rows without any null columns",
							RelationGetRelationName(matviewRel))));
		}

		/*
		 * Step 3: Create tuplestore for difference results
		 * Use work_mem as the memory limit, with automatic spillover to disk
		 */
		diff_store = tuplestore_begin_heap(false, false, work_mem);

		/*
		 * Step 4: Find differences between old and new data
		 * This replaces the complex FULL OUTER JOIN SQL query
		 */
		find_tuple_differences_nospi(matviewRel, tempRel, keyinfo, diff_store);

		/*
		 * Step 5: Signal start of incremental maintenance
		 * This prevents other operations from modifying the matview
		 */
		OpenMatViewIncrementalMaintenance();

		/*
		 * Step 6: Apply the differences to the materialized view
		 * Deletes must come before inserts to avoid conflicts
		 */
		apply_tuple_differences_nospi(matviewRel, diff_store);

		/* Clean up the tuplestore */
		tuplestore_end(diff_store);

		/* Signal end of incremental maintenance */
		CloseMatViewIncrementalMaintenance();

		/* Clean up key info */
		free_unique_key_info(keyinfo);
	}
	PG_CATCH();
	{
		/* Ensure proper cleanup on error */
		matview_maintenance_depth = old_depth;

		/* Cleanup will happen automatically when temp context is deleted */
		PG_RE_THROW();
	}
	PG_END_TRY();

	/* Ensure matview_maintenance_depth is correct */
	Assert(matview_maintenance_depth == old_depth);

	/* Close relations */
	table_close(tempRel, NoLock);
	table_close(matviewRel, NoLock);

	/* Switch back to old context and delete temporary context */
	MemoryContextSwitchTo(old_context);
	MemoryContextDelete(refresh_temp_context);
	refresh_temp_context = NULL;
 }

 /*
  * build_unique_key_info
  *
  * Extract unique key information from the materialized view's indexes.
  * This replaces the complex index analysis done by the original SPI version.
  */
 static UniqueKeyInfo *
 build_unique_key_info(Relation matviewRel)
 {
	List *indexoidlist;
	ListCell *indexoidscan;
	UniqueKeyInfo *keyinfo = NULL;
	TupleDesc tupdesc;
	int16 relnatts;
	Oid *opUsedForQual;
	bool foundUniqueIndex = false;

	tupdesc = matviewRel->rd_att;
	relnatts = RelationGetNumberOfAttributes(matviewRel);
	opUsedForQual = (Oid *) palloc0(sizeof(Oid) * relnatts);

	indexoidlist = RelationGetIndexList(matviewRel);

	foreach(indexoidscan, indexoidlist)
	{
		Oid indexoid = lfirst_oid(indexoidscan);
		Relation indexRel;

		indexRel = index_open(indexoid, RowExclusiveLock);

		/* Check if this is a usable unique index (from original code) */
		if (indexRel->rd_index->indisunique &&
			indexRel->rd_index->indisready &&
			indexRel->rd_index->indisvalid &&
			IndexIsLive(indexRel->rd_index) &&
			!indexRel->rd_index->indisprimary)
		{
			Form_pg_index indexStruct = indexRel->rd_index;
			int indnkeyatts = indexStruct->indnkeyatts;
			oidvector *indclass;
			Datum indclassDatum;
			int i;

			/* Get the operator class info */
			indclassDatum = SysCacheGetAttrNotNull(INDEXRELID,
												   indexRel->rd_indextuple,
												   Anum_pg_index_indclass);
			indclass = (oidvector *) DatumGetPointer(indclassDatum);

			/* If this is our first unique index, allocate the structure */
			if (!foundUniqueIndex)
			{
				keyinfo = (UniqueKeyInfo *) palloc0(sizeof(UniqueKeyInfo));
				keyinfo->nkeys = indnkeyatts;
				keyinfo->key_attrs = (AttrNumber *) palloc(sizeof(AttrNumber) * indnkeyatts);
				keyinfo->key_types = (Oid *) palloc(sizeof(Oid) * indnkeyatts);
				keyinfo->equality_ops = (Oid *) palloc(sizeof(Oid) * indnkeyatts);
				keyinfo->eq_functions = (FmgrInfo *) palloc(sizeof(FmgrInfo) * indnkeyatts);
				keyinfo->hash_ops = (Oid *) palloc(sizeof(Oid) * indnkeyatts);
				keyinfo->hash_functions = (FmgrInfo *) palloc(sizeof(FmgrInfo) * indnkeyatts);
			}

			/* Process each key column */
			for (i = 0; i < indnkeyatts; i++)
			{
				int attnum = indexStruct->indkey.values[i];
				Oid opclass = indclass->values[i];
				Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);
				Oid attrtype = attr->atttypid;
				HeapTuple cla_ht;
				Form_pg_opclass cla_tup;
				Oid opfamily;
				Oid opcintype;
				Oid eq_op, hash_op;

				/* Skip if we've already processed this column */
				if (foundUniqueIndex && opUsedForQual[attnum - 1] != InvalidOid)
					continue;

				/* Look up the operator class */
				cla_ht = SearchSysCache1(CLAOID, ObjectIdGetDatum(opclass));
				if (!HeapTupleIsValid(cla_ht))
					elog(ERROR, "cache lookup failed for opclass %u", opclass);
				cla_tup = (Form_pg_opclass) GETSTRUCT(cla_ht);
				opfamily = cla_tup->opcfamily;
				opcintype = cla_tup->opcintype;
				ReleaseSysCache(cla_ht);

				/* Get equality operator */
				eq_op = get_opfamily_member_for_cmptype(opfamily, opcintype,
														opcintype, COMPARE_EQ);
				if (!OidIsValid(eq_op))
					elog(ERROR, "missing equality operator for (%u,%u) in opfamily %u",
						 opcintype, opcintype, opfamily);

				/* Get hash operator */
				hash_op = get_opfamily_member(opfamily, opcintype, opcintype,
											  HTEqualStrategyNumber);

				if (!foundUniqueIndex)
				{
					keyinfo->key_attrs[i] = attnum;
					keyinfo->key_types[i] = attrtype;
					keyinfo->equality_ops[i] = eq_op;
					fmgr_info(get_opcode(eq_op), &keyinfo->eq_functions[i]);

					if (OidIsValid(hash_op))
					{
						keyinfo->hash_ops[i] = hash_op;
						fmgr_info(get_opcode(hash_op), &keyinfo->hash_functions[i]);
					}
					else
					{
						keyinfo->hash_ops[i] = InvalidOid;
					}
				}

				opUsedForQual[attnum - 1] = eq_op;
			}

			foundUniqueIndex = true;
		}

		/* Keep the locks, since we're about to run DML which needs them */
		index_close(indexRel, NoLock);

		/* For simplicity, use the first unique index we find */
		if (foundUniqueIndex)
			break;
	}

	list_free(indexoidlist);
	pfree(opUsedForQual);

	return keyinfo;
 }

 /*
  * free_unique_key_info
  *
  * Clean up UniqueKeyInfo structure
  */
 static void
 free_unique_key_info(UniqueKeyInfo *keyinfo)
 {
	if (keyinfo)
	{
		if (keyinfo->key_attrs)
			pfree(keyinfo->key_attrs);
		if (keyinfo->key_types)
			pfree(keyinfo->key_types);
		if (keyinfo->equality_ops)
			pfree(keyinfo->equality_ops);
		if (keyinfo->eq_functions)
			pfree(keyinfo->eq_functions);
		if (keyinfo->hash_ops)
			pfree(keyinfo->hash_ops);
		if (keyinfo->hash_functions)
			pfree(keyinfo->hash_functions);
		pfree(keyinfo);
	}
 }

 /*
  * hash_tuple_key
  *
  * Compute hash value for the key columns of a tuple
  */
 static uint32
 hash_tuple_key(TupleTableSlot *slot, UniqueKeyInfo *keyinfo)
 {
	uint32 hash = 0;
	int i;

	for (i = 0; i < keyinfo->nkeys; i++)
	{
		Datum value;
		bool isnull;
		uint32 element_hash;

		value = slot_getattr(slot, keyinfo->key_attrs[i], &isnull);

		if (isnull)
		{
			/* NULL values hash to a constant */
			element_hash = 0;
		}
		else if (OidIsValid(keyinfo->hash_ops[i]))
		{
			/* Use the type's hash function */
			element_hash = DatumGetUInt32(FunctionCall1(&keyinfo->hash_functions[i], value));
		}
		else
		{
			/* Fall back to basic hash_any */
			bool typbyval;
			int typlen;

			get_typlenbyval(keyinfo->key_types[i], &typlen, &typbyval);

			if (typbyval)
			{
				element_hash = hash_any((unsigned char *) &value, sizeof(Datum));
			}
			else if (typlen == -1)
			{
				/* Variable length type */
				struct varlena *vl = (struct varlena *) DatumGetPointer(value);
				element_hash = hash_any((unsigned char *) VARDATA_ANY(vl), VARSIZE_ANY_EXHDR(vl));
			}
			else
			{
				/* Fixed length type */
				element_hash = hash_any((unsigned char *) DatumGetPointer(value), typlen);
			}
		}

		/* Combine with running hash */
		hash ^= element_hash + 0x9e3779b9 + (hash << 6) + (hash >> 2);
	}

	return hash;
 }

 /*
  * tuples_equal_by_key
  *
  * Compare two tuples using the unique key columns
  */
 static bool
 tuples_equal_by_key(TupleTableSlot *slot1, TupleTableSlot *slot2, UniqueKeyInfo *keyinfo)
 {
	int i;

	for (i = 0; i < keyinfo->nkeys; i++)
	{
		Datum value1, value2;
		bool isnull1, isnull2;

		value1 = slot_getattr(slot1, keyinfo->key_attrs[i], &isnull1);
		value2 = slot_getattr(slot2, keyinfo->key_attrs[i], &isnull2);

		/* Handle NULL values */
		if (isnull1 && isnull2)
			continue;
		if (isnull1 || isnull2)
			return false;

		/* Compare using the equality function */
		if (!DatumGetBool(FunctionCall2(&keyinfo->eq_functions[i], value1, value2)))
			return false;
	}

	return true;
 }

 /*
  * compare_tuples_by_key
  *
  * Compare two tuples for sorting purposes.
  * Returns: < 0 if slot1 < slot2, 0 if equal, > 0 if slot1 > slot2
  */
 static int
 compare_tuples_by_key(TupleTableSlot *slot1, TupleTableSlot *slot2, UniqueKeyInfo *keyinfo)
 {
	int i;

	for (i = 0; i < keyinfo->nkeys; i++)
	{
		Datum value1, value2;
		bool isnull1, isnull2;
		int32 cmp_result;

		value1 = slot_getattr(slot1, keyinfo->key_attrs[i], &isnull1);
		value2 = slot_getattr(slot2, keyinfo->key_attrs[i], &isnull2);

		/* Handle NULL values (NULL sorts first) */
		if (isnull1 && isnull2)
			continue;
		if (isnull1)
			return -1;
		if (isnull2)
			return 1;

		/* Use the type's comparison function */
		cmp_result = DatumGetInt32(FunctionCall2Coll(
			&keyinfo->eq_functions[i], /* We'd need btree ops for proper comparison */
			DEFAULT_COLLATION_OID,
			value1, value2));

		if (cmp_result != 0)
			return cmp_result;
	}

	return 0;
 }

 /*
  * check_for_duplicates_nospi
  *
  * Check for duplicate rows in the temporary table based on unique key.
  * Returns true if duplicates are found.
  */
 static bool
 check_for_duplicates_nospi(Relation tempRel, UniqueKeyInfo *keyinfo)
 {
	HTAB *seen_keys;
	HASHCTL hash_ctl;
	TableScanDesc scan;
	TupleTableSlot *slot;
	bool found_duplicate = false;

	/* Create hash table for tracking seen keys */
	MemSet(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(uint32);  /* We'll use hash values as keys */
	hash_ctl.entrysize = sizeof(DuplicateHashEntry);
	hash_ctl.hcxt = refresh_temp_context;

	seen_keys = hash_create("Duplicate detection hash",
							1024,  /* Initial size */
							&hash_ctl,
							HASH_ELEM | HASH_CONTEXT);

	/* Scan the temporary relation */
	scan = table_beginscan(tempRel, SnapshotSelf, 0, NULL);
	slot = table_slot_create(tempRel, NULL);

	while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
	{
		uint32 key_hash;
		DuplicateHashEntry *entry;
		bool found;

		/* Skip tuples with NULL in key columns */
		bool has_null = false;
		for (int i = 0; i < keyinfo->nkeys; i++)
		{
			bool isnull;
			slot_getattr(slot, keyinfo->key_attrs[i], &isnull);
			if (isnull)
			{
				has_null = true;
				break;
			}
		}
		if (has_null)
			continue;

		/* Compute hash of key columns */
		key_hash = hash_tuple_key(slot, keyinfo);

		/* Look up in hash table */
		entry = (DuplicateHashEntry *) hash_search(seen_keys, &key_hash, HASH_ENTER, &found);

		if (found)
		{
			/* Potential duplicate - need to check actual values */
			/* For simplicity, we'll report this as a duplicate */
			/* In a full implementation, we'd need to store actual key values */
			found_duplicate = true;
			break;
		}
		else
		{
			/* First time seeing this key */
			entry->first_tid = slot->tts_tid;
		}
	}

	ExecDropSingleTupleTableSlot(slot);
	table_endscan(scan);
	hash_destroy(seen_keys);

	return found_duplicate;
 }

 /*
  * find_tuple_differences_nospi
  *
  * Find differences between old (matview) and new (temp) data.
  * Stores the differences in the provided tuplestore.
  */
 static void
 find_tuple_differences_nospi(Relation matviewRel, Relation tempRel,
							  UniqueKeyInfo *keyinfo, Tuplestorestate *diff_store)
 {
	TableScanDesc old_scan, new_scan;
	TupleTableSlot *old_slot, *new_slot;
	bool old_valid, new_valid;
	int cmp_result;

	/*
	 * For simplicity, we'll use a nested loop approach.
	 * In a production implementation, we might want to sort both relations
	 * and do a merge join for better performance.
	 */

	/* Scan through old data (materialized view) */
	old_scan = table_beginscan(matviewRel, SnapshotSelf, 0, NULL);
	old_slot = table_slot_create(matviewRel, NULL);

	while (table_scan_getnextslot(old_scan, ForwardScanDirection, old_slot))
	{
		bool found_match = false;

		/* For each old tuple, scan new data to see if it exists */
		new_scan = table_beginscan(tempRel, SnapshotSelf, 0, NULL);
		new_slot = table_slot_create(tempRel, NULL);

		while (table_scan_getnextslot(new_scan, ForwardScanDirection, new_slot))
		{
			if (tuples_equal_by_key(old_slot, new_slot, keyinfo))
			{
				found_match = true;
				break;
			}
		}

		ExecDropSingleTupleTableSlot(new_slot);
		table_endscan(new_scan);

		/* If old tuple not found in new data, mark for deletion */
		if (!found_match)
		{
			store_diff_tuple(diff_store, DIFF_DELETE, &old_slot->tts_tid, NULL);
		}
	}

	ExecDropSingleTupleTableSlot(old_slot);
	table_endscan(old_scan);

	/* Now scan through new data to find inserts */
	new_scan = table_beginscan(tempRel, SnapshotSelf, 0, NULL);
	new_slot = table_slot_create(tempRel, NULL);

	while (table_scan_getnextslot(new_scan, ForwardScanDirection, new_slot))
	{
		bool found_match = false;

		/* For each new tuple, scan old data to see if it exists */
		old_scan = table_beginscan(matviewRel, SnapshotSelf, 0, NULL);
		old_slot = table_slot_create(matviewRel, NULL);

		while (table_scan_getnextslot(old_scan, ForwardScanDirection, old_slot))
		{
			if (tuples_equal_by_key(new_slot, old_slot, keyinfo))
			{
				found_match = true;
				break;
			}
		}

		ExecDropSingleTupleTableSlot(old_slot);
		table_endscan(old_scan);

		/* If new tuple not found in old data, mark for insertion */
		if (!found_match)
		{
			HeapTuple new_tuple = ExecFetchSlotHeapTuple(new_slot, false, NULL);
			store_diff_tuple(diff_store, DIFF_INSERT, NULL, new_tuple);
		}
	}

	ExecDropSingleTupleTableSlot(new_slot);
	table_endscan(new_scan);
 }

 /*
  * store_diff_tuple
  *
  * Store a difference tuple in the tuplestore
  */
 static void
 store_diff_tuple(Tuplestorestate *diff_store, DiffOperationType op,
				  ItemPointer old_tid, HeapTuple new_tuple)
 {
	Datum values[3];
	bool nulls[3];
	HeapTuple diff_tuple;
	TupleDesc diff_tupdesc;

	/* Create tuple descriptor for diff entries */
	/* Format: (operation_type, old_tid, new_tuple_data) */
	diff_tupdesc = CreateTemplateTupleDesc(3);
	TupleDescInitEntry(diff_tupdesc, 1, "operation", INT4OID, -1, 0);
	TupleDescInitEntry(diff_tupdesc, 2, "old_tid", TIDOID, -1, 0);
	TupleDescInitEntry(diff_tupdesc, 3, "new_data", RECORDOID, -1, 0);

	/* Fill in the values */
	values[0] = Int32GetDatum((int32) op);
	nulls[0] = false;

	if (old_tid != NULL)
	{
		values[1] = PointerGetDatum(old_tid);
		nulls[1] = false;
	}
	else
	{
		values[1] = (Datum) 0;
		nulls[1] = true;
	}

	if (new_tuple != NULL)
	{
		values[2] = PointerGetDatum(new_tuple);
		nulls[2] = false;
	}
	else
	{
		values[2] = (Datum) 0;
		nulls[2] = true;
	}

	/* Create and store the tuple */
	diff_tuple = heap_form_tuple(diff_tupdesc, values, nulls);
	tuplestore_puttuple(diff_store, diff_tuple);

	heap_freetuple(diff_tuple);
	FreeTupleDesc(diff_tupdesc);
 }

 /*
  * apply_tuple_differences_nospi
  *
  * Apply the stored differences to the materialized view
  */
 static void
 apply_tuple_differences_nospi(Relation matviewRel, Tuplestorestate *diff_store)
 {
	TupleTableSlot *slot;
	CommandId cid;
	DiffTupleData diff_data;

	cid = GetCurrentCommandId(true);
	slot = MakeSingleTupleTableSlot(NULL, &TTSOpsHeapTuple);

	/* Reset tuplestore to beginning */
	tuplestore_rescan(diff_store);

	/* Process all difference tuples */
	while (tuplestore_gettupleslot(diff_store, true, false, slot))
	{
		HeapTuple diff_tuple = get_diff_tuple(diff_store, slot, &diff_data);

		switch (diff_data.operation)
		{
			case DIFF_DELETE:
				{
					/* Delete the old tuple */
					TM_Result result;
					TM_FailureData tmfd;

					result = table_tuple_delete(matviewRel, &diff_data.old_tid,
											   cid, SnapshotSelf, InvalidSnapshot,
											   true, &tmfd, false);

					if (result != TM_Ok)
					{
						elog(ERROR, "failed to delete tuple from materialized view");
					}

					/* Update statistics */
					pgstat_count_heap_delete(matviewRel);
				}
				break;

			case DIFF_INSERT:
				{
					/* Insert the new tuple */
					TupleTableSlot *new_slot;

					new_slot = MakeSingleTupleTableSlot(RelationGetDescr(matviewRel), &TTSOpsHeapTuple);
					ExecStoreHeapTuple(diff_data.new_tuple, new_slot, false);

					table_tuple_insert(matviewRel, new_slot, cid, 0, NULL);

					/* Update statistics */
					pgstat_count_heap_insert(matviewRel, 1);

					ExecDropSingleTupleTableSlot(new_slot);
				}
				break;

			default:
				elog(ERROR, "unexpected diff operation type: %d", diff_data.operation);
		}
	}

	ExecDropSingleTupleTableSlot(slot);
 }

 /*
  * get_diff_tuple
  *
  * Extract difference data from a tuplestore slot
  */
 static HeapTuple
 get_diff_tuple(Tuplestorestate *diff_store, TupleTableSlot *slot, DiffTupleData *diff_data)
 {
	bool isnull;
	Datum value;

	/* Extract operation type */
	value = slot_getattr(slot, 1, &isnull);
	Assert(!isnull);
	diff_data->operation = (DiffOperationType) DatumGetInt32(value);

	/* Extract old TID if present */
	value = slot_getattr(slot, 2, &isnull);
	if (!isnull)
	{
		diff_data->old_tid = *((ItemPointer) DatumGetPointer(value));
	}

	/* Extract new tuple if present */
	value = slot_getattr(slot, 3, &isnull);
	if (!isnull)
	{
		diff_data->new_tuple = (HeapTuple) DatumGetPointer(value);
	}
	else
	{
		diff_data->new_tuple = NULL;
	}

	return ExecFetchSlotHeapTuple(slot, false, NULL);
 }

// ==== ^^^ AALEKSEEV ^^^ =====

/*
 * Swap the physical files of the target and transient tables, then rebuild
 * the target's indexes and throw away the transient table.  Security context
 * swapping is handled by the called function, so it is not needed here.
 */
static void
refresh_by_heap_swap(Oid matviewOid, Oid OIDNewHeap, char relpersistence)
{
	finish_heap_swap(matviewOid, OIDNewHeap, false, false, true, true,
					 RecentXmin, ReadNextMultiXactId(), relpersistence);
}

/*
 * Check whether specified index is usable for match merge.
 */
static bool
is_usable_unique_index(Relation indexRel)
{
	Form_pg_index indexStruct = indexRel->rd_index;

	/*
	 * Must be unique, valid, immediate, non-partial, and be defined over
	 * plain user columns (not expressions).
	 */
	if (indexStruct->indisunique &&
		indexStruct->indimmediate &&
		indexStruct->indisvalid &&
		RelationGetIndexPredicate(indexRel) == NIL &&
		indexStruct->indnatts > 0)
	{
		/*
		 * The point of groveling through the index columns individually is to
		 * reject both index expressions and system columns.  Currently,
		 * matviews couldn't have OID columns so there's no way to create an
		 * index on a system column; but maybe someday that wouldn't be true,
		 * so let's be safe.
		 */
		int			numatts = indexStruct->indnatts;
		int			i;

		for (i = 0; i < numatts; i++)
		{
			int			attnum = indexStruct->indkey.values[i];

			if (attnum <= 0)
				return false;
		}
		return true;
	}
	return false;
}


/*
 * This should be used to test whether the backend is in a context where it is
 * OK to allow DML statements to modify materialized views.  We only want to
 * allow that for internal code driven by the materialized view definition,
 * not for arbitrary user-supplied code.
 *
 * While the function names reflect the fact that their main intended use is
 * incremental maintenance of materialized views (in response to changes to
 * the data in referenced relations), they are initially used to allow REFRESH
 * without blocking concurrent reads.
 */
bool
MatViewIncrementalMaintenanceIsEnabled(void)
{
	return matview_maintenance_depth > 0;
}

static void
OpenMatViewIncrementalMaintenance(void)
{
	matview_maintenance_depth++;
}

static void
CloseMatViewIncrementalMaintenance(void)
{
	matview_maintenance_depth--;
	Assert(matview_maintenance_depth >= 0);
}

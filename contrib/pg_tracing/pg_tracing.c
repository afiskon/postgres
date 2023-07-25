/*-------------------------------------------------------------------------
 *
 * pg_tracing.c
 *		Generate spans for distributed tracing from SQL query
 *
 * We rely on the caller to know whether the query needs to be traced or not.
 * The information is propagated through https://google.github.io/sqlcommenter/.
 *
 * A query with sqlcommenter will look like: /\*dddbs='postgres.db',traceparent='00-00000000000000000000000000000009-0000000000000005-01'*\/ select 1;
 * The traceparent fields are detailed in https://www.w3.org/TR/trace-context/#traceparent-header-field-values
 * 00000000000000000000000000000009: trace id
 * 0000000000000005: parent id
 * 01: trace flags (01 == sampled)
 *
 * If sampled is set in the trace flags, we will generate spans for the ongoing query.
 * A span represents an operation with a start and a duration.
 * We will track the following operations:
 * - Query Span: The top span for a query. They are created after extracting the traceid from traceparent or to represent a nested query.
 * - Planner: We track the time spent in the planner and report the planner counters
 * - Node Span: Created from planstate. The name is extracted from the node type (IndexScan, SeqScan),
 * - Executor: We trace the different steps of the Executor: Start, Run, Finish and End
 *
 * A typical traced query will generate the following spans:
 * +------------------------------------------------------------------------------------------------------------------------------------------------------+
 * | Name: Select                                                                                                                                         |
 * | Operation: Select * pgbench_accounts WHERE aid=$1;                                                                                                   |
 * +---+------------------------+-+------------------+--+------------------------------------------------------+-+--------------------+-+----------------++
 *     | Name: Planner          | | Name: Executor   |  |Name: Executor                                        | | Name: Executor     | | Name: Executor |
 *     | Operation: Planner     | | Operation: Start |  |Operation: Run                                        | | Operation: Finish  | | Operation: End |
 *     +------------------------+ +------------------+  +--+--------------------------------------------------++ +--------------------+ +----------------+
 *                                                         | Name: IndexScan                                  |
 *                                                         | Operation: IndexScan using pgbench_accounts_pkey |
 *                                                         |            on pgbench_accounts                   |
 *                                                         +--------------------------------------------------+
 *
 * IDENTIFICATION
 *	  contrib/pg_tracing/pg_tracing.c
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "explain.h"
#include "pg_tracing.h"
#include "query_process.h"
#include "span.h"

#include "access/xact.h"
#include "common/pg_prng.h"
#include "funcapi.h"
#include "nodes/extensible.h"
#include "optimizer/planner.h"
#include "parser/analyze.h"
#include "storage/ipc.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/xid8.h"

PG_MODULE_MAGIC;

typedef enum
{
	PG_TRACING_TRACK_NONE,		/* track no statements */
	PG_TRACING_TRACK_TOP,		/* only top level statements */
	PG_TRACING_TRACK_ALL		/* all statements, including nested ones */
}			PGTracingTrackLevel;

/* GUC variables */
static int	pg_tracing_max_span;
static int	pg_tracing_max_parameter_str;
static double pg_tracing_sample_rate = 1;
static int	pg_tracing_track = PG_TRACING_TRACK_ALL;

static const struct config_enum_entry track_options[] =
{
	{"none", PG_TRACING_TRACK_NONE, false},
	{"top", PG_TRACING_TRACK_TOP, false},
	{"all", PG_TRACING_TRACK_ALL, false},
	{NULL, 0, false}
};

#define pg_tracing_enabled(level) \
	(!IsParallelWorker() && \
	(pg_tracing_track == PG_TRACING_TRACK_ALL || \
	(pg_tracing_track == PG_TRACING_TRACK_TOP && (level) == 0)))


PG_FUNCTION_INFO_V1(pg_tracing_info);
PG_FUNCTION_INFO_V1(pg_tracing_spans);
PG_FUNCTION_INFO_V1(pg_tracing_reset);

/*
 * Current trace ids extracted from sqlcomment's traceparent
 */
struct pgTracingTraceparentParameter traceparent_parameter;

/*
 * Number of allocated query_spans and executor_ids If we have nested
 * queries, we will need to extend those fields
 */
static int	allocated_nested_level = 0;

/*
 * Currently tracked query spans by nested level
 */
static Span * query_spans;

/*
 * Span ids of executor run spans by nested level Executor run is used as
 * parent for spans generated from planstate
 */
static uint64 *executor_ids;

/*
 * Shared state with stats and file external state
 */
static pgTracingSharedState * pg_tracing = NULL;

/*
 * Shared state storing spans Query with sampled flag will add new spans to
 * the shared state Those spans will be consumed during calls to
 * pg_tracing_spans
 */
static pgTracingSharedSpans * shared_spans = NULL;

/*
 * Current nesting depth of ExecutorRun+ProcessUtility calls
 */
static int	exec_nested_level = 0;

/*
 * Maximum nested level for a query to know how many query spans we need to
 * copy in shared_spans
 */
static int	max_nested_level = 0;

/*
 * Current nesting depth of planner calls
 */
static int	plan_nested_level = 0;

/*
 * Start time of the begining of the trace
 */
static TimestampTz start_query_ts;
static instr_time start_query;

static void pg_tracing_shmem_request(void);
static void pg_tracing_shmem_startup(void);

/* Saved hook values in case of unload */
static shmem_request_hook_type prev_shmem_request_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static post_parse_analyze_hook_type prev_post_parse_analyze_hook = NULL;
static planner_hook_type prev_planner_hook = NULL;
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorRun_hook_type prev_ExecutorRun = NULL;
static ExecutorFinish_hook_type prev_ExecutorFinish = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;
static ProcessUtility_hook_type prev_ProcessUtility = NULL;

static void pg_tracing_post_parse_analyze(ParseState *pstate, Query *query, JumbleState *jstate);
static PlannedStmt *pg_tracing_planner_hook(Query *parse,
											const char *query_string,
											int cursorOptions,
											ParamListInfo boundParams);
static void pg_tracing_ExecutorStart(QueryDesc *queryDesc, int eflags);
static void pg_tracing_ExecutorRun(QueryDesc *queryDesc,
								   ScanDirection direction,
								   uint64 count, bool execute_once);
static void pg_tracing_ExecutorFinish(QueryDesc *queryDesc);
static void pg_tracing_ExecutorEnd(QueryDesc *queryDesc);
static void pg_tracing_ProcessUtility(PlannedStmt *pstmt, const char *queryString,
									  bool readOnlyTree,
									  ProcessUtilityContext context, ParamListInfo params,
									  QueryEnvironment *queryEnv,
									  DestReceiver *dest, QueryCompletion *qc);

static void generate_member_nodes(PlanState **planstates, int nplans,
								  uint64 trace_id, uint64 parent_id, int sql_error_code);
static void generate_span_from_planstate(PlanState *planstate,
										 uint64 trace_id, uint64 parent_id, int sql_error_code);

static MemoryContext pg_tracing_mem_ctx;

#define PG_TRACING_INFO_COLS	5
#define PG_TRACING_TRACES_COLS	39


/*
 * Module load callback
 */
void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		return;

	DefineCustomIntVariable("pg_tracing.max_span",
							"Maximum number of spans stored in shared memory.",
							NULL,
							&pg_tracing_max_span,
							1000,
							0,
							20000,
							PGC_SUSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("pg_tracing.max_parameter_size",
							"Maximum size of parameters. -1 to disable parameter in query span.",
							NULL,
							&pg_tracing_max_parameter_str,
							1024,
							0,
							10000,
							PGC_SUSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomEnumVariable("pg_tracing.track",
							 "Selects which statements are tracked by pg_tracing.",
							 NULL,
							 &pg_tracing_track,
							 PG_TRACING_TRACK_ALL,
							 track_options,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomRealVariable("pg_tracing.sample_rate",
							 "Fraction of queries to process.",
							 NULL,
							 &pg_tracing_sample_rate,
							 1.0,
							 0.0,
							 1.0,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	MarkGUCPrefixReserved("pg_tracing");

	/* For jumble state */
	EnableQueryId();

	/* Install hooks. */
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = pg_tracing_shmem_request;

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = pg_tracing_shmem_startup;

	prev_post_parse_analyze_hook = post_parse_analyze_hook;
	post_parse_analyze_hook = pg_tracing_post_parse_analyze;

	prev_planner_hook = planner_hook;
	planner_hook = pg_tracing_planner_hook;

	prev_ExecutorStart = ExecutorStart_hook;
	ExecutorStart_hook = pg_tracing_ExecutorStart;
	prev_ExecutorRun = ExecutorRun_hook;
	ExecutorRun_hook = pg_tracing_ExecutorRun;
	prev_ExecutorFinish = ExecutorFinish_hook;
	ExecutorFinish_hook = pg_tracing_ExecutorFinish;
	prev_ExecutorEnd = ExecutorEnd_hook;
	ExecutorEnd_hook = pg_tracing_ExecutorEnd;

	prev_ProcessUtility = ProcessUtility_hook;
	ProcessUtility_hook = pg_tracing_ProcessUtility;
}

/*
 * shmem_request hook: request additional shared resources.  We'll allocate
 * or attach to the shared resources in pgss_shmem_startup().
 */
static void
pg_tracing_shmem_request(void)
{
	Size		memsize;

	if (prev_shmem_request_hook)
		prev_shmem_request_hook();
	memsize = MAXALIGN(sizeof(pgTracingSharedState));
	RequestAddinShmemSpace(memsize);
}

/*
 * shmem_startup hook: allocate or attach to shared memory, Also create and
 * load the query-texts file, which is expected to exist (even if empty)
 * while the module is enabled.
 */
static void
pg_tracing_shmem_startup(void)
{
	bool		found;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	/* reset in case this is a restart within the postmaster */
	pg_tracing = NULL;

	/*
	 * Create or attach to the shared memory state
	 */
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	pg_tracing = ShmemInitStruct("pg_tracing", sizeof(pgTracingSharedState), &found);
	shared_spans = ShmemInitStruct("pg_tracing_spans",
								   sizeof(pgTracingSharedSpans) + pg_tracing_max_span * sizeof(Span),
								   &found);

	/*
	 * Initialise pg_tracing memory context
	 */
	pg_tracing_mem_ctx = AllocSetContextCreate(TopMemoryContext,
											   "pg_tracing memory context",
											   ALLOCSET_DEFAULT_SIZES);

	/*
	 * First time, let's init shared state
	 */
	if (!found)
	{
		shared_spans->end = 0;
		pg_tracing->stats.traces = 0;
		pg_tracing->stats.stats_reset = GetCurrentTimestamp();
		SpinLockInit(&pg_tracing->mutex);
		SpinLockInit(&pg_tracing->file_mutex);
	}

	LWLockRelease(AddinShmemInitLock);

	/*
	 * Done if some other process already completed our initialization.
	 */
	if (found)
		return;
}

static void
add_span_to_shared_buffer(Span * span)
{
	volatile	pgTracingSharedState *s = (volatile pgTracingSharedState *) pg_tracing;

	SpinLockAcquire(&s->mutex);
	if (shared_spans->end + 1 == pg_tracing_max_span)
	{
		s->stats.dropped_spans++;

		/*
		 * Shared memory is full no need to keep sampling running
		 */
		traceparent_parameter.sampled = false;
	}
	else
	{
		s->stats.spans++;
		shared_spans->spans[shared_spans->end++] = *span;
	}
	SpinLockRelease(&s->mutex);
}

/*
 * Add span to the shared memory This may fail if shared buffer is full
 */
static void
add_span(Span * span, const instr_time *end_time)
{
	set_span_duration_and_counters(span, end_time);
	add_span_to_shared_buffer(span);
}

/*
 * Check if we still have available space in the shared spans.
 *
 * Between the moment we check and the moment we want to insert, the buffer
 * may be full but we redo a check before appending the span. This is done
 * early when starting a query span to bail out early if the buffer is
 * already full since we don't immediately add the span in the shared buffer.
 */
static bool
check_full_shared_spans()
{
	if (shared_spans->end + 1 == pg_tracing_max_span)
	{
		volatile	pgTracingSharedState *s = (volatile pgTracingSharedState *) pg_tracing;

		SpinLockAcquire(&s->mutex);
		s->stats.dropped_spans++;
		SpinLockRelease(&s->mutex);
		return true;
	}
	return false;
}

/*
 * Process a query descriptor: Gather all query instrumentation in the query
 * span counters and generate span nodes from queryDesc planstate
 */
static void
process_query_desc(QueryDesc *queryDesc, int sql_error_code)
{
	MemoryContext oldcxt;
	uint64		parent_id = executor_ids[exec_nested_level];
	NodeCounters *node_counters = &query_spans[exec_nested_level].node_counters;

	oldcxt = MemoryContextSwitchTo(queryDesc->estate->es_query_cxt);

	/* Process total counters */
	if (queryDesc->totaltime)
	{
		InstrEndLoop(queryDesc->totaltime);
		node_counters->buffer_usage = queryDesc->totaltime->bufusage;
		node_counters->wal_usage = queryDesc->totaltime->walusage;
	}
	if (queryDesc->planstate)
	{
		generate_span_from_planstate(queryDesc->planstate, traceparent_parameter.trace_id,
									 parent_id, sql_error_code);
	}
	node_counters->rows = queryDesc->estate->es_total_processed;
	if (queryDesc->estate->es_jit)
	{
		node_counters->jit_usage = queryDesc->estate->es_jit->instr;
	}
	MemoryContextSwitchTo(oldcxt);
}

/*
 * When we catch an error (timeout, cancel query), we need to flag the ongoing
 * span with an error, send current spans in the shared buffer and clean
 * our memory context
 */
static void
handle_pg_error(Span * span, QueryDesc *queryDesc)
{
	instr_time	end;
	int			sql_error_code;

	/*
	 * If we're not sampling the query, bail out
	 */
	if (traceparent_parameter.sampled == 0 && !pg_tracing_enabled(exec_nested_level))
	{
		return;
	}
	sql_error_code = geterrcode();
	span->sql_error_code = sql_error_code;
	query_spans[exec_nested_level].sql_error_code = sql_error_code;

	/*
	 * Order matters there. We want to process query desc before getting the
	 * end time otherwise, the span nodes will have a higher duration than
	 * their parents
	 */
	if (queryDesc != NULL)
	{
		process_query_desc(queryDesc, sql_error_code);
	}
	INSTR_TIME_SET_CURRENT(end);

	add_span(span, &end);

	/*
	 * We 're at the end, add all spans to the shared memory
	 */
	if (exec_nested_level == 0)
	{
		for (int i = max_nested_level; i >= 0; i--)
		{
			add_span(&query_spans[i], &end);
		}
	}

	/*
	 * We can reset the memory context here
	 */
	MemoryContextReset(pg_tracing_mem_ctx);
}

/*
 * If we're at the end of the query, dump all of query spans in the shared
 * memory.
 */
static void
end_tracing(instr_time end)
{
	/*
	 * We 're at the end, add all spans to the shared memory
	 */
	if (exec_nested_level == 0)
	{
		for (int i = max_nested_level; i >= 0; i--)
		{
			add_span(&query_spans[i], &end);
		}

		/*
		 * We can reset the memory context here
		 */
		MemoryContextReset(pg_tracing_mem_ctx);
	}
}

/*
 * Get the parent id for the given nested level
 */
static uint64
get_parent_id(int nested_level)
{
	if (nested_level < 0)
	{
		return traceparent_parameter.parent_id;
	}
	Assert(nested_level <= allocated_nested_level);
	return query_spans[nested_level].span_id;
}

/*
 * Start a new query span if we enter a new nested level The start of a query
 * span can vary: prepared statement will skip parsing, the use of cached
 * plans will skip the planner hook.
 * Thus, a query span can start in either post parse, planner hook or executor run.
 *
 * Since this is called after the we've detected the start of a trace, we check for available
 * space in the buffer. If the buffer is full, we abort tracing by setting sampled to false.
 * Callers need to check that tracing was aborted.
 */
static void
start_query_span(CmdType commandType, const Query *query, const JumbleState *jstate,
				 const char *query_text)
{
	Span	   *current_top;
	int			query_len;
	const char *normalised_query;

	/*
	 * Check if we've already created a query span for this nested level
	 */
	if (exec_nested_level <= max_nested_level)
	{
		return;
	}

	if (check_full_shared_spans())
	{
		/* Buffer is full, abort sampling */
		traceparent_parameter.sampled = false;
		return;
	}

	/* First time */
	if (max_nested_level == -1)
	{
		MemoryContext oldcxt;

		Assert(pg_tracing_mem_ctx->isReset);

		/*
		 * We need to be able to pass 2 informations that depend on the nested
		 * level: - executor span ids: Since an executor run becomes the
		 * parent span, we need subsequent created node spans to have the
		 * correct parent - query spans: We create one query span per nested
		 * level and those are only inserted in the shared buffer at the end
		 */
		oldcxt = MemoryContextSwitchTo(pg_tracing_mem_ctx);
		/* initial allocation */
		allocated_nested_level = 1;
		executor_ids = palloc0(allocated_nested_level * sizeof(uint64));
		query_spans = palloc0(allocated_nested_level * sizeof(Span));
		MemoryContextSwitchTo(oldcxt);

		/* Start referential timers */
		INSTR_TIME_SET_CURRENT(start_query);
		start_query_ts = GetCurrentTimestamp();
	}
	else if (exec_nested_level >= allocated_nested_level)
	{
		MemoryContext oldcxt;

		oldcxt = MemoryContextSwitchTo(pg_tracing_mem_ctx);
		allocated_nested_level += 2;
		executor_ids = repalloc(executor_ids, allocated_nested_level * sizeof(uint64));
		query_spans = repalloc(query_spans, allocated_nested_level * sizeof(Span));
		MemoryContextSwitchTo(oldcxt);
	}

	max_nested_level = exec_nested_level;
	if (exec_nested_level == 0)
	{
		/* Start of a new trace */
		volatile	pgTracingSharedState *s = (volatile pgTracingSharedState *) pg_tracing;

		SpinLockAcquire(&s->mutex);
		s->stats.traces++;
		SpinLockRelease(&s->mutex);
	}

	current_top = query_spans + exec_nested_level;
	begin_span(current_top, command_type_to_span_type(commandType),
			   traceparent_parameter.trace_id, get_parent_id(exec_nested_level - 1),
			   start_query_ts, start_query, NULL);
	if (jstate && jstate->clocations_count > 0 && query != NULL)
	{
		char	   *paramStr;

		query_len = query->stmt_len;
		Assert(query_len > 0);
		normalised_query = normalise_query_parameters(jstate, query_text,
													  query->stmt_location,
													  &query_len, &paramStr);
		text_store(pg_tracing, paramStr, strlen(paramStr), &current_top->parameter_offset);
	}
	else
	{
		/*
		 * No jstate available
		 */
		query_len = strlen(query_text);
		normalised_query = normalise_query(query_text, &query_len);
	}
	text_store(pg_tracing, normalised_query, query_len,
			   &current_top->operation_name_offset);
}

/*
 * Post-parse-analysis hook: Extract traceparent parameters from the SQLCommenter
 * at the start of the query. If we detect a sampled query then start the span.
 */
static void
pg_tracing_post_parse_analyze(ParseState *pstate, Query *query, JumbleState *jstate)
{
	Span		parse_span;

	if (prev_post_parse_analyze_hook)
		prev_post_parse_analyze_hook(pstate, query, jstate);

	/* Safety check... */
	if (!pg_tracing || !pg_tracing_enabled(exec_nested_level))
		return;

	if (exec_nested_level == 0)
	{
		max_nested_level = -1;
		traceparent_parameter = extract_traceparent(pstate->p_sourcetext, false);
		if (traceparent_parameter.sampled == 1 && pg_tracing_sample_rate < 1.0)
		{
			/*
			 * We've parsed a query with sampled, let's apply our sample rate
			 */
			traceparent_parameter.sampled = (pg_prng_double(&pg_global_prng_state) < pg_tracing_sample_rate);
		}
	}

	if (traceparent_parameter.sampled == 0)
	{
		/*
		 * Either there was no SQLCommenter or the query had sampled=00
		 */
		return;
	}

	/*
	 * Either we're inside a nested sampled query or we've parsed a query with
	 * the sampled flag, start a new query span
	 */
	start_query_span(query->commandType, query, jstate,
					 pstate->p_sourcetext);

	/*
	 * We can deduct the time spent parsing by relying on stmtStartTimestamp
	 */
	initialize_span_fields(&parse_span, SPAN_PARSE, traceparent_parameter.trace_id,
						   traceparent_parameter.parent_id);
	parse_span.start = GetCurrentStatementStartTimestamp();
	parse_span.duration.ticks = (start_query_ts - parse_span.start) * NS_PER_US;
	add_span_to_shared_buffer(&parse_span);
}

/*
 * If the query was started as an prepared statement, we won't be able to
 * extract traceparent during query parsing since parsing was skipped
 * We assume that SQLCommenter content can be passed as a text in the
 * first parameter
 */
static void
check_traceparameter_in_parameter(ParamListInfo boundParams)
{
	if (traceparent_parameter.sampled == 0 && pg_tracing_enabled(plan_nested_level + exec_nested_level))
	{
		if (boundParams && boundParams->numParams > 0)
		{
			Oid			typoutput;
			bool		typisvarlena;
			char	   *pstring;
			ParamExternData param;

			param = boundParams->params[0];
			if (param.ptype == TEXTOID)
			{
				getTypeOutputInfo(param.ptype, &typoutput, &typisvarlena);
				pstring = OidOutputFunctionCall(typoutput, param.value);
				traceparent_parameter = extract_traceparent(pstring, true);
			}
		}
	}
}

/*
 * Planner hook: forward to regular planner, but measure planning time if
 * needed.
 */
static PlannedStmt *
pg_tracing_planner_hook(Query *parse,
						const char *query_string,
						int cursorOptions,
						ParamListInfo boundParams)
{
	PlannedStmt *result;
	Span		span;
	Span	   *current_top;

	check_traceparameter_in_parameter(boundParams);
	if (traceparent_parameter.sampled > 0 && pg_tracing_enabled(plan_nested_level + exec_nested_level))
	{
		/*
		 * We may have skipped parsing if statement was prepared, start a new
		 * query span if we don't have one
		 */
		start_query_span(parse->commandType, parse, NULL, query_string);

		/*
		 * Recheck for sampled as starting query span may have failed due to
		 * full buffer
		 */
		if (traceparent_parameter.sampled == false)
		{
			goto fallback;
		}
		current_top = query_spans + exec_nested_level;

		begin_span(&span, SPAN_PLANNER, traceparent_parameter.trace_id,
				   get_parent_id(exec_nested_level),
				   start_query_ts, start_query, NULL);

		plan_nested_level++;
		PG_TRY();
		{
			if (prev_planner_hook)
				result = prev_planner_hook(parse, query_string, cursorOptions,
										   boundParams);
			else
				result = standard_planner(parse, query_string, cursorOptions,
										  boundParams);
		}
		PG_CATCH();
		{
			plan_nested_level--;
			handle_pg_error(&span, NULL);
			PG_RE_THROW();
		}
		PG_END_TRY();
		plan_nested_level--;

		add_span(&span, NULL);

		/*
		 * If we have a prepared statement, add bound parameters to the query
		 * span
		 */
		if (boundParams != NULL)
		{
			char	   *paramStr = BuildParamLogString(boundParams,
													   NULL, pg_tracing_max_parameter_str);

			Assert(current_top->parameter_offset == -1);
			if (paramStr != NULL)
			{
				text_store(pg_tracing, paramStr, strlen(paramStr), &current_top->parameter_offset);
			}
		}
	}
	else
	{
fallback:
		if (prev_planner_hook)
			result = prev_planner_hook(parse, query_string, cursorOptions,
									   boundParams);
		else
			result = standard_planner(parse, query_string, cursorOptions,
									  boundParams);
	}

	return result;
}


/*
 * ExecutorStart hook: start up logging if needed
 */
static void
pg_tracing_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
	Span		span;

	check_traceparameter_in_parameter(queryDesc->params);
	if (traceparent_parameter.sampled > 0 && pg_tracing_enabled(exec_nested_level))
	{
		/*
		 * In case of a cached plan, we haven't gone through neither parsing
		 * nor planner hook.
		 */
		start_query_span(queryDesc->operation, NULL, NULL, queryDesc->sourceText);

		begin_span(&span, SPAN_EXECUTOR_START, traceparent_parameter.trace_id, get_parent_id(exec_nested_level),
				   start_query_ts, start_query, NULL);

		/*
		 * Activate query instrumentation to get timing, rows, buffers and WAL
		 * usage
		 */
		queryDesc->instrument_options = INSTRUMENT_ALL;
	}

	if (prev_ExecutorStart)
		prev_ExecutorStart(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);

	if (traceparent_parameter.sampled > 0 && pg_tracing_enabled(exec_nested_level))
	{
		/* Allocate instrumentation */
		if (queryDesc->totaltime == NULL)
		{
			MemoryContext oldcxt;

			oldcxt = MemoryContextSwitchTo(queryDesc->estate->es_query_cxt);
			queryDesc->totaltime = InstrAlloc(1, INSTRUMENT_ALL, false);
			queryDesc->planstate->instrument = InstrAlloc(1, INSTRUMENT_ALL, false);
			MemoryContextSwitchTo(oldcxt);
		}

		add_span(&span, NULL);
	}
}

/*
 * ExecutorRun hook: all we need do is track nesting depth
 * and create executor run span
 */
static void
pg_tracing_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction,
					   uint64 count, bool execute_once)
{
	Span		span;
	bool		tracing = traceparent_parameter.sampled != 0 && pg_tracing_enabled(exec_nested_level);

	if (tracing)
	{
		begin_span(&span, SPAN_EXECUTOR_RUN, traceparent_parameter.trace_id, get_parent_id(exec_nested_level),
				   start_query_ts, start_query, NULL);

		/*
		 * Executor run is used as the parent's span.
		 */
		executor_ids[exec_nested_level] = span.span_id;
	}

	exec_nested_level++;
	PG_TRY();
	{
		if (prev_ExecutorRun)
			prev_ExecutorRun(queryDesc, direction, count, execute_once);
		else
			standard_ExecutorRun(queryDesc, direction, count, execute_once);
	}
	PG_CATCH();
	{
		exec_nested_level--;
		handle_pg_error(&span, queryDesc);
		PG_RE_THROW();
	}
	PG_END_TRY();
	exec_nested_level--;

	if (tracing)
	{
		add_span(&span, NULL);
	}
}

/*
 * ExecutorFinish hook: all we need do is track nesting depth
 */
static void
pg_tracing_ExecutorFinish(QueryDesc *queryDesc)
{
	Span		span;

	if (traceparent_parameter.sampled != 0)
	{
		begin_span(&span, SPAN_EXECUTOR_FINISH, traceparent_parameter.trace_id, get_parent_id(exec_nested_level),
				   start_query_ts, start_query, NULL);
	}

	exec_nested_level++;
	PG_TRY();
	{
		if (prev_ExecutorFinish)
			prev_ExecutorFinish(queryDesc);
		else
			standard_ExecutorFinish(queryDesc);
	}
	PG_CATCH();
	{
		exec_nested_level--;
		handle_pg_error(&span, queryDesc);
		PG_RE_THROW();
	}
	PG_END_TRY();
	exec_nested_level--;

	if (traceparent_parameter.sampled != 0)
	{
		add_span(&span, NULL);
	}
}

/*
 * ExecutorEnd hook: process queryDesc and end tracing if we're at nested
 * level 0
 */
static void
pg_tracing_ExecutorEnd(QueryDesc *queryDesc)
{
	Span		span;

	if (traceparent_parameter.sampled > 0 && pg_tracing_enabled(exec_nested_level))
	{
		begin_span(&span, SPAN_EXECUTOR_END, traceparent_parameter.trace_id,
				   get_parent_id(exec_nested_level),
				   start_query_ts, start_query, NULL);

		/*
		 * Query finished normally, send 0 as error code
		 */
		process_query_desc(queryDesc, 0);
	}

	if (prev_ExecutorEnd)
		prev_ExecutorEnd(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);

	if (traceparent_parameter.sampled > 0)
	{
		instr_time	end;

		INSTR_TIME_SET_CURRENT(end);
		if (pg_tracing_enabled(exec_nested_level))
		{
			add_span(&span, &end);
		}
		end_tracing(end);
	}
}

/*
 * ProcessUtility hook
 */
static void
pg_tracing_ProcessUtility(PlannedStmt *pstmt, const char *queryString,
						  bool readOnlyTree,
						  ProcessUtilityContext context,
						  ParamListInfo params, QueryEnvironment *queryEnv,
						  DestReceiver *dest, QueryCompletion *qc)
{
	Span		span;
	instr_time	end;

	if (traceparent_parameter.sampled > 0 && pg_tracing_enabled(exec_nested_level))
	{
		begin_span(&span, SPAN_PROCESS_UTILITY, traceparent_parameter.trace_id,
				   get_parent_id(exec_nested_level), start_query_ts, start_query, NULL);

		exec_nested_level++;

		PG_TRY();
		{
			if (prev_ProcessUtility)
				prev_ProcessUtility(pstmt, queryString, readOnlyTree,
									context, params, queryEnv,
									dest, qc);
			else
				standard_ProcessUtility(pstmt, queryString, readOnlyTree,
										context, params, queryEnv,
										dest, qc);
		}
		PG_CATCH();
		{
			exec_nested_level--;
			handle_pg_error(&span, NULL);
			PG_RE_THROW();
		}
		PG_END_TRY();
		exec_nested_level--;

		/* TODO Get command tag? */
		span.node_counters.rows = qc->nprocessed;

		INSTR_TIME_SET_CURRENT(end);
		add_span(&span, &end);
		end_tracing(end);
	}
	else
	{
		if (prev_ProcessUtility)
			prev_ProcessUtility(pstmt, queryString, readOnlyTree,
								context, params, queryEnv,
								dest, qc);
		else
			standard_ProcessUtility(pstmt, queryString, readOnlyTree,
									context, params, queryEnv,
									dest, qc);
	}
}

/*
 * Walk through the planstate tree generating a node span for each node.
 * We pass possible error code to tag unfinished node with it
 */
static void
generate_span_from_planstate(PlanState *planstate, uint64 trace_id, uint64 parent_id, int sql_error_code)
{
	Span		span;
	ListCell   *l;
	char const *span_name;
	char const *operation_name;
	Plan const *plan = planstate->plan;
	instr_time	node_duration;

	InstrEndLoop(planstate->instrument);

	begin_span(&span, SPAN_NODE, trace_id, parent_id,
			   start_query_ts, start_query, &planstate->instrument->firsttime);
	/* first tuple time */
	span.startup = planstate->instrument->startup * NS_PER_S;

	/*
	 * If we have a Result node, make it the span parent of the next query
	 * span if we have any
	 */
	if (nodeTag(plan) == T_Result && exec_nested_level < max_nested_level)
	{
		query_spans[exec_nested_level + 1].parent_id = span.span_id;
	}

	/*
	 * Generate names and store them
	 */
	span_name = plan_to_span_name(plan);
	operation_name = plan_to_operation(planstate, span_name);

	text_store(pg_tracing, span_name, strlen(span_name), &span.name_offset);
	text_store(pg_tracing, operation_name, strlen(operation_name), &span.operation_name_offset);

	span.node_counters.rows = (int64) planstate->instrument->ntuples / planstate->instrument->nloops;
	span.node_counters.nloops = (int64) planstate->instrument->nloops;
	span.node_counters.buffer_usage = planstate->instrument->bufusage;
	span.node_counters.wal_usage = planstate->instrument->walusage;

	span.plan_counters.startup_cost = plan->startup_cost;
	span.plan_counters.total_cost = plan->total_cost;
	span.plan_counters.plan_rows = plan->plan_rows;
	span.plan_counters.plan_width = plan->plan_width;

	if (!planstate->state->es_finished)
	{
		/*
		 * We're processing this node in an error handler, stop the node
		 * instrumentation to get it's current state
		 */
		InstrStopNode(planstate->instrument, planstate->state->es_processed);
		InstrEndLoop(planstate->instrument);
		span.sql_error_code = sql_error_code;
	}
	node_duration = planstate->instrument->firsttime;
	Assert(planstate->instrument->total > 0);
	node_duration.ticks += planstate->instrument->total * NS_PER_S;
	add_span(&span, &node_duration);

	/*
	 * Walk the planstate tree
	 */
	if (planstate->lefttree)
	{
		generate_span_from_planstate(planstate->lefttree, trace_id, span.span_id, sql_error_code);
	}
	if (planstate->righttree)
	{
		generate_span_from_planstate(planstate->righttree, trace_id, span.span_id, sql_error_code);
	}

	/*
	 * Handle init plans and subplans
	 */
	foreach(l, planstate->initPlan)
	{
		SubPlanState *sstate = (SubPlanState *) lfirst(l);
		PlanState  *splan = sstate->planstate;

		generate_span_from_planstate(splan, trace_id, span.span_id, sql_error_code);
	}

	foreach(l, planstate->subPlan)
	{
		SubPlanState *sstate = (SubPlanState *) lfirst(l);
		PlanState  *splan = sstate->planstate;

		generate_span_from_planstate(splan, trace_id, span.span_id, sql_error_code);
	}

	/*
	 * Handle special nodes with children nodes
	 */
	switch (nodeTag(plan))
	{
		case T_Append:
			generate_member_nodes(((AppendState *) planstate)->appendplans,
								  ((AppendState *) planstate)->as_nplans, trace_id, span.span_id, sql_error_code);
			break;
		case T_MergeAppend:
			generate_member_nodes(((MergeAppendState *) planstate)->mergeplans,
								  ((MergeAppendState *) planstate)->ms_nplans, trace_id, span.span_id, sql_error_code);
			break;
		case T_BitmapAnd:
			generate_member_nodes(((BitmapAndState *) planstate)->bitmapplans,
								  ((BitmapAndState *) planstate)->nplans, trace_id, span.span_id, sql_error_code);
			break;
		case T_BitmapOr:
			generate_member_nodes(((BitmapOrState *) planstate)->bitmapplans,
								  ((BitmapOrState *) planstate)->nplans, trace_id, span.span_id, sql_error_code);
			break;
		case T_SubqueryScan:
			generate_span_from_planstate(((SubqueryScanState *) planstate)->subplan, trace_id, span.span_id, sql_error_code);
			break;
			/* case T_CustomScan: */
			/* ExplainCustomChildren((CustomScanState *) planstate, */
			/* ancestors, es); */
			/* break; */
		default:
			break;
	}
}

static void
generate_member_nodes(PlanState **planstates, int nplans, uint64 trace_id, uint64 parent_id, int sql_error_code)
{
	int			j;

	for (j = 0; j < nplans; j++)
		generate_span_from_planstate(planstates[j], trace_id, parent_id, sql_error_code);
}

static int
add_plan_counters(const PlanCounters * plan_counters, int i, Datum *values)
{
	values[i++] = Float8GetDatumFast(plan_counters->startup_cost);
	values[i++] = Float8GetDatumFast(plan_counters->total_cost);
	values[i++] = Float8GetDatumFast(plan_counters->plan_rows);
	values[i++] = Int32GetDatum(plan_counters->plan_width);
	return i;
}

static int
add_node_counters(const NodeCounters * node_counters, int i, Datum *values)
{
	Datum		wal_bytes;
	char		buf[256];

	values[i++] = Int64GetDatumFast(node_counters->rows);
	values[i++] = Int64GetDatumFast(node_counters->nloops);

	/* Buffer usage */
	values[i++] = Int64GetDatumFast(node_counters->buffer_usage.shared_blks_hit);
	values[i++] = Int64GetDatumFast(node_counters->buffer_usage.shared_blks_read);
	values[i++] = Int64GetDatumFast(node_counters->buffer_usage.shared_blks_dirtied);
	values[i++] = Int64GetDatumFast(node_counters->buffer_usage.shared_blks_written);

	values[i++] = Int64GetDatumFast(node_counters->buffer_usage.local_blks_hit);
	values[i++] = Int64GetDatumFast(node_counters->buffer_usage.local_blks_read);
	values[i++] = Int64GetDatumFast(node_counters->buffer_usage.local_blks_dirtied);
	values[i++] = Int64GetDatumFast(node_counters->buffer_usage.local_blks_written);

	values[i++] = Float8GetDatumFast(INSTR_TIME_GET_MILLISEC(node_counters->buffer_usage.blk_read_time));
	values[i++] = Float8GetDatumFast(INSTR_TIME_GET_MILLISEC(node_counters->buffer_usage.blk_write_time));

	values[i++] = Int64GetDatumFast(node_counters->buffer_usage.temp_blks_read);
	values[i++] = Int64GetDatumFast(node_counters->buffer_usage.temp_blks_written);
	values[i++] = Float8GetDatumFast(INSTR_TIME_GET_MILLISEC(node_counters->buffer_usage.temp_blk_read_time));
	values[i++] = Float8GetDatumFast(INSTR_TIME_GET_MILLISEC(node_counters->buffer_usage.temp_blk_write_time));

	/* WAL usage */
	values[i++] = Int64GetDatumFast(node_counters->wal_usage.wal_records);
	values[i++] = Int64GetDatumFast(node_counters->wal_usage.wal_fpi);

	snprintf(buf, sizeof buf, UINT64_FORMAT, node_counters->wal_usage.wal_bytes);
	/* Convert to numeric. */
	wal_bytes = DirectFunctionCall3(numeric_in,
									CStringGetDatum(buf),
									ObjectIdGetDatum(0),
									Int32GetDatum(-1));
	values[i++] = wal_bytes;

	/* JIT usage */
	values[i++] = Int8GetDatum(node_counters->jit_usage.created_functions);
	values[i++] = Float8GetDatumFast(INSTR_TIME_GET_MILLISEC(node_counters->jit_usage.generation_counter));
	values[i++] = Float8GetDatumFast(INSTR_TIME_GET_MILLISEC(node_counters->jit_usage.inlining_counter));
	values[i++] = Float8GetDatumFast(INSTR_TIME_GET_MILLISEC(node_counters->jit_usage.optimization_counter));
	values[i++] = Float8GetDatumFast(INSTR_TIME_GET_MILLISEC(node_counters->jit_usage.emission_counter));

	return i;
}

static void
add_result_span(ReturnSetInfo *rsinfo, Span * span,
				const char *qbuffer, Size qbuffer_size)
{
	Datum		values[PG_TRACING_TRACES_COLS] = {0};
	bool		nulls[PG_TRACING_TRACES_COLS] = {0};

	int			i = 0;

	values[i++] = Int64GetDatum(span->trace_id);
	values[i++] = Int64GetDatum(span->parent_id);
	values[i++] = Int64GetDatum(span->span_id);
	values[i++] = CStringGetTextDatum(get_span_name(span, qbuffer));
	values[i++] = CStringGetTextDatum(get_operation_name(span, qbuffer));
	values[i++] = Int64GetDatum(span->start);

	values[i++] = Int64GetDatum(INSTR_TIME_GET_NANOSEC(span->duration));
	values[i++] = CStringGetTextDatum(unpack_sql_state(span->sql_error_code));
	values[i++] = UInt32GetDatum(span->be_pid);

	if ((span->type >= SPAN_NODE && span->type <= SPAN_NODE_UNKNOWN)
		|| span->type == SPAN_PLANNER)
	{
		i = add_plan_counters(&span->plan_counters, i, values);
		i = add_node_counters(&span->node_counters, i, values);

		values[i++] = Int64GetDatum(span->startup);
		if (span->parameter_offset != -1)
		{
			values[i++] = CStringGetTextDatum(qbuffer + span->parameter_offset);
		}
	}

	for (int j = i; j < PG_TRACING_TRACES_COLS; j++)
	{
		nulls[j] = 1;
	}

	tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
}

/*
 * Return spans as a result set.
 *
 * Accept a consume parameter. When consume is set,
 * we empty the shared buffer and query text.
 */
Datum
pg_tracing_spans(PG_FUNCTION_ARGS)
{
	bool		consume;
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Span	   *span;
	const char *qbuffer;
	Size		qbuffer_size;

	consume = PG_GETARG_BOOL(0);
	if (!pg_tracing)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pg_tracing must be loaded via shared_preload_libraries")));
	InitMaterializedSRF(fcinfo, 0);

	qbuffer = qtext_load_file(&qbuffer_size);
	{
		volatile	pgTracingSharedState *s = (volatile pgTracingSharedState *) pg_tracing;

		SpinLockAcquire(&s->mutex);
		for (int i = 0; i < shared_spans->end; i++)
		{
			span = shared_spans->spans + i;
			add_result_span(rsinfo, span, qbuffer, qbuffer_size);
		}

		/*
		 * Consume is set, remove spans from the shared buffer and reset query
		 * file
		 */
		if (consume)
		{
			shared_spans->end = 0;
			s->extent = 0;
		}
		SpinLockRelease(&s->mutex);
	}
	return (Datum) 0;
}

/*
 * Return statistics of pg_tracing.
 */
Datum
pg_tracing_info(PG_FUNCTION_ARGS)
{
	pgTracingGlobalStats stats;
	TupleDesc	tupdesc;
	Datum		values[PG_TRACING_INFO_COLS] = {0};
	bool		nulls[PG_TRACING_INFO_COLS] = {0};
	int			i = 0;

	if (!pg_tracing)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pg_tracing must be loaded via shared_preload_libraries")));

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	/* Get a copy of the pg_tracing stats */
	{
		volatile	pgTracingSharedState *s = (volatile pgTracingSharedState *) pg_tracing;

		SpinLockAcquire(&s->mutex);
		stats = s->stats;
		SpinLockRelease(&s->mutex);
	}

	values[i++] = Int64GetDatum(stats.traces);
	values[i++] = Int64GetDatum(stats.spans);
	values[i++] = Int64GetDatum(stats.dropped_spans);
	values[i++] = TimestampTzGetDatum(stats.last_consume);
	values[i++] = TimestampTzGetDatum(stats.stats_reset);

	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}

/*
 * Reset pg_tracing statistics.
 */
Datum
pg_tracing_reset(PG_FUNCTION_ARGS)
{
	/*
	 * Reset global statistics for pg_tracing since all entries are removed.
	 */
	{
		volatile	pgTracingSharedState *s = (volatile pgTracingSharedState *) pg_tracing;
		TimestampTz stats_reset = GetCurrentTimestamp();

		SpinLockAcquire(&s->mutex);
		s->stats.traces = 0;
		s->stats.stats_reset = stats_reset;
		SpinLockRelease(&s->mutex);
	}
	PG_RETURN_VOID();
}

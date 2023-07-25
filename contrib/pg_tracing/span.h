/*-------------------------------------------------------------------------
 * contrib/pg_tracing/span.h
 *
 * 	Header for span.
 *
 * IDENTIFICATION
 *	  contrib/pg_tracing/span.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _SPAN_H_
#define _SPAN_H_

#include "postgres.h"

#include "jit/jit.h"
#include "pgstat.h"
#include "access/transam.h"

/*
 * SpanType: Type of the span
 */
typedef enum
{
	SPAN_PARSE,					/* Wraps query parsing */
	SPAN_PLANNER,				/* Wraps planner execution in planner hook */
	SPAN_FUNCTION,				/* Wraps function in fmgr hook */
	SPAN_PROCESS_UTILITY,		/* Wraps ProcessUtility execution */

	SPAN_EXECUTOR_START,		/* Executor Spans wrapping the matching hooks */
	SPAN_EXECUTOR_RUN,
	SPAN_EXECUTOR_END,
	SPAN_EXECUTOR_FINISH,

	SPAN_NODE,					/* Represents a node execution, generated from
								 * planstate */

	SPAN_NODE_SELECT,			/* Query Span types. They are created from the
								 * query cmdType */
	SPAN_NODE_INSERT,
	SPAN_NODE_UPDATE,
	SPAN_NODE_DELETE,
	SPAN_NODE_MERGE,
	SPAN_NODE_UTILITY,
	SPAN_NODE_NOTHING,
	SPAN_NODE_UNKNOWN,
}			SpanType;


/*
 * Counters extracted from query instrumentation
 */
typedef struct NodeCounters
{
	int64		rows;			/* # of tuples processed */
	int64		nloops;			/* # of cycles for this node */

	BufferUsage buffer_usage;	/* total buffer usage for this node */
	WalUsage	wal_usage;		/* total WAL usage for this node */
	JitInstrumentation jit_usage;	/* total JIT usage for this node */
}			NodeCounters;

/*
 * Counters extracted from query's plan
 */
typedef struct PlanCounters
{
	/*
	 * estimated execution costs for plan (see costsize.c for more info)
	 */
	Cost		startup_cost;	/* cost expended before fetching any tuples */
	Cost		total_cost;		/* total cost (assuming all tuples fetched) */

	/*
	 * planner's estimate of result size of this plan step
	 */
	Cardinality plan_rows;		/* number of rows plan is expected to emit */
	int			plan_width;		/* average row width in bytes */
}			PlanCounters;

/*
 * The Span data structure represents an operation with a start, a duration.
 * It contains the minimum needed to represent a span and serves as a base
 * for the SpanNode. It is used for simple operations that don't need
 * significant metadata like Executor spans.
 */
typedef struct Span
{
	uint64		trace_id;		/* Trace id extracted from the SQLCommenter's
								 * traceparent */
	uint64		span_id;		/* Span Identifier generated from a random
								 * uint64 */
	uint64		parent_id;		/* Span's parent id. For the top span, it's
								 * extracted from SQLCommenter's traceparent.
								 * For other spans, we pass the parent's span. */

	TimestampTz start;			/* Start of the span. Except for the  */
	instr_time	duration;		/* Duration of the span in nanoseconds. */
	SpanType	type;			/* Type of the span. Used to generate the
								 * span's name for all spans except SPAN_NODE. */
	int			be_pid;			/* Pid of the backend process */

	/*
	 * We store variable size metadata in an external file. Those represent
	 * the position of NULL terminated strings in the file. Set to -1 if
	 * unused.
	 */
	Size		name_offset;	/* span name offset in external file */
	Size		operation_name_offset;	/* operation name offset in external
										 * file */
	Size		parameter_offset;	/* parameters offset in external file */

	PlanCounters plan_counters; /* Counters with plan costs */
	NodeCounters node_counters; /* Counters with node costs (jit, wal,
								 * buffers) */
	int64		startup;		/* Time to the first tuple */
	int			sql_error_code; /* query error code extracted from ErrorData,
								 * 0 if query was successful */
}			Span;

void		begin_span(Span * span, SpanType type, uint64 trace_id, uint64 parent_id,
					   TimestampTz start_query_ts, instr_time start_query_instr_time,
					   const instr_time *start_time);
void		set_span_duration_and_counters(Span * span, const instr_time *end_time);
void		initialize_span_fields(Span * span, SpanType type, uint64 trace_id, uint64 parent_id);

SpanType	command_type_to_span_type(CmdType cmd_type);
const char *get_span_name(const Span * span, const char *qbuffer);
const char *get_operation_name(const Span * span, const char *qbuffer);

#endif

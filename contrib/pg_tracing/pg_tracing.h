/*-------------------------------------------------------------------------
 * pg_tracing.h
 *
 * IDENTIFICATION
 *	  contrib/pg_tracing/pg_tracing.h
 *-------------------------------------------------------------------------
 */
#ifndef _PG_TRACING_H_
#define _PG_TRACING_H_

#include "postgres.h"
#include "span.h"

#include "storage/s_lock.h"

/*
 * Global statistics for pg_tracing
 */
typedef struct pgTracingGlobalStats
{
	int64		traces;
	int64		dropped_spans;
	int64		spans;
	TimestampTz last_consume;
	TimestampTz stats_reset;
}			pgTracingGlobalStats;

/*
 * Global shared state
 */
typedef struct pgTracingSharedState
{
	slock_t		file_mutex;		/* protects query file fields: */
	Size		extent;			/* current extent of query file */
	int			n_writers;		/* number of active writers to query file */
	slock_t		mutex;
	pgTracingGlobalStats stats; /* global statistics for pg_tracing */
}			pgTracingSharedState;

/*
 * Structure to store spans in shared memory.
 * The size is fixed at startup.
 */
typedef struct pgTracingSharedSpans
{
	int			end;			/* Index of last element */
	Span		spans[FLEXIBLE_ARRAY_MEMBER];
}			pgTracingSharedSpans;

#endif

#include "span.h"

#include "nodes/extensible.h"
#include "common/pg_prng.h"


SpanType
command_type_to_span_type(CmdType cmd_type)
{
	switch (cmd_type)
	{
		case CMD_SELECT:
			return SPAN_NODE_SELECT;
		case CMD_INSERT:
			return SPAN_NODE_INSERT;
		case CMD_UPDATE:
			return SPAN_NODE_UPDATE;
		case CMD_DELETE:
			return SPAN_NODE_DELETE;
		case CMD_MERGE:
			return SPAN_NODE_MERGE;
		case CMD_UTILITY:
			return SPAN_NODE_UTILITY;
		case CMD_NOTHING:
			return SPAN_NODE_UTILITY;
		default:
			return SPAN_NODE_UNKNOWN;
	}
}

void
initialize_span_fields(Span * span, SpanType type, uint64 trace_id, uint64 parent_id)
{
	span->trace_id = trace_id;
	span->type = type;
	span->parent_id = parent_id;
	span->span_id = pg_prng_uint64(&pg_global_prng_state);
	span->name_offset = -1;
	span->operation_name_offset = -1;
	span->parameter_offset = -1;
	span->sql_error_code = 0;
	span->startup = 0;
	span->be_pid = MyProcPid;
	INSTR_TIME_SET_ZERO(span->duration);
	memset(&span->node_counters, 0, sizeof(NodeCounters));
	memset(&span->plan_counters, 0, sizeof(PlanCounters));
	if (type == SPAN_PLANNER || span->type == SPAN_PROCESS_UTILITY)
	{
		/*
		 * Store the starting buffer and wal usage for planner and process
		 * utility spans
		 */
		span->node_counters.buffer_usage = pgBufferUsage;
		span->node_counters.wal_usage = pgWalUsage;
	}
}

static
void
initialize_span_time(Span * span,
					 TimestampTz start_query_ts,
					 instr_time start_query_instr_time,
					 const instr_time *start_span_time)
{
	instr_time	delta_start;

	/*
	 * If no start span is provided, get the current one
	 */
	if (start_span_time == NULL)
		INSTR_TIME_SET_CURRENT(delta_start);
	else
		delta_start = *start_span_time;

	/*
	 * We use duration to temporarily store instr_time span start
	 */
	span->duration = delta_start;

	/*
	 * Compute the span timestamp start by using delta instr_time between
	 * start query and start span.
	 */
	INSTR_TIME_SUBTRACT(delta_start, start_query_instr_time);
	span->start = start_query_ts + INSTR_TIME_GET_MICROSEC(delta_start);
}

/*
 * Initialize span and set span starting time.
 *
 * A span needs a start timestamp and a duration.
 * Given that spans can have a very short duration (less than 1ms), we
                 * need to rely on monotonic clock as much as possible to have the best precision.
                 *
                 * For that, after we established that a query was sampled, we get both the start
                 * timestamp and instr_time. All subsequent times will be taken instr_time and we
                 * deduct the starting time from the delta between the current instr_time and the
                 * start_instr_time.
 *
 * We provide multiple times:
 * - start_query_ts is the earliest timestamp we've captured for the current trace.
 * - start_query_instr_time is the instr_time taken at the same moment as the start_query_ts.
 * - start_span_time is an optional start instr_time of the provided span. It will be used
 *   to get the start timestamp of the span. If nothing is provided, we use the current instr_time.
 *   This parameter is mostly used when generating spans from planstate as we need to rely on the
 *   query instrumentation to find the node start.
 */
void
begin_span(Span * span, SpanType type, uint64 trace_id, uint64 parent_id,
		   TimestampTz start_query_ts,
		   instr_time start_query_instr_time,
		   const instr_time *start_span_time)
{
	initialize_span_fields(span, type, trace_id, parent_id);
	initialize_span_time(span, start_query_ts, start_query_instr_time, start_span_time);
}

/*
 * Set span duration and accumulated buffers
 * end_time is optional, if NULL is passed, we use
 * the current time
 */
void
set_span_duration_and_counters(Span * span, const instr_time *end_time)
{
	BufferUsage buffer_usage;
	WalUsage	wal_usage;

	/*
	 * We used span->duration as a temporary storage for the instr_time at the
	 * begining of the span, fetch it
	 */
	instr_time	start_span = span->duration;

	Assert(span->trace_id > 0);

	/*
	 * Set span duration with the end time before substrating the start
	 */
	if (end_time == NULL)
	{
		INSTR_TIME_SET_CURRENT(span->duration);
	}
	else
	{
		span->duration = *end_time;
	}
	INSTR_TIME_SUBTRACT(span->duration, start_span);

	if (span->type == SPAN_PLANNER || span->type == SPAN_PROCESS_UTILITY)
	{
		/* calc differences of buffer counters. */
		memset(&buffer_usage, 0, sizeof(BufferUsage));
		BufferUsageAccumDiff(&buffer_usage, &pgBufferUsage,
							 &span->node_counters.buffer_usage);
		/* calc differences of WAL counters. */
		memset(&wal_usage, 0, sizeof(wal_usage));
		WalUsageAccumDiff(&wal_usage, &pgWalUsage,
						  &span->node_counters.wal_usage);
	}

}

const char *
get_span_name(const Span * span, const char *qbuffer)
{
	switch (span->type)
	{
		case SPAN_PARSE:
			return "Parse";
		case SPAN_PLANNER:
			return "Planner";
		case SPAN_FUNCTION:
			return "Function";
		case SPAN_PROCESS_UTILITY:
			return "ProcessUtility";
		case SPAN_EXECUTOR_START:
			return "Executor";
		case SPAN_EXECUTOR_RUN:
			return "Executor";
		case SPAN_EXECUTOR_END:
			return "Executor";
		case SPAN_EXECUTOR_FINISH:
			return "Executor";

		case SPAN_NODE_SELECT:
			return "Select";
		case SPAN_NODE_INSERT:
			return "Insert";
		case SPAN_NODE_UPDATE:
			return "Update";
		case SPAN_NODE_DELETE:
			return "Delete";
		case SPAN_NODE_MERGE:
			return "Merge";
		case SPAN_NODE_UTILITY:
			return "Utility";
		case SPAN_NODE_NOTHING:
			return "Nothing";
		case SPAN_NODE_UNKNOWN:
			return "Unknown";

		case SPAN_NODE:
			if (span->name_offset == -1)
				return "Node";
			return qbuffer + span->name_offset;
		default:
			elog(ERROR, "unexpected span->type");
	}
}

const char *
get_operation_name(const Span * span, const char *qbuffer)
{
	switch (span->type)
	{
		case SPAN_PARSE:
			return "Parse";
		case SPAN_PLANNER:
			return "Planner";
		case SPAN_FUNCTION:
			return "Function";
		case SPAN_PROCESS_UTILITY:
			return "ProcessUtility";
		case SPAN_EXECUTOR_START:
			return "Start";
		case SPAN_EXECUTOR_RUN:
			return "Run";
		case SPAN_EXECUTOR_END:
			return "End";
		case SPAN_EXECUTOR_FINISH:
			return "Finish";

		case SPAN_NODE_SELECT:
		case SPAN_NODE_INSERT:
		case SPAN_NODE_UPDATE:
		case SPAN_NODE_DELETE:
		case SPAN_NODE_MERGE:
		case SPAN_NODE_UTILITY:
		case SPAN_NODE_NOTHING:
		case SPAN_NODE_UNKNOWN:
		case SPAN_NODE:
			if (span->operation_name_offset == -1)
				return "Node";
			return qbuffer + span->operation_name_offset;
		default:
			elog(ERROR, "unexpected span->type");
	}
}

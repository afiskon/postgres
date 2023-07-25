#ifndef _QUERY_PROCESS_H_
#define _QUERY_PROCESS_H_

#include "pg_tracing.h"
#include "nodes/queryjumble.h"
#include "parser/parse_node.h"

typedef struct pgTracingTraceparentParameter
{
	uint64		trace_id;
	uint64		parent_id;
	int			sampled;
}			pgTracingTraceparentParameter;

/*
 * Normalise query: - Comments are removed - Constants are replaced by $x -
 * All tokens are separated by a single space
 */
const char *normalise_query_parameters(const JumbleState *jstate, const char *query,
									   int query_loc, int *query_len_p, char **paramStr);

pgTracingTraceparentParameter extract_traceparent(const char *query_str, bool is_parameter);

/*
 * Normalise simple query
 */
const char *normalise_query(const char *query, int *query_len_p);
bool		text_store(pgTracingSharedState * pg_tracing, const char *query,
					   int query_len, Size *query_offset);
const char *qtext_load_file(Size *buffer_size);

#endif

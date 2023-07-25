#ifndef _EXPLAIN_H_
#define _EXPLAIN_H_

#include "span.h"
#include "executor/execdesc.h"

const char *plan_to_span_name(const Plan *plan);
const char *plan_to_operation(const PlanState *planstate, const char *spanName);

#endif

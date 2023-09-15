-- Trace nothing
SET pg_tracing.sample_rate = 0.0;
SET pg_tracing.caller_sample_rate = 0.0;
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000021-0000000000000021-01'*/ SELECT 1;

select count(distinct(trace_id)) from pg_tracing_spans(false);
select resource, parameters from pg_tracing_spans order by span_start, duration desc;

-- Trace everything
SET pg_tracing.sample_rate = 1.0;
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000022-0000000000000022-01'*/ SELECT 1;
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000023-0000000000000023-00'*/ SELECT 2;
SELECT 3;
SELECT 4;

select count(distinct(trace_id)) from pg_tracing_spans(false);
select resource, parameters from pg_tracing_spans(false) order by span_start, duration desc;
-- Top spans should reuse generated ids
select resource, parameters from pg_tracing_spans(false) where trace_id = parent_id and parent_id = span_id order by span_start, duration desc;
select resource, parameters from pg_tracing_spans order by span_start, duration desc;

-- Cleanup
SET plan_cache_mode='auto';
SET pg_tracing.sample_rate = 0.0;

-- Only trace query with sampled flag
SET pg_tracing.caller_sample_rate = 1.0;
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000024-0000000000000024-01'*/ SELECT 1;
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000025-0000000000000025-00'*/ SELECT 2;
SELECT 1;
select count(distinct(trace_id)) from pg_tracing_spans(false);
select resource, parameters from pg_tracing_spans(false) order by span_start, duration desc;
select resource, parameters from pg_tracing_spans order by span_start;

-- Track statements in create extension
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000001-0000000000000001-01'*/ CREATE EXTENSION pg_tracing;

SELECT span_id AS top_utility,
		extract(epoch from span_start) as top_utility_start,
		round(extract(epoch from span_start) + duration / 1000000000.0, 6) as top_utility_end
		from pg_tracing_spans(false) where parent_id=1 and name='Utility' \gset

SELECT span_id AS top_process_utility,
		extract(epoch from span_start) as top_process_utility_start,
		round(extract(epoch from span_start) + duration / 1000000000.0, 6) as top_process_utility_end
		from pg_tracing_spans(false) where parent_id=:top_utility and name='ProcessUtility' \gset

SELECT span_id AS first_utility,
		extract(epoch from span_start) as first_utility_start,
		round(extract(epoch from span_start) + duration / 1000000000.0, 6) as first_utility_end
		from pg_tracing_spans(false) where parent_id=:top_process_utility and name='Utility' limit 1 \gset

SELECT span_id AS first_process_utility,
		extract(epoch from span_start) as first_process_utility_start,
		round(extract(epoch from span_start) + duration / 1000000000.0, 6) as first_process_utility_end
		from pg_tracing_spans(false) where parent_id=:first_utility and name='ProcessUtility' \gset

SELECT span_id AS second_utility,
		extract(epoch from span_start) as second_utility_start,
		round(extract(epoch from span_start) + duration / 1000000000.0, 6) as second_utility_end
		from pg_tracing_spans(false) where parent_id=:top_process_utility and name='Utility' limit 1 offset 1 \gset

SELECT :top_utility_start < :top_process_utility_start AS top_utility_start_before_process_utility,
		:top_utility_end >= :top_process_utility_end AS top_end_after_process_utility,

		:first_process_utility_end < :first_utility_end AS nested_process_utility_ends_before,
		:first_utility_end < :second_utility_start AS second_utility_start_after_first_one;

-- Clean current spans
select count(*) from pg_tracing_spans;

-- Test utility off
SET pg_tracing.track_utility = off;

-- Test prepared statement
PREPARE test_prepared_one_param (integer) AS SELECT $1;
EXECUTE test_prepared_one_param(100);

select count(*) from pg_tracing_spans;

-- Test prepared statement with generic plan
SET plan_cache_mode='force_generic_plan';
EXECUTE test_prepared_one_param(200);

select count(*) from pg_tracing_spans;


-- With utility on
SET pg_tracing.track_utility = on;

-- Test prepared statement
PREPARE test_prepared_one_param_2 (integer) AS SELECT $1;
EXECUTE test_prepared_one_param_2(100);

select count(distinct(trace_id)) from pg_tracing_spans(false);
select resource, parameters from pg_tracing_spans(false) order by span_start, duration desc;
select resource, parameters from pg_tracing_spans where trace_id = parent_id and parent_id = span_id order by span_start;

-- Test prepared statement with generic plan
SET plan_cache_mode='force_generic_plan';
EXECUTE test_prepared_one_param(200);

select count(distinct(trace_id)) from pg_tracing_spans(false);
select resource, parameters from pg_tracing_spans(false) order by span_start, duration desc;
select resource, parameters from pg_tracing_spans where trace_id = parent_id and parent_id = span_id order by span_start;


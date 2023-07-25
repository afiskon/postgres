CREATE EXTENSION pg_tracing;

/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000001-0000000000000001-01'*/ SELECT 1;

-- Get top span id
SELECT span_id AS top_span_id from pg_tracing_spans(false) where parent_id=1 and name!='Parse' \gset

-- Check parameters
SELECT parameters from pg_tracing_spans(false) where span_id=:top_span_id;

-- Check the number of children
SELECT count(*) from pg_tracing_spans(false) where parent_id=:'top_span_id';

-- Check resource
SELECT resource from pg_tracing_spans(false) where trace_id=1 order by span_start;

-- Check reported number of trace
SELECT traces from pg_tracing_info;


CREATE OR REPLACE FUNCTION test_function(a int) RETURNS SETOF oid AS
$BODY$
BEGIN
	RETURN QUERY SELECT oid from pg_class where oid = a;
END;
$BODY$
LANGUAGE plpgsql;

/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000002-0000000000000002-01'*/ select test_function(1);

SELECT span_id AS top_span,
		extract(epoch from span_start) as top_start,
		round(extract(epoch from span_start) + duration / 1000000000.0) as top_end
		from pg_tracing_spans(false) where parent_id=2 and name!='Parse' \gset
SELECT span_id AS top_run_span,
		extract(epoch from span_start) as top_run_start,
		round(extract(epoch from span_start) + duration / 1000000000.0) as top_run_end
		from pg_tracing_spans(false) where parent_id=:top_span and name='Executor' and resource='Run' \gset
SELECT span_id AS top_project,
		extract(epoch from span_start) as top_project_start,
		round(extract(epoch from span_start) + duration / 1000000000.0) as top_project_end
		from pg_tracing_spans(false) where parent_id=:top_run_span and name='ProjectSet' \gset
SELECT span_id AS top_result,
		extract(epoch from span_start) as top_result_start,
		round(extract(epoch from span_start) + duration / 1000000000.0) as top_result_end
		from pg_tracing_spans(false) where parent_id=:top_project and name='Result' \gset
SELECT span_id AS nested_select,
		extract(epoch from span_start) as select_start,
		round(extract(epoch from span_start) + duration / 1000000000.0) as select_end
		from pg_tracing_spans(false) where parent_id=:top_result and name='Select' \gset
SELECT span_id AS nested_run,
		extract(epoch from span_start) as run_start,
		round(extract(epoch from span_start) + duration / 1000000000.0) as run_end
		from pg_tracing_spans(false) where parent_id=:nested_select and resource='Run' \gset

SELECT :top_start < :top_run_start,
		:top_end >= :top_run_end,

		:top_run_start <= :top_project_start,

		:top_run_end >= :top_project_end,
		:top_run_end >= :select_end,
		:top_run_end >= :run_end,

		:run_end >= :select_end;


SELECT resource from pg_tracing_spans(false) where parent_id=:nested_run order by resource;

/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000003-0000000000000003-01'*/ SELECT * from current_database();
SELECT resource from pg_tracing_spans(false) where trace_id=3 order by resource;

/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000004-0000000000000004-01'*/ SELECT s.relation_size + s.index_size
FROM (SELECT
      pg_relation_size(C.oid) as relation_size,
      pg_indexes_size(C.oid) as index_size
    FROM pg_class C) as s limit 1;
SELECT resource from pg_tracing_spans(false) where trace_id=4 order by resource;

-- Check tracking option
set pg_tracing.track = 'top';
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000005-0000000000000005-01'*/ select test_function(1);
SELECT count(*) from pg_tracing_spans where trace_id=5;
set pg_tracing.track = 'none';
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000006-0000000000000006-01'*/ select test_function(1);
SELECT count(*) from pg_tracing_spans where trace_id=6;
set pg_tracing.track = 'all';

-- Check that we're in a correct state after a timeout
set statement_timeout=200;
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000007-0000000000000007-01'*/ select * from pg_sleep(10);
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000008-0000000000000008-01'*/ select 1;
SELECT trace_id, resource, sql_error_code from pg_tracing_spans order by span_start;

-- Test prepared statement
PREPARE test_prepared (text, integer) AS /*$1*/ SELECT 1;
EXECUTE test_prepared('dddbs=''postgres.db'',traceparent=''00-00000000000000000000000000000009-0000000000000009-01''', 1);
SELECT trace_id, resource from pg_tracing_spans order by span_start;

-- Test prepared statement with generic plan
SET plan_cache_mode='force_generic_plan';
EXECUTE test_prepared('dddbs=''postgres.db'',traceparent=''00-00000000000000000000000000000010-0000000000000010-01''', 1);
SELECT trace_id, resource from pg_tracing_spans order by span_start;

DROP function test_function;

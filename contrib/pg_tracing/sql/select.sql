/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000001-0000000000000001-01'*/ SELECT 1;

-- Get top span id
SELECT span_id AS top_span_id from pg_tracing_spans(false) where parent_id=1 and name!='Parse' \gset

-- Check parameters
SELECT parameters from pg_tracing_spans(false) where span_id=:top_span_id;

-- Check the number of children
SELECT count(*) from pg_tracing_spans(false) where parent_id=:'top_span_id';

-- Check resource and query id
SELECT resource, query_id from pg_tracing_spans(false) where trace_id=1 order by span_start, duration desc, resource;

-- Check reported number of trace
SELECT traces from pg_tracing_info;

/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000003-0000000000000003-01'*/ SELECT * from current_database();
SELECT resource from pg_tracing_spans where trace_id=3 order by resource;

/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000004-0000000000000004-01'*/ SELECT s.relation_size + s.index_size
FROM (SELECT
      pg_relation_size(C.oid) as relation_size,
      pg_indexes_size(C.oid) as index_size
    FROM pg_class C) as s limit 1;
SELECT resource from pg_tracing_spans where trace_id=4 order by resource;

-- Check that we're in a correct state after a timeout
set statement_timeout=200;
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000007-0000000000000007-01'*/ select * from pg_sleep(10);
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000008-0000000000000008-01'*/ select 1;
SELECT trace_id, resource, sql_error_code from pg_tracing_spans order by span_start, duration desc, resource;
set statement_timeout=0;

-- Test prepared statement
PREPARE test_prepared (text, integer) AS /*$1*/ SELECT $2;
EXECUTE test_prepared('dddbs=''postgres.db'',traceparent=''00-00000000000000000000000000000009-0000000000000009-01''', 1);
SELECT trace_id, resource, parameters from pg_tracing_spans order by span_start, duration desc, resource;

-- Check explain is untracked
explain (costs off)
select * from pg_class where oid < 0 limit 100;
SELECT trace_id, resource, parameters from pg_tracing_spans order by span_start, duration desc, resource;

-- Never executed node
/*dddbs='postgres.db',traceparent='00-0000000000000000000000000000000a-000000000000000a-01'*/ select 1 limit 0;
SELECT trace_id, resource, parameters from pg_tracing_spans order by span_start, duration desc, resource;

-- Test prepared statement with generic plan
SET plan_cache_mode='force_generic_plan';
EXECUTE test_prepared('dddbs=''postgres.db'',traceparent=''00-00000000000000000000000000000010-0000000000000010-01''', 10);
SELECT trace_id, resource, parameters from pg_tracing_spans order by span_start, duration desc, resource;

-- Test multiple statements
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000011-0000000000000012-01'*/ select 1; select 2;
select resource, parameters from pg_tracing_spans order by span_start, duration desc;

-- Cleanup
SET plan_cache_mode='auto';
DEALLOCATE test_prepared;

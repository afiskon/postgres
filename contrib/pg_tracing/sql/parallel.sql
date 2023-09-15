begin;
-- encourage use of parallel plans
set parallel_setup_cost=0;
set parallel_tuple_cost=0;
set min_parallel_table_scan_size=0;
set max_parallel_workers_per_gather=4;
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000001-0000000000000001-01'*/ select 1 from pg_class limit 1;
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000002-0000000000000002-01'*/ select 2 from pg_class limit 2;
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000003-0000000000000003-00'*/ select 3 from pg_class limit 3;
commit;

SELECT resource from pg_tracing_spans order by resource;

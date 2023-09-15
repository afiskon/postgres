-- Create test functions
CREATE OR REPLACE FUNCTION test_function(a int) RETURNS SETOF oid AS
$BODY$
BEGIN
	RETURN QUERY SELECT oid from pg_class where oid = a;
END;
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE sum_one(i int) AS $$
DECLARE
  r int;
BEGIN
  SELECT (i + i)::int INTO r;
END; $$ LANGUAGE plpgsql;

/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000051-0000000000000051-01'*/ select test_function(1);

SELECT span_id AS top_span,
		extract(epoch from span_start) as top_start,
		round(extract(epoch from span_start) + duration / 1000000000.0, 6) as top_end
		from pg_tracing_spans(false) where parent_id=81 and name!='Parse' \gset
SELECT span_id AS top_run_span,
		extract(epoch from span_start) as top_run_start,
		round(extract(epoch from span_start) + duration / 1000000000.0, 6) as top_run_end
		from pg_tracing_spans(false) where parent_id=:top_span and name='Executor' and resource='Run' \gset
SELECT span_id AS top_project,
		extract(epoch from span_start) as top_project_start,
		round(extract(epoch from span_start) + duration / 1000000000.0, 6) as top_project_end
		from pg_tracing_spans(false) where parent_id=:top_run_span and name='ProjectSet' \gset
SELECT span_id AS top_result,
		extract(epoch from span_start) as top_result_start,
		round(extract(epoch from span_start) + duration / 1000000000.0, 6) as top_result_end
		from pg_tracing_spans(false) where parent_id=:top_project and name='Result' \gset
SELECT span_id AS nested_select,
		extract(epoch from span_start) as select_start,
		round(extract(epoch from span_start) + duration / 1000000000.0, 6) as select_end
		from pg_tracing_spans(false) where parent_id=:top_result and name='Select' \gset
SELECT span_id AS nested_run,
		extract(epoch from span_start) as run_start,
		round(extract(epoch from span_start) + duration / 1000000000.0, 6) as run_end
		from pg_tracing_spans(false) where parent_id=:nested_select and resource='Run' \gset

SELECT :top_start < :top_run_start AS top_query_before_run,
		:top_end >= :top_run_end AS top_end_after_run_end,

		:top_run_start <= :top_project_start AS top_run_start_before_project,

		:top_run_end >= :top_project_end AS top_run_end_after_project_end,
		:top_run_end >= :select_end AS top_run_end_before_select_end,
		:top_run_end >= :run_end AS top_run_end_after_nested_run_end,

		:run_end >= :select_end AS run_end_after_select_end;

WITH max_duration AS (select max(duration) from pg_tracing_spans(false))
SELECT duration = max_duration.max from pg_tracing_spans(false), max_duration
    where span_id = :top_span;

SELECT resource from pg_tracing_spans(false) where parent_id=:nested_run order by resource;


-- Check tracking option
set pg_tracing.track = 'top';
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000052-0000000000000052-01'*/ select test_function(1);
SELECT count(*) from pg_tracing_spans where trace_id=82;
set pg_tracing.track = 'none';
/*dddbs='postgres.db',traceparent='00-00000000000000000000000000000053-0000000000000053-01'*/ select test_function(1);
SELECT count(*) from pg_tracing_spans where trace_id=83;
set pg_tracing.track = 'all';

/*traceparent='00-00000000000000000000000000000054-0000000000000054-01'*/ CALL sum_one(3);
SELECT resource from pg_tracing_spans order by span_start, duration desc, resource;


set pg_tracing.track_utility=off;

/*traceparent='00-00000000000000000000000000000055-0000000000000055-01'*/ CALL sum_one(10);
SELECT resource from pg_tracing_spans order by span_start, duration desc, resource;

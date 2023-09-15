SET pg_tracing.sample_rate = 1.0;
BEGIN;
SAVEPOINT s1;
INSERT INTO pg_tracing_test VALUES(generate_series(1, 2), 'aaa');
SAVEPOINT s2;
INSERT INTO pg_tracing_test VALUES(generate_series(1, 2), 'aaa');
SAVEPOINT s3;
SELECT 1;
COMMIT;

select resource, parameters, subxact_count from pg_tracing_spans order by span_start, duration desc;


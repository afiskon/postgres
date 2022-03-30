-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_slru" to load this file. \quit

CREATE OR REPLACE FUNCTION  test_slru() RETURNS VOID
AS 'MODULE_PATHNAME', 'test_slru' LANGUAGE C;

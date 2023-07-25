# Copyright (c) 2022-2023, PostgreSQL Global Development Group

pg_tracing_sources = files(
  'pg_tracing.c',
  'query_process.c',
  'explain.c',
  'span.c',
)

if host_system == 'windows'
  pg_tracing_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_tracing',
    '--FILEDESC', 'pg_tracing - Add support for distributed tracing',])
endif

pg_tracing = shared_module('pg_tracing',
  pg_tracing_sources,
  kwargs: contrib_mod_args,
)
contrib_targets += pg_tracing

install_data(
  'pg_tracing.control',
  'pg_tracing--1.0.sql',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'pg_tracing',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
	'sql': [
	  'select',
      'reset'
    ],
	'regress_args': ['--temp-config', files('pg_tracing.conf')],
  },
}

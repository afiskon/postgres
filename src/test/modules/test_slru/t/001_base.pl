
# Copyright (c) 2022, PostgreSQL Global Development Group

use strict;
use warnings;

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $result;
my $cmdret;
my $node = PostgreSQL::Test::Cluster->new('main');

$node->init;
$node->append_conf('postgresql.conf', qq{shared_preload_libraries = 'test_slru'});
$node->append_conf('postgresql.conf', qq{log_min_messages = 'NOTICE'});
$node->start;

$node->safe_psql("postgres", "CREATE EXTENSION test_slru;");

# The test is executed twice to make sure the state is cleaned up properly
$result = $node->safe_psql("postgres", "SELECT test_slru();");
ok($result eq "", 'the message is empty');

$result = $node->safe_psql("postgres", "SELECT test_slru();");
ok($result eq "", 'the message is empty');

$node->safe_psql("postgres", "DROP EXTENSION test_slru;");

$node->stop;

done_testing();

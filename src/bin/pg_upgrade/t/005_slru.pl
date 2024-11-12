# Copyright (c) 2024, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# This test ensures that pg_upgrade renames SLRU segments.
# After the upgrade all segments should have long file names.

my @slru_dirs = (
	"pg_xact",
	"pg_commit_ts",
	"pg_multixact/offsets",
	"pg_multixact/members",
	"pg_subtrans",
	"pg_serial",
);

my $short_segment_name = "1234";
my $long_segment_name = "000000000001234";

my $oldnode = PostgreSQL::Test::Cluster->new('old_node');
$oldnode->init();
my $oldbindir = $oldnode->config_data('--bindir');

my $newnode = PostgreSQL::Test::Cluster->new('new_node');
$newnode->init();
my $newbindir = $newnode->config_data('--bindir');

# Fill data_dir of the old node with SLRU segments that use short file names.
# pg_upgeade renames the files without looking at the content, so the content
# is not important.
foreach my $dir (@slru_dirs)
{
	open my $fh, ">", $dir."/".$short_segment_name;
	close $fh;
}

command_ok(
	[
		'pg_upgrade',
		'--old-datadir', $oldnode->data_dir,
		'--new-datadir', $newnode->data_dir,
		'--old-bindir', $oldbindir,
		'--new-bindir', $newbindir,
	],
	'run of pg_upgrade');

# Check that pg_upgrade renamed the SLRU segments we created
foreach my $dir (@slru_dirs)
{
	ok(-e $dir."/".$long_segment_name);
}

done_testing();

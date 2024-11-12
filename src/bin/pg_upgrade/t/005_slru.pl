# Copyright (c) 2024, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# This test ensures that pg_upgrade renames SLRU segments.
# After the upgrade all segments should have long file names.

# equals SLRU_SEG_FILENAMES_CHANGE_CAT_VER in pg_upgrade.h
my $slru_seg_filenames_change_cat_ver = 202411121;

my @slru_dirs = (
	"pg_xact",
#	"pg_commit_ts", # is empty
	"pg_multixact/offsets",
	"pg_multixact/members",
#	"pg_subtrans", # only has 000000000000000
#	"pg_serial", # is empty
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
# pg_upgrade renames the files without looking at the content, so the content
# is not important.
foreach my $dir (@slru_dirs)
{
	my $fname = $oldnode->data_dir."/".$dir."/".$short_segment_name;
	open my $fh, ">", $fname or die $!;
	close $fh;
}

# Modify pg_control file of the old node to make it look like a version that
# needs migration. Otherwise pg_upgrade will bypass it.

command_ok(
	[
		$newbindir.'/pg_upgrade',
		'--old-datadir', $oldnode->data_dir,
		'--new-datadir', $newnode->data_dir,
		'--old-bindir', $oldbindir,
		'--new-bindir', $newbindir,
	],
	'run of pg_upgrade');

# Check that pg_upgrade renamed the SLRU segments we created
foreach my $dir (@slru_dirs)
{
	ok(-e $newnode->data_dir."/".$dir."/".$long_segment_name);
}

done_testing();

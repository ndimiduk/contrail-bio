#!/usr/bin/perl -w
use strict;
use lib ".";
use PAsm;
use Data::Dumper;


my $compressibleneighborhood = 0;
my $noncompressible = 0;

sub	updatetag
{
  my $nodeid = shift;
  my $node = shift;

  return if !defined $nodeid;

  if ((exists $node->{"$CANCOMPRESS$fwd"} && $node->{"$CANCOMPRESS$fwd"}->[0]) ||
      (exists $node->{"$CANCOMPRESS$rev"} && $node->{"$CANCOMPRESS$rev"}->[0]) ||
      (exists $node->{$COMPRESSPAIR}))
  {
    $compressibleneighborhood++;
    $node->{$MERTAG}->[0] = 0;
  }
  else
  {
    $noncompressible++;
    $node->{$MERTAG}->[0] = int(rand(123456789)) + 1;
  }

  print_node($nodeid, $node);
}



my $node = {};
my $nodeid = undef;

while (<>)
{
  #print "==> $_";
  chomp;
  my @vals = split /\t/, $_;

  my $curnodeid = shift @vals;
  if (defined $nodeid && $curnodeid ne $nodeid)
  {
    updatetag($nodeid, $node);
    $node = {};
  }

  $nodeid = $curnodeid;

  my $msgtype = shift @vals;

  if ($msgtype eq $NODEMSG)
  {
    parse_node($node, \@vals);
  }
  elsif ($msgtype eq $COMPRESSPAIR)
  {
    $node->{$COMPRESSPAIR} = 1;
  }
  else
  {
    die "Unknown msg: $_\n";
  }
}

updatetag($nodeid, $node);


hadoop_counter("non_compressible", $noncompressible);
hadoop_counter("compressible_neighborhood", $compressibleneighborhood);

#!/usr/bin/perl -w
use strict;
use lib ".";
use PAsm;

my $compressible = 0;
my $totalnodes = 0;

sub markCompressible
{
  my $nodeid = shift;
  my $node = shift;

  return if !defined $nodeid;

  my $degree;
  my $buddy;
  my $buddyt;

  $totalnodes++;

  foreach my $adj (qw/f r/)
  {
    if ((defined $node->{"$CANCOMPRESS$adj"}->[0]) &&
        ($node->{"$CANCOMPRESS$adj"}->[0]))
    {
      ## We already set this via a local memory update
      $compressible++;
      next;
    }

    $degree = 0;
    $buddyt = "f";

    foreach my $t (qw/f r/)
    {
      my $tt = "$adj$t";

      if (exists $node->{$tt})
      {
        $degree += scalar @{$node->{$tt}};
        $buddyt = $t;
        $buddy = $node->{$tt}->[0];
      }
    }

    my $canCompress = 0;

    if ($degree == 1)
    {
      my $fbuddyt = flip_dir($buddyt);

      if (exists $node->{$HASUNIQUEP} &&
          exists $node->{$HASUNIQUEP}->{$fbuddyt} &&
          exists $node->{$HASUNIQUEP}->{$fbuddyt}->{$buddy})
      {
        $canCompress = 1;
      }
    }

    $node->{"$CANCOMPRESS$adj"}->[0] = $canCompress;

    if ($canCompress)
    {
      $compressible++;
    }
  }

  print_node($nodeid, $node);
}

my $node = {};
my $nodeid = undef;

while (<>)
{
  chomp;

  my @vals = split /\t/, $_;

  my $curnodeid = shift @vals;
  if (defined $nodeid && $curnodeid ne $nodeid)
  {
    markCompressible($nodeid, $node);
    $node = {};
  }

  $nodeid = $curnodeid;

  my $msgtype = shift @vals;

  if ($msgtype eq $NODEMSG)
  {
    parse_node($node, \@vals);
  }
  elsif ($msgtype eq $HASUNIQUEP)
  {
    my $x = shift @vals;
    my $d = shift @vals;
    $node->{$HASUNIQUEP}->{$d}->{$x} = 1;
  }
  else
  {
    die "Unknown msg: $_\n";
  }
}

markCompressible($nodeid, $node);

hadoop_counter("totalnodes",   $totalnodes);
hadoop_counter("compressible", $compressible);

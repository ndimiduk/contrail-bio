#!/usr/bin/perl -w
use strict;
use lib ".";
use PAsm;
use Data::Dumper;

my $removed_edges = 0;

sub	updateNode
{
  my $nodeid = shift;
  my $node = shift;

  return if !defined $nodeid;

  if (exists $node->{$KILLLINKMSG})
  {
    foreach my $et (keys %{$node->{$KILLLINKMSG}})
    {
      foreach my $vt (keys %{$node->{$KILLLINKMSG}->{$et}})
      {
        print STDERR "Removing $nodeid $et $vt\n";
        node_removelink($node, $vt, $et);
        $removed_edges++;
      }
    }
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
    updateNode($nodeid, $node);
    $node = {};
  }

  $nodeid = $curnodeid;

  my $msgtype = shift @vals;

  if ($msgtype eq $NODEMSG)
  {
    parse_node($node, \@vals);
  }
  elsif ($msgtype eq $KILLLINKMSG)
  {
    my $et = shift @vals;
    my $v  = shift @vals;

    $node->{$KILLLINKMSG}->{$et}->{$v}++;
  }
  else
  {
    die "Unknown msg: $_\n";
  }
}

updateNode($nodeid, $node);

hadoop_counter("removed_edges", $removed_edges);

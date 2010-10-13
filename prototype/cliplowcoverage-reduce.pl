#!/usr/bin/perl -w
use strict;
use lib ".";
use PAsm;
use Data::Dumper;

my $linksremoved = 0;
my $threadsremoved = 0;

sub clipNodes
{
  my $nodeid = shift;
  my $node = shift;

  return if !defined $nodeid;

  ## There could be adjacent low coverage nodes
  if (exists $node->{nodemsg})
  {
    if (exists $node->{trim})
    {
      foreach my $et (keys %{$node->{trim}})
      {
        foreach my $v (@{$node->{trim}->{$et}})
        {
          node_removelink($node, $v, $et);
          $linksremoved++;
        }
      }

      $threadsremoved += node_cleanthreads($node);
    }

    print_node($nodeid, $node);
  }
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
    clipNodes($nodeid, $node);
    $node = {};
  }

  $nodeid = $curnodeid;

  my $msgtype = shift @vals;

  if ($msgtype eq $NODEMSG)
  {
    parse_node($node, \@vals);

    $node->{nodemsg} = 1;
  }
  elsif ($msgtype eq $TRIMMSG)
  {
    my $et = shift @vals;
    my $v  = shift @vals;

    push @{$node->{trim}->{$et}}, $v;
  }
  else
  {
    die "Unknown msg: $_\n";
  }
}

clipNodes($nodeid, $node);

hadoop_counter("removed_threads", $threadsremoved);
hadoop_counter("removed_links",   $linksremoved);

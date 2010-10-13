#!/usr/bin/perl -w
use strict;
use lib ".";
use PAsm;
use Data::Dumper;

my $updates = 0;

sub	updateNode
{
  my $nodeid = shift;
  my $node = shift;

  return if !defined $nodeid;

  my $fdegree = node_degree($node, "f");
  my $rdegree = node_degree($node, "r");

  if (($fdegree > 1) || ($rdegree > 1))
  {
    my %threadports;

    if (exists $node->{update})
    {
      $updates++;

      foreach my $thread (@{$node->{update}})
      {
        my @steps = split /-/, $thread;

        my $fport = $steps[1];
        my $rport = $steps[-2];

        my ($ft, $fn) = split /:/, $fport;
        my ($rt, $rn) = split /:/, $rport;

        $threadports{$ft}->{$fn} = $thread;
        $threadports{$rt}->{$rn} = $thread;

        if (!node_haslink($node, $ft, $fn) ||
            !node_haslink($node, $rt, $rn))
        {
          print STDERR "Invalid thread: $nodeid $thread\n";
          node_printlinks($node);
          die;
        }

        push @{$node->{$THREADPATH}}, $thread;
      }
    }

    if (exists $node->{$KILLLINKMSG})
    {
      foreach my $et (keys %{$node->{$KILLLINKMSG}})
      {
        foreach my $vt (keys %{$node->{$KILLLINKMSG}->{$et}})
        {
          if (!exists $threadports{$et}->{$vt})
          {
            print STDERR "Removing $nodeid $et $vt\n";
            print "$nodeid\t$KILLLINKMSG\t$et\t$vt\n";

            my $ret = flip_link($et);
            print "$vt\t$KILLLINKMSG\t$ret\t$nodeid\n";
          }
          else
          {
            my $t = $threadports{$et}->{$vt};
            print STDERR "Don't remove $nodeid $et $vt, in thread $t\n";
          }
        }
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
  elsif ($msgtype eq $UPDATEMSG)
  {
    my $thread = shift @vals;
    push @{$node->{update}}, $thread;
  }
  elsif ($msgtype eq $KILLLINKMSG)
  {
    my $et = shift @vals;
    my $v  = shift @vals;

    $node->{$KILLLINKMSG}->{$et}->{$v} = 1;
  }
  else
  {
    die "Unknown msg: $_\n";
  }
}

updateNode($nodeid, $node);


hadoop_counter("updates", $updates);

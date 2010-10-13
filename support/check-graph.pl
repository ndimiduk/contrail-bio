#!/usr/bin/perl -w
use strict;

use File::Basename;
use lib ".";
use lib dirname($0);
use PAsm;

my %graph;

while (<>)
{
  chomp;

  my $node = {};

  my @vals = split /\t/, $_;

  my $nodeid = shift @vals;

  my $msgtype = shift @vals; ## nodemsg

  if ($msgtype eq $NODEMSG)
  {
    parse_node($node, \@vals);
  }
  else
  {
    die "Unknown msg: $_\n";
  }

  $graph{$nodeid} = $node;
}

my $nodes = 0;
my $edges = 0;
my $edgesok = 0;
my $edgesbad = 0;
my $missing = 0;
my $nodelensum = 0;

my $threadok = 0;
my $threadbad = 0;
my $dupedges = 0;

foreach my $u (keys %graph)
{
  my $node = $graph{$u};
  my $str = node_str($node);

  $nodelensum += length($str);
  print "checking $u $str\n";
  $nodes++;

  my %edges;

  foreach my $ut (qw/f r/)
  {
    my $ustr = $str;
    if ($ut eq "r") { $ustr = rc($ustr); }

    foreach my $vt (qw/f r/)
    {
      my $t = "$ut$vt";

      if ((exists $node->{$t}) &&
          (scalar @{$node->{$t}} > 0))
      {
        foreach my $v (@{$node->{$t}})
        {
          if (!exists $graph{$v})
          {
            $missing++;
            print "  $t $v missing\n";
            next;
          }

          if (exists $edges{$t}->{$v})
          {
            print STDERR "Dup edge $u $t $v\n";
            $dupedges++;
          }

          $edges{$t}->{$v} = 1;

          my $vstr = node_str($graph{$v});
          if ($vt eq "r") { $vstr = rc($vstr); }

          ## check that the last k-1 bp of u == first k-1 bp of v

          my $ustrk = substr $ustr, -($K-1);
          my $vstrk = substr $vstr, 0, $K-1;

          my $ok;
          if ($ustrk eq $vstrk)
          {
            $ok = "ok";
            $edgesok++;
          }
          else
          {
            $ok = "bad";
            $edgesbad++;
            print "  $t $v $vstr $ok\n";
          }
        }
      }
      else
      {
        #print "  $t 0\n";
      }
    }
  }

  if ($THREADREADS && defined $node->{$THREAD})
  {
    foreach my $thread (@{$node->{$THREAD}})
    {
      my ($t,$link,$read) = split /:/, $thread;
      
      my $ok;
      if (exists $edges{$t} && exists $edges{$t}->{$link})
      {
        $ok = "ok";
        $threadok++;
      }
      else
      {
        $ok = "bad";
        $threadbad++;
        print "  $thread $ok\n";
      }
    }
  }
}


print "==\nChecked $nodelensum bp in $nodes nodes, edges: $edgesok ok, $dupedges dup, $missing missing, $edgesbad bad, thread: $threadok ok $threadbad bad\n";


#!/usr/bin/perl -w
use strict;

use File::Basename;
use lib ".";
use lib dirname($0);
use PAsm;


my %stats;

my $types = {$STR=>1,$COVERAGE=>1,ff=>1,fr=>1,rr=>1,rf=>1};

while (<>)
{
  my ($nodeid, $node) = parse_node_lite(\$_, $types);

  my $len = node_len($node);

  if ($len == 0)
  {
    print "# $_\n";
  }
  else
  {
    my $fdegree = node_degree($node, "f");
    my $rdegree = node_degree($node, "r");
    my $cov     = node_cov($node);

    if ($len >= 100)
    {
      print "$len\t$fdegree\t$rdegree\t$cov\n";
    }
    else
    {
      ## Use an in-memory combiner for the very short contigs
      if ($len >= 50)
      {
        $stats{50}->{cnt}++;
        $stats{50}->{sum}    += $len;
        $stats{50}->{degree} += ($fdegree + $rdegree) * $len;
        $stats{50}->{cov}    += $cov * $len;
      }

      $stats{1}->{cnt}++;
      $stats{1}->{sum}    += $len;
      $stats{1}->{degree} += ($fdegree + $rdegree) * $len;
      $stats{1}->{cov}    += $cov * $len;
    }
  }
}

foreach my $cutoff (keys %stats)
{
  my $scnt = $stats{$cutoff}->{cnt};
  my $ssum = $stats{$cutoff}->{sum};
  my $sdeg = $stats{$cutoff}->{degree};
  my $scov = $stats{$cutoff}->{cov};

  print "SHORT\t$cutoff\t$scnt\t$ssum\t$sdeg\t$scov\n";
}


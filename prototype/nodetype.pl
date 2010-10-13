#!/usr/bin/perl -w
use strict;
use Getopt::Long;
use File::Basename;
use lib ".";
use lib dirname($0);
use PAsm;

my $HELPFLAG;

my $result = GetOptions(
"h"         => \$HELPFLAG,
);

if ($HELPFLAG)
{
  print "USAGE: nodetype.pl graph > graph.types\n";
  exit 0;
}

my %types;
my $totalcnt = 0;
my $totallen = 0;

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

  my $fdegree = node_degree($node, "f");
  my $rdegree = node_degree($node, "r");

  my $type = node_branchtype($nodeid, $node);
  my $len  = node_len($node);
  my $cov  = node_cov($node);

  print "$nodeid\t$type\t$fdegree\t$rdegree\t$len\t$cov\n";

  $totalcnt++;
  $totallen+=$len;

  $types{$type}->{cnt}++;
  $types{$type}->{len} += $len;
}


printf STDERR "% s\t% s\t% s\t% s\t% s\n", 
              "type", "len", "\%len", "cnt", "\%cnt";

foreach my $t (sort {$types{$b}->{len} <=> $types{$a}->{len}} keys %types)
{
  my $c = $types{$t}->{cnt};
  my $l = $types{$t}->{len};

  printf STDERR "% s\t% s\t%0.2f\t% s\t%0.2f\n", 
                $t, $l, 100*$l/$totallen, $c, 100*$c/$totalcnt;
}

printf STDERR "% s\t% s\t%0.2f\t% s\t%0.2f\n", 
              "total", $totallen, 100, $totalcnt, 100;



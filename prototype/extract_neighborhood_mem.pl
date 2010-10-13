#!/usr/bin/perl -w
use strict;

use File::Basename;
use lib ".";
use lib dirname($0);
use PAsm;

my $USAGE = "extract_neighborhood.pl graph seedid dist\n";

my $graphfile = shift @ARGV; die $USAGE if !defined $graphfile;
my $seedid    = shift @ARGV; die $USAGE if !defined $seedid;
my $maxdist   = shift @ARGV; die $USAGE if !defined $maxdist;

my %keep;
my %nodes;

$keep{$seedid}->{dist} = 0;

open GRAPH, "< $graphfile" or die "Can't open $graphfile ($!)\n";

my $nodecnt = 0;

while (<GRAPH>)
{
    chomp;

    my $node = {};

    my @vals = split /\t/, $_;

    my $nodeid = shift @vals;

    my $msgtype = shift @vals; ## nodemsg

    if ($msgtype ne $NODEMSG)
    {
      die "Unknown msg: $_\n";
    }

    parse_node($node, \@vals);

    $nodes{$nodeid} = $node;
    $nodecnt++;

    if (($nodecnt % 1000) == 0)
    {
      print STDERR "Loaded $nodecnt\n";
    }
}

my $searchcount = 1;
my $total = 0;

for (my $round = 0; $round <= $maxdist; $round++)
{
  print STDERR "Dist $round: $searchcount nodes\n";

  $searchcount = 0;

  foreach my $nodeid (keys %nodes)
  {
    if ((exists $keep{$nodeid}) && ($keep{$nodeid}->{dist} == $round))
    {
      if (!exists $keep{$nodeid}->{printed})
      {
        my $node = $nodes{$nodeid};

        $keep{$nodeid}->{printed} = 1;

        print_node($nodeid, $node);
        $total++;

        my $mydist = $keep{$nodeid}->{dist};

        foreach my $t (qw/ff fr rr rf/)
        {
          if (exists $node->{$t} && scalar @{$node->{$t}})
          { 
            foreach my $b (@{$node->{$t}})
            { 
              if (!exists $keep{$b})
              {
                $keep{$b}->{dist} = $mydist + 1;
                $searchcount++;
              }
            }
          }
        }
      }
    }
  }

}

print STDERR "Total: $total nodes within $maxdist hops of $seedid\n";

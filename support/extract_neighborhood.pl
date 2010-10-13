#!/usr/bin/perl -w
use strict;

use File::Basename;
use lib ".";
use lib dirname($0);
use PAsm;

my $USAGE = "extract_neighborhood.pl graph seedid dist [dest]\n";

my $graphfile = shift @ARGV; die $USAGE if !defined $graphfile;
my $seedid    = shift @ARGV; die $USAGE if !defined $seedid;
my $maxdist   = shift @ARGV; die $USAGE if !defined $maxdist;

my $dest = shift @ARGV;

my %keep;

$keep{$seedid}->{dist} = 0;
$keep{$seedid}->{src}  = undef;

open GRAPH, "< $graphfile" or die "Can't open $graphfile ($!)\n";

my $searchcount = 1;
my $total = 1;
my $printed = 0;

for (my $round = 0; $round <= $maxdist; $round++)
{
  print STDERR "Dist $round: $searchcount nodes $total total\n";

  $searchcount = 0;

  while (<GRAPH>)
  {
    chomp;

    my $node = {};

    my @vals = split /\t/, $_;

    my $nodeid = shift @vals;

    if ((exists $keep{$nodeid}) && ($keep{$nodeid}->{dist} == $round))
    {
      if (!exists $keep{$nodeid}->{printed})
      {
        $keep{$nodeid}->{printed} = 1;
        print "$_\n";
        $printed++;

        if (defined $dest && $dest eq $nodeid)
        {
          print STDERR "Found path: ";
          my $cur = $dest;
          while (defined $cur)
          {
            print STDERR " $cur";
            $cur = $keep{$cur}->{src};
          }

          print STDERR "\n";

        }

        my $msgtype = shift @vals; ## nodemsg

        if ($msgtype ne $NODEMSG)
        {
          die "Unknown msg: $_\n";
        }

        parse_node($node, \@vals);

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
                $keep{$b}->{src}  = $nodeid;
                $searchcount++;
                $total++;
              }
            }
          }
        }
      }
    }
  }

  seek GRAPH, 0, 0;
}

print STDERR "Total: $printed nodes within $maxdist hops of $seedid\n";

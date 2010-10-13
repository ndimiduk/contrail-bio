#!/usr/bin/perl -w
use strict;

use Getopt::Long;
use File::Basename;
use lib ".";
use lib dirname($0);
use PAsm;


my $HELPFLAG = 0;
my $COV_HEADER = 0;
my $SHOW_FULL_HEADER = 0;

my $result = GetOptions(
"h"  => \$HELPFLAG,
"c"  => \$COV_HEADER,
"f"  => \$SHOW_FULL_HEADER,
);

if ($HELPFLAG)
{ 
  print "USAGE: graph2fa.pl [options] file.graph > file.fa\n";
  print "\n";
  print "Options\n";
  print " -c : Include coverage in seq identifier\n";
  print " -f : show all fields on header line\n";

  exit 0;
}


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
  
  my $str = node_str($node);
  my $len = length($str);
  my $cov = $node->{$COVERAGE}->[0];

  if ($COV_HEADER)
  {
    print ">$nodeid\_$cov len=$len";
  }
  else
  {
    print ">$nodeid len=$len cov=$cov";
  }

  if ($SHOW_FULL_HEADER)
  {
    foreach my $t (sort keys %{$node})
    {
      next if $t eq $COVERAGE;
      next if $t eq $STR;

      if (exists $node->{$t})
      {
        print " *$t";
        foreach my $l (@{$node->{$t}})
        {
          print " $l";
        }
      }
    }
  }

  print "\n";

  my $linelen = 60;
  for (my $i = 0; $i < $len; $i+=$linelen)
  {
    print substr($str, $i, $linelen), "\n"
  }
}

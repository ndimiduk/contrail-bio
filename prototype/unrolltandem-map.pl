#!/usr/bin/perl -w
use strict;
use lib ".";
use PAsm;

my $tandem = 0;
my $simpletandem = 0;

while (<>)
{
  chomp;

  my @vals = split /\t/, $_;

  my $nodeid  = shift @vals;
  my $msgtype = shift @vals;

  my $node = {};

  if ($msgtype eq $NODEMSG)
  {
    parse_node($node, \@vals);
  }
  else
  {
    die "Unknown msg: $_\n";
  }

  my $istandem = 0;

  my @selfedges;
  my @otheredges;

  foreach my $et (qw/ff fr rr rf/)
  {
    if (exists $node->{$et})
    {
      foreach my $v (@{$node->{$et}})
      {
        if ($v eq $nodeid) 
        { 
          if ($et ne "rr")
          {
            push @selfedges, $et;
          }
        }
        else
        {
          push @otheredges, "$et:$v";
        }
      }
    }
  }

  if (scalar @selfedges > 0)
  {
    $tandem++;

    print STDERR "Self edge: $nodeid (@selfedges)\n";

    if ((scalar @otheredges <= 2) && (scalar @selfedges == 1))
    {
      $simpletandem++;

      my $str = node_str($node);
      my $selfedge = $selfedges[0];

      if ($selfedge eq "ff")
      {
        ## simple cycle
        $str = str_concat($str, $str);
      }
      else
      {
        my $rc = rc($str);

        if ($selfedge eq "fr")
        {
          $str = str_concat($str, $rc);
        }
        else ## selfedge eq "rf"
        {
          $str = str_concat($rc, $str);
          $str = rc($str);
        }

        if (scalar @otheredges == 2)
        {
          my $port = $otheredges[0];
          my ($et,$v) = split /:/, $port;
          my ($x,$y) = split //, $et;

          my $net = flip_dir($x) . $y;

          node_replacelink($nodeid, $node, $v, $et, $v, $net);

          my $odir = flip_link($et);
          my $ndir = flip_link($net);

          print "$v\t$UPDATEMSG\t$nodeid\t$odir\t$nodeid\t$ndir\n";
        }
      }

      node_removelink($node, $nodeid, $selfedge);
      if ($selfedge eq "ff") { node_removelink($node, $nodeid, "rr"); }

      node_cleanthreads($node);

      ## We don't know where the reads go anymore
      delete $node->{$R5};

      node_setstr($node, $str);
      $node->{$COVERAGE}->[0] /= 2;
    }
  }

  print_node($nodeid, $node);
}

hadoop_counter("tandem", $tandem);
hadoop_counter("simpletandem", $simpletandem);

